using System.Buffers;
using System.Buffers.Binary;
using System.IO.Compression;
using Ameto.Otel;

namespace Ameto.Integration.Tests;

/// <summary>
/// The gRPC framing — the only part of an OTLP Export that is not the protobuf the HTTP
/// receivers already decode.
///
/// <para>These are unit tests on purpose. The receiver's HTTP behaviour cannot honestly be tested
/// through TestServer: it supplies a response-trailers feature that Kestrel does not, and reports
/// HTTP/2 for what is not an HTTP/2 connection, so a green test there can assert behaviour the
/// real server does not have — which is precisely what an earlier version of this file did, and
/// what hid a success path that answered with no grpc-status at all. The transport is verified
/// against a real Kestrel instead (docs/API.md carries the curl that does it); what is left to
/// pin here is the framing, and that needs no host.</para>
/// </summary>
public sealed class OtlpGrpcFramingTests
{
    private const int Limit = 1024 * 1024;   // stands in for Ingestion.MaxOtlpBatchBytes

    private static byte[] Frame(byte[] message, bool gzip = false)
    {
        byte[] payload = message;
        if (gzip)
        {
            using var ms = new MemoryStream();
            using (var z = new GZipStream(ms, CompressionMode.Compress, leaveOpen: true)) z.Write(message);
            payload = ms.ToArray();
        }

        var framed = new byte[5 + payload.Length];
        framed[0] = gzip ? (byte)1 : (byte)0;
        BinaryPrimitives.WriteUInt32BigEndian(framed.AsSpan(1), (uint)payload.Length);
        payload.CopyTo(framed.AsSpan(5));
        return framed;
    }

    private static UnframeResult Unframe(byte[] framed, string? encoding, out byte[] message, int limit = Limit)
    {
        var result = OtlpGrpcFraming.TryUnframe(framed, encoding, limit, out var span, out var rented, out _);
        message = span.ToArray();
        if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        return result;
    }

    // ── The frame ─────────────────────────────────────────────────────────────

    [Fact]
    public void An_uncompressed_frame_yields_its_message()
    {
        byte[] payload = [1, 2, 3, 4, 5, 6, 7];
        Assert.Equal(UnframeResult.Ok, Unframe(Frame(payload), null, out var message));
        Assert.Equal(payload, message);
    }

    [Fact]
    public void The_compression_flag_decides_not_the_header()
    {
        // grpc-encoding only names HOW a compressed message was compressed. An identity frame is
        // valid whatever the header says, and reading it otherwise would refuse traffic a
        // conforming client is entitled to send.
        byte[] payload = [9, 9, 9];
        Assert.Equal(UnframeResult.Ok, Unframe(Frame(payload), "snappy", out var message));
        Assert.Equal(payload, message);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(4)]
    public void A_body_too_short_to_hold_a_header_is_malformed(int length)
        => Assert.Equal(UnframeResult.Malformed, Unframe(new byte[length], null, out _));

    [Fact]
    public void A_length_that_overruns_the_body_is_malformed()
    {
        // Trusting the declared length would slice past the end of the buffer we were handed.
        var framed = Frame([1, 2, 3]);
        BinaryPrimitives.WriteUInt32BigEndian(framed.AsSpan(1), uint.MaxValue);
        Assert.Equal(UnframeResult.Malformed, Unframe(framed, null, out _));
    }

    [Fact]
    public void An_empty_message_is_valid()
    {
        Assert.Equal(UnframeResult.Ok, Unframe(Frame([]), null, out var message));
        Assert.Empty(message);
    }

    // ── Compression ───────────────────────────────────────────────────────────

    [Fact]
    public void A_gzipped_frame_is_inflated()
    {
        var payload = System.Text.Encoding.UTF8.GetBytes(new string('x', 4096));
        Assert.Equal(UnframeResult.Ok, Unframe(Frame(payload, gzip: true), "gzip", out var message));
        Assert.Equal(payload, message);
    }

    [Fact]
    public void A_compression_we_do_not_speak_is_named_as_such()
    {
        // So the caller can answer UNIMPLEMENTED with grpc-accept-encoding, which is what makes
        // an exporter retry uncompressed instead of dropping the batch.
        Assert.Equal(UnframeResult.UnsupportedEncoding, Unframe(Frame([1, 2, 3], gzip: true), "snappy", out _));
    }

    [Fact]
    public void Bytes_that_are_not_gzip_are_malformed_rather_than_a_crash()
    {
        // Flag says compressed, bytes are not a gzip stream. Built by hand, because the helper
        // above would produce real gzip and test nothing.
        byte[] framed = [1, 0, 0, 0, 4, 1, 2, 3, 4];
        Assert.Equal(UnframeResult.Malformed, Unframe(framed, "gzip", out _));
    }

    [Fact]
    public void A_message_that_inflates_past_the_limit_is_refused_before_it_is_materialised()
    {
        // THE ONE THAT MATTERS. Deflate reaches about 1032:1, so a frame small enough to pass the
        // wire check inflates to gigabytes — and the buffer doubling on the way there commits
        // several times that again. One valid ingest key, the credential on every application
        // that ships logs, was enough to take the process down, repeatably, with the failure
        // swallowed and nothing left to alert on.
        var bomb   = new byte[Limit * 8];               // zeroes: compresses to a few KB
        var framed = Frame(bomb, gzip: true);
        Assert.True(framed.Length < Limit, "the compressed frame must pass the wire limit for this to be the bomb");

        Assert.Equal(UnframeResult.TooLarge, Unframe(framed, "gzip", out _));
    }

    [Fact]
    public void A_message_exactly_at_the_limit_is_accepted()
    {
        // The boundary from the other side: refusing here would drop legitimate batches.
        var payload = new byte[Limit];
        Assert.Equal(UnframeResult.Ok, Unframe(Frame(payload, gzip: true), "gzip", out var message));
        Assert.Equal(Limit, message.Length);
    }

    // ── The response ──────────────────────────────────────────────────────────

    [Fact]
    public void Everything_accepted_is_an_empty_response()
    {
        // An empty Export…ServiceResponse is what "no partial success to report" looks like.
        Assert.Empty(OtlpGrpcFraming.ExportResponse(0));
        Assert.Empty(OtlpGrpcFraming.ExportResponse(-1));
    }

    [Fact]
    public void Rejected_records_are_reported_as_partial_success()
    {
        // Decoded by hand against the .proto, because nothing else here would catch a wrong field
        // number or wire type — a client would simply ignore the bytes and read a clean success.
        //   ExportLogsServiceResponse { partial_success = 1 }
        //   ExportLogsPartialSuccess  { rejected = 1 (varint), error_message = 2 (string) }
        var bytes = OtlpGrpcFraming.ExportResponse(5);

        Assert.Equal(0x0A, bytes[0]);          // field 1, length-delimited
        Assert.Equal(2,    bytes[1]);          // the submessage is two bytes long
        Assert.Equal(0x08, bytes[2]);          // field 1, varint
        Assert.Equal(5,    bytes[3]);          // rejected = 5
        Assert.Equal(4,    bytes.Length);
    }

    [Fact]
    public void A_reason_travels_with_the_count()
    {
        var bytes = OtlpGrpcFraming.ExportResponse(1, "full");

        Assert.Equal(0x0A, bytes[0]);
        Assert.Equal(0x08, bytes[2]);
        Assert.Equal(1,    bytes[3]);
        Assert.Equal(0x12, bytes[4]);          // field 2, length-delimited
        Assert.Equal(4,    bytes[5]);          // "full"
        Assert.Equal("full", System.Text.Encoding.UTF8.GetString(bytes, 6, 4));
    }

    [Fact]
    public void A_large_count_survives_its_varint()
    {
        // A byte-at-a-time varint writer is exactly the kind of thing that is right for 5 and
        // wrong for 300.
        var bytes = OtlpGrpcFraming.ExportResponse(300);
        Assert.Equal(0xAC, bytes[3]);
        Assert.Equal(0x02, bytes[4]);
    }

    [Fact]
    public void A_framed_response_carries_its_length()
    {
        var framed = OtlpGrpcFraming.Frame(OtlpGrpcFraming.ExportResponse(5));
        Assert.Equal(0, framed[0]);
        Assert.Equal(4u, BinaryPrimitives.ReadUInt32BigEndian(framed.AsSpan(1)));
        Assert.Equal(9, framed.Length);
    }

    [Fact]
    public void An_empty_response_frames_to_five_bytes()
    {
        // What a client sees on a clean export: a header and nothing after it.
        var framed = OtlpGrpcFraming.Frame(OtlpGrpcFraming.ExportResponse(0));
        Assert.Equal(5, framed.Length);
        Assert.Equal(0u, BinaryPrimitives.ReadUInt32BigEndian(framed.AsSpan(1)));
    }
}
