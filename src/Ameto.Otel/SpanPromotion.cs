using System.Buffers.Text;

namespace Ameto.Otel;

/// <summary>
/// What a span attribute KEY means to the columns a span carries beside its attribute map.
/// </summary>
internal enum SpanKeyKind : byte
{
    /// <summary>An ordinary attribute: written to the map and nothing else.</summary>
    Plain,
    /// <summary><c>http.response.status_code</c> — the semconv key that WINS.</summary>
    HttpStatusNew,
    /// <summary><c>http.status_code</c> — the old key, used only until a new one is seen.</summary>
    HttpStatusOld,
    /// <summary>One of the four URL keys, read for the self-ingest check and nothing else.</summary>
    Url,
}

/// <summary>
/// THE PROMOTIONS A SPAN'S OWN ATTRIBUTES MAKE — the HTTP status code that becomes
/// <c>SpanIngestItem.HttpStatusCode</c>, and the "this CLIENT span is us calling ourselves" bit
/// that drops a self-ingest span — <b>in one place, for both streaming parsers</b>.
///
/// <para><b>Why this type exists.</b> <see cref="OtlpTraceStreamParser"/> (OTLP/JSON) and
/// <see cref="OtlpTraceProtoParser"/> (OTLP/protobuf, the road every SDK exporter takes) each
/// carried a private copy of the same four things: the key-kind enum, the key list that fills it,
/// the status latch, and the URL check. Each parser is held to the DOM path by a parity suite of
/// its own, and neither suite compares the two parsers, so a copy could drift and only the road
/// nobody tests on would notice. <b>Two had already drifted</b>, both on the JSON side, and both
/// away from <c>OtlpTraceMapper.ExtractHttpStatusCode</c>, which is the behaviour both are
/// supposed to reproduce:</para>
/// <list type="number">
/// <item><b>The latch.</b> The mapper <c>break</c>s at the first <c>http.response.status_code</c>
/// that parses, so the FIRST one wins and nothing after it — new key or old — can change the
/// answer. The protobuf parser has that latch. The JSON parser had a <c>httpFromNew</c> flag it
/// re-tested but never blocked on, so a SECOND new-key attribute overwrote the first: a span
/// carrying <c>http.response.status_code</c> twice came back 500 over protobuf and 503 over
/// JSON.</item>
/// <item><b>The parse.</b> The mapper's <c>short.TryParse</c> takes the WHOLE string or nothing.
/// The protobuf parser checks <c>consumed == text.Length</c>; the JSON parser discarded the
/// consumed count, so <c>"200abc"</c> promoted as 200 there and as nothing on the other two
/// paths.</item>
/// </list>
///
/// <para>A <c>struct</c> passed by <c>ref</c> through the parse, so the whole of this costs no
/// allocation and no indirection: three fields in the caller's frame, and every method below is
/// a leaf the JIT can inline into the attribute loop.</para>
///
/// <para><b>Resource attributes promote nothing</b> — only a span's own do. Both parsers say so
/// by passing <see cref="SpanKeyKind.Plain"/> for a resource key, and by handing the resource
/// pass a throwaway instance.</para>
/// </summary>
internal struct SpanPromotion
{
    /// <summary>The promoted code, 0 when no key on this span carried a usable one.</summary>
    public short HttpStatus;

    /// <summary>
    /// Set once a <c>http.response.status_code</c> has parsed. The DOM mapper's loop
    /// <c>break</c>s at that point, so nothing after it — old key or new — can change the
    /// answer; this latch IS that <c>break</c>.
    /// </summary>
    public bool HttpLatched;

    /// <summary>This span's URL points at one of Ameto's own ingest endpoints.</summary>
    public bool AmetoInternal;

    /// <summary>
    /// The longest URL <see cref="AmetoIngestEndpoints.Matches(ReadOnlySpan{byte})"/> compares; it
    /// refuses anything longer outright. Here rather than in either parser because it is a
    /// property of the matcher, and the JSON parser sizes a <c>stackalloc</c> from it.
    /// </summary>
    internal const int MaxMatchableUrlBytes = 512;

    /// <summary>
    /// The keys the DOM mapper promotes out of a span's attributes into its own columns. ORDER IS
    /// NOT SIGNIFICANT here — each key means one thing — but MEMBERSHIP is: a key that is not on
    /// this list reaches the attribute map and nothing else.
    /// </summary>
    internal static SpanKeyKind KindOf(ReadOnlySpan<byte> key) =>
        key.SequenceEqual("http.response.status_code"u8) ? SpanKeyKind.HttpStatusNew :
        key.SequenceEqual("http.status_code"u8)          ? SpanKeyKind.HttpStatusOld :
        key.SequenceEqual("url.full"u8) || key.SequenceEqual("url.path"u8) ||
        key.SequenceEqual("http.url"u8) || key.SequenceEqual("http.target"u8)
                                                         ? SpanKeyKind.Url
                                                         : SpanKeyKind.Plain;

    /// <summary>
    /// A TEXT value under a promoted key — the protobuf <c>string_value</c> and the JSON
    /// <c>stringValue</c>, as UTF-8 either way.
    ///
    /// <para>The status parse takes the whole span or nothing, which is
    /// <c>short.TryParse</c>'s rule minus the surrounding whitespace it would have allowed (no
    /// exporter sends it). A URL is handed to the matcher as bytes, which refuses one over
    /// <see cref="MaxMatchableUrlBytes"/> rather than comparing it.</para>
    /// </summary>
    internal void Capture(SpanKeyKind kind, ReadOnlySpan<byte> utf8)
    {
        if (kind == SpanKeyKind.Url)
        {
            if (!AmetoInternal) AmetoInternal = AmetoIngestEndpoints.Matches(utf8);
            return;
        }
        if (kind == SpanKeyKind.Plain) return;

        if (Utf8Parser.TryParse(utf8, out long v, out int consumed) && consumed == utf8.Length)
            CaptureStatus(kind, v);
    }

    /// <summary>
    /// An INTEGER value under a promoted key — <c>int_value</c> / <c>intValue</c>. A URL that
    /// arrives as an integer is not a URL and promotes nothing, exactly as the mapper's
    /// <c>StringValue ?? IntValue</c> on a non-status key promoted nothing.
    /// </summary>
    internal void Capture(SpanKeyKind kind, long value)
    {
        if (kind is SpanKeyKind.HttpStatusNew or SpanKeyKind.HttpStatusOld) CaptureStatus(kind, value);
    }

    private void CaptureStatus(SpanKeyKind kind, long value)
    {
        if (HttpLatched) return;
        if (value is < short.MinValue or > short.MaxValue) return;   // short.TryParse said no
        HttpStatus = (short)value;
        if (kind == SpanKeyKind.HttpStatusNew) HttpLatched = true;   // the mapper's break
    }
}
