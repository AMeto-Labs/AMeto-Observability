using System.Buffers.Binary;
using System.Text;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;

namespace Ameto.Query.Tests;

/// <summary>
/// An index sink that writes a group the way every build before the vouching one did, so that a
/// test can hold a query to the segments that are already on somebody's disk: the
/// <c>@x.exists</c> marker filed only for an exception the builder could READ, and no word that
/// the marker is complete.
///
/// <para>It is the real <see cref="SegmentIndexBuilder"/> with exactly those two differences put
/// back. A row whose exception map cannot be read is handed over without its exception — the
/// old builder filed no <c>@x</c> term of any kind for such a row, which is what a row without
/// one gets — and the builder's word, the first and empty property of the inverted section, is
/// cut out of the bytes before they are framed.</para>
/// </summary>
internal sealed class PreVouchingIndexSink(int estimatedEventCount, int estimatedTermsPerEvent) : ISegmentIndexSink
{
    private readonly SegmentIndexBuilder _builder = new(estimatedEventCount, 5, estimatedTermsPerEvent);

    public void Add(uint fileOrdinal, in SegmentEventRef ev)
    {
        if (ev.ExceptionPayload.IsEmpty ||
            ExceptionInfo.TryReadIndexFields(ev.ExceptionPayload.ToArray(), out _, out _, out _))
        {
            _builder.Add(fileOrdinal, in ev);
            return;
        }

        var unread = new SegmentEventRef(ev.Id, ev.TimestampUtcTicks, ev.Level, ev.TraceIdHi, ev.TraceIdLo, ev.SpanId,
                                         ev.MessageTemplate, ev.ServiceName, ev.Properties);
        _builder.Add(fileOrdinal, in unread);
    }

    public long BloomTermsAdded   => _builder.BloomTermsAdded;
    public long BloomTermCapacity => _builder.BloomTermCapacity;

    public void WriteSections(Stream destination, out long invertedOffset, out long trigramOffset, out long bloomOffset)
    {
        var (inverted, trigram, bloom) = _builder.Serialise();
        invertedOffset = ISegmentIndexSink.WriteFramed(destination, WithoutTheWord(inverted));
        trigramOffset  = ISegmentIndexSink.WriteFramed(destination, trigram);
        bloomOffset    = ISegmentIndexSink.WriteFramed(destination, bloom);
    }

    public void Dispose() => _builder.Dispose();

    /// <summary>
    /// The inverted section minus its first property, which has to be the builder's word: after
    /// the magic and the property count, <c>uint16 nameLen, name, uint32 valueCount</c> — zero.
    /// Checked rather than assumed, so that a builder which stops writing the word first, or
    /// stops writing it at all, fails here instead of quietly handing these tests a vouched group.
    /// </summary>
    internal static byte[] WithoutTheWord(byte[] inverted)
    {
        const int Header = 8;
        byte[] name  = Encoding.UTF8.GetBytes(ClefFields.ExceptionExistsComplete);
        int    entry = 2 + name.Length + 4;

        Assert.True(inverted.Length >= Header + entry, "the section is too short to open with the builder's word");
        Assert.Equal(name.Length, BinaryPrimitives.ReadUInt16LittleEndian(inverted.AsSpan(Header)));
        Assert.True(inverted.AsSpan(Header + 2, name.Length).SequenceEqual(name), "the first property is not the builder's word");
        Assert.Equal(0u, BinaryPrimitives.ReadUInt32LittleEndian(inverted.AsSpan(Header + 2 + name.Length)));

        var cut = new byte[inverted.Length - entry];
        inverted.AsSpan(0, Header).CopyTo(cut);
        BinaryPrimitives.WriteUInt32LittleEndian(cut.AsSpan(4), BinaryPrimitives.ReadUInt32LittleEndian(inverted.AsSpan(4)) - 1);
        inverted.AsSpan(Header + entry).CopyTo(cut.AsSpan(Header));
        return cut;
    }
}
