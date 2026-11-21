using System.Globalization;

namespace Ameto.Mcp.Masking;

/// <summary>
/// The text form of a token: <c>h:</c> and twelve lowercase hex digits — the first six bytes of
/// the value's HMAC. Six bytes because a token is read and typed back by a model: long enough
/// that two values in one investigation do not share one, short enough to copy without loss.
/// </summary>
public static class MaskToken
{
    public const string Prefix = "h:";
    public const int    Length = 14;   // "h:" + 12 hex digits

    public static string Format(ulong token) =>
        string.Create(Length, token, static (span, value) =>
        {
            span[0] = 'h';
            span[1] = ':';
            value.TryFormat(span[2..], out _, "x12", CultureInfo.InvariantCulture);
        });

    public static bool TryParse(ReadOnlySpan<char> text, out ulong token)
    {
        token = 0;
        return text.Length == Length
            && text.StartsWith(Prefix, StringComparison.Ordinal)
            && ulong.TryParse(text[2..], NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out token);
    }
}

/// <summary>
/// Remembers which value a token stands for, so that a token the AI hands back in a filter
/// (<c>CardNumber = 'h:91c0e2aa5d17'</c>) can be searched for without the AI ever holding the
/// value. A token is a one-way HMAC: without this there is no road from it back to the value
/// short of hashing every event in the window.
///
/// <para>Bounded by two generations of at most <c>generationSize</c> entries each. When the
/// current one fills it becomes the previous one and the older previous one is dropped, so the
/// vault holds the values of the most recent answers and forgets the rest; a token that has
/// been forgotten is refused with a sentence saying to run the search again. Values longer than
/// <see cref="MaxValueLength"/> are never kept — nobody pivots on a paragraph.</para>
///
/// <para>It holds raw values in memory, as the hot tier holds the events they came from; it
/// writes nothing to disk and is emptied when the secret is rotated, since every token then
/// changes.</para>
/// </summary>
public sealed class TokenVault(int generationSize = 20_000)
{
    public const int MaxValueLength = 256;

    /// <summary>A remembered value: its text, and whether it was stored as a number.</summary>
    public readonly record struct Entry(string Text, bool IsNumber);

    private readonly Lock _gate = new();
    private Dictionary<ulong, Entry> _current  = [];
    private Dictionary<ulong, Entry> _previous = [];

    public void Remember(ulong token, string text, bool isNumber)
    {
        if (text.Length > MaxValueLength) return;
        lock (_gate)
        {
            if (_current.ContainsKey(token)) return;
            if (_current.Count >= generationSize)
            {
                _previous = _current;
                _current  = new Dictionary<ulong, Entry>(generationSize);
            }
            _current[token] = new Entry(text, isNumber);
        }
    }

    public bool TryResolve(ulong token, out Entry entry)
    {
        lock (_gate)
        {
            if (_current.TryGetValue(token, out entry)) return true;
            if (!_previous.TryGetValue(token, out entry)) return false;
            // Used again, so it belongs to the current generation: a value an investigation
            // keeps coming back to does not age out from under it.
            if (_current.Count < generationSize) _current[token] = entry;
            return true;
        }
    }

    public void Clear()
    {
        lock (_gate)
        {
            _current  = [];
            _previous = [];
        }
    }
}
