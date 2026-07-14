using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Xml.XPath;
using Microsoft.Extensions.Logging;

namespace Ameto.Alerts;

/// <summary>
/// Runs an <see cref="HttpFlowChannel"/>: sequential HTTP steps sharing a variable context.
/// <c>{{var}}</c> tokens in URL / header key+value / body are substituted from the context;
/// each step can capture a response value (JSONPath / XPath / header / regex / status) into a
/// variable for later steps. A non-2xx response or a failed extraction stops the flow (logged).
/// </summary>
internal static partial class HttpFlowExecutor
{
    public static async Task RunAsync(
        HttpFlowChannel ch,
        IReadOnlyDictionary<string, string> alertVars,
        HttpClient http,
        ILogger logger,
        CancellationToken ct)
    {
        // Context: alert.* vars + secret.* (already decrypted in the in-memory rule) + captured vars.
        var vars = new Dictionary<string, string>(alertVars, StringComparer.Ordinal);
        foreach (var (k, v) in ch.Secrets) vars["secret." + k] = v;

        for (int i = 0; i < ch.Steps.Count; i++)
        {
            var step  = ch.Steps[i];
            var label = string.IsNullOrWhiteSpace(step.Name) ? $"#{i + 1}" : step.Name;

            HttpResponseMessage resp;
            string body;
            try
            {
                var url = Substitute(step.Url, vars);
                using var req = new HttpRequestMessage(new HttpMethod(step.Method.ToUpperInvariant()), url);
                foreach (var h in step.Headers)
                {
                    var key = Substitute(h.Key, vars);
                    if (key.Length == 0) continue;
                    req.Headers.TryAddWithoutValidation(key, Substitute(h.Value, vars));
                }
                SetBody(req, step, vars);

                resp = await http.SendAsync(req, ct);
                body = await resp.Content.ReadAsStringAsync(ct);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "HTTP flow step '{Step}' request failed", label);
                return;
            }

            using (resp)
            {
                if (!resp.IsSuccessStatusCode)
                {
                    logger.LogWarning("HTTP flow stopped at step '{Step}': status {Status}", label, (int)resp.StatusCode);
                    return;
                }

                foreach (var ext in step.Extracts)
                {
                    if (string.IsNullOrWhiteSpace(ext.Var)) continue;
                    var value = Extract(ext, resp, body);
                    if (value is null)
                    {
                        logger.LogWarning("HTTP flow stopped: step '{Step}' could not extract '{Var}' via {Source}", label, ext.Var, ext.Source);
                        return;
                    }
                    vars[ext.Var] = value;
                }
            }
        }
    }

    // ── Body ──────────────────────────────────────────────────────────────────────

    private static void SetBody(HttpRequestMessage req, HttpFlowStep step, Dictionary<string, string> vars)
    {
        if (step.BodyType is "none" || string.IsNullOrEmpty(step.Body)) return;
        var content = Substitute(step.Body, vars);
        var mediaType = step.BodyType switch
        {
            "json" => "application/json",
            "xml"  => "application/xml",
            "form" => "application/x-www-form-urlencoded",
            _      => "text/plain",
        };
        req.Content = new StringContent(content, Encoding.UTF8, mediaType);
    }

    // ── Variable substitution ───────────────────────────────────────────────────

    [GeneratedRegex(@"\{\{\s*([\w.\-]+)\s*\}\}")]
    private static partial Regex VarPattern();

    /// <summary>Replaces <c>{{name}}</c> with its variable value; unknown names → empty.</summary>
    private static string Substitute(string? template, IReadOnlyDictionary<string, string> vars)
    {
        if (string.IsNullOrEmpty(template)) return template ?? string.Empty;
        return VarPattern().Replace(template, m => vars.TryGetValue(m.Groups[1].Value, out var v) ? v : string.Empty);
    }

    // ── Extraction ───────────────────────────────────────────────────────────────

    private static string? Extract(HttpExtract ext, HttpResponseMessage resp, string body) => ext.Source switch
    {
        "json"   => SelectJsonPath(body, ext.Expr),
        "xml"    => SelectXPath(body, ext.Expr),
        "regex"  => RegexExtract(body, ext.Expr),
        "status" => ((int)resp.StatusCode).ToString(),
        "header" => resp.Headers.TryGetValues(ext.Expr, out var hv) || resp.Content.Headers.TryGetValues(ext.Expr, out hv)
                        ? string.Join(",", hv) : null,
        _        => null,
    };

    private static string? RegexExtract(string body, string pattern)
    {
        try
        {
            var m = Regex.Match(body, pattern, RegexOptions.None, TimeSpan.FromSeconds(2));
            if (!m.Success) return null;
            return m.Groups.Count > 1 ? m.Groups[1].Value : m.Value; // first capture group, else whole match
        }
        catch { return null; }
    }

    private static string? SelectXPath(string xml, string xpath)
    {
        try
        {
            var nav  = new XPathDocument(new StringReader(xml)).CreateNavigator();
            var node = nav.SelectSingleNode(xpath);
            return node?.Value;
        }
        catch { return null; }
    }

    /// <summary>
    /// JSONPath subset: root <c>$</c>, child <c>.key</c> / <c>['key']</c> / <c>["key"]</c>,
    /// and array index <c>[n]</c> (negative = from end). Covers token-extraction cases.
    /// </summary>
    private static string? SelectJsonPath(string json, string path)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var cur = doc.RootElement;
            foreach (var seg in TokenizePath(path))
            {
                if (seg.Index is int idx)
                {
                    if (cur.ValueKind != JsonValueKind.Array) return null;
                    int len = cur.GetArrayLength();
                    int i = idx < 0 ? len + idx : idx;
                    if (i < 0 || i >= len) return null;
                    cur = cur[i];
                }
                else
                {
                    if (cur.ValueKind != JsonValueKind.Object || !cur.TryGetProperty(seg.Key!, out var next)) return null;
                    cur = next;
                }
            }
            return cur.ValueKind switch
            {
                JsonValueKind.String => cur.GetString(),
                JsonValueKind.Null   => null,
                _                    => cur.GetRawText(),
            };
        }
        catch { return null; }
    }

    private static IEnumerable<(string? Key, int? Index)> TokenizePath(string path)
    {
        int i = 0;
        int n = path.Length;
        if (i < n && path[i] == '$') i++;
        while (i < n)
        {
            char c = path[i];
            if (c == '.')
            {
                i++;
                int start = i;
                while (i < n && path[i] != '.' && path[i] != '[') i++;
                if (i > start) yield return (path[start..i], null);
            }
            else if (c == '[')
            {
                i++;
                if (i < n && (path[i] == '\'' || path[i] == '"'))
                {
                    char q = path[i++];
                    int start = i;
                    while (i < n && path[i] != q) i++;
                    yield return (path[start..i], null);
                    if (i < n) i++;            // closing quote
                    if (i < n && path[i] == ']') i++;
                }
                else
                {
                    int start = i;
                    while (i < n && path[i] != ']') i++;
                    if (int.TryParse(path[start..i], out var idx)) yield return (null, idx);
                    if (i < n) i++;            // closing ]
                }
            }
            else i++; // skip stray chars
        }
    }
}
