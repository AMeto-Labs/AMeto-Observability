using System.Reflection;
using System.Text;
using System.Text.Json;
using Ameto.Core;
using Ameto.Mcp;
using Ameto.Mcp.Masking;
using Ameto.Mcp.Tools;
using Ameto.Query.Filtering;

namespace Ameto.Integration.Tests;

/// <summary>
/// What stands between a filter an AI wrote and the engine, held to the ways around it that the
/// first review of this code found. Each test names the road it closes; none needs a host.
///
/// <para>Two facts carry most of it. A token becomes its value in the TREE, and only as the
/// operand of an equality — never in the text, where a value put between somebody else's quotes
/// moves them. And a field the rules hide is hidden by every road the engine reads it: under
/// its parent's rule, in another service, through a subscript spelled as a number.</para>
/// </summary>
public sealed class McpFilterGuardTests
{
    private static readonly byte[] Secret = Encoding.UTF8.GetBytes("a secret for tests, thirty-two b.");
    private static readonly char Sep = PropertyPath.Separator;

    private static McpMaskRule Rule(string field, MaskAction action, params string[] services) =>
        new() { Id = Guid.NewGuid().ToString("N"), Field = field, Action = action, Services = services };

    private static MaskingPolicy Policy(McpMaskingMode mode, TokenVault vault, params McpMaskRule[] rules) =>
        new(mode, rules, MaskDetectors.All, Secret, vault);

    private static MaskingPolicy Standard(params McpMaskRule[] rules) => Policy(McpMaskingMode.Standard, new TokenVault(), rules);

    private static readonly McpProfile Payments = new() { Id = "a", Name = "a", KeyHash = "", Services = ["payments-api"] };

    private static LogEvent Event(string template, Dictionary<string, object?> properties, string? service = "payments-api", ExceptionInfo? exception = null) => new()
    {
        Id              = new EventId(1_000, 1, 1),
        Timestamp       = new DateTimeOffset(EventIdGenerator.Epoch, TimeSpan.Zero).AddMilliseconds(1_000),
        Level           = LogLevel.Information,
        MessageTemplate = template,
        Properties      = properties,
        ServiceName     = service,
        Exception       = exception,
    };

    private static Dictionary<string, string> Cells(MaskedEvent ev) => ev.Properties.ToDictionary(static p => p.Key, static p => p.Value);

    private static Dictionary<string, JsonElement> Json(string json) =>
        JsonDocument.Parse(json).RootElement.EnumerateObject().ToDictionary(static p => p.Name, static p => p.Value.Clone());

    /// <summary>A numeric token and a string token, as an answer would have carried them.</summary>
    private static (MaskingPolicy Policy, TokenVault Vault, string Numeric, string Text) WithTokens(McpMaskingMode mode = McpMaskingMode.Standard)
    {
        var vault  = new TokenVault();
        var policy = Policy(mode, vault, Rule("CardNumber", MaskAction.Hash), Rule("MerchantId", MaskAction.Hash));
        var cells  = Cells(policy.Mask(Event("x", new() { ["CardNumber"] = "4111111111111111", ["MerchantId"] = 6456116363L }), new MaskStats()));
        return (policy, vault, cells["MerchantId"], cells["CardNumber"]);
    }

    /// <summary>The filter as the engine is given it for a profile of one service, compiled.</summary>
    private static CompiledFilter Run(FilterGuard.Prepared prepared)
    {
        Assert.Null(prepared.Error);
        return CompiledFilter.Compile(FilterGuard.Compose(prepared.Node, Payments));
    }

    // ── A token is a value in the tree, never text in the filter ──────────────

    [Fact]
    public void ATokenInsideAnotherString_StaysText_AndNoQuoteMoves()
    {
        var (policy, vault, numeric, _) = WithTokens();
        Assert.Contains("hidden field", FilterGuard.Prepare("CardNumber like '4111%'", policy, vault).Error);

        // As written: A equals one string, or B equals another. The first string ends in a token
        // and the second holds a pattern test — both are text, and stay text.
        string written = $"A = 'p\\'{numeric}' or B = ' or CardNumber like \\'4111%'";
        var prepared = FilterGuard.Prepare(written, policy, vault);
        var compiled = Run(prepared);

        Assert.False(compiled.Matches(Event("x", new() { ["CardNumber"] = "4111111111111111" })));
        Assert.True(compiled.Matches(Event("x", new() { ["A"] = "p'" + numeric })));
        Assert.True(compiled.Matches(Event("x", new() { ["B"] = " or CardNumber like '4111%" })));
        // The text that runs carries no value a token stands for.
        Assert.DoesNotContain("6456116363", FilterGuard.Compose(prepared.Node, Payments));
    }

    [Fact]
    public void AnAggregation_KeepsItsColumnsAsTheyWereWritten_WhateverTheyName()
    {
        var (policy, vault, numeric, text) = WithTokens();
        Assert.Contains("hidden field", FilterGuard.PrepareAggregation("select min(CardNumber)", policy, vault, Payments).Error);

        // The alias ends in a token, and the where-clause is one long string that reads like more columns.
        var (query, error, _) = FilterGuard.PrepareAggregation(
            $"select count(*) as 'x\\'{numeric}' where A = ', min(CardNumber) as \\'m'", policy, vault, Payments);
        Assert.Null(error);
        var only = Assert.Single(query!.Aggregates);
        Assert.Equal(AggregateKind.Count, only.Kind);
        Assert.Equal("x'" + numeric, only.Alias);                      // the token, as written — not what it stands for
        Assert.DoesNotContain("6456116363", query.FilterText);
        Assert.Contains("min(CardNumber)", query.FilterText);          // still inside the string it was written in

        // A token as a column heading, or as a property name, is a heading and a name.
        var (asAlias, _, _) = FilterGuard.PrepareAggregation($"select count(*) as '{text}'", policy, vault, Payments);
        Assert.Equal(text, Assert.Single(asAlias!.Aggregates).Alias);
        var (asProperty, _, _) = FilterGuard.PrepareAggregation($"select count(['{text}'])", policy, vault, Payments);
        Assert.Equal(text, Assert.Single(asProperty!.Aggregates).Property);

        // And the numbers are taken from numbers only.
        Assert.True(query.NumbersOnly);
    }

    [Theory]
    [InlineData("Elapsed > '{n}'")]
    [InlineData("Elapsed <= '{n}'")]
    [InlineData("Path >= '{t}'")]
    [InlineData("length(@mt) > '{n}'")]
    [InlineData("length(@mt) = '{n}'")]
    [InlineData("startsWith(Path, '{t}')")]
    [InlineData("ci_contains(@mt, '{t}')")]
    [InlineData("endsWith(Path, '{t}')")]
    [InlineData("Path like '{t}'")]
    [InlineData("regexMatch(Path, '{t}')")]
    [InlineData("'{t}'")]                                // a token as a plain word
    [InlineData("declined '{t}'")]
    [InlineData("@l = 'Error' and not (Elapsed > '{n}')")]
    public void AToken_StandsOnlyAsTheValueOfAnEquality(string filter)
    {
        var (policy, vault, numeric, text) = WithTokens();
        var prepared = FilterGuard.Prepare(filter.Replace("{n}", numeric).Replace("{t}", text), policy, vault);

        Assert.Contains("token for a hidden value", prepared.Error);
        Assert.Equal(RefusalKind.Masking, prepared.Kind);
        Assert.Null(prepared.Node);
        // The refusal names the token, as the caller wrote it — never the value.
        Assert.DoesNotContain("6456116363", prepared.Error);
        Assert.DoesNotContain("4111111111111111", prepared.Error);
    }

    [Theory]
    [InlineData("MerchantId = '{n}'", true)]
    [InlineData("MerchantId != '{n}'", false)]
    [InlineData("MerchantId in ['{n}', 'x']", true)]
    [InlineData("MerchantId not in ['{n}']", false)]
    [InlineData("Other = 'x' or CardNumber = '{t}'", true)]
    public void AToken_UnderEquality_IsTheValueItStandsFor(string filter, bool matches)
    {
        var (policy, vault, numeric, text) = WithTokens();
        var compiled = Run(FilterGuard.Prepare(filter.Replace("{n}", numeric).Replace("{t}", text), policy, vault));
        Assert.Equal(matches, compiled.Matches(Event("x", new() { ["CardNumber"] = "4111111111111111", ["MerchantId"] = 6456116363L })));
    }

    [Fact]
    public void ANumericToken_IsPutBackAsTheNumber_AlsoWhenDotNetWouldWriteItWithAnExponent()
    {
        var vault  = new TokenVault();
        var policy = Policy(McpMaskingMode.Standard, vault, Rule("Rate", MaskAction.Hash));
        foreach (double rate in (double[])[1e-7, 2.5e-12, 1.5e20, 0.1, -3.25])
        {
            var ev = Event("x", new() { ["Rate"] = rate });
            string token = Cells(policy.Mask(ev, new MaskStats()))["Rate"];
            var prepared = FilterGuard.Prepare($"Rate = '{token}'", policy, vault);
            string text  = FilterGuard.Compose(prepared.Node, Payments);
            Assert.DoesNotContain("E", text.Replace("Rate", ""));       // no exponent: the lexer has no reading of one
            Assert.True(CompiledFilter.Compile(text).Matches(ev), text);
        }
    }

    // ── A field the rules hide is hidden by every road ────────────────────────

    [Fact]
    public void AKeepForOneService_OpensTheFieldInNoOther()
    {
        var vault  = new TokenVault();
        var strict = Policy(McpMaskingMode.Strict, vault, Rule("UserName", MaskAction.Keep, "shop"));

        // An event of another service: hidden in a search…
        Assert.StartsWith("h:", Cells(strict.Mask(Event("x", new() { ["UserName"] = "Ivan Petrov" }, "billing"), new MaskStats()))["UserName"]);
        // …and so hidden wherever a question is not tied to one service.
        Assert.True(strict.HidesField("UserName"));
        Assert.StartsWith("h:", strict.MaskGroupKey("UserName", "Ivan Petrov", new MaskStats()));
        Assert.Contains("hidden field", FilterGuard.Prepare("UserName like 'Iv%'", strict, vault).Error);
        Assert.Contains("hidden field", FilterGuard.PrepareAggregation("select min(UserName)", strict, vault, Payments).Error);

        // A keep that holds everywhere is the allow-list entry it reads as.
        var kept = Policy(McpMaskingMode.Strict, vault, Rule("UserName", MaskAction.Keep));
        Assert.False(kept.HidesField("UserName"));
        Assert.Equal("Ivan Petrov", kept.MaskGroupKey("UserName", "Ivan Petrov", new MaskStats()));
        Assert.Null(FilterGuard.Prepare("UserName like 'Iv%'", kept, vault).Error);

        // Standard: a keep for one service does not let a detector's find out through a group key.
        var standard = Policy(McpMaskingMode.Standard, vault, Rule("Contact", MaskAction.Keep, "support"));
        Assert.StartsWith("h:", Cells(standard.Mask(Event("x", new() { ["Contact"] = "a.serikov@example.com" }, "billing"), new MaskStats()))["Contact"]);
        Assert.StartsWith("h:", standard.MaskGroupKey("Contact", "a.serikov@example.com", new MaskStats()));
    }

    [Theory]
    [InlineData(MaskAction.Hash)]
    [InlineData(MaskAction.Remove)]
    [InlineData(MaskAction.KeepLast4)]
    public void ARuleOnAnObject_HidesEverythingUnderIt(MaskAction action)
    {
        var vault  = new TokenVault();
        var policy = Policy(McpMaskingMode.Standard, vault, Rule("Customer", action));
        var cells  = Cells(policy.Mask(Event("x", new()
        {
            ["Customer"] = new Dictionary<string, object?> { ["Name"] = "Ivan Petrov", ["Tier"] = "gold" },
        }), new MaskStats()));
        Assert.DoesNotContain("Ivan", string.Join("|", cells.Values));

        Assert.True(policy.HidesField("Customer.Name"));
        Assert.True(policy.HidesField("Customer.Address.City"));
        Assert.Contains("hidden field", FilterGuard.Prepare("Customer.Name like 'Iv%'", policy, vault).Error);
        Assert.Contains("hidden field", FilterGuard.Prepare("Customer.Address.City > 'A'", policy, vault).Error);
        Assert.Contains("hidden field", FilterGuard.PrepareAggregation("select max(Customer.Age)", policy, vault, Payments).Error);

        // Grouping by it is allowed, as for any hidden field — and gives no name back.
        Assert.Null(FilterGuard.PrepareAggregation("select count(*) group by Customer.Name", policy, vault, Payments).Error);
        Assert.DoesNotContain("Ivan", policy.MaskGroupKey("Customer.Name", "Ivan Petrov", new MaskStats()));
    }

    [Theory]
    [InlineData("Customer.Iin")]
    [InlineData("Iin")]
    public void AHiddenChild_IsNotReadThroughItsParent(string rule)
    {
        var vault  = new TokenVault();
        var policy = Policy(McpMaskingMode.Standard, vault, Rule(rule, MaskAction.Hash));
        Assert.Contains("hidden field", FilterGuard.Prepare("Customer.Iin > '9001'", policy, vault).Error);

        // The functions that read a container are not a filter's to use here, on any field.
        foreach (string filter in (string[])
                 [
                     "elementAt(Customer, 'Iin') > '9001'",
                     "toJson(Customer) > '{\"Iin\":\"9001'",
                     "keys(Customer) = '[\"Iin\"]'",
                     "values(Customer) = 'x'",
                     "fromJson(Body).Iin > '9001'",
                     "has(fromJson(Body).Iin)",
                 ])
        {
            var prepared = FilterGuard.Prepare(filter, policy, vault);
            Assert.Contains("is not available here", prepared.Error);
            Assert.Equal(RefusalKind.Input, prepared.Kind);
        }

        // The parent itself, read as a value, is a container: the engine answers a pattern test
        // on it from its type's name, and an ordering with false. Nothing of the child is in it.
        var ev = Event("x", new() { ["Customer"] = new Dictionary<string, object?> { ["Iin"] = "900101300126", ["Tier"] = "gold" } });
        foreach (string filter in (string[])["Customer like '%9001%'", "ci_contains(Customer, '9001')", "Customer > '9'", "regexMatch(Customer, '9001')"])
            Assert.False(Run(FilterGuard.Prepare(filter, policy, vault)).Matches(ev), filter);
    }

    [Fact]
    public void ANumberInAPath_IsJudgedAsTheSubscriptItCanBe()
    {
        var vault  = new TokenVault();
        var policy = Policy(McpMaskingMode.Standard, vault, Rule("Items.Sku", MaskAction.Hash), Rule("Limits.5", MaskAction.Hash));

        // Items is a list: its elements are named by the list's own path, however the filter spells the subscript.
        foreach (string path in (string[])["Items.Sku", "Items[0].Sku", "Items.0.Sku", "Items.12.Sku"])
        {
            Assert.Contains("hidden field", FilterGuard.Prepare($"{path} like 'A%'", policy, vault).Error);
            Assert.Null(FilterGuard.Prepare($"{path} = 'A-1'", policy, vault).Error);
        }
        Assert.True(policy.HidesField("Items.0.Sku"));
        Assert.StartsWith("h:", policy.MaskGroupKey("Items.0.Sku", "A-1", new MaskStats()));

        // Limits is a map with a key that is a number: the rule names it as it is.
        Assert.Contains("hidden field", FilterGuard.Prepare("Limits.5 > 3", policy, vault).Error);
        Assert.Null(FilterGuard.Prepare("Limits.6 > 3", policy, vault).Error);
        Assert.Null(FilterGuard.Prepare("Items.0.Qty > 3", policy, vault).Error);
    }

    [Fact]
    public void APropertyWhoseNameBeginsWithAnAtSign_IsAProperty_AndTheEnginesOwnFieldsAreNot()
    {
        var vault  = new TokenVault();
        var strict = Policy(McpMaskingMode.Strict, vault);

        // Not one of the engine's fields: a property, hidden like any other in Strict —
        // in a filter, and as the key of a table.
        Assert.True(FilterGuard.IsHidden(strict, "@custom"));
        Assert.Contains("hidden field", FilterGuard.Prepare("@custom like 'Iv%'", strict, vault).Error);
        Assert.StartsWith("h:", strict.MaskGroupKey("@custom", "Ivan Petrov", new MaskStats()));
        Assert.StartsWith("h:", strict.MaskGroupKey("@x2", "Ivan Petrov", new MaskStats()));

        // The engine's own, by any of the names it answers to, are never hidden ones.
        foreach (string builtIn in (string[])["@mt", "@l", "@t", "@service", "@tr", "@sp", "@id", "@x", "Level", "Message", "Exception"])
            Assert.False(FilterGuard.IsHidden(strict, builtIn), builtIn);
        Assert.False(FilterGuard.IsHidden(strict, "@x" + Sep + "message"));
        Assert.Null(FilterGuard.Prepare("@mt like 'Paid%' and @l = 'Error' and ci_contains(@x.message, 'refused') and @t > '2026-01-01'", strict, vault).Error);
    }

    // ── Plain words ───────────────────────────────────────────────────────────

    [Fact]
    public void PlainWords_AreLookedForInTheTemplateAndTheException_NotInAnyValue()
    {
        var (policy, vault, _, _) = WithTokens(McpMaskingMode.Strict);

        // The road the review walked: fifty-five searches, one more digit each, spelled a card number out.
        var card = Event("Paid", new() { ["CardNumber"] = "4111111111111111", ["Note"] = "declined by issuer" });
        foreach (string words in (string[])["41111111", "'411111111111111'", "4111 1111", "issuer"])
            Assert.False(Run(FilterGuard.Prepare(words, policy, vault)).Matches(card), words);

        // What plain words are for still works: the text of the message, and of what was thrown.
        var declined = Event("Payment {PaymentId} declined: {Reason}", new() { ["PaymentId"] = "pay_1" },
            exception: new ExceptionInfo { Type = "System.TimeoutException", Message = "The gateway did not answer" });
        Assert.True(Run(FilterGuard.Prepare("declined", policy, vault)).Matches(declined));
        Assert.True(Run(FilterGuard.Prepare("payment DECLINED", policy, vault)).Matches(declined));
        Assert.True(Run(FilterGuard.Prepare("gateway", policy, vault)).Matches(declined));
        Assert.True(Run(FilterGuard.Prepare("timeoutexception declined", policy, vault)).Matches(declined));
        Assert.False(Run(FilterGuard.Prepare("declined refunded", policy, vault)).Matches(declined));   // every word, not any
        Assert.True(Run(FilterGuard.Prepare("declined and @l = 'Information'", policy, vault)).Matches(declined));
        Assert.False(Run(FilterGuard.Prepare("pay_1", policy, vault)).Matches(declined));                // a value, not text
    }

    // ── What the engine is given is the tree that was judged ──────────────────

    [Theory]
    [InlineData("@l = 'Error'")]
    [InlineData("Error")]
    [InlineData("Name = 'can''t' or Name = 'it\\'s' or Path = 'C:\\\\temp\\\\x'")]
    [InlineData("Name = 'x'' or has(@mt) or ''y'")]
    [InlineData("StatusCode >= 500 and Elapsed > 2000.5 and Delta = -3 and Ratio < 0.000001")]
    [InlineData("Flag = true and Other = false and Gone = null")]
    [InlineData("@l in ['Error', 'Fatal'] and StatusCode not in [200, 204, 2.5]")]
    [InlineData("has(UserId) and not has(@x) and isDefined(Request.User.Id)")]
    [InlineData("@mt like '%timed out%' and Path not like '/health%'")]
    [InlineData("startsWith(Path, '/api') or ci_startsWith(Path, '/API') or contains(@mt, 'x') or ci_contains(@x.message, 'refused')")]
    [InlineData("endsWith(Path, '.png') or ci_endsWith(Path, '.PNG') or regexMatch(Path, '^/v[0-9]+/') or length(Path) > 20")]
    [InlineData("Request.User.Id = 42 and Headers['X-Forwarded-For'] = '10.0.0.1' and Items[0].Sku = 'a' and Items[12].Tags[3] = 'b'")]
    [InlineData("['length'] = 1 and ['and'] = 2 and ['true'].x = 3 and ['we''ird name'] = 4 and ['a.b'] = 5")]
    [InlineData("http.request.method = 'GET' and @service = 'payments-api' and @tr = '0123456789abcdef0123456789abcdef'")]
    [InlineData("A = B and Left.Side != Right.Side and Elapsed > Budget")]
    [InlineData("not (A = 1 or (B = 2 and not C = 3))")]
    [InlineData("payment declined")]
    [InlineData("'quoted phrase' and @l = 'Warning'")]
    [InlineData("@t > '2026-11-16T09:00:00Z' and @t <= '2026-11-16T10:00:00Z'")]
    public void TheTextThatRuns_ReadsBackAsTheTreeThatWasJudged(string filter)
    {
        var prepared = FilterGuard.Prepare(filter, Standard(), new TokenVault());
        Assert.Null(prepared.Error);

        string text = FilterGuard.Write(prepared.Node!);
        Assert.Equal(FilterGuard.Describe(prepared.Node!), FilterGuard.Describe(FilterParser.Parse(text)));
        CompiledFilter.Compile(FilterGuard.Compose(prepared.Node, Payments));
    }

    public static TheoryData<string> AwkwardTexts() =>
    [
        "", " ", "'", "''", "\\", "\\'", "'\\", "x' or has(@mt) or 'y", "h:0123456789ab'", "a\nb", "tab\there",
        "%_%", ") or (", "]", "['x']", "{name}", "кириллица и ‘кавычки’", "\u0001", "null", "true", "-1", "1E-05",
    ];

    [Theory]
    [MemberData(nameof(AwkwardTexts))]
    public void AnyValueAndAnyName_AreWrittenSoThatTheyReadBackAsThemselves(string text)
    {
        // Built as trees, the way a token's value arrives: nothing here ever was filter text.
        FilterNode[] trees =
        [
            new CompareNode("Field", CompareOp.Eq, text),
            new InNode("Field", [text, 5L, 2.5, null, true]),
            new ContainsNode("Field", text, ci: true),
            new LikeNode("Field", text),
            new AndNode(new HasNode("Field"), new NotNode(new StartsWithNode("Other", text, ci: false))),
        ];
        foreach (var tree in trees)
        {
            string written = FilterGuard.Write(tree);
            Assert.Equal(FilterGuard.Describe(tree), FilterGuard.Describe(FilterParser.Parse(written)));
        }

        // The same text as the NAME of a property, on its own and under another: a name has no
        // control characters in it and is not empty, and anything else has to survive.
        if (text.Length == 0 || text.Contains('\u0001')) return;
        foreach (string path in (string[])[text, "Outer" + Sep + text, text + Sep + "Inner"])
        {
            var tree = new CompareNode(path, CompareOp.Ne, "v");
            Assert.Equal(FilterGuard.Describe(tree), FilterGuard.Describe(FilterParser.Parse(FilterGuard.Write(tree))));
        }
    }

    [Theory]
    [InlineData("A = 1 or B = 2")]
    [InlineData("A = 1 or B = 2 or C = 3")]
    [InlineData("A = 1 or B = 2 and C = 3")]
    [InlineData("B = 2 and C = 3 or A = 1")]
    [InlineData("not (B = 2 and not (A = 1 or C = 3)) or A = 1")]
    [InlineData("A = 1 or not A = 1")]
    [InlineData("declined or A = 1")]
    [InlineData("(((A = 1))) or ((B = 2) and (C = 3))")]
    public void NoOr_SlipsOutFromUnderTheScope(string filter)
    {
        // Each of these is true of the event — which belongs to a service the profile does not
        // see. Joined to the scope as one clause it matches nothing there; joined carelessly,
        // `a or b and scope`, the left side of the `or` would answer for every service.
        var prepared = FilterGuard.Prepare(filter, Standard(), new TokenVault());
        var theirs = Event("Payment declined", new() { ["A"] = 1, ["B"] = 2, ["C"] = 3 }, service: "payroll");
        var ours   = Event("Payment declined", new() { ["A"] = 1, ["B"] = 2, ["C"] = 3 }, service: "payments-api");

        Assert.False(Run(prepared).Matches(theirs));
        Assert.True(Run(prepared).Matches(ours));
        // With a service named by the call as well: both hold, on either side of every `or`.
        var narrowed = CompiledFilter.Compile(FilterGuard.Compose(prepared.Node, Payments with { Services = [] }, "payments-api"));
        Assert.False(narrowed.Matches(theirs));
        Assert.True(narrowed.Matches(ours));
        Assert.False(narrowed.Matches(Event("Payment declined", new() { ["A"] = 1, ["B"] = 2, ["C"] = 3 }, service: "ameto-mcp")));
    }

    [Fact]
    public void ALongFilter_IsWrittenLong_NotDeep()
    {
        var policy = Standard();
        // Three hundred clauses, five hundred negations, two dozen words: each is a chain, and a
        // chain is written flat. The text the engine is given nests no deeper than its meaning.
        string clauses = string.Join(" and ", Enumerable.Range(0, 300).Select(static i => "A = " + i % 10));
        string either  = string.Join(" or ", Enumerable.Range(0, 300).Select(static i => "A = " + i % 10));
        string nots    = string.Concat(Enumerable.Repeat("not ", 500)) + "A = 1";
        string words   = string.Join(' ', Enumerable.Range(0, FilterGuard.MaxWords).Select(static i => "w" + i));

        string mixed   = $"({string.Join(" or ", Enumerable.Range(0, 120).Select(static i => "B = " + i))}) and {string.Concat(Enumerable.Repeat("not ", 200))}C = 1 and {words}";

        foreach (string filter in (string[])[clauses, either, nots, words, mixed])
        {
            Assert.True(filter.Length <= FilterGuard.MaxFilterChars);
            var prepared = FilterGuard.Prepare(filter, policy, new TokenVault());
            Assert.Null(prepared.Error);
            string text = FilterGuard.Compose(prepared.Node, Payments);
            Assert.False(FilterGuard.TooDeep(text), $"written {text.Length} characters, nested past {FilterGuard.MaxNesting}");
            Assert.Equal(FilterGuard.Describe(FilterParser.Parse(text)), FilterGuard.Describe(FilterParser.Parse(FilterGuard.Write(FilterParser.Parse(text)))));
            // Compiled and run against an event: a filter this long is walked by recursion at
            // every step, and none of the steps may be where the stack ends.
            CompiledFilter.Compile(text).Matches(Event("w0 w1", new() { ["A"] = 1, ["B"] = 2, ["C"] = 1 }));
        }

        // The engine's own parser has no such limit: some two hundred nested parentheses are
        // more stack than a thread has, and that ends the process. Here they are counted first.
        Assert.Contains("nests parentheses", FilterGuard.Prepare(new string('(', 250) + "A = 1" + new string(')', 250), policy, new TokenVault()).Error);

        var tooMany = FilterGuard.Prepare(words + " onemore", policy, new TokenVault());
        Assert.Contains("plain words", tooMany.Error);
        Assert.Equal(RefusalKind.Input, tooMany.Kind);
    }

    [Fact]
    public void ATreeThatCannotBeWrittenAsItself_IsNotRun()
    {
        Assert.Throws<InvalidOperationException>(() => FilterGuard.Write(new CompareNode("Field", CompareOp.Eq, double.NaN)));
        Assert.Throws<InvalidOperationException>(() => FilterGuard.Write(new CompareNode("Field", CompareOp.Eq, new object())));
        Assert.Throws<InvalidOperationException>(() => FilterGuard.Write(new ToJsonCompareNode("Field", CompareOp.Eq, "x")));
        // A subscript that is not a whole number is not a path the grammar has.
        Assert.Throws<InvalidOperationException>(() => FilterGuard.Write(new HasNode("Items" + Sep + PropertyPath.IndexMarker + "0] or has(@mt")));
    }

    // ── The list of what a filter may be built from ───────────────────────────

    private static readonly HashSet<Type> Taught =
    [
        typeof(AndNode), typeof(OrNode), typeof(NotNode), typeof(CompareNode), typeof(InNode), typeof(HasNode), typeof(IsDefinedNode),
        typeof(LevelNode), typeof(LikeNode), typeof(StartsWithNode), typeof(ContainsNode), typeof(EndsWithNode), typeof(RegexMatchNode),
        typeof(LengthCompareNode), typeof(FreeTextNode),
    ];

    /// <summary>
    /// Every node type the query language has is tried on the guard, built with harmless
    /// arguments over a visible field. The ones it was taught pass; every other is refused —
    /// including any type added to the language after this was written, which is the point: a
    /// new function reads values by some road, and until somebody has decided which, an AI's
    /// filter does not get to use it.
    /// </summary>
    [Fact]
    public void ANodeTypeTheGuardWasNotTaught_IsRefused()
    {
        var policy = Standard();
        var types  = typeof(FilterNode).Assembly.GetTypes()
            .Where(static t => !t.IsAbstract && typeof(FilterNode).IsAssignableFrom(t))
            .ToList();
        Assert.True(types.Count > 40, "the language's node types were not found");
        Assert.Subset(new HashSet<Type>(types), Taught);

        foreach (var type in types)
        {
            var node     = Build(type);
            var prepared = FilterGuard.Judged(node, policy, new TokenVault());
            if (Taught.Contains(type))
            {
                Assert.True(prepared.Error is null, $"{type.Name}: {prepared.Error}");
                continue;
            }
            Assert.True(prepared.Node is null, $"{type.Name} was let through");
            Assert.Equal(RefusalKind.Input, prepared.Kind);
        }
    }

    private static FilterNode Build(Type type)
    {
        var ctor = type.GetConstructors().OrderByDescending(static c => c.GetParameters().Length).First();
        object?[] arguments = [.. ctor.GetParameters().Select(Argument)];
        return (FilterNode)ctor.Invoke(arguments);

        static object? Argument(ParameterInfo p)
        {
            Type t = p.ParameterType;
            if (t == typeof(string))     return p.Name is "timeZoneId" ? "UTC" : p.Name is "part" ? "hour" : "Visible";
            if (t == typeof(string[]))   return (string[])["word"];
            if (t == typeof(object))     return "value";
            if (t == typeof(object[]))   return (object?[])["value"];
            if (t == typeof(FilterNode)) return new HasNode("Visible");
            if (t == typeof(CompareOp))  return CompareOp.Eq;
            if (t == typeof(LogLevel))   return LogLevel.Error;
            if (t == typeof(IReadOnlyList<CoalesceArgNode>)) return (CoalesceArgNode[])[new CoalescePropertyArgNode("Visible"), new CoalesceLiteralArgNode("x")];
            if (t == typeof(IReadOnlyList<ConcatArgNode>))   return (ConcatArgNode[])[new ConcatPropertyArgNode("Visible"), new ConcatLiteralArgNode("x")];
            if (t.IsEnum)                return Enum.GetValues(t).GetValue(0);
            if (Nullable.GetUnderlyingType(t) is not null) return null;
            return t.IsValueType ? Activator.CreateInstance(t) : null;
        }
    }

    [Theory]
    [InlineData("toLower(Name) = 'x'")]
    [InlineData("toUpper(Name) = 'X'")]
    [InlineData("toNumber(Code) >= 4")]
    [InlineData("substring(Name, 0, 4) = '4111'")]
    [InlineData("indexOf(Name, '4') = 0")]
    [InlineData("replace(Name, '4', '5') = 'x'")]
    [InlineData("concat(Name, '-') = 'x'")]
    [InlineData("coalesce(Name, 'none') = 'none'")]
    [InlineData("typeOf(Name) = 'string'")]
    [InlineData("round(Elapsed, 1) > 2")]
    [InlineData("regexExtract(Path, '/v([0-9]+)/') = '2'")]
    [InlineData("fromBase64(Body) = 'x'")]
    public void AFunctionOutsideTheList_IsRefusedByName_OnAVisibleFieldToo(string filter)
    {
        var prepared = FilterGuard.Prepare(filter, Standard(), new TokenVault());
        Assert.Contains("is not available here", prepared.Error);
        Assert.Contains(filter[..filter.IndexOf('(')], prepared.Error, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(RefusalKind.Input, prepared.Kind);
    }

    [Fact]
    public void AFilter_IsOfALength_AndOfADepth()
    {
        var policy = Standard();
        string Nested(int depth) => new string('(', depth) + "A = 1" + new string(')', depth);

        Assert.Null(FilterGuard.Prepare(Nested(FilterGuard.MaxNesting), policy, new TokenVault()).Error);
        Assert.Contains("nests parentheses", FilterGuard.Prepare(Nested(FilterGuard.MaxNesting + 1), policy, new TokenVault()).Error);
        Assert.Contains("nests parentheses", FilterGuard.Prepare(Nested(1_900), policy, new TokenVault()).Error);
        // Parentheses in a string are text.
        Assert.Null(FilterGuard.Prepare($"A = '{new string('(', 200)}'", policy, new TokenVault()).Error);
        Assert.Null(FilterGuard.Prepare($"A = 'it''s {new string('(', 200)} \\' {new string('[', 200)}'", policy, new TokenVault()).Error);

        string longest = $"A = '{new string('x', FilterGuard.MaxFilterChars - 6)}'";
        Assert.Equal(FilterGuard.MaxFilterChars, longest.Length);
        Assert.Null(FilterGuard.Prepare(longest, policy, new TokenVault()).Error);
        Assert.Contains("at most", FilterGuard.Prepare("B = 1 or " + longest, policy, new TokenVault()).Error);
        Assert.Contains("at most", FilterGuard.PrepareAggregation("select count(*) where " + longest, policy, new TokenVault(), Payments).Error);
    }

    // ── Published tools ───────────────────────────────────────────────────────

    private static readonly McpPublishedTool OrdersOf = new()
    {
        Id = "t", Name = "orders_of", Description = "d",
        Filter = "Owner = {owner} and Region = {region}",
        Parameters = [new() { Name = "owner", Required = true }, new() { Name = "region", Required = true }],
    };

    [Fact]
    public void AnArgument_IsAValue_WhateverItHolds()
    {
        var (policy, vault, numeric, _) = WithTokens();
        Assert.Null(PublishedToolBinder.Validate(OrdersOf, []));

        // The pair that, with tokens replaced in the text, used to turn the tool into "every event".
        var binding = PublishedToolBinder.Bind(OrdersOf, new Dictionary<string, JsonElement>
        {
            ["owner"]  = JsonSerializer.SerializeToElement("x'" + numeric),
            ["region"] = JsonSerializer.SerializeToElement("or has(@mt) or CardNumber like '4111%' or C = 'c"),
        });
        Assert.Null(binding.Error);

        var prepared  = FilterGuard.Prepare(binding.Filter, policy, vault);
        var reference = FilterGuard.Prepare(binding.Reference, policy, vault);
        Assert.Null(prepared.Error);
        Assert.Equal(FilterGuard.Shape(reference.Node), FilterGuard.Shape(prepared.Node));
        Assert.Equal("and(cmp(5:Owner;Eq,?),cmp(6:Region;Eq,?))", FilterGuard.Shape(prepared.Node));

        var compiled = Run(prepared);
        Assert.False(compiled.Matches(Event("anything at all", new() { ["Owner"] = "somebody else", ["Region"] = "elsewhere" })));
        Assert.True(compiled.Matches(Event("x", new()
        {
            ["Owner"] = "x'" + numeric, ["Region"] = "or has(@mt) or CardNumber like '4111%' or C = 'c",
        })));

        // A token given whole as an argument is a token: the tool can be asked for "the same owner as before".
        var byToken = PublishedToolBinder.Bind(OrdersOf, Json($$"""{"owner":"{{numeric}}","region":"eu"}"""));
        var same    = FilterGuard.Prepare(byToken.Filter, policy, vault);
        Assert.Equal(FilterGuard.Shape(FilterGuard.Prepare(byToken.Reference, policy, vault).Node), FilterGuard.Shape(same.Node));
        Assert.True(Run(same).Matches(Event("x", new() { ["Owner"] = 6456116363L, ["Region"] = "eu" })));
    }

    [Fact]
    public void TheShapeOfAFilter_IsItsNodesPathsAndOperators_AndNoValue()
    {
        var policy = Standard();
        string Shape(string filter) => FilterGuard.Shape(FilterGuard.Prepare(filter, policy, new TokenVault()).Node);

        Assert.Equal(Shape("Owner = 'a' and Region = 'eu'"), Shape("Owner = 'somebody else' and Region = 'x'' y'"));
        Assert.Equal(Shape("Elapsed > 1"), Shape("Elapsed > 2.5"));
        Assert.Equal(Shape("Id in ['a', 'b']"), Shape("Id in [1, null]"));
        Assert.NotEqual(Shape("Owner = 'a' and Region = 'eu'"), Shape("Owner = 'a' or Region = 'eu'"));
        Assert.NotEqual(Shape("Owner = 'a'"), Shape("Owner != 'a'"));
        Assert.NotEqual(Shape("Owner = 'a'"), Shape("Other = 'a'"));
        Assert.NotEqual(Shape("Owner = 'a'"), Shape("Owner = Other"));
        Assert.NotEqual(Shape("Id in ['a']"), Shape("Id in ['a', 'b']"));
        Assert.NotEqual(Shape("contains(Name, 'a')"), Shape("ci_contains(Name, 'a')"));
        Assert.NotEqual(Shape("Owner = 'a'"), Shape("Owner = 'a' and has(@mt)"));
        Assert.Equal("", FilterGuard.Shape(null));
    }

    [Fact]
    public void AToolWhoseEveryClauseIsOptional_IsNotSaved_AndNotRunWithoutOne()
    {
        var tool = new McpPublishedTool
        {
            Id = "t", Name = "orders_of", Description = "d",
            Filter = "(Owner = {owner} or @l = 'Error')",
            Parameters = [new() { Name = "owner" }],
        };
        var problem = PublishedToolBinder.Validate(tool, []);
        Assert.Contains("no condition left", problem?.Error);
        Assert.Equal("filter", problem?.Field);

        // One saved before the check existed is not run as a search for everything either.
        var binding = PublishedToolBinder.Bind(tool, new Dictionary<string, JsonElement>());
        Assert.Contains("needs at least one of its parameters", binding.Error);
        Assert.Null(binding.Filter);
        Assert.NotNull(PublishedToolBinder.Bind(tool, Json("""{"owner":"alice"}""")).Filter);

        // Beside a condition that is always there, the optional alternative is what it reads as.
        Assert.Null(PublishedToolBinder.Validate(tool with { Filter = "@service = 'orders' and (Owner = {owner} or @l = 'Error')" }, []));
        // The advice no longer leads here.
        var topLevelOr = PublishedToolBinder.Validate(tool with { Filter = "Owner = {owner} or @l = 'Error'" }, []);
        Assert.Contains("beside a condition that is always there", topLevelOr?.Error);
    }

    [Theory]
    [InlineData("0.00001", "Elapsed > 0.00001", 0.5, true)]
    [InlineData("100000000000000000000", "Elapsed > 100000000000000000000.0", 1e21, true)]
    [InlineData("1e3", "Elapsed > 1000.0", 999d, false)]
    [InlineData("-2.50", "Elapsed > -2.5", -2d, true)]
    [InlineData("42", "Elapsed > 42", 43d, true)]
    public void ANumberArgument_IsWrittenAsDigits_NeverWithAnExponent(string value, string bound, double elapsed, bool matches)
    {
        var tool = new McpPublishedTool
        {
            Id = "t", Name = "slow", Description = "d", Filter = "Elapsed > {min}",
            Parameters = [new() { Name = "min", Type = McpToolParameter.Number, Required = true }],
        };
        var binding = PublishedToolBinder.Bind(tool, Json($$"""{"min":"{{value}}"}"""));
        Assert.Equal(bound, binding.Filter);
        Assert.Equal(matches, Run(FilterGuard.Prepare(binding.Filter, Standard(), new TokenVault())).Matches(Event("x", new() { ["Elapsed"] = elapsed })));

        Assert.Contains("takes a number", PublishedToolBinder.Bind(tool, Json("""{"min":"NaN"}""")).Error);
        Assert.Contains("takes a number", PublishedToolBinder.Bind(tool, Json("""{"min":"Infinity"}""")).Error);
    }

    [Fact]
    public void AToolsFilter_HasALength_AndADepth()
    {
        var tool = OrdersOf with { Filter = "Owner = {owner} and Region = {region} and Note = '" + new string('x', PublishedToolBinder.MaxTemplateChars) + "'" };
        Assert.Contains("at most", PublishedToolBinder.Validate(tool, [])?.Error);
        var deep = OrdersOf with { Filter = new string('(', 40) + "Owner = {owner} and Region = {region}" + new string(')', 40) };
        Assert.Contains("nests parentheses", PublishedToolBinder.Validate(deep, [])?.Error);
    }

    // ── Values that are hidden wherever they turn up ──────────────────────────

    [Fact]
    public void ANameThatHoldsAValue_GoesPastTheDetectors()
    {
        foreach (var mode in (McpMaskingMode[])[McpMaskingMode.Standard, McpMaskingMode.Strict])
        {
            var stats = new MaskStats();
            var masked = Policy(mode, new TokenVault(), Rule("Limits", MaskAction.Keep)).Mask(Event("x", new()
            {
                ["Limits"] = new Dictionary<string, object?> { ["a.serikov@example.com"] = 5, ["4111111111111111"] = "x", ["Daily"] = 3 },
                ["4111 1111 1111 1111"] = "top-level key",
            }), stats);

            string all = string.Join("|", masked.Properties.Select(static p => p.Key + "=" + p.Value));
            Assert.DoesNotContain("a.serikov@example.com", all);
            Assert.DoesNotContain("4111111111111111", all);
            Assert.DoesNotContain("4111 1111 1111 1111", all);
            Assert.Contains("\"Daily\":3", all);                         // an ordinary name is a name
            Assert.Equal(1, stats.ByDetector["email"]);
            Assert.Equal(2, stats.ByDetector["cardNumber"]);
        }
    }

    [Fact]
    public void AWholeNumberStoredAsADouble_IsTheNumberItIs()
    {
        var policy = Standard(Rule("MerchantId", MaskAction.Hash));
        var cells = Cells(policy.Mask(
            Event("x", new() { ["AsLong"] = 4111111111111111L, ["AsDouble"] = 4111111111111111d, ["Ratio"] = 0.25, ["Count"] = 3d }), new MaskStats()));
        Assert.StartsWith("h:", cells["AsLong"]);
        Assert.Equal(cells["AsLong"], cells["AsDouble"]);              // one value, one token
        Assert.Equal("0.25", cells["Ratio"]);
        Assert.Equal("3", cells["Count"]);

        string asLong   = Cells(policy.Mask(Event("x", new() { ["MerchantId"] = 6456116363L }), new MaskStats()))["MerchantId"];
        string asDouble = Cells(policy.Mask(Event("x", new() { ["MerchantId"] = 6456116363d }), new MaskStats()))["MerchantId"];
        Assert.Equal(asLong, asDouble);
    }

    [Fact]
    public void ANumberAnAggregateCameTo_IsHiddenLikeTheValueItCanBe()
    {
        var policy = Standard(Rule("Total", MaskAction.Keep));
        var stats  = new MaskStats();

        Assert.Equal("42", policy.MaskNumber("Elapsed", 42, stats));
        Assert.Equal("12.5", policy.MaskNumber("Elapsed", 12.5, stats));
        Assert.Equal("-7", policy.MaskNumber("Delta", -7, stats));
        Assert.Equal(0, stats.Total);

        // max(ClientIin) is somebody's IIN; min(Pan) is a card number.
        Assert.Matches("^h:[0-9a-f]{12}$", policy.MaskNumber("ClientIin", 900101300126, stats));
        Assert.Matches("^h:[0-9a-f]{12}$", policy.MaskNumber("Pan", 4111111111111111, stats));
        Assert.Equal(1, stats.ByDetector["iin"]);
        Assert.Equal(1, stats.ByDetector["cardNumber"]);
        // The token is the one a search gives for the same value.
        Assert.Equal(policy.TokenOfText("4111111111111111", isNumber: true), policy.MaskNumber("Pan", 4111111111111111, new MaskStats()));

        // Past what a double holds exactly its digits are not the value's own — most of a long
        // identifier, shown as a measurement. Not shown.
        string tooLong = policy.MaskNumber("Pan", 6011000990139424123d, stats);
        Assert.DoesNotContain("6011", tooLong);
        Assert.Contains("hidden", tooLong);

        // A field kept in every service is sent as it is.
        Assert.Equal("4111111111111111", policy.MaskNumber("Total", 4111111111111111, new MaskStats()));
    }

    // ── Times ─────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("-999999999999999999999d")]
    [InlineData("-NaNd")]
    [InlineData("-Infinityh")]
    [InlineData("-∞d")]
    [InlineData("-1e9d")]
    [InlineData("-4000d")]
    [InlineData("- 5m")]
    [InlineData("-5x")]
    [InlineData("0001-01-01T00:00:00Z")]
    [InlineData("9999-12-31T23:59:59Z")]
    public void ATimeThatIsNotOne_IsRefused_NotThrown(string text)
    {
        var now = new DateTimeOffset(2026, 11, 16, 9, 0, 0, TimeSpan.Zero);
        foreach (var (from, to) in ((string?, string?)[])[(text, null), (null, text)])
        {
            Assert.False(TimeWindow.TryResolve(from, to, now, TimeSpan.FromDays(366), TimeWindow.Default, out _, out _, out _, out string? error));
            Assert.Contains("not a time", error);
        }
    }

    [Fact]
    public void ARefusalQuotesTheBeginningOfAnArgument_NotAllOfIt()
    {
        var now = new DateTimeOffset(2026, 11, 16, 9, 0, 0, TimeSpan.Zero);
        Assert.False(TimeWindow.TryResolve(new string('z', 20_000), null, now, TimeSpan.FromDays(1), TimeWindow.Default, out _, out _, out _, out string? error));
        Assert.True(error!.Length < 200);
    }

    // ── The record of calls ───────────────────────────────────────────────────

    [Fact]
    public void ACallOverTheMinute_IsCounted_AndOnlyEveryDoublingIsWrittenDown()
    {
        var limiter = new McpCallLimiter();
        long now = 1_800_000_000_000;
        Assert.True(limiter.TryCount("p", 2, now, out _, out int refused));
        Assert.Equal(0, refused);
        Assert.True(limiter.TryCount("p", 2, now + 1, out _, out _));

        var written = new List<int>();
        for (int i = 1; i <= 100; i++)
        {
            Assert.False(limiter.TryCount("p", 2, now + 1 + i, out int retryAfter, out refused));
            Assert.Equal(i, refused);
            Assert.InRange(retryAfter, 1, 61);
            if (McpAudit.Keeps(refused)) written.Add(refused);
        }
        Assert.Equal(new[] { 1, 2, 4, 8, 16, 32, 64 }, written);

        // Another profile has its own minute, and the next minute starts again.
        Assert.True(limiter.TryCount("q", 2, now, out _, out _));
        Assert.True(limiter.TryCount("p", 2, now - now % 60_000 + 60_000, out _, out refused));
        Assert.Equal(0, refused);
        Assert.True(McpAudit.Keeps(0));                                 // a call that was not such a refusal is always kept
    }

    [Theory]
    [InlineData("ameto-mcp", "ingested:ameto-mcp")]
    [InlineData("AMETO-MCP", "ingested:ameto-mcp")]
    [InlineData("Ameto-Mcp", "ingested:ameto-mcp")]
    [InlineData("ameto-mcp ", "ameto-mcp ")]
    [InlineData("ameto-mcp2", "ameto-mcp2")]
    [InlineData("ameto_mcp", "ameto_mcp")]
    [InlineData("payments-api", "payments-api")]
    [InlineData("", "")]
    public void TheServersOwnServiceName_IsNotOneAnIngestedEventCanCarry(string claimed, string filedUnder) =>
        Assert.Equal(filedUnder, Encoding.UTF8.GetString(ReservedServices.ForIngestion(Encoding.UTF8.GetBytes(claimed))));
}
