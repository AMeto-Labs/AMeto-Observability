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

        // And which stored values an aggregate takes is decided here, not left to the engine.
        Assert.NotNull(query.Admits);
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

    /// <summary>
    /// A built-in field holds no hidden value, and every answer shows it as it is — the service
    /// and level in a group's header line, the exception's type in an event's detail. A token
    /// compared with one could only find the hidden value lying there in the open, in a place
    /// the answer cannot leave out; so it is refused, under every spelling of the field.
    /// </summary>
    [Theory]
    [InlineData("@service = '{t}'")]
    [InlineData("service.name != '{t}'")]
    [InlineData("@l = '{t}'")]
    [InlineData("@mt = '{t}'")]
    [InlineData("@x.type = '{t}'")]
    [InlineData("Exception = '{t}'")]
    [InlineData("@x.message = '{t}'")]
    [InlineData("@tr = '{n}'")]
    [InlineData("@service in ['payments-api', '{t}']")]
    [InlineData("A = 1 and not (@x.stack in ['{n}'])")]
    public void AToken_IsNotComparedWithABuiltInField(string filter)
    {
        var (policy, vault, numeric, text) = WithTokens();
        var prepared = FilterGuard.Prepare(filter.Replace("{n}", numeric).Replace("{t}", text), policy, vault);

        Assert.Contains("is a built-in field", prepared.Error);
        Assert.Equal(RefusalKind.Masking, prepared.Kind);
        Assert.Null(prepared.Node);
        Assert.DoesNotContain("4111111111111111", prepared.Error);
        // The same comparisons with a value of one's own are ordinary filters.
        Assert.Null(FilterGuard.Prepare("@service = 'payments-api' and @x.type = 'System.TimeoutException'", policy, vault).Error);
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

    /// <summary>
    /// What the test above samples, held as a property: whatever segment the ENGINE reads an
    /// element of a list by, the guard judges as a path to that element. The second review
    /// found the spelling the first fix had not thought of — <c>Items[' 0']</c>, because the
    /// engine reads a position with <c>int.TryParse</c> and that takes white space, a sign, a
    /// trailing NUL. A list of spellings is only as good as the imagination behind it; this
    /// asks the engine itself, about several thousand of them.
    /// </summary>
    [Fact]
    public void WhateverTheEngineReadsAsAPosition_TheGuardJudgesAsOne()
    {
        var vault  = new TokenVault();
        var policy = Policy(McpMaskingMode.Standard, vault, Rule("Basket.Sku", MaskAction.Hash), Rule("Basket.Price", MaskAction.Hash));
        var ev = Event("x", new()
        {
            ["Basket"] = new List<object?>
            {
                new Dictionary<string, object?> { ["Sku"] = "ZX-SECRET-41", ["Price"] = 1250 },
                new Dictionary<string, object?> { ["Sku"] = "QQ-OTHER-7", ["Price"] = 90 },
            },
        });

        // The spellings the review tried, then twenty thousand made of the characters that
        // could matter: white space of every kind, signs, digits of three scripts, a NUL.
        var segments = new List<string>
        {
            "0", "00", "000", " 0", "0 ", "  0  ", "+0", "-0", " +0 ", "\t0", "0\t", "\n0", "0\n", "\r0", "\v0", "\f0", "0\0", "\00",
            " 0", "0 ", "​0", "٠", "０", "0.0", "0,0", "1e0", "0x0", "(0)", "0L", "+", "-", "", "1", " 1", "+1", "1\0", "2", "-1",
        };
        char[] alphabet = [' ', '\t', '\n', '\r', '\v', '\f', '\0', '+', '-', '0', '1', '2', '9', '.', ',', 'x', 'e', '_', ' ', ' ', '​', '٠', '０'];
        var random = new Random(162);
        for (int i = 0; i < 20_000; i++)
        {
            var made = new char[random.Next(1, 5)];
            for (int c = 0; c < made.Length; c++) made[c] = alphabet[random.Next(alphabet.Length)];
            segments.Add(new string(made));
        }

        int reached = 0;
        foreach (string segment in segments)
        {
            string path = "Basket" + Sep + segment + Sep + "Sku";
            if (FilterEvaluator.ReadProperty(ev, path) is not string sku) continue;
            reached++;
            string spelled = string.Concat(segment.Select(static c => c is < ' ' or > '~' ? $"\\u{(int)c:X4}" : c.ToString()));
            Assert.True(FilterGuard.IsHidden(policy, path), $"'{spelled}' reads an element of the list and is not judged as one");
            Assert.DoesNotContain(sku, policy.MaskGroupKey(FilterGuard.RulePath(path), sku, new MaskStats()));
        }
        // Not an empty promise: the engine does read elements by a great many of these.
        Assert.True(reached > 500, $"only {reached} spellings reached an element");

        // And through the front door, as a filter and as a query.
        foreach (string position in (string[])["[0]", ".0", "[' 0']", "['0 ']", "['+0']", "['-0']", "['\t0']", "[' +1 ']"])
        {
            Assert.Contains("hidden field", FilterGuard.Prepare($"Basket{position}.Sku like 'ZX%'", policy, vault).Error);
            Assert.Contains("hidden field", FilterGuard.PrepareAggregation($"select max(Basket{position}.Price)", policy, vault, Payments).Error);
        }
    }

    /// <summary>
    /// A token is a question about where a hidden value is, and the events it finds are read again
    /// by every call after it. Compared with a field that shows its values as stored, it would
    /// find the value lying in the open — and a sum with a count beside it, the next logs_context,
    /// an ordering of the same open field would each read it there (the third review did all
    /// three). So a token meets only a field hidden whole in every service; or, for a value a
    /// detector hides by its shape, any field no keep or keepLast4 rule shows.
    /// </summary>
    [Fact]
    public void AToken_IsComparedOnlyWhereItsValueIsHiddenEverywhere()
    {
        var vault  = new TokenVault();
        var policy = Policy(McpMaskingMode.Standard, vault,
            Rule("PatientAge", MaskAction.Hash), Rule("Buyer", MaskAction.Hash), Rule("Reference", MaskAction.Keep),
            Rule("Region", MaskAction.Hash, "payments-api"), Rule("Contact", MaskAction.Keep, "support"), Rule("Customer", MaskAction.Remove),
            Rule("Masked", MaskAction.KeepLast4), Rule("Account", MaskAction.Hash), Rule("Account", MaskAction.KeepLast4, "support"),
            Rule("Comment", MaskAction.Hash));
        var hidden = Cells(policy.Mask(Event("x", new()
        {
            ["PatientAge"] = 7391, ["Buyer"] = "Ivan Petrov", ["Region"] = "north", ["Note"] = "card 4111111111111111",
            ["Pan"] = "4111111111111111", ["Comment"] = "card 4111111111111111 failed",
            ["Iban"] = "GB82WEST12345698765432", ["Mail"] = "Ivan.Petrov@Example.com",
            ["Phone"] = "+7 701 555 01 23", ["YoungIin"] = "050312501234", ["Iin"] = "900101300126",
        }), new MaskStats()));
        string age = hidden["PatientAge"], buyer = hidden["Buyer"], region = hidden["Region"], comment = hidden["Comment"];
        string card = hidden["Pan"], iban = hidden["Iban"], mail = hidden["Mail"], phone = hidden["Phone"];
        string youngIin = hidden["YoungIin"], iin = hidden["Iin"];
        foreach (string token in (string[])[card, iban, mail, phone, youngIin, iin])
            Assert.Matches("^h:[0-9a-f]{12}$", token);                  // hidden by a detector, in a field no rule names

        // Fields that show their values as stored somewhere: refused, whatever the operator of equality.
        foreach (string filter in (string[])
                 [
                     $"Attempt = '{age}'", $"Attempt != '{age}'", $"Attempt in ['{age}', 'x']", $"Owner = '{buyer}'",
                     $"Reference = '{buyer}'",                              // kept by a rule: shown as stored
                     $"Region = '{region}'",                                // hidden in one service only, open in the others
                     $"Tags = '{buyer}'", $"Items[0].Name = '{buyer}'",
                     $"Contact = '{card}'",                                 // a keep for one service lets even a card through there
                     $"Masked = '{buyer}'", $"Masked = '{card}'",           // the last four characters: the whole of a short value
                     $"OtherNote = '{comment}'",                            // a detector would hide the card in it, and show "failed"
                     $"Iban = '{iban}'", $"OtherIban = '{iban}'",           // matched in small letters too, where no detector knows it
                     // A text that reads as a number equals that number in a numeric field, and
                     // 77015550123 or 50312501234 is shown there as it is: no detector knows it.
                     $"Phone = '{phone}'", $"Msisdn = '{phone}'", $"YoungIin = '{youngIin}'", $"ClientNo = '{youngIin}'",
                 ])
        {
            var refused = FilterGuard.Prepare(filter, policy, vault);
            Assert.Contains("shows its values as they are stored", refused.Error);
            Assert.Equal(RefusalKind.Masking, refused.Kind);
            Assert.DoesNotContain("7391", refused.Error);
        }

        // Fields hidden in every event of every service: the question is answered.
        foreach (string filter in (string[])
                 [
                     $"PatientAge = '{age}'", $"Buyer = '{buyer}'", $"Buyer.Name = '{buyer}'",   // under a rule on its parent
                     $"Customer.Iin != '{age}'",                                                // removed: hidden all the same
                     $"Pan = '{card}'", $"OtherNote = '{card}'", $"Items[0].Pan in ['{card}']", // a value the detector hides wherever it lies
                     $"Account = '{buyer}'",                                // hashed in every service: the hash wins over a keepLast4 anywhere
                     $"Comment = '{comment}'",
                     $"Mail = '{mail}'", $"ReplyTo = '{mail}'",             // an address is found in any case of its letters
                     $"Iin = '{iin}'", $"ClientNo = '{iin}'",               // as a number too, it is an IIN by its digits
                 ])
            Assert.Null(FilterGuard.Prepare(filter, policy, vault).Error);

        // What logs_fields says of each: "= only" is where a token may go.
        Assert.True(policy.HidesEverywhere("PatientAge"));
        Assert.True(policy.HidesEverywhere("Account"));
        foreach (string shown in (string[])["Region", "Masked", "Contact", "Reference", "Attempt"])
            Assert.False(policy.HidesEverywhere(shown), shown);
        Assert.True(policy.HidesField("Region") && policy.HidesField("Masked"));    // "= only, no token"

        // Strict: every field no rule shows is hidden everywhere, so a token meets it.
        var strict = Policy(McpMaskingMode.Strict, vault, Rule("PatientAge", MaskAction.Hash), Rule("Reference", MaskAction.Keep),
                            Rule("Contact", MaskAction.Keep, "support"), Rule("Masked", MaskAction.KeepLast4));
        Assert.Null(FilterGuard.Prepare($"Attempt = '{age}'", strict, vault).Error);
        foreach (string field in (string[])["Reference", "Contact", "Masked"])
            Assert.Contains("shows its values as they are stored", FilterGuard.Prepare($"{field} = '{age}'", strict, vault).Error);
    }

    // ── The answer and the guard read a path the same way ─────────────────────

    /// <summary>
    /// A rule on a path covers whatever is spelled under it, whether a dot stands between an
    /// object and its property or inside one name. The fourth review found the two apart: under
    /// a rule on <c>client</c> the OTLP attribute named <c>client.id</c> — one property, with a
    /// dot in its name — went out as stored, while a filter on it was judged as one on a hidden
    /// field, and a token compared with it read its value in one call.
    /// </summary>
    [Fact]
    public void ARuleOnAPath_CoversWhateverIsSpelledUnderIt()
    {
        var vault  = new TokenVault();
        var policy = Policy(McpMaskingMode.Standard, vault,
            Rule("client", MaskAction.Hash), Rule("A.B", MaskAction.Hash), Rule("Card", MaskAction.KeepLast4),
            Rule("Session", MaskAction.Remove), Rule("region", MaskAction.Hash, "payments-api"));

        var cells = Cells(policy.Mask(Event("x", new()
        {
            ["client.id"]     = "C-77-0412",                                                   // an OTLP attribute: one name
            ["client"]        = new Dictionary<string, object?> { ["id"] = "C-77-0412" },      // an object and its property
            ["A"]             = new Dictionary<string, object?> { ["B.C"] = "V-31337", ["Open"] = "seen" },
            ["Card"]          = "4111111111111111",
            ["Card.Pan"]      = "4111111111111111",
            ["Session.Token"] = "s3cr3t",
            ["region.code"]   = "north",
        }), new MaskStats()));

        Assert.Matches("^h:[0-9a-f]{12}$", cells["client.id"]);
        Assert.Matches("^h:[0-9a-f]{12}$", cells["client"]);
        Assert.Matches("""^\{"B\.C":"h:[0-9a-f]{12}","Open":"seen"\}$""", cells["A"]);
        Assert.Equal("****1111", cells["Card"]);                    // the value at the rule's own path: its last four
        Assert.Matches("^h:[0-9a-f]{12}$", cells["Card.Pan"]);      // under it: hidden whole, as Card: { Pan } is
        Assert.False(cells.ContainsKey("Session.Token"));
        Assert.Matches("^h:[0-9a-f]{12}$", cells["region.code"]);

        // In a service the rule does not name, the attribute is as open as the object would be.
        Assert.Equal("north", Cells(policy.Mask(Event("x", new() { ["region.code"] = "north" }, service: "support"), new MaskStats()))["region.code"]);

        // The guard says of the path what the answer does with it — and a table shows no more.
        Assert.True(policy.HidesField("client.id") && policy.HidesEverywhere("client.id"));
        Assert.True(policy.HidesField("region.code") && !policy.HidesEverywhere("region.code"));
        Assert.Matches("^h:[0-9a-f]{12}$", policy.MaskGroupKey("Card.Pan", "4111111111111111", new MaskStats())!);
        Assert.Equal("****1111", policy.MaskGroupKey("Card", "4111111111111111", new MaskStats()));

        // A keep reaches under its path too: in Strict it lets through what is spelled there, to
        // the answer and to the guard alike — not hidden in one and free to order in the other.
        // Let through, not vouched for (AKeepAboveAPath_…).
        var strict = Policy(McpMaskingMode.Strict, vault, Rule("client", MaskAction.Keep));
        var kept = Cells(strict.Mask(Event("x", new() { ["client.id"] = "C-77-0412", ["other.id"] = "C-77-0412" }), new MaskStats()));
        Assert.Equal("C-77-0412", kept["client.id"]);
        Assert.Matches("^h:[0-9a-f]{12}$", kept["other.id"]);
        Assert.True(!strict.HidesField("client.id") && !strict.KeepsEverywhere("client.id"));
        Assert.True(strict.HidesField("other.id"));
    }

    /// <summary>
    /// A <c>keep</c> vouches for the value at its own path, and for no other. What is spelled
    /// under that path it lets through Strict mode — and the detectors go on looking through
    /// it, in an object and in a name with dots alike.
    ///
    /// <para>The fifth review. After the fourth round a <c>keep</c> reached under its path as a
    /// <c>hash</c> does, and took the detectors off everything there; and since a pattern
    /// without a dot is tried against the last segment at every level, a <c>keep</c> on
    /// <c>card</c> — written for the property that holds a card's brand — sent the attribute
    /// <c>payment.card.number</c> out as stored, in Standard mode as well.</para>
    /// </summary>
    [Fact]
    public void AKeepAboveAPath_LetsItThroughStrictMode_AndLeavesTheDetectorsOn()
    {
        const string card = "4111111111111111", mail = "a.serikov@example.com";
        foreach (var mode in (McpMaskingMode[])[McpMaskingMode.Standard, McpMaskingMode.Strict])
        {
            var vault  = new TokenVault();
            var policy = Policy(mode, vault,
                Rule("card", MaskAction.Keep), Rule("user", MaskAction.Keep), Rule("user.email", MaskAction.Keep), Rule("url.*", MaskAction.Keep));
            var cells = Cells(policy.Mask(Event("x", new()
            {
                ["Card"]                = "VISA",                 // the rule's own path
                ["payment.card.number"] = card,                   // under it, by a part of a name with dots
                ["payment.card.holder"] = "A. SERIKOV",
                ["Payment"]             = new Dictionary<string, object?>
                {
                    ["Card"] = new Dictionary<string, object?> { ["Number"] = card, ["Digits"] = 4111111111111111L, ["Brand"] = "VISA" },
                },
                ["user.id"]             = "u-1042",
                ["user.email"]          = mail,                   // named by a keep of its own
                ["user.contact"]        = mail,
                ["url.query"]           = $"card={card}",         // url.* names everything under url
                ["Other"]               = "n-7",
            }), new MaskStats()));

            Assert.Equal("VISA", cells["Card"]);
            Assert.Matches("^h:[0-9a-f]{12}$", cells["payment.card.number"]);
            Assert.Equal("A. SERIKOV", cells["payment.card.holder"]);      // let through, in Strict too; no detector knows a name
            // An object reads as the name with dots does: the card under Card, as a text and as a number.
            Assert.Matches("""^\{"Card":\{"Number":"(h:[0-9a-f]{12})","Digits":"\1","Brand":"VISA"\}\}$""", cells["Payment"]);
            Assert.Equal("u-1042", cells["user.id"]);
            Assert.Equal(mail, cells["user.email"]);                       // of two keeps the one on the path itself decides
            Assert.Matches("^h:[0-9a-f]{12}$", cells["user.contact"]);
            Assert.Equal($"card={card}", cells["url.query"]);
            if (mode == McpMaskingMode.Strict) Assert.Matches("^h:[0-9a-f]{12}$", cells["Other"]);
            else                               Assert.Equal("n-7", cells["Other"]);

            // The guard says of each path what the answer does there. Under a keep a field is
            // one a filter may order and match — and not one whose every value is shown: a
            // table's key and the number an aggregate comes to go past the detectors, and an
            // aggregate leaves a card number out.
            foreach (string under in (string[])["payment.card.number", "Payment.Card.Number", "user.contact", "user.id"])
                Assert.True(!policy.HidesField(under) && !policy.KeepsEverywhere(under), under);
            foreach (string named in (string[])["Card", "Payment.Card", "user.email", "url.query", "url.a.b"])
                Assert.True(policy.KeepsEverywhere(named), named);
            Assert.Matches("^h:[0-9a-f]{12}$", policy.MaskGroupKey("payment.card.number", card, new MaskStats())!);
            Assert.Matches("^h:[0-9a-f]{12}$", policy.MaskNumber("payment.card.number", 4111111111111111, new MaskStats()));
            Assert.Equal(card, policy.MaskGroupKey("url.query", card, new MaskStats()));
            var sums = FilterGuard.PrepareAggregation("select sum(payment.card.number), sum(url.query)", policy, vault, Payments).Query!;
            Assert.False(sums.Admits!(sums.Aggregates[0], 4111111111111111L));
            Assert.True(sums.Admits!(sums.Aggregates[1], 4111111111111111L));

            // A token of the card is compared where a detector hides the card — under the keep,
            // as in a field no rule names (Note) — and not where a keep shows it.
            string token = cells["payment.card.number"];
            foreach (string under in (string[])["payment.card.number", "Payment.Card.Number", "Payment.Card.Digits", "Note"])
                Assert.Null(FilterGuard.Prepare($"{under} = '{token}'", policy, vault).Error);
            foreach (string shown in (string[])["url.query", "user.email", "Card"])
                Assert.Contains("shows its values as they are stored", FilterGuard.Prepare($"{shown} = '{token}'", policy, vault).Error);
        }
    }

    /// <summary>
    /// A table's key is cut to its last four characters only where every event its row can be
    /// made of shows as much. Under a <c>keepLast4</c> for one service the others leave the
    /// value to the detectors, and a card number they hide whole: <c>group by Pan</c> gave its
    /// last four digits all the same.
    /// </summary>
    [Fact]
    public void ATablesKey_IsNotCutWhereASearchElsewhereHidesItWhole()
    {
        const string card = "4111111111111111";
        var one = Standard(Rule("Pan", MaskAction.KeepLast4, "payments-api"));
        Assert.Equal("****1111", Cells(one.Mask(Event("x", new() { ["Pan"] = card }), new MaskStats()))["Pan"]);
        Assert.Matches("^h:[0-9a-f]{12}$", Cells(one.Mask(Event("x", new() { ["Pan"] = card }, service: "support"), new MaskStats()))["Pan"]);
        Assert.Matches("^h:[0-9a-f]{12}$", one.MaskGroupKey("Pan", card, new MaskStats())!);
        // What no detector knows the other services show whole: its last four are the least any of them shows.
        Assert.Equal("****-042", one.MaskGroupKey("Pan", "ORDER-042", new MaskStats()));

        // The rule for every service: every event shows the last four, and so does the table.
        Assert.Equal("****1111", Standard(Rule("Pan", MaskAction.KeepLast4)).MaskGroupKey("Pan", card, new MaskStats()));

        // Another reading of the path leaves it to the detectors as another service does:
        // Items.0.Pan is the Pan of the first item, or of a property named "0".
        var list = Standard(Rule("Items.Pan", MaskAction.KeepLast4));
        Assert.Equal("****1111", list.MaskGroupKey("Items.Pan", card, new MaskStats()));
        Assert.Matches("^h:[0-9a-f]{12}$", list.MaskGroupKey("Items.0.Pan", card, new MaskStats())!);
    }

    /// <summary>
    /// A <c>keepLast4</c> on a list cuts each of its elements: the rules name a list's elements
    /// by the list's path, and so does a table — <c>group by SavedCards[0]</c> is judged by
    /// <c>SavedCards</c>. While the answer hashed the list whole, that table showed the last four
    /// characters of every element a search showed none of (the sixth review). A true or a
    /// false is cut as the text it is written with, which is what the engine makes a key of:
    /// hashed in the answer, it was <c>****alse</c> in a table — which names the value, so a
    /// value shorter than eight characters is now the mark alone, in the answer and in a key.
    /// </summary>
    [Fact]
    public void AKeepLast4OnAList_CutsEachElement_AndATableOfThemShowsNoMore()
    {
        static string Path(string spelling) => ((CompareNode)FilterParser.Parse($"{spelling} = 'x'")).Property;

        foreach (var mode in (McpMaskingMode[])[McpMaskingMode.Standard, McpMaskingMode.Strict])
        {
            var policy = Policy(mode, new TokenVault(), Rule("SavedCards", MaskAction.KeepLast4), Rule("Flag", MaskAction.KeepLast4));
            string Shown(string name, object? value) => Cells(policy.Mask(Event("x", new() { [name] = value }), new MaskStats()))[name];

            Assert.Equal("""["****1111","****0004"]""", Shown("SavedCards", new List<object?> { "4111111111111111", "5500000000000004" }));
            Assert.Equal("""["****6363","****"]""",     Shown("SavedCards", new List<object?> { 6456116363L, 77L }));
            Assert.Equal("""[["****KWXY"],null,"****"]""", Shown("SavedCards", new List<object?> { new List<object?> { "ZQ-77-KWXY" }, null, false }));
            Assert.Equal("[]", Shown("SavedCards", new List<object?>()));
            // A map has no last four characters: one token for it, in its place in the list.
            Assert.Matches("""^\["h:[0-9a-f]{12}","\*\*\*\*1111"\]$""",
                Shown("SavedCards", new List<object?> { new Dictionary<string, object?> { ["n"] = "4111111111111111" }, "4111111111111111" }));
            // And not a list at all: as before.
            Assert.Equal("****1111", Shown("SavedCards", "4111111111111111"));

            // A table's key of an element is what a search shows of that element — the engine's own reading of it.
            var ev = Event("x", new() { ["SavedCards"] = new List<object?> { "4111111111111111", new List<object?> { "x", "5500000000000004" } } });
            foreach (var (spelling, cut) in ((string, string)[])[("SavedCards[0]", "****1111"), ("SavedCards[1][1]", "****0004")])
            {
                string path = Path(spelling);
                string read = Assert.IsType<string>(FilterEvaluator.ReadProperty(ev, path));
                Assert.Equal(cut, policy.MaskGroupKey(FilterGuard.RulePath(path), read, new MaskStats(), Payments.Services));
                Assert.Contains($"\"{cut}\"", Cells(policy.Mask(ev, new MaskStats()))["SavedCards"]);
            }

            // A true and a false: their text, cut — and too short for four characters to be a part of it.
            foreach (bool flag in (bool[])[true, false])
            {
                Assert.Equal("****", Shown("Flag", flag));
                Assert.Equal("****", policy.MaskGroupKey("Flag", flag ? "true" : "false", new MaskStats(), Payments.Services));
            }

            // Four characters are shown of a value of eight or more: of a shorter one they are
            // the value or most of it. The same cut in the answer and in a table's key.
            Assert.Equal(8, MaskingPolicy.ShortestCut);
            foreach (var (value, cut) in ((object, string)[])[("1234567", "****"), ("12345678", "****5678"), (123456L, "****"), (12345678L, "****5678"), ("", "****")])
            {
                Assert.Equal(cut, Shown("Flag", value));
                Assert.Equal(cut, policy.MaskGroupKey("Flag", Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture), new MaskStats(), Payments.Services));
            }
        }

        // A list under an object: cut in its place, the object's other properties as they were.
        var nested = Standard(Rule("Customer.Cards", MaskAction.KeepLast4));
        var customer = new Dictionary<string, object?> { ["Cards"] = new List<object?> { "4111111111111111", 5500000000000004L }, ["Tier"] = "gold" };
        Assert.Equal("""{"Cards":["****1111","****0004"],"Tier":"gold"}""", Cells(nested.Mask(Event("x", new() { ["Customer"] = customer }), new MaskStats()))["Customer"]);
    }

    /// <summary>
    /// Strict mode lets booleans pass, and an equality with the text of one finds them. So the
    /// token of a true or a false — a table gives one, for <c>group by</c> a field that is
    /// hidden — compared with a field Strict mode alone hides would be read off the first event
    /// it found. It is compared with nothing. (Found by putting a false among the values of
    /// the property test below.)
    /// </summary>
    [Fact]
    public void ATokenOfATrueOrAFalse_StandsAgainstNothing()
    {
        var vault  = new TokenVault();
        var policy = Policy(McpMaskingMode.Strict, vault, Rule("IsVip", MaskAction.Hash));

        // A table's key under the rule: a token, and one the vault knows.
        string token = policy.MaskGroupKey("IsVip", "false", new MaskStats())!;
        Assert.Matches("^h:[0-9a-f]{12}$", token);
        Assert.True(MaskToken.TryParse(token, out ulong bits) && vault.TryResolve(bits, out _));

        // A boolean no rule names goes out as it is, in Strict mode too; the one under the rule as that token.
        var cells = Cells(policy.Mask(Event("x", new() { ["Retried"] = false, ["IsVip"] = false, ["Note"] = "n" }), new MaskStats()));
        Assert.Equal("false", cells["Retried"]);
        Assert.Equal(token, cells["IsVip"]);
        Assert.StartsWith("h:", cells["Note"]);

        // So it is compared with nothing — not with what Strict mode hides, not with what the rule does.
        foreach (string filter in (string[])[$"Retried = '{token}'", $"IsVip = '{token}'", $"Retried in ['{token}', 'x']", $"Retried != '{token}'", $"Note = '{token}'"])
            Assert.NotNull(FilterGuard.Prepare(filter, policy, vault, Payments.Services).Error);

        // In any case of its letters, as the engine's equality takes it; and nothing else is caught by it.
        Assert.False(policy.TokenMayMeet("IsVip", "TRUE",  isNumber: false));
        Assert.False(policy.TokenMayMeet("IsVip", "False", isNumber: false));
        Assert.True(policy.TokenMayMeet("IsVip", "falsehood", isNumber: false));
        Assert.True(policy.TokenMayMeet("IsVip", "1", isNumber: true));
    }

    /// <summary>
    /// The path the guard judges is the path the answer judges: every name in its place, an
    /// empty one too. <c>[''].Pin</c> was <c>Pin</c> to the guard and <c>.Pin</c> to the answer,
    /// and under a rule on <c>*.Pin</c> a filter and a table read what the answer hid (the
    /// fifth review).
    /// </summary>
    [Fact]
    public void AnEmptyName_KeepsItsPlaceInThePathTheGuardJudges()
    {
        static string Named(string spelling) => FilterGuard.RulePath(((CompareNode)FilterParser.Parse($"{spelling} = 'x'")).Property);

        Assert.Equal(".Pin",     Named("[''].Pin"));
        Assert.Equal(".Pin",     Named("['']['Pin']"));
        Assert.Equal(".Pin",     Named("[''][0].Pin"));
        Assert.Equal("..Pin",    Named("[''][''].Pin"));
        Assert.Equal("A..Pin",   Named("A[''].Pin"));
        Assert.Equal("A.Pin.",   Named("A.Pin['']"));
        Assert.Equal("",         Named("['']"));
        Assert.Equal(".Pin",     Named("['.Pin']"));
        Assert.Equal("Items.Sku", Named("Items[0].Sku"));
        Assert.Equal("Items.0.Sku", Named("Items.0.Sku"));
        // One segment is a name as it is stored, whatever it begins with: the engine looks it up as it is.
        Assert.Equal("\u00020", FilterGuard.RulePath("\u00020"));

        var vault  = new TokenVault();
        var policy = Policy(McpMaskingMode.Standard, vault, Rule("*.Pin", MaskAction.Hash));
        var cells  = Cells(policy.Mask(Event("x", new()
        {
            [""]     = new Dictionary<string, object?> { ["Pin"] = "7316" },
            [".Pin"] = "7316",
            ["Pin"]  = "7316",                                              // not under anything: the rule does not name it
        }), new MaskStats()));
        Assert.Matches("""^\{"Pin":"h:[0-9a-f]{12}"\}$""", cells[""]);
        Assert.Matches("^h:[0-9a-f]{12}$", cells[".Pin"]);
        Assert.Equal("7316", cells["Pin"]);

        foreach (string spelling in (string[])["[''].Pin", "['']['Pin']", "[''][0].Pin", "['.Pin']"])
        {
            Assert.Contains("hidden field", FilterGuard.Prepare($"{spelling} like '73%'", policy, vault).Error);
            Assert.Contains("hidden field", FilterGuard.Prepare($"{spelling} > 7000", policy, vault).Error);
            Assert.Contains("hidden field", FilterGuard.PrepareAggregation($"select max({spelling})", policy, vault, Payments).Error);
            string path = ((CompareNode)FilterParser.Parse($"{spelling} = 'x'")).Property;
            Assert.Matches("^h:[0-9a-f]{12}$", policy.MaskGroupKey(FilterGuard.RulePath(path), "7316", new MaskStats())!);
            // And the token is at home there: the value is hidden in every reading of the path.
            Assert.Null(FilterGuard.Prepare($"{spelling} = '{cells[".Pin"]}'", policy, vault).Error);
        }
        Assert.Null(FilterGuard.Prepare("Pin like '73%'", policy, vault).Error);
    }

    /// <summary>
    /// A token is compared where its value is hidden in every event the asking profile can be
    /// shown — which, for a profile of some services, is a question about those services' rules.
    /// </summary>
    [Fact]
    public void AToken_IsJudgedByTheServicesItsProfileReads()
    {
        var vault  = new TokenVault();
        var policy = Policy(McpMaskingMode.Standard, vault,
            Rule("TenantNo", MaskAction.Hash, "payments-api"), Rule("Contact", MaskAction.Keep, "support"));
        var cells = Cells(policy.Mask(Event("x", new() { ["TenantNo"] = "T-778899", ["Pan"] = "4111111111111111" }), new MaskStats()));
        string tenant = cells["TenantNo"], card = cells["Pan"];

        string[] payments = ["payments-api"], both = ["payments-api", "support"], support = ["Support"];

        // Hidden in the one service this profile reads: the question is answered there.
        Assert.Null(FilterGuard.Prepare($"TenantNo = '{tenant}'", policy, vault, payments).Error);
        Assert.True(policy.HidesEverywhere("TenantNo", payments));
        // Open in another service the profile reads, or in one it might: refused.
        foreach (string[]? reads in (string[]?[])[both, support, null, []])
        {
            Assert.Contains("shows its values as they are stored", FilterGuard.Prepare($"TenantNo = '{tenant}'", policy, vault, reads).Error);
            Assert.False(policy.HidesEverywhere("TenantNo", reads));
        }

        // A keep of a service the profile does not read shows it nothing: the detector hides the card there.
        Assert.Null(FilterGuard.Prepare($"Contact = '{card}'", policy, vault, payments).Error);
        foreach (string[]? reads in (string[]?[])[both, support, null])
            Assert.Contains("shows its values as they are stored", FilterGuard.Prepare($"Contact = '{card}'", policy, vault, reads).Error);

        // The same through a table's where-clause, which carries the profile.
        Assert.Null(FilterGuard.PrepareAggregation($"select count(*) where TenantNo = '{tenant}'", policy, vault, Payments).Error);
        Assert.Contains("shows its values as they are stored",
            FilterGuard.PrepareAggregation($"select count(*) where TenantNo = '{tenant}'", policy, vault, Payments with { Services = [] }).Error);
    }

    /// <summary>
    /// What a profile may ask of a field is judged by the services it reads, as what a token
    /// may meet is. A rule for a service the profile does not read hides nothing it is shown —
    /// and held its filters to "equality only" all the same, with <c>logs_fields</c> saying of a
    /// field shown as stored that it was a hidden one (the fifth review).
    /// </summary>
    [Fact]
    public void AField_IsAHiddenOneToAProfile_ByTheRulesOfTheServicesItReads()
    {
        var vault  = new TokenVault();
        var policy = Policy(McpMaskingMode.Standard, vault,
            Rule("Region", MaskAction.Hash, "payments-api"), Rule("Total", MaskAction.Keep, "support"));
        string[] payments = ["payments-api"], support = ["Support"], both = ["payments-api", "support"];

        // Hidden to a profile that reads the service the rule names, alone or among others…
        foreach (string[]? reads in (string[]?[])[payments, both, null, []])
        {
            Assert.True(policy.HidesField("Region", reads));
            Assert.Contains("hidden field", FilterGuard.Prepare("Region like 'no%'", policy, vault, reads).Error);
            Assert.Matches("^h:[0-9a-f]{12}$", policy.MaskGroupKey("Region", "north", new MaskStats(), reads)!);
        }
        // …and a field like any other to one that does not: every event it is shown has it as stored.
        Assert.False(policy.HidesField("Region", support));
        Assert.Null(FilterGuard.Prepare("Region like 'no%' and Region > 'a'", policy, vault, support).Error);
        Assert.Equal("north", policy.MaskGroupKey("Region", "north", new MaskStats(), support));
        Assert.Equal("north", Cells(policy.Mask(Event("x", new() { ["Region"] = "north" }, service: "support"), new MaskStats()))["Region"]);

        // A keep likewise holds throughout for the profile of its service, and for no profile that reads more.
        Assert.True(policy.KeepsEverywhere("Total", support));
        Assert.Equal("4111111111111111", policy.MaskNumber("Total", 4111111111111111, new MaskStats(), support));
        foreach (string[]? reads in (string[]?[])[payments, both, null])
        {
            Assert.False(policy.KeepsEverywhere("Total", reads));
            Assert.Matches("^h:[0-9a-f]{12}$", policy.MaskNumber("Total", 4111111111111111, new MaskStats(), reads));
        }

        // Through a table, which carries the profile: its aggregate, its where-clause and its scope.
        var own = FilterGuard.PrepareAggregation("select max(Region), sum(Total) where Region like 'no%'", policy, vault, Payments with { Services = ["support"] });
        Assert.Null(own.Error);
        Assert.True(own.Query!.Admits!(own.Query.Aggregates[1], 4111111111111111L));     // kept in the one service it reads
        Assert.Contains("hidden field", FilterGuard.PrepareAggregation("select max(Region)", policy, vault, Payments).Error);
        var wide = FilterGuard.PrepareAggregation("select sum(Total)", policy, vault, Payments with { Services = ["payments-api", "support"] });
        Assert.False(wide.Query!.Admits!(wide.Query.Aggregates[0], 4111111111111111L));  // not kept in the other
    }

    /// <summary>
    /// The fourth review's oracle, held as a property over modes × rules × spellings of a path ×
    /// the shapes a value is stored in × services × profiles: what the guard lets a QUESTION do
    /// never goes past what the answer does with the VALUE — for the profile that asks, in the
    /// events that profile is shown.
    /// <list type="bullet">
    /// <item>A filter may order or match a field only if every value the engine reads there is
    /// one the answer shows as stored. (Of a value no detector knows: see the secrets below.)</item>
    /// <item>An aggregate takes every value of a field, and a table shows a key whole or by its
    /// last four characters, only where a search shows as much.</item>
    /// <item>A token is compared with a field only if no event it can find shows anything of the
    /// value there — not the value, not its last four characters.</item>
    /// </list>
    /// The engine reads <c>client.id</c> as an object's property or as one name with a dot in
    /// it, a segment that reads as a number as a position in a list or as a name; the answer
    /// judges each by the path it walked. The guard has to be right for all of them at once.
    /// </summary>
    [Fact]
    public void WhatTheGuardAllows_TheAnswerShows_HoweverAPathIsSpelledAndStored()
    {
        const string a = "payments-api", b = "support";

        // A text and a number no detector knows. Shown: the value itself. Cut: what keepLast4
        // leaves of it — for the text its last four letters, which no token can hold; for the
        // number the mark itself, since a token is made of digits too.
        //
        // And a card number, as a text and as a number: Known to a detector, which hides it
        // in a field no rule names as well. Of such a value the first promise is not made —
        // detectors work on answers, and a filter may read a field no rule names — and the
        // others are: nothing is kept, shown in a table or found by a token past what the
        // answer shows, and there the answer shows less than it does of the other two.
        //
        // And a false, which is no secret and is here for its type: the engine makes a table's
        // key of a true or a false as it does of a text, and what a rule does to the one it has
        // to do to the other (the sixth review's round: a keepLast4 cut the key and hashed the value).
        (object Value, string Shown, string Cut, bool Known)[] secrets =
        [
            ("ZQ-77-KWXY", "ZQ-77-KWXY", "KWXY", false), (73915628, "73915628", "****5628", false),
            ("4111111111111111", "4111111111111111", "****1111", true), (4111111111111111L, "4111111111111111", "****1111", true),
            (false, "false", "alse", false),
        ];

        static Dictionary<string, object?> Map(string key, object? value) => new() { [key] = value };

        // The secret, once in each event, at a place some spelling below reaches.
        static Dictionary<string, object?>[] Shapes(object secret) =>
        [
            Map("client.id", secret),                                                           // one name with a dot
            Map("client", Map("id", secret)),                                                   // an object and its property
            Map("L", new List<object?> { Map("F", secret) }),                                   // a list of objects
            Map("L.F", secret),
            Map("L", Map("0", Map("F", secret))),                                               // a map whose key reads as a position
            Map("L.0.F", secret),
            Map("A", Map("B.C", secret)),                                                       // a dot in a name inside an object
            Map("A", Map("B", Map("C", secret))),
            Map("A.B.C", secret),
            Map("A.B", Map("C", secret)),
            Map("A", new List<object?> { Map("B", Map("1", Map("C", secret))) }),               // a list, then a map keyed by a number
            Map("A", Map("0", Map("B", new List<object?> { "x", Map("C", secret) }))),          // the other way round
            Map("", Map("X", secret)),                                                          // a property with no name, and X under it
            Map(".X", secret),                                                                  // one name that begins with a dot
            Map("", new List<object?> { Map("X", secret) }),
            Map("", Map("", Map("X", secret))),                                                 // no name twice over
            Map("..X", secret),
            Map("L", new List<object?> { secret }),                                             // a list of the values themselves
            Map("L", new List<object?> { new List<object?> { "x", secret } }),                  // and a list of such lists
            Map("L", Map("0", secret)),                                                         // a map whose key reads as a position
        ];
        string[] spellings =
        [
            "client.id", "['client.id']", "client['id']",
            "L.F", "['L.F']", "L[0].F", "L.0.F", "L[' 0'].F", "L['0'].F", "['L.0.F']",
            "A.B.C", "A['B.C']", "['A.B.C']", "['A.B'].C", "A.B['C']",
            "A[0].B.1.C", "A.0.B.1.C", "A.0.B['1'].C", "A['0'].B[1].C",
            "[''].X", "['']['X']", "[''][0].X", "['.X']", "[''][''].X", "['..X']",
            "L[0]", "L.0", "L['0']", "L[0][1]", "L.0.1", "L[0]['1']",
        ];
        McpMaskRule[][] ruleSets =
        [
            [],
            [Rule("client", MaskAction.Hash)],
            [Rule("client", MaskAction.Hash, a)],
            [Rule("client.id", MaskAction.Hash)],
            [Rule("client", MaskAction.Keep)],
            [Rule("client", MaskAction.KeepLast4)],
            [Rule("client.id", MaskAction.KeepLast4)],
            [Rule("client", MaskAction.Remove)],
            [Rule("id", MaskAction.Hash)],
            [Rule("client", MaskAction.Hash), Rule("client.id", MaskAction.Keep, b)],
            [Rule("client", MaskAction.Keep, a), Rule("client.id", MaskAction.KeepLast4, b)],
            [Rule("*ient*", MaskAction.Hash)],
            [Rule("L", MaskAction.Hash)],
            [Rule("L.F", MaskAction.Hash)],
            [Rule("L.0.F", MaskAction.Hash)],
            [Rule("L", MaskAction.Keep), Rule("L.F", MaskAction.Hash, a)],
            [Rule("F", MaskAction.KeepLast4)],
            [Rule("A.B", MaskAction.Hash)],
            [Rule("A.B.C", MaskAction.Hash)],
            [Rule("A", MaskAction.Keep)],
            [Rule("A.B", MaskAction.Keep, a)],
            [Rule("A.B.1.C", MaskAction.Hash)],
            [Rule("A.B.C", MaskAction.Hash), Rule("A.0.B.1.C", MaskAction.Hash)],               // both ends, not the middle
            [Rule("C", MaskAction.Remove)],
            [Rule("B", MaskAction.Keep)],                                                       // a keep on the middle of a name
            [Rule("client", MaskAction.Keep), Rule("client.id", MaskAction.Keep, a)],           // above for all, on the path for one
            [Rule("F", MaskAction.KeepLast4, a)],                                               // cut in one service, the detectors' in the other
            [Rule("*.X", MaskAction.Hash)],                                                     // needs a dot before the name
            [Rule("*.X", MaskAction.KeepLast4, a)],
            [Rule("*.X", MaskAction.Keep)],
            [Rule(".X", MaskAction.Hash, b)],
            [Rule("*", MaskAction.Keep, a), Rule("*.*", MaskAction.Remove, b)],
            [Rule("L", MaskAction.KeepLast4)],                                                  // a list's elements are the list's path
            [Rule("L", MaskAction.KeepLast4, a)],
            [Rule("L", MaskAction.Keep, a), Rule("L", MaskAction.KeepLast4, b)],
            [Rule("L", MaskAction.Remove, b)],
        ];
        McpProfile[] profiles =
        [
            Payments with { Services = [] }, Payments, Payments with { Services = [b] }, Payments with { Services = [a, b] },
        ];

        int read = 0, compared = 0, found = 0;
        foreach (var (secret, whole, cut, known) in secrets)
        {
            foreach (var mode in (McpMaskingMode[])[McpMaskingMode.Standard, McpMaskingMode.Strict])
            {
                for (int set = 0; set < ruleSets.Length; set++)
                {
                    var vault  = new TokenVault();
                    var policy = Policy(mode, vault, [.. ruleSets[set], Rule("AccountRef", MaskAction.Hash)]);
                    string token = Cells(policy.Mask(Event("x", new() { ["AccountRef"] = secret }), new MaskStats()))["AccountRef"];

                    var events = (from shape in Shapes(secret) from service in (string[])[a, b] select Event("x", shape, service)).ToList();
                    var shown  = events.ConvertAll(e => string.Join('\n', policy.Mask(e, new MaskStats()).Properties.Select(static p => $"{p.Key}={p.Value}")));

                    foreach (string spelling in spellings)
                    {
                        string path  = ((CompareNode)FilterParser.Parse($"{spelling} = 'x'")).Property;
                        string named = FilterGuard.RulePath(path);

                        foreach (var profile in profiles)
                        {
                            // What a question of this profile's may do with the field, by the
                            // services it reads — against what the answer does with the value
                            // in each event the profile is shown.
                            bool hidden = FilterGuard.IsHidden(policy, path, profile.Services), kept = policy.KeepsEverywhere(named, profile.Services);
                            string key  = policy.MaskGroupKey(named, whole, new MaskStats(), profile.Services)!;

                            for (int i = 0; i < events.Count; i++)
                            {
                                if (!FilterGuard.InScope(profile, events[i])) continue;
                                if (FilterEvaluator.ReadProperty(events[i], path) is not { } value || !value.Equals(secret)) continue;
                                read++;
                                string at = $"{mode}, rules #{set}, {spelling}, profile of [{string.Join(", ", profile.Services)}], in {events[i].ServiceName}: {shown[i]}";
                                bool open = shown[i].Contains(whole);
                                Assert.True(known || hidden || open, $"a filter may read what the answer hides — {at}");
                                Assert.True(!kept || open, $"kept for an aggregate, hidden in the answer — {at}");
                                Assert.True(open || !key.Contains(whole), $"a table's key shows what the answer hides — {at}");
                                Assert.True(open || shown[i].Contains(cut) || !key.Contains(cut), $"a table's key shows the last four the answer hides — {at}");
                            }

                            foreach (string filter in (string[])[$"{spelling} = '{token}'", $"{spelling} in ['{token}', 'x']"])
                            {
                                var prepared = FilterGuard.Prepare(filter, policy, vault, profile.Services);
                                if (prepared.Error is not null) continue;
                                compared++;
                                var compiled = CompiledFilter.Compile(FilterGuard.Compose(prepared.Node, profile));
                                for (int i = 0; i < events.Count; i++)
                                {
                                    if (!compiled.Matches(events[i])) continue;
                                    found++;
                                    Assert.False(shown[i].Contains(whole) || shown[i].Contains(cut),
                                        $"a token read its value — {mode}, rules #{set}, {filter}, profile of [{string.Join(", ", profile.Services)}], in {events[i].ServiceName}: {shown[i]}");
                                }
                            }
                        }
                    }
                }
            }
        }

        // Not an empty promise: the engine reads the value by these paths, and tokens do find it.
        Assert.True(read > 20_000, $"the engine read the value {read} times");
        Assert.True(compared > 5_000, $"{compared} comparisons with a token were let through");
        Assert.True(found > 5_000, $"a token found its value {found} times");
    }

    [Fact]
    public void AnAggregate_TakesNoValueASearchWouldHide()
    {
        var policy = Standard(Rule("Total", MaskAction.Keep));
        var left   = new MaskStats();

        // Measurements, whatever they are stored as.
        foreach (object measured in (object[])[42L, 7, 12.5, 1250d, true, 99_999_999_999L, 4111111111111112L])
            Assert.True(policy.AdmitsToAggregate(measured, left));
        // A text is not one — and is not counted as hidden either: it never was a number.
        Assert.False(policy.AdmitsToAggregate("+77015550123", left));
        Assert.False(policy.AdmitsToAggregate("200", left));
        Assert.Equal(0, left.Total);

        // What a search hides. The result of an aggregate is masked too — but the sum of two
        // IINs is no IIN, and three sums of pairs are three equations in three unknowns.
        Assert.True(SensitiveValueDetectors.IsIin("850315400055") && SensitiveValueDetectors.IsIin("780720300233"));
        Assert.False(policy.AdmitsToAggregate(900101300126L, left));
        Assert.False(policy.AdmitsToAggregate(850315400055L, left));
        Assert.False(policy.AdmitsToAggregate(4111111111111111L, left));
        Assert.False(policy.AdmitsToAggregate(4111111111111111d, left));
        Assert.False(policy.AdmitsToAggregate(900101300126L, leftOut: null));
        Assert.Equal(2, left.ByDetector["iin"]);
        Assert.Equal(2, left.ByDetector["cardNumber"]);

        // As a query asks it: every aggregate over the field refuses the value, and it is one
        // value — counted once, however many columns were made of the field.
        var counted  = new MaskStats();
        var prepared = FilterGuard.PrepareAggregation(
            "select sum(ClientRef), avg(ClientRef), max(ClientRef), count(ClientRef), sum(Total)", policy, new TokenVault(), Payments, counted);
        var admits     = prepared.Query!.Admits!;
        var aggregates = prepared.Query.Aggregates;
        foreach (var aggregate in aggregates.Take(3)) Assert.False(admits(aggregate, 900101300126L));
        Assert.Equal(1, counted.Total);
        Assert.True(admits(aggregates[0], 1250L));
        // A field a rule keeps as stored in every service: every value of it is taken, as every value of it is shown.
        Assert.True(admits(aggregates[4], 900101300126L));
        Assert.Equal(1, counted.Total);
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
        // Three hundred clauses, the most negations a filter may stack, two dozen words: each is
        // a chain, and a chain is written flat. The text the engine is given nests no deeper
        // than its meaning.
        string clauses = string.Join(" and ", Enumerable.Range(0, 300).Select(static i => "A = " + i % 10));
        string either  = string.Join(" or ", Enumerable.Range(0, 300).Select(static i => "A = " + i % 10));
        string nots    = string.Concat(Enumerable.Repeat("not ", FilterGuard.MaxNesting)) + "A = 1";
        string words   = string.Join(' ', Enumerable.Range(0, FilterGuard.MaxWords).Select(static i => "w" + i));
        // The densest chain 4 000 characters hold of what a filter here may be: six characters a
        // clause, 667 clauses — a left-deep tree that deep, walked by recursion at each step. (The
        // densest of all, a lone '-' three characters a clause, is plain words, and 1 334 of them
        // are refused by the count of words — see below.)
        string densest = string.Concat(Enumerable.Repeat("a=1or ", 666)) + "a=1";

        string mixed   = $"({string.Join(" or ", Enumerable.Range(0, 120).Select(static i => "B = " + i))}) and {string.Concat(Enumerable.Repeat("not ", 30))}C = 1 and {words}";

        foreach (string filter in (string[])[clauses, either, nots, words, mixed, densest])
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

        // The engine's own parser had no such limit: some two hundred nested parentheses were
        // more stack than a thread has, and that ended the process. Here they are counted first.
        Assert.Contains("nests parentheses", FilterGuard.Prepare(new string('(', 250) + "A = 1" + new string(')', 250), policy, new TokenVault()).Error);

        // A 'not' is a level as a parenthesis is — the engine counts it so, and refuses past 100.
        // Refused here far below that, with a sentence of this server's, also where the text
        // has no parenthesis at all: the scope joined around an 'or' is one level more, and the
        // text written for the engine must never be the one it refuses.
        foreach (string deep in (string[])
                 [
                     string.Concat(Enumerable.Repeat("not ", FilterGuard.MaxNesting + 1)) + "A = 1",
                     string.Concat(Enumerable.Repeat("not ", 500)) + "A = 1",
                     "A = 1 or " + string.Concat(Enumerable.Repeat("not ", 100)) + "B = 1",
                     string.Concat(Enumerable.Repeat("not (A = 1 or ", 17)) + "B = 1" + new string(')', 17),
                 ])
        {
            var refused = FilterGuard.Prepare(deep, policy, new TokenVault());
            Assert.Contains("nests more than", refused.Error);
            Assert.Equal(RefusalKind.Input, refused.Kind);
        }
        // The same count on a tree: a 'not' is a level; an 'or' inside an 'and' is one; a chain is none.
        Assert.Equal(0, FilterGuard.WrittenDepth(FilterParser.Parse(clauses)));
        Assert.Equal(1, FilterGuard.WrittenDepth(FilterParser.Parse("A = 1 and (B = 1 or C = 1)")));
        Assert.Equal(3, FilterGuard.WrittenDepth(FilterParser.Parse("not (A = 1 or not B = 1)")));
        Assert.Equal(FilterGuard.MaxNesting, FilterGuard.WrittenDepth(FilterParser.Parse(nots)));

        var tooMany = FilterGuard.Prepare(words + " onemore", policy, new TokenVault());
        Assert.Contains("plain words", tooMany.Error);
        Assert.Equal(RefusalKind.Input, tooMany.Kind);

        // Counted across the filter, not per run of words: the third review's 450 one-word runs,
        // and the densest chain of all, are a full scan with 1 350 and 4 002 tests an event.
        foreach (string spread in (string[])
                 [
                     string.Join(" and ", Enumerable.Range(0, 450).Select(static i => "w" + i)),
                     string.Concat(Enumerable.Repeat("-or", 1_333)) + "-",
                     string.Join(" or ", Enumerable.Range(0, FilterGuard.MaxWords + 1).Select(static i => "w" + i)),
                 ])
        {
            var refused = FilterGuard.Prepare(spread, policy, new TokenVault());
            Assert.Contains("plain words", refused.Error);
            Assert.Equal(RefusalKind.Input, refused.Kind);
        }
        Assert.Null(FilterGuard.Prepare(string.Join(" or ", Enumerable.Range(0, FilterGuard.MaxWords).Select(static i => "w" + i)), policy, new TokenVault()).Error);
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

        // A token given whole as an argument is a token, held to the same rule as in a filter: the
        // tool can be asked for "the same merchant as before" where the merchant is a hidden field…
        var byMerchant = OrdersOf with { Filter = "MerchantId = {owner} and Region = {region}" };
        var byToken    = PublishedToolBinder.Bind(byMerchant, Json($$"""{"owner":"{{numeric}}","region":"eu"}"""));
        var same       = FilterGuard.Prepare(byToken.Filter, policy, vault);
        Assert.Null(same.Error);
        Assert.Equal(FilterGuard.Shape(FilterGuard.Prepare(byToken.Reference, policy, vault).Node), FilterGuard.Shape(same.Node));
        Assert.True(Run(same).Matches(Event("x", new() { ["MerchantId"] = 6456116363L, ["Region"] = "eu" })));

        // …and not where the field shows its values as stored: there it would find the value in the open.
        var open = FilterGuard.Prepare(PublishedToolBinder.Bind(OrdersOf, Json($$"""{"owner":"{{numeric}}","region":"eu"}""")).Filter, policy, vault);
        Assert.Contains("shows its values as they are stored", open.Error);
        Assert.Equal(RefusalKind.Masking, open.Kind);
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

    [Fact]
    public void AParameterWhereANameStands_IsRefusedWhenTheToolIsSaved_NotAtEveryCall()
    {
        // It reads well, and it parses with any one argument — but its filter is another filter
        // for every name, and every real call would be refused for changing it.
        var tool = new McpPublishedTool
        {
            Id = "t", Name = "by_header", Description = "d",
            Filter = "@service = 'gateway' and Headers[{name}] = {value}",
            Parameters = [new() { Name = "name", Required = true }, new() { Name = "value", Required = true }],
        };
        var problem = PublishedToolBinder.Validate(tool, []);
        Assert.Contains("{name} stands where a name stands", problem?.Error);
        Assert.Equal("filter", problem?.Field);

        // The same parameters, each where a value stands.
        Assert.Null(PublishedToolBinder.Validate(tool with { Filter = "@service = 'gateway' and Headers['X-Tenant'] = {value} and Name = {name}" }, []));

        // Every type of parameter, with and without a default: none of them changes a shape.
        var typed = new McpPublishedTool
        {
            Id = "t", Name = "typed", Description = "d",
            Filter = "Owner = {owner} and Elapsed > {minMs} and Retried = {retried} and Reason in [{reason}, 'other'] and startsWith(Path, {prefix}) and {word}",
            Parameters =
            [
                new() { Name = "owner", Required = true, Default = "y" },
                new() { Name = "minMs", Type = McpToolParameter.Number, Default = "2" },
                new() { Name = "retried", Type = McpToolParameter.Boolean },
                new() { Name = "reason", Type = McpToolParameter.Enum, Values = ["a", "b"] },
                new() { Name = "prefix" },
                new() { Name = "word" },
            ],
        };
        Assert.Null(PublishedToolBinder.Validate(typed, []));
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
