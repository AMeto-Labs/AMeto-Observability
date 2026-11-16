using System.Text;
using Ameto.Core;
using Ameto.Mcp;
using Ameto.Mcp.Masking;
using Ameto.Mcp.Tools;
using Ameto.Query.Filtering;

namespace Ameto.Integration.Tests;

/// <summary>
/// What the AI may see of a value: the detectors, the field rules, the tokens, and the limits on
/// what a filter may ask about a hidden field. No host: these are the parts under the endpoint.
/// </summary>
public sealed class McpMaskingTests
{
    private static readonly byte[] Secret = Encoding.UTF8.GetBytes("a secret for tests, thirty-two b.");

    private static McpMaskRule Rule(string field, MaskAction action, params string[] services) =>
        new() { Id = Guid.NewGuid().ToString("N"), Field = field, Action = action, Services = services };

    private static MaskingPolicy Policy(McpMaskingMode mode = McpMaskingMode.Standard, TokenVault? vault = null, params McpMaskRule[] rules) =>
        new(mode, rules, MaskDetectors.All, Secret, vault ?? new TokenVault());

    private static LogEvent Event(string template, Dictionary<string, object?> properties, string? service = "payments-api") => new()
    {
        Id              = new EventId(1_000, 1, 1),
        Timestamp       = new DateTimeOffset(EventIdGenerator.Epoch, TimeSpan.Zero).AddMilliseconds(1_000),
        Level           = LogLevel.Information,
        MessageTemplate = template,
        Properties      = properties,
        ServiceName     = service,
    };

    private static Dictionary<string, string> Cells(MaskedEvent ev) => ev.Properties.ToDictionary(static p => p.Key, static p => p.Value);

    // ── Detectors ─────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("4111111111111111", true)]
    [InlineData("4111 1111 1111 1111", true)]
    [InlineData("5555-5555-5555-4444", true)]
    [InlineData("4111111111111112", false)]      // fails the Luhn check
    [InlineData("411111111111", false)]          // twelve digits is not a card number
    public void CardNumbers_AreThirteenToNineteenDigitsThatPassLuhn(string text, bool expected) =>
        Assert.Equal(expected, SensitiveValueDetectors.IsCardNumber(text));

    [Theory]
    [InlineData("900101300126", true)]           // 9·1 + 1·4 + 1·6 + 3·7 + 1·10 + 2·11 = 72, 72 mod 11 = 6
    [InlineData("900101300123", false)]
    [InlineData("000000000101", true)]           // first pass gives 10, so the second weighting decides: 1
    [InlineData("90010130012", false)]
    public void AnIin_IsTwelveDigitsWithItsCheckDigit(string text, bool expected) =>
        Assert.Equal(expected, SensitiveValueDetectors.IsIin(text));

    [Theory]
    [InlineData("GB82WEST12345698765432", true)]
    [InlineData("GB82WEST12345698765433", false)]
    public void AnIban_PassesModulo97(string text, bool expected) =>
        Assert.Equal(expected, SensitiveValueDetectors.IsIban(text));

    [Theory]
    [InlineData("paid with 4111 1111 1111 1111 today", "4111 1111 1111 1111")]
    [InlineData("callback to a.serikov@example.com failed", "a.serikov@example.com")]
    [InlineData("call +7 701 555 01 23 back", "+7 701 555 01 23")]
    [InlineData("call 8 (701) 555-01-23 back", "8 (701) 555-01-23")]
    [InlineData("iin 900101300126 checked", "900101300126")]
    [InlineData("Authorization: Bearer abcdefghijklmnop1234", "abcdefghijklmnop1234")]
    [InlineData("token eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0In0.c2lnbmF0dXJl sent", "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0In0.c2lnbmF0dXJl")]
    [InlineData("to GB82WEST12345698765432 now", "GB82WEST12345698765432")]
    public void AText_LosesTheValueADetectorFinds_AndKeepsTheRest(string text, string hidden)
    {
        var stats = new MaskStats();
        string masked = Policy().MaskText(text, stats);

        Assert.DoesNotContain(hidden, masked);
        Assert.Equal(1, stats.Total);
        Assert.Equal(1, stats.Count(MaskReason.Detector));   // counted as a detector's find, under its id
        Assert.Contains(Assert.Single(stats.ByDetector).Key, (string[])["cardNumber", "email", "phone", "iin", "secret", "iban"]);
        Assert.Matches("h:[0-9a-f]{12}", masked);
        // Everything around the value is as it was.
        int at = text.IndexOf(hidden, StringComparison.Ordinal);
        Assert.StartsWith(text[..at], masked);
        Assert.EndsWith(text[(at + hidden.Length)..], masked);
    }

    [Theory]
    [InlineData("order 4111111111111112 shipped")]              // sixteen digits, not a card number
    [InlineData("took 123456789012 ticks")]                     // twelve digits, not an IIN
    [InlineData("Payment {PaymentId} for card {CardNumber} declined: {Reason}")]
    [InlineData("2026-11-16 09:12:44.103 request 550e8400-e29b-41d4-a716-446655440000")]
    public void AText_WithNothingToHide_IsTheSameString(string text)
    {
        var stats = new MaskStats();
        Assert.Same(text, Policy().MaskText(text, stats));
        Assert.Equal(0, stats.Total);
    }

    [Fact]
    public void OneValueSpelledTwoWays_IsOneToken()
    {
        var policy = Policy(rules: Rule("CardNumber", MaskAction.Hash));
        var masked = policy.Mask(Event("Paid", new()
        {
            ["CardNumber"] = "4111111111111111",
            ["Note"]       = "card 4111 1111 1111 1111 on file",
            ["Contact"]    = "A.Serikov@Example.com and a.serikov@example.com",
        }), new MaskStats());

        var cells = Cells(masked);
        string card = cells["CardNumber"];
        Assert.Equal($"card {card} on file", cells["Note"]);
        string[] contacts = cells["Contact"].Split(" and ");
        Assert.Equal(contacts[0], contacts[1]);
    }

    // ── Field rules ───────────────────────────────────────────────────────────

    [Theory]
    [InlineData("CardNumber", "CardNumber", true)]
    [InlineData("CardNumber", "cardnumber", true)]
    [InlineData("CardNumber", "CardNumberHash", false)]
    [InlineData("*Password*", "OldPasswordHash", true)]
    [InlineData("Headers.*", "Headers.Authorization", true)]
    [InlineData("Headers.*", "Headers", false)]
    [InlineData("*", "Anything.At.All", true)]
    public void AFieldPattern_MatchesWholePaths_WhateverTheirCase(string pattern, string path, bool expected) =>
        Assert.Equal(expected, MaskingPolicy.Glob(pattern, path));

    [Fact]
    public void ARule_HidesItsField_AtAnyDepthWhenItNamesNoPath_AndTheMoreProtectiveRuleWins()
    {
        var card      = Rule("CardNumber", MaskAction.Hash);
        var passwords = Rule("*Password*", MaskAction.Remove);
        var lastFour  = Rule("Pan", MaskAction.KeepLast4);
        var keptPan   = Rule("Pan", MaskAction.Keep);            // loses to KeepLast4 on the same field
        var policy = Policy(rules: [card, Rule("Customer.Iin", MaskAction.Hash), lastFour, passwords, keptPan]);
        var stats = new MaskStats();
        var masked = policy.Mask(Event("Paid {Amount}", new()
        {
            ["Amount"]     = 12500,
            ["CardNumber"] = "4111111111111111",
            ["Pan"]        = "5555555555554444",
            ["Password"]   = "hunter2",
            ["Customer"]   = new Dictionary<string, object?> { ["Iin"] = "900101300123", ["Tier"] = "gold", ["OldPasswordHash"] = "x" },
            ["Payment"]    = new Dictionary<string, object?> { ["CardNumber"] = "4012888888881881", ["Ok"] = true },
        }), stats);

        var cells = Cells(masked);
        Assert.Equal("12500", cells["Amount"]);
        Assert.Matches("^h:[0-9a-f]{12}$", cells["CardNumber"]);
        Assert.Equal("****4444", cells["Pan"]);
        Assert.False(cells.ContainsKey("Password"));
        Assert.Matches("""^\{"Iin":"h:[0-9a-f]{12}","Tier":"gold"\}$""", cells["Customer"]);
        Assert.Matches("""^\{"CardNumber":"h:[0-9a-f]{12}","Ok":true\}$""", cells["Payment"]);
        Assert.Equal(6, stats.Total);
        Assert.Equal(1, stats.ByRule["Customer.Iin"]);
        Assert.Equal(1, stats.ByRule["Payment.CardNumber"]);

        // All six were a rule's doing, and each is counted for the rule that decided.
        Assert.Equal(6, stats.Count(MaskReason.Rule));
        Assert.Empty(stats.ByDetector);
        Assert.Empty(stats.ByStrict);
        Assert.Equal(2, stats.RuleHits[card.Id]);          // the field itself and the one under Payment
        Assert.Equal(2, stats.RuleHits[passwords.Id]);     // Password and Customer.OldPasswordHash
        Assert.Equal(1, stats.RuleHits[lastFour.Id]);
        Assert.False(stats.RuleHits.ContainsKey(keptPan.Id));
    }

    [Fact]
    public void AFieldAndADetectorOfOneName_AreCountedApart()
    {
        // A property called "email" hidden by a rule, and an address the email detector found in
        // another property: two lines of the breakdown, not one.
        var rule   = Rule("email", MaskAction.Hash);
        var stats  = new MaskStats();
        Policy(rules: rule).Mask(Event("Signed up", new()
        {
            ["email"] = "a.serikov@example.com",
            ["Note"]  = "also reachable at b.omarov@example.com",
        }), stats);

        Assert.Equal(2, stats.Total);
        Assert.Equal(1, stats.ByRule["email"]);
        Assert.Equal(1, stats.ByDetector["email"]);
        Assert.Equal(1, stats.RuleHits[rule.Id]);
    }

    [Fact]
    public void ARuleForOneService_LeavesTheOthersAlone()
    {
        var policy = Policy(rules: Rule("MerchantId", MaskAction.Hash, "payments-api"));
        var properties = new Dictionary<string, object?> { ["MerchantId"] = 6456116363L };

        Assert.StartsWith("h:", Cells(policy.Mask(Event("x", properties, "payments-api"), new MaskStats()))["MerchantId"]);
        Assert.Equal("6456116363", Cells(policy.Mask(Event("x", properties, "acquiring-gw"), new MaskStats()))["MerchantId"]);
        // A filter is not tied to a service, so the field counts as hidden wherever it could be.
        Assert.True(policy.HidesField("MerchantId"));
    }

    [Fact]
    public void ANumberAndItsText_AreOneToken_AndACardStoredAsAnIntegerIsStillACard()
    {
        var policy = Policy(rules: Rule("MerchantId", MaskAction.Hash));
        string asNumber = Cells(policy.Mask(Event("x", new() { ["MerchantId"] = 6456116363L }), new MaskStats()))["MerchantId"];
        string asText   = Cells(policy.Mask(Event("x", new() { ["MerchantId"] = "6456116363" }), new MaskStats()))["MerchantId"];
        Assert.Equal(asNumber, asText);

        var stats = new MaskStats();
        var cells = Cells(policy.Mask(Event("x", new() { ["Account"] = 4111111111111111L, ["Count"] = 4111111111111112L }), stats));
        Assert.StartsWith("h:", cells["Account"]);
        Assert.Equal("4111111111111112", cells["Count"]);
        Assert.Equal(1, stats.ByDetector["cardNumber"]);
    }

    [Fact]
    public void InStrictMode_EverythingIsHidden_ExceptWhatARuleKeeps()
    {
        var policy = Policy(McpMaskingMode.Strict, rules:
        [
            Rule("Reason", MaskAction.Keep),
            Rule("Elapsed", MaskAction.Keep),
        ]);
        var stats = new MaskStats();
        var cells = Cells(policy.Mask(Event("Declined {Reason}", new()
        {
            ["Reason"] = "insufficient_funds", ["Elapsed"] = 812, ["Amount"] = 12500, ["PaymentId"] = "pay_1", ["Retry"] = true,
        }), stats));

        // Hidden by the mode, not by a rule: the two are told apart in a call's record.
        Assert.Equal(2, stats.Count(MaskReason.Strict));
        Assert.Equal(1, stats.ByStrict["Amount"]);
        Assert.Equal(1, stats.ByStrict["PaymentId"]);
        Assert.Empty(stats.ByRule);
        Assert.Empty(stats.RuleHits);

        Assert.Equal("insufficient_funds", cells["Reason"]);
        Assert.Equal("812", cells["Elapsed"]);
        Assert.StartsWith("h:", cells["Amount"]);
        Assert.StartsWith("h:", cells["PaymentId"]);
        Assert.Equal("true", cells["Retry"]);          // a boolean hides nothing
        Assert.True(policy.HidesField("PaymentId"));
        Assert.False(policy.HidesField("Reason"));
        Assert.False(FilterGuard.IsHidden(policy, "@mt"));          // a built-in field is never a hidden one
    }

    [Fact]
    public void TheSameSecret_GivesTheSameToken_AndAnotherSecretAnother()
    {
        var one = new MaskingPolicy(McpMaskingMode.Standard, [], MaskDetectors.None, Secret, new TokenVault());
        var two = new MaskingPolicy(McpMaskingMode.Strict,   [], MaskDetectors.None, Secret, new TokenVault());
        var other = new MaskingPolicy(McpMaskingMode.Standard, [], MaskDetectors.None, Encoding.UTF8.GetBytes("another secret"), new TokenVault());

        Assert.Equal(one.TokenOfText("4111111111111111", false), two.TokenOfText("4111111111111111", false));
        Assert.NotEqual(one.TokenOfText("4111111111111111", false), other.TokenOfText("4111111111111111", false));
        Assert.NotEqual(one.TokenOfText("4111111111111111", false), one.TokenOfText("4111111111111112", false));
    }

    // ── The vault ─────────────────────────────────────────────────────────────

    [Fact]
    public void TheVault_KeepsTheRecentValues_AndForgetsTheOldest()
    {
        var vault = new TokenVault(generationSize: 2);
        for (ulong i = 1; i <= 3; i++) vault.Remember(i, "value " + i, isNumber: false);
        Assert.True(vault.TryResolve(1, out var first));          // one generation back: still here
        Assert.Equal("value 1", first.Text);

        for (ulong i = 10; i <= 14; i++) vault.Remember(i, "value " + i, isNumber: false);
        Assert.False(vault.TryResolve(2, out _));                  // two generations back: gone
        Assert.True(vault.TryResolve(14, out _));

        vault.Remember(99, new string('x', TokenVault.MaxValueLength + 1), isNumber: false);
        Assert.False(vault.TryResolve(99, out _));                 // too long to be worth a place

        vault.Clear();
        Assert.False(vault.TryResolve(14, out _));
    }

    // ── What a filter may ask ─────────────────────────────────────────────────

    [Theory]
    [InlineData("CardNumber = '4111111111111111'")]
    [InlineData("CardNumber != 'x'")]
    [InlineData("CardNumber in ['a', 'b']")]
    [InlineData("has(CardNumber) and @l = 'Error'")]
    [InlineData("not has(CardNumber)")]
    [InlineData("Reason like 'insufficient%' and Elapsed > 500")]
    [InlineData("payment declined")]
    public void AFilter_MayTestAHiddenFieldForEquality_AndAnythingElseFreely(string filter)
    {
        var prepared = FilterGuard.Prepare(filter, Policy(rules: Rule("CardNumber", MaskAction.Hash)), new TokenVault());
        Assert.Null(prepared.Error);
    }

    [Theory]
    [InlineData("CardNumber like '4111%'")]
    [InlineData("CardNumber > '4'")]
    [InlineData("startsWith(CardNumber, '41')")]
    [InlineData("ci_contains(CardNumber, '1111')")]
    [InlineData("endsWith(CardNumber, '1111')")]
    [InlineData("length(CardNumber) > 15")]
    [InlineData("regexMatch(CardNumber, '^41')")]
    [InlineData("CardNumber = OtherNumber")]
    [InlineData("OtherNumber = CardNumber")]
    [InlineData("@l = 'Error' and (Reason = 'x' or not (Payment.CardNumber like '4%'))")]
    public void AFilter_MayNotReadAHiddenFieldAnyOtherWay(string filter)
    {
        var prepared = FilterGuard.Prepare(filter, Policy(rules: Rule("CardNumber", MaskAction.Hash)), new TokenVault());
        Assert.NotNull(prepared.Error);
        Assert.Contains("hidden field", prepared.Error);
        Assert.Equal(RefusalKind.Masking, prepared.Kind);
        Assert.Null(prepared.Node);
    }

    [Fact]
    public void ATokenInAFilter_BecomesItsValue_AndTheMessagesNeverCarryIt()
    {
        var vault  = new TokenVault();
        var policy = Policy(vault: vault, rules: [Rule("CardNumber", MaskAction.Hash), Rule("MerchantId", MaskAction.Hash)]);
        var cells  = Cells(policy.Mask(Event("x", new() { ["CardNumber"] = "4111'1111\\1111", ["MerchantId"] = 6456116363L }), new MaskStats()));

        var prepared = FilterGuard.Prepare($"CardNumber = '{cells["CardNumber"]}' or MerchantId in ['{cells["MerchantId"]}']", policy, vault);
        Assert.Null(prepared.Error);
        // A string comes back quoted with its quote and backslash escaped; a number bare.
        string text = FilterGuard.Write(prepared.Node!);
        Assert.Equal(@"CardNumber = '4111\'1111\\1111' or MerchantId in [6456116363]", text);
        Assert.True(CompiledFilter.Compile(text).Matches(Event("x", new() { ["CardNumber"] = "4111'1111\\1111" })));
        Assert.True(CompiledFilter.Compile(text).Matches(Event("x", new() { ["MerchantId"] = 6456116363L })));

        var unknown = FilterGuard.Prepare("CardNumber = 'h:0123456789ab'", policy, vault);
        Assert.Contains("h:0123456789ab", unknown.Error);
        Assert.Null(unknown.Node);
    }

    [Fact]
    public void AnAggregation_MayCountAHiddenField_ButNotRangeOrSumIt()
    {
        var policy  = Policy(rules: Rule("Amount", MaskAction.Hash));
        var profile = new McpProfile { Id = "a", Name = "a", KeyHash = "" };
        Assert.Null(FilterGuard.PrepareAggregation("select count(*), count(Amount) group by Amount", policy, new TokenVault(), profile).Error);
        foreach (string aggregate in (ReadOnlySpan<string>)["sum", "min", "max", "avg"])
            Assert.Contains("hidden field",
                FilterGuard.PrepareAggregation($"select {aggregate}(Amount)", policy, new TokenVault(), profile).Error);
        Assert.Contains("hidden field",
            FilterGuard.PrepareAggregation("select count(*) where Amount > 100", policy, new TokenVault(), profile).Error);
    }

    [Fact]
    public void AProfilesScope_IsJoinedToTheFilter_AndTheAuditTrailIsNobodys()
    {
        var open   = new McpProfile { Id = "a", Name = "a", KeyHash = "" };
        var scoped = open with { Services = ["payments-api", "o'brien"] };
        var errors = FilterGuard.Prepare("@l = 'Error'", Policy(), new TokenVault()).Node;

        Assert.Equal("@service != 'ameto-mcp'", FilterGuard.Compose(null, open));
        Assert.Equal("@l = 'Error' and @service != 'ameto-mcp'", FilterGuard.Compose(errors, open));
        Assert.Equal(@"@l = 'Error' and @service in ['payments-api', 'o\'brien']", FilterGuard.Compose(errors, scoped));
        // The service a call names is one more clause, never text pasted into another.
        Assert.Equal(@"@l = 'Error' and @service = 'it\'s' and @service != 'ameto-mcp'", FilterGuard.Compose(errors, open, "it's"));
        Assert.Equal(@"@service = 'it\'s' and @service != 'ameto-mcp'", FilterGuard.Compose(null, open, "it's"));
        // An `or` is one clause of the chain, in its own parentheses: the scope holds for both sides of it.
        var either = FilterGuard.Prepare("A = 1 or B = 2", Policy(), new TokenVault()).Node;
        Assert.Equal(@"(A = 1 or B = 2) and @service in ['payments-api', 'o\'brien']", FilterGuard.Compose(either, scoped));

        Assert.False(FilterGuard.InScope(open, Event("x", [], "ameto-mcp")));
        Assert.True(FilterGuard.InScope(open, Event("x", [], null)));
        Assert.False(FilterGuard.InScope(scoped, Event("x", [], "acquiring-gw")));
        Assert.False(FilterGuard.MaySee(open, "AMETO-MCP"));
        Assert.True(FilterGuard.MaySee(scoped, "Payments-API"));
    }

    // ── References, cursors, windows ──────────────────────────────────────────

    [Theory]
    [InlineData(0)]          // the id carries the event's own millisecond
    [InlineData(-3_607)]     // an event that arrived late: its id is younger than its timestamp
    [InlineData(250)]
    public void AnEventRef_FindsTheMillisecondOfTheEvent(long deltaMs)
    {
        const ulong idMs = 90_000_000_000;
        var ev = new LogEvent
        {
            Id              = new EventId(idMs, 3, 77),
            Timestamp       = new DateTimeOffset(EventIdGenerator.Epoch, TimeSpan.Zero).AddMilliseconds((long)idMs + deltaMs).AddTicks(4_321),
            Level           = LogLevel.Information,
            MessageTemplate = "x",
        };

        string reference = EventRef.Format(ev);

        Assert.Equal(deltaMs == 0, !reference.Contains('-') && !reference.Contains('+'));
        Assert.True(EventRef.TryParse(reference, out ulong id, out long msStart));
        Assert.Equal(ev.Id.RawValue, id);
        Assert.InRange(ev.Timestamp.UtcTicks, msStart, msStart + TimeSpan.TicksPerMillisecond - 1);
    }

    [Theory]
    [InlineData("")]
    [InlineData("not a ref!")]
    [InlineData("zzzzzzzzzzzzzzzzzzzz")]
    [InlineData("12ab-")]
    public void SomethingThatIsNotARef_IsRefused(string text) =>
        Assert.False(EventRef.TryParse(text, out _, out _));

    [Fact]
    public void ACursor_RoundTrips()
    {
        long ticks = DateTimeOffset.UtcNow.UtcTicks;
        ulong id = new EventId(123_456_789, 5, 9).RawValue;
        Assert.True(SearchCursor.TryParse(SearchCursor.Format(ticks, id), out long ticksBack, out ulong idBack));
        Assert.Equal((ticks, id), (ticksBack, idBack));
        Assert.False(SearchCursor.TryParse("nothing", out _, out _));
    }

    [Fact]
    public void ATimeWindow_TakesRelativeAndAbsoluteTimes_AndIsNarrowedToWhatTheProfileMayAsk()
    {
        var now = new DateTimeOffset(2026, 11, 16, 9, 0, 0, TimeSpan.Zero);
        var day = TimeSpan.FromDays(1);

        Assert.True(TimeWindow.TryResolve(null, null, now, day, TimeWindow.Default, out var from, out var to, out string? note, out _));
        Assert.Equal((now.AddHours(-1), now), (from, to));
        Assert.Null(note);

        Assert.True(TimeWindow.TryResolve("-15m", "now", now, day, TimeWindow.Default, out from, out to, out _, out _));
        Assert.Equal(now.AddMinutes(-15), from);

        Assert.True(TimeWindow.TryResolve("2026-11-16T08:00:00+05:00", "2026-11-16T04:00:00Z", now, day, TimeWindow.Default, out from, out to, out _, out _));
        Assert.Equal(new DateTimeOffset(2026, 11, 16, 3, 0, 0, TimeSpan.Zero), from);

        Assert.True(TimeWindow.TryResolve("-7d", null, now, day, TimeWindow.Default, out from, out to, out note, out _));
        Assert.Equal(now - day, from);
        Assert.Contains("narrowed", note);

        Assert.False(TimeWindow.TryResolve("yesterday-ish", null, now, day, TimeWindow.Default, out _, out _, out _, out string? error));
        Assert.Contains("'from'", error);
        Assert.False(TimeWindow.TryResolve("-1h", "-2h", now, day, TimeWindow.Default, out _, out _, out _, out error));
        Assert.Contains("later", error);
    }

    // ── The table ─────────────────────────────────────────────────────────────

    [Fact]
    public void Events_AreWrittenGroupedByTemplate_WithSharedValuesInTheHeader()
    {
        var policy = Policy(rules: Rule("CardNumber", MaskAction.Hash));
        var start  = new DateTimeOffset(2026, 11, 16, 9, 12, 44, 103, TimeSpan.Zero);
        LogEvent At(int n, string template, LogLevel level, Dictionary<string, object?> properties)
        {
            var ts = start.AddSeconds(n);
            return new LogEvent
            {
                Id              = new EventId((ulong)((ts.UtcTicks - EventIdGenerator.Epoch) / TimeSpan.TicksPerMillisecond), 0, (uint)n),
                Timestamp       = ts,
                Level           = level,
                MessageTemplate = template,
                Properties      = properties,
                ServiceName     = "payments-api",
            };
        }
        var stats  = new MaskStats();
        var events = new[]
        {
            At(0, "Payment {PaymentId} declined: {Reason}", LogLevel.Error, new() { ["PaymentId"] = "pay_1", ["Reason"] = "insufficient_funds", ["CardNumber"] = "4111111111111111" }),
            At(1, "Payment {PaymentId} authorised",          LogLevel.Information, new() { ["PaymentId"] = "pay_2" }),
            At(2, "Payment {PaymentId} declined: {Reason}", LogLevel.Error, new() { ["PaymentId"] = "pay_3", ["Reason"] = "insufficient_funds", ["CardNumber"] = "4111111111111111" }),
        }.Select(e => policy.Mask(e, stats)).ToList();
        string card = policy.TokenOfText("4111111111111111", false);

        var sb = new StringBuilder();
        EventTable.Write(sb, events);

        string[] refs = [.. events.Select(static e => EventRef.Format(e.Source))];
        Assert.Equal(
            $"""
            date 2026-11-16 (UTC)

            [1] Payment {"{PaymentId}"} declined: {"{Reason}"}
            ×2 · Error · payments-api · Reason = insufficient_funds · CardNumber = {card}
            ref           @t            PaymentId
            {refs[0]}  09:12:44.103  pay_1
            {refs[2]}  09:12:46.103  pay_3

            [2] Payment {"{PaymentId}"} authorised
            ×1 · Information · payments-api
            ref           @t            PaymentId
            {refs[1]}  09:12:45.103  pay_2

            """.ReplaceLineEndings("\n"),
            sb.ToString());
        Assert.Equal(2, stats.Total);
    }
}
