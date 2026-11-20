# AI access (Model Context Protocol)

Ameto serves a [Model Context Protocol](https://modelcontextprotocol.io) endpoint at `/mcp`, so an
AI assistant — Claude Code, Cursor, VS Code, an agent of your own — can investigate logs: look at
the shape of a time window, read events, count and group them, open one event with what happened
around it.

Three things make it different from handing an assistant a read API key:

- **The assistant gets tools, not the REST API.** Five read-only log tools, and the tools you
  publish yourself: a saved query with parameters, which the assistant calls by name. Answers are
  written for a model: grouped by message template, cut to a size budget, with a cursor to continue.
- **Confidential values are hidden on the way out.** Field rules and value detectors replace them
  with tokens before an answer leaves the server. The store, its indexes and the UI are untouched.
- **Every call is recorded** in the log store itself, with what was asked and how much was hidden.

It covers logs. Traces and metrics are not exposed yet.

The endpoint is on by default and useless until an admin creates a profile: without a profile key
every request is answered `401`. `Ameto:Mcp:Enabled: false` removes it altogether (see
[CONFIGURATION.md](CONFIGURATION.md#ai-endpoint-ametomcp)).

## Connecting a client

Create a profile under **Settings → AI / MCP** (or `POST /api/mcp/profiles`) and copy its key. The
key is shown once.

**Claude Code**

```bash
claude mcp add --transport http ameto https://ameto.example.com/mcp --header "Authorization: Bearer $AMETO_MCP_KEY"
```

**A repository's `.mcp.json`**, shared with the team; each developer keeps the key in the
environment variable, so it never goes into the file:

```json
{
  "mcpServers": {
    "ameto": {
      "type": "http",
      "url": "https://ameto.example.com/mcp",
      "headers": { "Authorization": "Bearer ${AMETO_MCP_KEY}" }
    }
  }
}
```

**Anything else:** the transport is Streamable HTTP and the credential is
`Authorization: Bearer <profile key>`. Under `Ameto:BasePath` the address is `<prefix>/mcp`.
Settings → AI / MCP → Connection writes the snippet for Claude Code, Cursor and VS Code with the
server's own address in it.

To check a key from a terminal — the tools it is offered:

```bash
curl -s https://ameto.example.com/mcp \
  -H "Authorization: Bearer $AMETO_MCP_KEY" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -H "MCP-Protocol-Version: 2026-07-28" \
  -H "Mcp-Method: tools/list" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}'
```

A request of revision `2026-07-28` has no session to lean on, so it names its method in the
`Mcp-Method` header (and, for `tools/call`, the tool in `Mcp-Name`) and carries the revision and
the client's capabilities in `_meta`; the server answers `400` with the missing piece named when
one is left out. An MCP client library does all of this itself.

Two generations of the protocol are served on the one address. A client on revision `2026-07-28`
is served per request, with no session. A client that opens with `initialize` (`2025-11-25` and
earlier) gets the session it expects; such sessions end after 30 idle minutes, and at most 200 idle
ones are kept.

| Answer | Meaning |
|--------|---------|
| `401` | No profile key, or a key that belongs to no profile. An ingest or read API key is not a profile key, and neither is a user's token. |
| `503` | The endpoint is switched off in Settings. |

## In the UI

**Settings → AI / MCP** (administrators) is the whole of this document as a page, in five tabs:

| Tab | |
|-----|---|
| Connection | The endpoint's address and its switch, a ready snippet per client, the last 24 hours in four figures, and the server-wide [limits](#limits). |
| Profiles | The [profiles](#profiles): a list, and the card of one — its tools, services, masking mode and limits. A key is shown once, when a profile is created or its key rotated. |
| Masking | The [field rules](#field-rules) with how often each hid a value in the last day, the [detectors](#detectors), the token secret, and a preview: one real event as it is stored next to the same event as an answer would carry it. |
| Tools | A switch per built-in tool for the whole server, the [published tools](#published-tools) with their editor and its test run, and the tool list one profile's key is offered. |
| Activity | [The record of calls](#the-record-of-calls), newest first, and one call taken apart: arguments, the filter as written, what was hidden and by what. |

Everything the page does goes through the [settings API](#settings-api) below.

## Profiles

A profile is a key and what that key may do.

| Setting | Default | Meaning |
|---------|---------|---------|
| Tools | all five | Which tools the profile is offered. A tool that is switched off is not in its tool list and cannot be called. |
| Tools mode | built-in and published | `publishedOnly` offers no built-in tool: the profile can call the [published tools](#published-tools) it was granted and nothing else. |
| Services | all | The services whose events the profile sees. Events of other services are neither returned nor counted. |
| Masking | Standard | `standard` or `strict` — see [Hidden values](#hidden-values). |
| Free-form filters | on | Off withdraws `logs_search` and `logs_aggregate`, the two tools that take a filter the assistant writes. |
| Events per call | 200 | The most events one answer returns. |
| Time window | 24 h | The widest window one call may ask for. A wider one is narrowed to its most recent part, and the answer says so. |
| Calls per minute | 30 | Counted per calendar minute; every call counts, one that is refused included. A call past it is refused with the seconds left. |

The server-wide limits (Settings → AI / MCP → Connection) are ceilings: a profile can only be
stricter.

A profile key is accepted on `/mcp` and nowhere else. It is not an API key: it cannot read
`/api/events` or any other route, so there is no unmasked road for it. Rotating a key replaces it
at once; deleting a profile ends its access at once.

No profile sees the service `ameto-mcp`, where the calls themselves are recorded, unless it is
listed among the profile's services.

## Tools

| Tool | What it answers |
|------|-----------------|
| `logs_overview` | The shape of a window: events by level and by service, the most frequent message templates, the exception types. |
| `logs_search` | Events matching a filter, grouped by message template. |
| `logs_aggregate` | A table: `select count(*) where @l = 'Error' group by @service`. |
| `logs_context` | One event in full, the events of its service just before and after it, and the events of its trace. |
| `logs_fields` | The properties found in recent events: how often, which type, sample values, whether hidden — `= only` for a field hidden in every service the profile reads, `= only, no token` for one that some of them show as stored (see [Tokens](#tokens)). Ends with a filter-syntax reference. |

A built-in tool can be switched off for the whole server (Settings → AI / MCP → Tools, or
`disabledTools` in `PUT /api/mcp/settings`). It is then offered to no profile, whatever the
profile's own list says: not in a tool list, and refused when called by name.

**Time.** `from` and `to` take ISO-8601 timestamps or relative values: `-15m`, `-2h`, `-1d`, `-1w`,
`now`. The default window is the last hour. Times are UTC.

**Filters** are the [filter expressions](FILTER_EXPRESSIONS.md) of the Logs page, with two
differences. Both are there for the hidden values (see
[What a filter may ask](#what-a-filter-may-ask-about-a-hidden-field)).

- **Plain words search the message template and the exception** — its type and its message — and
  not the property values. On the Logs page they search everything; here that would let any
  hidden value be asked for one substring at a time. To search a property, name it:
  `ci_contains(Note, 'refund')`.
- **A filter is built from a short list**: `=`, `!=`, `<`, `<=`, `>`, `>=`, `in`, `like`,
  `has()`, `isDefined()`, `startsWith()`, `contains()`, `endsWith()` and their `ci_` forms,
  `length()`, `regexMatch()`, joined with `and`, `or`, `not` and parentheses. The other functions
  of the language (`toJson()`, `elementAt()`, `substring()`, …) are refused by name: several of
  them read a value by a road a field rule does not name.

A filter or a query is at most 4 000 characters, with parentheses nested at most 32 deep, and
holds at most 24 plain words in all, however they are grouped.

### What an answer looks like

```
16 events · 3 templates · 2026-11-16 08:14:31 … 2026-11-16 09:14:31 UTC · newest first
date 2026-11-16 (UTC)

[1] Payment {PaymentId} for card {CardNumber} declined: {Reason}
×9 · Error · payments-api · Reason = insufficient_funds · Currency = KZT · MerchantId = h:9269d3828e5a
ref           @t            PaymentId   CardNumber      Amount  Elapsed
2w3ki0jnxips  09:12:44.103  pay_8f31c8  h:92f7e6d86459  12580   836
2w3khurke1og  09:12:41.103  pay_8f31c7  h:f20d54d5fa6b  12570   829
…
more: call again with cursor=4uuudmgf6pne.2w3khozgukn4
```

- **The message is not rendered.** The server does not store rendered messages; the assistant gets
  the template and the properties, as the event was written.
- **A template is printed once**, with the rows under it. A value every row of a group shares
  (the level, the service, `Reason` above) moves into the group's header line.
- **`ref`** identifies an event for `logs_context`.
- **The answer is cut to a budget** (8 000 tokens by default, estimated at three characters a
  token). What is shown is a prefix of the result in the order asked for; the last line gives a
  `cursor` to continue, or says `end of results`.
- At most 14 property columns are printed for a group; the rest are named, and `fields` selects
  which ones to see.

`logs_overview` runs three aggregations (events by service and level, the 12 most frequent
templates, the 10 most frequent exception types). `logs_aggregate` returns at most 200 rows and
marks an answer `PARTIAL` when the scan stopped early — the numbers are then floors. Its `sum`,
`min`, `max` and `avg` are made of measurements (see [Aggregates](#aggregates)).
`logs_context` looks one hour either side of the event for its neighbours and 30 minutes either
side for its trace, and keeps to the answer budget like every tool: of an event with more
properties than fit, it says how many were left out. `logs_fields` reads up to the 1 000 most
recent events of the window and lists the 60 most frequent properties.

## Published tools

A published tool is a saved query the assistant calls by name. An admin writes the filter and
names its parameters; the assistant supplies values for them and nothing else. Define one under
**Settings → AI / MCP → Tools**, or with `POST /api/mcp/tools`.

Nothing is compiled or registered per tool. A definition is a row in the database, and a
profile's tool list is built from the rows it was granted each time a client asks for it — so a
tool exists from the moment it is saved.

```json
{
  "name": "payment_declines",
  "description": "Declined payments of one merchant in a time window. Use it when asked why payments of a merchant fail.",
  "parameters": [
    { "name": "merchantId", "type": "string", "required": true, "description": "Merchant id from the back office" },
    { "name": "reason", "type": "enum", "values": ["insufficient_funds", "do_not_honor", "expired_card"] }
  ],
  "filter": "@service = 'payments-api' and MerchantId = {merchantId} and Reason = {reason}",
  "fields": ["PaymentId", "CardNumber", "Reason", "Amount", "Elapsed"],
  "defaultWindowMinutes": 60,
  "maxEvents": 200,
  "order": "newest",
  "published": true,
  "profiles": ["<profile id>"]
}
```

| Field | |
|-------|---|
| `name` | 3–64 characters: lowercase letters, digits, underscores. Unique, and not a built-in tool's name. |
| `description` | What the tool answers, written for the assistant: it chooses a tool by this text. |
| `parameters` | Up to 16. A type is `string`, `number`, `boolean` or `enum` (with `values`). An optional parameter may have a `default`. |
| `filter` | A [filter expression](FILTER_EXPRESSIONS.md) with `{name}` where a parameter's value goes. At most 2 000 characters. |
| `fields` | The properties an answer carries. Empty: all of them. |
| `defaultWindowMinutes`, `maxEvents`, `order` | The window a call gets when it names none; the most events of one answer (the profile's own limit still applies); `newest` or `oldest` first. |
| `published` | `false` keeps a draft: saved and editable, offered to nobody. |
| `profiles` | The ids of the profiles that may call it. The same grant can be set from the profile's side, as its `publishedTools`. |

Every published tool also takes `from`, `to` and `cursor`, as `logs_search` does, and answers in
the same form.

**A parameter is a value, whatever it contains.** The server puts it into the filter as a literal
— quoted and escaped for a string, checked for a number, matched against the list for an enum —
so a value cannot add a condition, close a parenthesis or name another field.
`x' or @l = 'Error` is searched for as that text. This is checked on every call, not only relied
on: the filter with the arguments in it has to have the shape of the definition with plain values
in their place — the same conditions, on the same fields, joined the same way — or the call is
not run.

**Placeholders are read outside quotes only.** A message template is full of braces, and
`@mt = 'Payment {PaymentId} declined'` is an ordinary filter: a brace inside quotes is text. So a
parameter stands where a whole value stands — `MerchantId = {merchantId}`,
`startsWith(Path, {prefix})` — never inside one.

**An optional parameter that is not given takes its clause with it.** The filter above, called
without `reason`, runs as `@service = 'payments-api' and MerchantId = …`. That works per top-level
`and` clause, so a filter with an `or` at its top level cannot have an optional parameter without
a default: put the alternatives in parentheses. The clause goes whole — `(Owner = {owner} or
@l = 'Error')` without `owner` is no condition at all, not "errors only" — so a filter has to keep
one condition that is always there. A definition whose every clause is optional is refused, and a
call that would leave a tool with no condition is refused too.

**The rules for hidden values hold.** A tool's filter is checked like any filter the assistant
could write — it may not read a hidden field with anything but `=`, `!=`, `in` or `has()`, and a
definition that does is refused when it is saved. Answers are masked as every answer is, and a
token is a valid argument where the tool compares it with a field a token may meet (see
[Tokens](#tokens)): `payments_by_card(card: "h:92f7e6d86459")` finds that card's payments.

**When a client sees a new tool.** A client on protocol revision `2026-07-28` is told with every
list that the list is good for 60 seconds. A client that holds a session is sent
`notifications/tools/list_changed` as soon as a tool or a profile changes, or a built-in tool is
switched off or on for the server.

`POST /api/mcp/tools/test` runs a definition — saved or not — as a chosen profile would get it,
and returns the answer with the filter the arguments made of the template and the definition the
assistant would be shown. It is the admin's call: not counted against the profile and not written
to the record of calls.

## Hidden values

A value is hidden when the answer is written. Nothing is changed in the store, so a rule added
today applies to events stored last year, and removing a rule shows them again.

### Field rules

A rule names a property and what happens to its value.

| Action | Effect |
|--------|--------|
| `hash` | The value is replaced by its token. |
| `remove` | The property is left out of the answer. |
| `keepLast4` | `****` and the last four characters. |
| `keep` | The value at the rule's own path goes out as stored; the detectors leave it alone. What is spelled under that path is let through Strict mode and still goes past the detectors. The allow-list of Strict mode. |

- The field is a property path as a filter spells it — `Customer.Iin` — with `*` for any run of
  characters, compared without regard to case.
- A pattern without a dot is also tried against the last segment of a path: `CardNumber` hides
  `CardNumber` and `Payment.CardNumber`. A pattern with a dot matches whole paths only.
- A rule on a property that holds an object or a list hides the whole value under one token,
  and everything under it: with a rule on `Buyer`, a filter or a `group by` on `Buyer.Name` is
  one on a hidden field.
- "Under it" is by the dots of the path as it is spelled, whether a dot stands between an object
  and its property or inside one name. An OTLP attribute is one property with dots in its name —
  `client.address`, `user.id` — and a filter spells it as it spells a property of an object, so a
  rule reads both the same way: a rule on `client` covers the object `client` and the attributes
  `client.address` and `client.id` alike. Every name keeps its place in a path, an empty one
  too: the property `Pin` of a property with no name is `.Pin`, to a rule and to a filter.
- `keepLast4` and `keep` are about the value at their own path. Under a `keepLast4` everything is
  hidden whole — there are no last four characters of an object.
- Under a `keep` everything is let through Strict mode, and nothing is vouched for: the detectors
  look through it as they do through a property no rule names. A `keep` on `card`, meant for the
  property that holds a card's brand, does not send `payment.card.number` as stored. A pattern
  without a dot is tried against the last segment at every level, so `card` is above that
  attribute as much as `payment` is — and in Strict mode it lets `payment.card.holder` through
  as well, a name no detector knows. To send what is under a path as stored, name it:
  `payment.card.number`, or `payment.*` for everything under `payment`. Of two `keep`s the one
  on the path itself decides.
- The elements of a list are named by the list's own path: `Items.Sku` is the `Sku` of every
  item, whichever way a filter spells the position — `Items[0].Sku`, `Items.0.Sku`,
  `Items[' 0'].Sku`. What counts as a position is asked of the query engine itself, so the
  rules and the engine cannot read one spelling two ways.
- A rule may be limited to some services. An event of another service is then answered as if the
  rule were not there. A filter or a table is not about one service but about every service its
  profile reads, so there a field counts as hidden when a rule hides it in *any* of them — and a
  `keep` counts only when it holds for all of them. A rule for a service the profile does not
  read is nothing to that profile: not in what it is shown, and not in what it may ask.
- When several rules match, the most protective wins: remove, then hash, then keepLast4, then keep.

A new installation starts with three `remove` rules: `*Password*`, `*Secret*`, `Authorization`.

### Detectors

Detectors find values by their shape in every string no rule has claimed: property values, the
message template (a template built by string interpolation carries its values in its text), and an
exception's message and stack trace.

| Detector | Finds | On by default |
|----------|-------|---------------|
| Card number | 13–19 digits, spaces or dashes allowed, that pass the Luhn check | yes |
| Email address | | yes |
| Phone number | `+` and 10–15 digits with common separators; the national spelling of `+7 7xx` numbers | yes |
| IIN / BIN | 12 digits with the check digit | yes |
| Bearer and JWT tokens | a JWT, or the credential after `Bearer` | yes |
| IBAN | passes the modulo-97 check | no |

A number that is a card number or an IIN is hidden as well, whether it is stored as an integer or
as a floating-point number. So is a property's *name* when it holds a value — a map keyed by email
address or by card number. A detector does not know what a name, an address or an internal
identifier looks like: name those fields in a rule, or use Strict mode.

**Detectors work on answers, not on questions.** They do not make a field a hidden one. A filter
may still order, pattern-match and aggregate a field no rule names, and so learn about a value a
detector would have hidden in the answer. What must not be asked about needs a rule — or Strict
mode, where every field is hidden unless a `keep` names it or a path above it.

### Modes

- **Standard** — hide what the rules name and what the detectors find.
- **Strict** — hide every string and number, except the properties a `keep` rule names, which go
  out as stored, and what is spelled under them, which goes past the detectors. Booleans and
  nulls pass. The message template, exception texts and property names still go through the
  detectors.

### Tokens

A token is `h:` and twelve hex digits: the first six bytes of HMAC-SHA256 over the value, with a
secret the server generates on first start and keeps encrypted in its database.

- **A value hidden the same way has the same token**, in every answer and for every profile. The
  assistant can tell that two events concern the same customer, and can count and group by the
  token.
- **A number and its text are one token.** A value a *detector* found is hashed in a normal form
  — a card number without its spaces, an email address in lower case — so two spellings of it are
  one token. A value a *rule* hides is hashed as it is stored: `4111 1111 1111 1111` under a
  `hash` rule and the same card found by the detector in another property's text are two tokens.
  Where tokens have to match across fields, store the value in one spelling.
- **A token cannot be turned back into its value** without the secret. Rotating the secret
  (Settings → Masking, or `POST /api/mcp/secret/rotate`) changes every token.

**Searching by a token.** In `CardNumber = 'h:92f7e6d86459'` the server puts the value in the
token's place, so the search uses the index as any other. It does that in the parsed filter, where
the token is the value of `=`, `!=` or `in`, and nowhere else: a token in a column heading, in a
property name or inside a longer string stays the text it is, and a token under anything but
equality — `Elapsed > 'h:…'`, `startsWith(Path, 'h:…')`, a plain word — is refused, because it
would order or match a visible value against the hidden one. The filter that runs is written out
from the checked tree and read back before it is run; it is never the caller's text with pieces
replaced.

**A token is compared only where its value is hidden wherever it is read.** A token finds the
events that hold its value, and every call after it reads those events again. Compared with a
field that shows its values as stored — `Attempt = 'h:…'` — it would find the value lying in the
open there, and the next `logs_context`, a `sum` with a `count` beside it, or a run of
`Attempt > n` would read it. So a token may stand only against:

- a property hidden whole in every service the profile reads: in each of them a `hash` or a
  `remove` decides it (on the property or on a path above it), or the profile is in Strict mode
  and no rule shows it. A profile of one service is judged by that service's rules; a profile
  that reads every service needs a rule that holds for every service. `logs_fields` marks such a
  property `= only`;
- for a value a detector hides whole wherever an equality can find it — a card number, an IIN,
  an email address — any property no `keep` or `keepLast4` rule names in a service the profile
  reads: the detector hides it there as well, under a `keep` on a path above it too. An
  equality compares texts without regard to case, and a text
  that reads as a number with that number in a numeric field; so not an IBAN or a JWT, which the
  detectors know by the case of their letters, not a phone number (`'+77015550123'` equals
  `77015550123`, and no detector knows a phone number stored as a number), and not an IIN with a
  leading zero (it equals an eleven-digit number). Nor a text a detector hides only a part of:
  under a `hash` rule, `card 4111… failed` found in an open field would be shown with only the
  number gone.

Anything else is refused with a sentence that says so, before anything is read: a property no rule
hides, one a rule hides in only some of the services the profile reads (`= only, no token` in
`logs_fields`), one a `keep` or a `keepLast4` shows in any of them — the last four characters are
the whole of a short value — and a built-in field (`@service = 'h:…'`, `@x.type = 'h:…'`), which no
hidden value is kept in and every answer shows as it is.

A path is judged in every way the engine can read it. `Items.0.Sku` is the first element of a
list (which the rules name `Items.Sku`) or the property `Sku` of a property named `0`, and
`client.id` an object's property or one name: a token is compared only if the value is hidden in
all of them, and a filter is held to "equality only" if it is hidden in any. (A path with more
than six segments that read as positions is not read out every way: it counts as hidden, and
takes no token.)

The events a token finds are shown as any answer shows them. An open field that happens to hold
the same value is shown as stored, not as the token: shown as the token, it would say which open
value the hidden one is, and the next answer — which shows that field as stored — would say what
it is.

For this the server remembers, in memory only, the values of the tokens it handed out recently —
up to 40 000 of them, none longer than 256 characters. A token it no longer remembers (an old one,
or any after a restart) is refused with a sentence telling the assistant to run the search again.

### What a filter may ask about a hidden field

A filter is a question, and `CardNumber like '4400%'` tells, by whether anything comes back, that
a card number starts with 4400. So a hidden field may be tested with `=`, `!=`, `in` and `has()`
only — against a token, or against a value the person already has. Anything else (`like`,
`contains`, `startsWith`, `<`, `length()`, `regexMatch()`, a comparison with another field) is
refused, and so is an aggregate other than a count: `min(CardNumber)` is a card number.

A field is hidden by every road to it: under a rule on its parent or on the first part of its
name, under a name that is empty, in any service the profile reads, through a list position
however it is spelled. And nothing
reads a hidden value through its parent — the functions that could (`toJson()`, `elementAt()`,
`keys()`, `values()`, `fromJson()`) are not among those a filter here is built from.

### Aggregates

- **A hidden field can be counted and grouped by, nothing else.** `count(CardNumber)` and
  `group by CardNumber` work; the group keys come back as tokens.
- **`sum`, `min`, `max` and `avg` are made of measurements.** Two kinds of stored value take
  no part in them, in a field no rule names:
  - *a text that reads as a number* — more often than not an identifier: a phone number, an IIN
    with its leading zeros. It would come back as a measurement, its spelling gone and with it
    everything a detector knows it by;
  - *a number a search would hide* — a card number or an IIN by its digits. Hiding the result is
    not enough for these: the sum of two IINs is no IIN, and three sums of pairs are three
    equations in three unknowns.

  The answer says how many values were left out, so a sum that lacks some of its values does not
  pass for a whole one. `count` counts every value, whatever it is. A field a `keep` rule names
  for every service the profile reads is taken whole, as it is shown whole; one that is only
  under a `keep` is not.
- **A table's key shows what a search shows of the value, and no more.** Under `keepLast4` it is
  the last four characters — unless the rule holds for only some of the services the profile
  reads and a detector finds something in the value: the other services hide it whole, and so
  does the table.
- **The number an aggregate comes to goes past the detectors** as well, as a number does in a
  search, and a whole number too long for the arithmetic to hold exactly is not shown at all: its
  leading digits would be right, and most of an identifier.

### What this does not do

- **Detectors do not restrict questions**, only answers — see [Detectors](#detectors). In
  Standard mode a field no rule names can be ordered and pattern-matched, and a value a detector
  hides in the answer can be learnt that way, a comparison at a time.
- **Equality can be guessed at.** `CardNumber = '4111…'` with a guessed value answers yes or no,
  and `in` asks about several values at once. A field with few possible values — a status, a
  small number, a yes or no — is found out this way whatever hides it. The calls-per-minute limit
  and the record of every call are what stand against it.
- **The text of events is searched as it is — in Strict mode too.** Plain words, `like`,
  `contains` and `regexMatch` work on the message template and on an exception's message and
  stack trace: they are what an investigation reads, and no rule and no mode hides them. A value
  that is *in* such a text — a card number a driver put into its error message, an address a
  service interpolated into its template — is replaced by a token in the answer, and can still be
  searched for piece by piece: `@x.message like '%Card 4%'`, then `'%Card 41%'`. For a template
  the cure is to log values as properties. For an exception's text there is none here; a profile
  that must not have this gets free-form filters switched off and published tools instead.
- **A rule hides a field, not a value.** A value hidden in one field and logged in the open in
  another is in the open there, to any search that reads that field. A token is not compared with
  such a field (see [Tokens](#tokens)), but nothing stops a search that does not go by the token.
  Hide every field the value is logged in: a pattern without a dot covers a name at any depth.
- **The shape of a hidden value can be asked about.** `has(Recipients[2])` says whether a hidden
  list has a third element, `has(Limits['a@example.com'])` whether a hidden map has that key, and
  `Customer = Payer` whether two objects are equal, hidden fields and all. Each is one yes or no,
  of the same kind as a guessed equality. So is what a refusal tells: a token compared with a
  field no rule hides is let through only when its value is a card number, an IIN or an email
  address, so whether a value hidden by a rule has one of those shapes can be told.
- **Built-in fields are never hidden**: the timestamp, level, service, trace and span ids, event
  references.

## Limits

| Limit | Default | |
|-------|---------|---|
| Response budget | 8 000 tokens | Per answer. |
| Widest time window | 7 days | |
| Events per call | 500 | |
| Calls per key | 60 / min | Every call counts, one that is refused included. |
| AI searches at once | 2 | A third call waits up to 10 s for a place, then is refused. |
| Filter or query | 4 000 characters | Fixed. Parentheses nest at most 32 deep. |
| Arguments of one call | 6 000 characters of JSON | Fixed. |

The last two are what the record of calls keeps of a call, whole. A call is refused rather than
run with more than its record holds, so there is no part of a filter past the record's reach.

An AI call also takes one of the server's search slots and runs under the search time budget
(`Ameto:Query`), like a search from the Logs page. A call that runs out of time returns what it
found and says it is a part.

## The record of calls

Every call is written to the log store as an event of the service `ameto-mcp`, with the template
`AI call {Tool} by {Profile}: {Status}`:

| Property | |
|----------|---|
| `Tool`, `Profile`, `ProfileId` | |
| `Status` | `OK`, `Trimmed` (cut to the budget, or a table whose scan stopped early), `Refused`, `Timed out`, `Failed` |
| `Events`, `Masked`, `Tokens`, `ElapsedMs` | Events returned, values hidden, estimated size, time taken. |
| `MaskedBy` | Values hidden, by what hid them: `rules` (a field rule) and `strict` (Strict mode) count by field path, `detectors` by detector id — `cardNumber`, `email`, `phone`, `iin`, `secret`, `iban`, plus `unscannable` and `tooDeep` for a text or a value hidden whole because it could not be looked through. `ruleIds` counts the same values as `rules`, by the id of the rule that decided. |
| `Arguments` | As the assistant sent them, whole: a token stays a token. |
| `Filter` | The filter a search asked with. For a published tool, its template with the call's arguments in place — what the arguments alone do not show. As written and whole: a token in it is still a token, and the filter with the value in the token's place is not kept anywhere. Absent when the call had no filter. |
| `Refusal` | Why a refused call was refused. |
| `RefusalKind` | The same in one word: `input` (the arguments: a filter that does not parse, an unknown parameter, a bad cursor or time), `limit` (calls per minute, AI searches at once, the server's search slots), `masking` (a hidden field asked about in a way that would spell it out, or a token nobody was given), `scope` (a tool, a service or an event the profile does not reach), `unavailable` (the log store has shut down). An alert rule on `RefusalKind = 'masking'` watches for an assistant probing hidden values. |
| `Repeats` | On a call refused because the profile's minute was spent: which such refusal of that minute it is, from `1`. Only the 1st, 2nd, 4th, 8th… are written down, so a client that keeps asking fills no store, and the last record of a minute says about how many calls there were. Absent on every other call. |
| `Client` | The client's name and version, when it gives them, and the protocol revision it spoke: `my-agent 1.4 · MCP 2026-07-28`. |

They are ordinary events: `@service = 'ameto-mcp'` finds them on the Logs page, retention applies,
and an alert rule can watch them. The arguments and the filter are what the person or the
assistant typed, and may contain a value typed in full.

**The name `ameto-mcp` is the server's own.** An event that arrives over ingestion — CLEF or OTLP
— claiming that service is filed under `ingested:ameto-mcp` instead, so an ingest key cannot
write the record of a call that was never made. Nothing is dropped; the name says how it came.

## Settings API

Under `/api/mcp`. Reading needs the `manager` role, every change `admin`.

| Endpoint | |
|----------|---|
| `GET /api/mcp` | The section: `path`, `enabled`, `limits`, `detectors`, `tools` (the built-in ones, each with `enabled`), `secretCreatedAt`. |
| `PUT /api/mcp/settings` | `enabled`, `detectors` (ids), `disabledTools` (names of built-in tools), `responseBudgetTokens`, `maxWindowMinutes`, `maxEvents`, `callsPerMinute`, `maxConcurrent`. A field left out keeps its value. |
| `GET /api/mcp/summary` | The calls of the last `hours` (24 by default) added up: `calls`, `profiles`, `events`, `medianEvents` (of the calls that returned any), `masked` and `maskedBy` (`rules`, `strict`, `detectors`), `statuses`, `refusals` by kind, `unrecorded`, and the hidden values by rule id (`rules`) and by detector id (`detectors`). `calls` and `statuses` count the calls that were written down. Refusals past a profile's minute are counted by what their records say (`Repeats`), not by how many records there are: `refusals.limit` is at least that many, `unrecorded` is how many of them left no record of their own, and `thinned` is true whenever there are any — then every figure they are part of is a floor, `unrecorded` 0 included: a minute whose last record is its 2nd refusal may have had a 3rd. `partial` is true when the window holds more than 50 000 calls or the read ran out of time: the figures are then of the newest calls only. |
| `POST /api/mcp/secret/rotate` | A new token secret. |
| `GET /api/mcp/profiles` | Profiles, each with an 8-character preview of its key's hash and the ids of the published tools it is granted (`publishedTools`). |
| `POST /api/mcp/profiles` | Create: `name`, `description`, `toolsMode`, `tools`, `services`, `maskingMode`, `freeFilters`, `maxEvents`, `maxWindowMinutes`, `callsPerMinute`, `publishedTools`. Answers `{ profile, key }` — the only time the key is returned. |
| `PUT /api/mcp/profiles/{id}` | Change the profile's settings. A field left out keeps its value, as for `PUT /api/mcp/settings`: `{ "name": "…" }` renames the profile and changes nothing else. A list that is given is the whole list — `services: []` is every service, and a tool left out of `publishedTools` is withdrawn. `maskingMode` and `toolsMode` are names; a number is refused. |
| `GET /api/mcp/profiles/{id}/tools` | The tools the profile's key is offered, as `tools/list` answers it: `name`, `description`, `builtIn`, `inputSchema`. |
| `POST /api/mcp/profiles/{id}/rotate-key` | A new key; the old one stops working. |
| `DELETE /api/mcp/profiles/{id}` | |
| `GET /api/mcp/rules`, `POST /api/mcp/rules`, `PUT /api/mcp/rules/{id}`, `DELETE /api/mcp/rules/{id}` | Field rules: `field`, `services`, `action` (`hash`, `remove`, `keepLast4` or `keep`, by name). |
| `GET /api/mcp/tools` | `{ builtIn, published }`: the built-in tools, and the published ones with their definitions and input schemas. |
| `POST /api/mcp/tools`, `PUT /api/mcp/tools/{id}`, `DELETE /api/mcp/tools/{id}` | A [published tool](#published-tools). A definition that cannot work is a `400` with `error` and the `field` it is about. |
| `POST /api/mcp/tools/test` | `{ tool, profileId, arguments, id? }` → `{ text, isError, status, filter, events, masked, maskedBy, tokens, elapsedMs, definition }`, `maskedBy` in the three groups of the record of calls. Saves nothing. |
| `GET /api/mcp/tools/usage` | Calls by tool over the last `days` (7 by default), from the record of calls. |
| `POST /api/mcp/preview` | The most recent event matching `filter`, as stored (`stored`) and as an answer would carry it (`sent`) under `mode` (or a profile's), with what was hidden (`masked`: `total`, `rules`, `strict`, `detectors`). Each of `sent.properties` is `{ name, value, stored }`: `stored` is the name the property is stored under, which differs from `name` when the name itself held a value and went out as a token. Admin only: it returns the stored event. |
| `GET /api/mcp/activity` | The record of calls, newest first: `profile`, `tool`, `status`, `from`, `to`, `count`, and `afterTs` / `afterId` to continue. Each call carries the properties of [the record](#the-record-of-calls), `repeats` among them. |
