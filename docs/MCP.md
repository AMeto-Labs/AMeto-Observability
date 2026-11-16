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
| Calls per minute | 30 | Counted per calendar minute. A call past it is refused with the seconds left. |

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
| `logs_fields` | The properties found in recent events: how often, which type, sample values, whether hidden. Ends with a filter-syntax reference. |

A built-in tool can be switched off for the whole server (Settings → AI / MCP → Tools, or
`disabledTools` in `PUT /api/mcp/settings`). It is then offered to no profile, whatever the
profile's own list says: not in a tool list, and refused when called by name.

**Time.** `from` and `to` take ISO-8601 timestamps or relative values: `-15m`, `-2h`, `-1d`, `-1w`,
`now`. The default window is the last hour. Times are UTC.

**Filters** are the [filter expressions](FILTER_EXPRESSIONS.md) of the Logs page, plain words
included.

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
marks an answer `PARTIAL` when the scan stopped early — the numbers are then floors.
`logs_context` looks one hour either side of the event for its neighbours and 30 minutes either
side for its trace. `logs_fields` reads up to the 1 000 most recent events of the window and lists
the 60 most frequent properties.

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
| `filter` | A [filter expression](FILTER_EXPRESSIONS.md) with `{name}` where a parameter's value goes. |
| `fields` | The properties an answer carries. Empty: all of them. |
| `defaultWindowMinutes`, `maxEvents`, `order` | The window a call gets when it names none; the most events of one answer (the profile's own limit still applies); `newest` or `oldest` first. |
| `published` | `false` keeps a draft: saved and editable, offered to nobody. |
| `profiles` | The ids of the profiles that may call it. The same grant can be set from the profile's side, as its `publishedTools`. |

Every published tool also takes `from`, `to` and `cursor`, as `logs_search` does, and answers in
the same form.

**A parameter is a value, whatever it contains.** The server puts it into the filter as a literal
— quoted and escaped for a string, checked for a number, matched against the list for an enum —
so a value cannot add a condition, close a parenthesis or name another field.
`x' or @l = 'Error` is searched for as that text.

**Placeholders are read outside quotes only.** A message template is full of braces, and
`@mt = 'Payment {PaymentId} declined'` is an ordinary filter: a brace inside quotes is text. So a
parameter stands where a whole value stands — `MerchantId = {merchantId}`,
`startsWith(Path, {prefix})` — never inside one.

**An optional parameter that is not given takes its clause with it.** The filter above, called
without `reason`, runs as `@service = 'payments-api' and MerchantId = …`. That works per top-level
`and` clause, so a filter with an `or` at its top level cannot have an optional parameter without
a default: put the alternatives in parentheses.

**The rules for hidden values hold.** A tool's filter is checked like any filter the assistant
could write — it may not read a hidden field with anything but `=`, `!=`, `in` or `has()`, and a
definition that does is refused when it is saved. Answers are masked as every answer is, and a
token is a valid argument: `payments_by_card(card: "h:92f7e6d86459")` finds that card's payments.

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
| `keep` | The value goes out as stored; the detectors leave it alone. The allow-list of Strict mode. |

- The field is a property path as a filter spells it — `Customer.Iin` — with `*` for any run of
  characters, compared without regard to case.
- A pattern without a dot is also tried against the last segment of a path: `CardNumber` hides
  `CardNumber` and `Payment.CardNumber`. A pattern with a dot matches whole paths only.
- A rule on a property that holds an object or a list hides the whole value under one token.
- A rule may be limited to some services.
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

An integer property that is a card number or an IIN is hidden as well. A detector does not know
what a name, an address or an internal identifier looks like: name those fields in a rule, or use
Strict mode.

### Modes

- **Standard** — hide what the rules name and what the detectors find.
- **Strict** — hide every string and number, except the properties a `keep` rule names. Booleans
  and nulls pass. The message template and exception texts still go through the detectors.

### Tokens

A token is `h:` and twelve hex digits: the first six bytes of HMAC-SHA256 over the value, with a
secret the server generates on first start and keeps encrypted in its database.

- **One value, one token**, in every answer and for every profile. The assistant can tell that two
  events concern the same customer, and can count and group by the token.
- **A number and its text are one token**; so are a card number with and without spaces, and an
  email address in either case.
- **A token cannot be turned back into its value** without the secret. Rotating the secret
  (Settings → Masking, or `POST /api/mcp/secret/rotate`) changes every token.

**Searching by a token.** `CardNumber = 'h:92f7e6d86459'` in a filter is replaced by the value on
the server, so the search uses the index as any other. For this the server remembers, in memory
only, the values of the tokens it handed out recently — up to 40 000 of them, none longer than 256
characters. A token it no longer remembers (an old one, or any after a restart) is refused with a
sentence telling the assistant to run the search again.

### What a filter may ask about a hidden field

A filter is a question, and `CardNumber like '4400%'` tells, by whether anything comes back, that
a card number starts with 4400. So a hidden field may be tested with `=`, `!=`, `in` and `has()`
only — against a token, or against a value the person already has. Anything else (`like`,
`contains`, `startsWith`, `<`, `length()`, `substring()`, …) is refused, and so is an aggregate
other than a count: `min(CardNumber)` is a card number.

### What this does not do

- **Plain-word search is not restricted.** Words are matched against the text of events, hidden
  property values included, so a search can confirm that a word occurs. A profile that must not
  have even that gets free-form filters switched off.
- **Equality can be guessed at.** `CardNumber = '4111…'` with a guessed value answers yes or no.
  The calls-per-minute limit and the record of every call are what stand against it.
- **Built-in fields are never hidden**: the timestamp, level, service, trace and span ids, event
  references.

## Limits

| Limit | Default | |
|-------|---------|---|
| Response budget | 8 000 tokens | Per answer. |
| Widest time window | 7 days | |
| Events per call | 500 | |
| Calls per key | 60 / min | |
| AI searches at once | 2 | A third call waits up to 10 s for a place, then is refused. |

An AI call also takes one of the server's search slots and runs under the search time budget
(`Ameto:Query`), like a search from the Logs page. A call that runs out of time returns what it
found and says it is a part.

## The record of calls

Every call is written to the log store as an event of the service `ameto-mcp`, with the template
`AI call {Tool} by {Profile}: {Status}`:

| Property | |
|----------|---|
| `Tool`, `Profile`, `ProfileId` | |
| `Status` | `OK`, `Trimmed` (cut to the budget), `Refused`, `Timed out`, `Failed` |
| `Events`, `Masked`, `Tokens`, `ElapsedMs` | Events returned, values hidden, estimated size, time taken. |
| `MaskedBy` | Values hidden, by what hid them: `rules` (a field rule) and `strict` (Strict mode) count by field path, `detectors` by detector id — `cardNumber`, `email`, `phone`, `iin`, `secret`, `iban`, plus `unscannable` and `tooDeep` for a text or a value hidden whole because it could not be looked through. `ruleIds` counts the same values as `rules`, by the id of the rule that decided. |
| `Arguments` | As the assistant sent them: a token stays a token. |
| `Filter` | The filter a search asked with. For a published tool, its template with the call's arguments in place — what the arguments alone do not show. As written: a token in it is still a token, and the filter with the value in the token's place is not kept anywhere. Absent when the call had no filter. |
| `Refusal` | Why a refused call was refused. |
| `RefusalKind` | The same in one word: `input` (the arguments: a filter that does not parse, an unknown parameter, a bad cursor or time), `limit` (calls per minute, AI searches at once, the server's search slots), `masking` (a hidden field asked about in a way that would spell it out, or a token nobody was given), `scope` (a tool, a service or an event the profile does not reach), `unavailable` (the log store has shut down). An alert rule on `RefusalKind = 'masking'` watches for an assistant probing hidden values. |
| `Client` | The client's name and version, when it gives them, and the protocol revision it spoke: `my-agent 1.4 · MCP 2026-07-28`. |

They are ordinary events: `@service = 'ameto-mcp'` finds them on the Logs page, retention applies,
and an alert rule can watch them. The arguments and the filter are what the person or the
assistant typed, and may contain a value typed in full.

## Settings API

Under `/api/mcp`. Reading needs the `manager` role, every change `admin`.

| Endpoint | |
|----------|---|
| `GET /api/mcp` | The section: `path`, `enabled`, `limits`, `detectors`, `tools` (the built-in ones, each with `enabled`), `secretCreatedAt`. |
| `PUT /api/mcp/settings` | `enabled`, `detectors` (ids), `disabledTools` (names of built-in tools), `responseBudgetTokens`, `maxWindowMinutes`, `maxEvents`, `callsPerMinute`, `maxConcurrent`. A field left out keeps its value. |
| `GET /api/mcp/summary` | The calls of the last `hours` (24 by default) added up: `calls`, `profiles`, `events`, `medianEvents` (of the calls that returned any), `masked` and `maskedBy` (`rules`, `strict`, `detectors`), `statuses`, `refusals` by kind, and the hidden values by rule id (`rules`) and by detector id (`detectors`). `partial` is true when the window holds more than 50 000 calls or the read ran out of time: the figures are then of the newest calls only. |
| `POST /api/mcp/secret/rotate` | A new token secret. |
| `GET /api/mcp/profiles` | Profiles, each with an 8-character preview of its key's hash and the ids of the published tools it is granted (`publishedTools`). |
| `POST /api/mcp/profiles` | Create: `name`, `description`, `toolsMode`, `tools`, `services`, `maskingMode`, `freeFilters`, `maxEvents`, `maxWindowMinutes`, `callsPerMinute`, `publishedTools`. Answers `{ profile, key }` — the only time the key is returned. |
| `PUT /api/mcp/profiles/{id}` | Replace the profile's settings. `publishedTools` is the whole list of granted tools — a tool left out of it is withdrawn; without the field the grants stay as they are. |
| `GET /api/mcp/profiles/{id}/tools` | The tools the profile's key is offered, as `tools/list` answers it: `name`, `description`, `builtIn`, `inputSchema`. |
| `POST /api/mcp/profiles/{id}/rotate-key` | A new key; the old one stops working. |
| `DELETE /api/mcp/profiles/{id}` | |
| `GET /api/mcp/rules`, `POST /api/mcp/rules`, `PUT /api/mcp/rules/{id}`, `DELETE /api/mcp/rules/{id}` | Field rules: `field`, `services`, `action`. |
| `GET /api/mcp/tools` | `{ builtIn, published }`: the built-in tools, and the published ones with their definitions and input schemas. |
| `POST /api/mcp/tools`, `PUT /api/mcp/tools/{id}`, `DELETE /api/mcp/tools/{id}` | A [published tool](#published-tools). A definition that cannot work is a `400` with `error` and the `field` it is about. |
| `POST /api/mcp/tools/test` | `{ tool, profileId, arguments, id? }` → `{ text, isError, status, filter, events, masked, maskedBy, tokens, elapsedMs, definition }`, `maskedBy` in the three groups of the record of calls. Saves nothing. |
| `GET /api/mcp/tools/usage` | Calls by tool over the last `days` (7 by default), from the record of calls. |
| `POST /api/mcp/preview` | The most recent event matching `filter`, as stored (`stored`) and as an answer would carry it (`sent`) under `mode` (or a profile's), with what was hidden (`masked`: `total`, `rules`, `strict`, `detectors`). Admin only: it returns the stored event. |
| `GET /api/mcp/activity` | The record of calls, newest first: `profile`, `tool`, `status`, `from`, `to`, `count`, and `afterTs` / `afterId` to continue. |
