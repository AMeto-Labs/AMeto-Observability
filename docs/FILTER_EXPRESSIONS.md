# Seq Filter Expression Reference

Ameto implements the [Seq Filter Expression](https://docs.datalust.co/docs/filter-expressions) syntax.

## Free-text search

Anything that isn't a valid expression is treated as **free text**: every
whitespace-separated term must appear (case-insensitive substring) in the rendered
message or a property value.

```
timeout                 # events whose text contains "timeout"
payment failed          # contains BOTH "payment" AND "failed"
```

Free text can't be mixed into an expression, but the expression language covers the
same intent (`ci_contains(@mt, 'timeout')`).

## Comparison operators

```
@l = 'Error'
StatusCode != 200
Elapsed > 500
Elapsed <= 100
UserId >= 'alice'
```

Properties are compared case-insensitively when both sides are strings.

## Logical connectives

```
@l = 'Error' and has(UserId)
@l = 'Error' or @l = 'Fatal'
not has(UserId)
(@l = 'Error' or @l = 'Fatal') and Elapsed > 1000
```

Operator precedence: `not` > `and` > `or`.

## Property paths

Nested properties are addressed with dot notation:

```
Headers.Foo = 'bar'
Request.User.Id = 42
```

When a segment contains characters that are **not** valid in an identifier
(hyphens, dots, spaces, slashes, …) use single-quoted bracket notation:

```
Headers['Api-Request-MerchantId'] = '6456116363'
Headers['X-Forwarded-For'] startsWith '10.'
Request.Cookies['session.id'] != null
```

Bracket and dot segments may be freely mixed:
`Outer['weird-key'].Inner['another-weird'].Value`.

Inside the brackets, escape a literal single quote with `\'`.

> Top-level property indexes only cover the first segment, so filters on
> nested paths are evaluated by full segment scan (without the bloom/inverted
> index pre-filter). They are still trigram-accelerated when the right-hand
> side is a string literal.

## Built-in CLEF fields

| Field | Alias | Description |
|-------|-------|-------------|
| `@l` | `Level` | Log level string: `Verbose`, `Debug`, `Information`, `Warning`, `Error`, `Fatal`. |
| `@mt` | `MessageTemplate` | The raw message template. |
| `@m` | `Message` | Alias of `@mt` (server-side rendered messages are not stored — rendered on the client). |
| `@x` | `Exception` | Top-level exception type name (`null` when no exception). |
| `@x.type` | | Exception type (string). |
| `@x.message` | | Exception message (string). |
| `@x.stack` | | Exception stack trace (string). |
| `@x.inner.type` | | Inner exception type. |
| `@x.inner.message` | | Inner exception message. |
| `@x.exists` | | `true` when the event has an exception. |
| `@t` | `Timestamp` | ISO-8601 timestamp string. |

## Functions

### `has(Property)` / `isDefined(Property)`

Returns `true` if the property is present and non-null.

```
has(UserId)
isDefined(RequestId)
not has(Exception)
```

### `startsWith(Property, 'prefix')`

Case-sensitive prefix match.

```
startsWith(@mt, 'User')
```

### `ci_startsWith(Property, 'prefix')`

Case-insensitive prefix match.

```
ci_startsWith(Path, '/api/admin')
```

### `contains(Property, 'substring')`

Case-sensitive substring search.

```
contains(@x, 'NullReferenceException')
contains(@x.message, 'timeout')
@x.type = 'System.InvalidOperationException'
@x.exists = true and @x.inner.type = 'System.IO.IOException'
```

### `ci_contains(Property, 'substring')`

Case-insensitive substring search.

```
ci_contains(@mt, 'failed')
```

### `endsWith(Property, 'suffix')`

Case-sensitive suffix match.

```
endsWith(@mt, 'completed')
```

### `length(Property)` (in comparisons)

Returns string/array length and is used with comparison operators.

```
length(UserId) > 3
length(Tags) >= 2
```

### `toJson(Property)` (in comparisons)

Serializes the property value to JSON string.

```
toJson(StatusCode) = '200'
toJson(Payload) like '%"id":42%'
```

### `fromJson(Property)` (in comparisons)

Parses JSON from string property and compares parsed value.

```
fromJson(JsonValue) = 42
fromJson(JsonFlag) = true
```

### `coalesce(arg0, arg1, ..., fallback)` (in comparisons)

Returns the first non-null argument.

```
coalesce(UserId, RequestId, 'anonymous') = 'anonymous'
```

### `toLower(Property)` / `toUpper(Property)` (in comparisons)

String case conversion.

```
toLower(UserName) = 'alice'
toUpper(UserName) = 'ALICE'
```

### `toNumber(Property)` (in comparisons)

Parses string/number property into numeric value.

```
toNumber(StatusCode) >= 400
```

### `substring(Property, start, length?)` (in comparisons)

Extracts substring from string property.

```
substring(Path, 1, 3) = 'api'
substring(Path, 1) like 'api%'
```

### `indexOf(Property, 'text')` / `lastIndexOf(Property, 'text')` (in comparisons)

Returns zero-based index or -1 when not found.

```
indexOf(Path, '/api') = 0
lastIndexOf(Path, '/') >= 0
```

### `replace(Property, 'from', 'to')` (in comparisons)

Replaces all literal occurrences.

```
replace(Path, '/api', '/v1') = '/v1/users'
```

### `concat(arg0, arg1, ...)` (in comparisons)

Concatenates string arguments.

```
concat(FirstName, ' ', LastName) = 'John Smith'
```

### `datePart(Property, 'part', offsetHours?)` (in comparisons)

Supported parts: `year`, `month`, `day`, `hour`, `minute`, `second`, `weekday`.

```
datePart(StartedAt, 'hour', 0) = 10
```

### `timeOfDay(Property, offsetHours)` (in comparisons)

Returns time-of-day as ticks.

```
timeOfDay(StartedAt, 0) > 0
```

### `timeSpan(Property)` / `totalMilliseconds(Property)` (in comparisons)

`timeSpan()` parses a timespan string into ticks. `totalMilliseconds()` converts ticks/time-span text to milliseconds.

```
timeSpan(DurationText) > 0
totalMilliseconds(DurationTicks) >= 1000
```

### `toTimeString(Property)` / `toHexString(Property)` (in comparisons)

```
toTimeString(DurationTicks) = '00:00:01'
toHexString(StatusCode) = '0xc8'
```

### `bucket(Property, error)` (in comparisons)

```
bucket(Duration, 0.1) > 0
```

### `offsetIn('TimeZoneId', InstantProperty)` (in comparisons)

Returns offset ticks for timezone at instant.

```
offsetIn('UTC', StartedAt) = 0
```

### `arrived(Property)` (in comparisons)

Can be used with `@id`.

```
arrived(@id) > 0
```

## `in` operator

```
@l in ['Error', 'Fatal']
StatusCode in [400, 401, 403, 404]
```

## `like` operator

SQL-style wildcard match. `%` matches any sequence of characters.

```
@mt like '%timed out%'
Path like '/api/users/%'
```

## Examples

```
# All errors in the last hour with a UserId property
@l = 'Error' and has(UserId)

# Slow requests
Elapsed > 2000 and @l != 'Verbose'

# 4xx or 5xx responses
StatusCode >= 400

# Exceptions containing a specific type
contains(@x, 'SqlException')
@x.type = 'System.Data.SqlClient.SqlException'

# High-severity events from a specific service
ServiceName = 'payments' and @l in ['Error', 'Fatal']
```
