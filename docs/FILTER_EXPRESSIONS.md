# Seq Filter Expression Reference

Rd.Log implements the [Seq Filter Expression](https://docs.datalust.co/docs/filter-expressions) syntax.

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
