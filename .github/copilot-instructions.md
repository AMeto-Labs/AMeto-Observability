# Rd.Log — Copilot Instructions

Rd.Log is a **high-performance, self-hosted structured log server** — a self-contained alternative to Datalust Seq.
It ingests 100 000+ events/s via a zero-allocation hot path, stores data with LZ4-compressed cold segments, and ships as a single-binary .NET 10 process with an Angular 21 SPA.

See [ARCHITECTURE.md](../ARCHITECTURE.md) and [docs/](../docs/) for design rationale.

---

## Solution Layout

```
src/
  Rd.Log.Core/          ← Value types, core interfaces (IQueryExecutor, ISegmentProvider…)
  Rd.Log.Storage/       ← Hot/cold tier, WAL, LZ4 segments, NativeMemory arenas
  Rd.Log.Ingestion/     ← MPMC lock-free ring buffer, IngestionEndpoint, backpressure
  Rd.Log.Indexing/      ← Inverted index, bloom filter, trigram index (built post-flush)
  Rd.Log.Query/         ← Filter expression evaluation, SSE streaming, segment merging
  Rd.Log.Alerts/        ← Alert rules (SQLite), threshold eval, webhook/SMTP dispatch
  Rd.Log.Replication/   ← Symmetric segment replication, peer discovery
  Rd.Log.Otel/          ← OTLP/HTTP endpoints, JSON+Protobuf decoders, OtlpEndpointMapper
  Rd.Log.Tracing/       ← Separate trace storage, TraceQueryEndpointMapper
  Rd.Log.Metrics/       ← Separate metric storage (hot/cold .mts files), MetricQueryEndpointMapper
  Rd.Log.Serilog/       ← Serilog sink for self-ingestion
  Rd.Log.Server/        ← ASP.NET Core host, Program.cs, all *EndpointMapper registrations
tests/
  Rd.Log.Core.Tests/
  Rd.Log.Storage.Tests/
  Rd.Log.Query.Tests/
  Rd.Log.Integration.Tests/
  Rd.Log.Perf/          ← BenchmarkDotNet + CI smoke tests
client/                 ← Angular 21 SPA
```

---

## .NET Backend

### Stack
- **.NET 10**, **C# 14** (`<Nullable>enable</Nullable>`, `<AllowUnsafeBlocks>true</AllowUnsafeBlocks>` in storage layer)
- **Minimal APIs** — no MVC controllers; all routes registered in `*EndpointMapper.cs` static classes
- **No EF Core** — custom binary storage (`NativeMemory`, LZ4, MessagePack)
- **Auth:** JWT Bearer (8 h) for UI endpoints + in-memory API-key cache for the hot ingest path

### Module pattern — every module exposes two static classes

```csharp
// 1. DI registration  (src/Rd.Log.Foo/FooServiceExtensions.cs)
public static class FooServiceExtensions {
    public static IServiceCollection AddRdLogFoo(this IServiceCollection services) { ... }
}

// 2. Endpoint mapping  (src/Rd.Log.Foo/FooEndpointMapper.cs  OR  src/Rd.Log.Server/...)
public static class FooEndpointMapper {
    public static void MapFooEndpoints(this WebApplication app) { ... }
}
```

`Program.cs` chains them in dependency order:
```csharp
builder.Services
    .AddRdLogStorage()    // must be first
    .AddRdLogIndexing()   // after storage
    .AddRdLogIngestion()  // after storage
    .AddRdLogQuery();     // after storage + indexing
// ... alerts, tracing, metrics, auth, replication
app.MapRdLogEndpoints();
app.MapOtlpEndpoints();
// ... other mappers
```

### Core interfaces (`src/Rd.Log.Core/Interfaces.cs`)

Implement or inject these — do not bypass with concrete types:

| Interface | Responsibility |
|-----------|---------------|
| `IEventIngester` | Non-blocking `TryIngest(ReadOnlySpan<byte>)` — ring buffer entry |
| `ISegmentProvider` | `GetSegments(from, to)` + `OpenHotTierReader()` |
| `ISegmentReader` | `ReadEventsAsync()` on a single cold segment |
| `IQueryExecutor` | `ExecuteAsync(QueryRequest)` — hot + cold merge, filter evaluation |
| `ISegmentManager` | `FlushHotTierAsync()`, `DeleteSegmentAsync()`, `ListSegments()` |
| `ISegmentIndex` | Index lookup: inverted, bloom, trigram |

### Zero-allocation rules (hot path only — `Rd.Log.Ingestion`, `Rd.Log.Storage`)

- Use `ReadOnlySpan<byte>` / `Span<T>` for parsing; avoid intermediate arrays
- `LogEventHeader` is a **40-byte struct** (`[StructLayout(Sequential, Pack=1, Size=40)]`) — keep it that way
- `StringInternPool` deduplicates MessageTemplates — call `Intern()` instead of storing raw strings
- MPMC ring buffer uses only `Interlocked` — no locks, no `Monitor`
- These rules do **not** apply to query, alert, auth, or UI endpoints

### Metrics storage specifics

- `MetricDataPoint`: `{ TimestampUnixNano: long, Value: double, Count: long, Sum: double }`  
  `Value = Sum / Count` for histograms; `Count = 0` means no valid histogram data
- `OtlpEndpointMapper` serialises `count` as `null` when `Count == 0`
- Cold segments: `metrics-{name}-{minNano}-{maxNano}-raw.mts` (LZ4 + MessagePack)
- `QueryAsync` yields one `MetricSeries` per cold segment per label set — callers must merge by label if needed

### Error handling

```csharp
Results.Ok(data)                                      // 200
Results.BadRequest("message")                         // 400
Results.Unauthorized()                                // 401
StatusCodes.Status503ServiceUnavailable               // ring buffer full → client retries
_logger.LogError(ex, "Descriptive {StructuredKey}", value);
```

---

## Angular Frontend (`client/`)

### Stack
- **Angular 21** — standalone components only (`imports: [...]` inside `@Component`)
- **Signals** for all local state (`signal()`, `computed()`, `effect()`)
- **RxJS** only for HTTP / SSE streams from `ApiService`
- **No NgRx** — no global store
- `ChangeDetectionStrategy.OnPush` on every component; call `cdr.markForCheck()` after async updates
- **SCSS** per component; global CSS vars in `client/src/styles/`

### Component skeleton

```typescript
@Component({
  selector: 'app-foo',
  imports: [FormsModule, LucideAngularModule, /* … */],
  templateUrl: './foo.html',
  styleUrl: './foo.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class FooComponent implements OnInit, OnDestroy {
  private api = inject(ApiService);
  private cdr = inject(ChangeDetectorRef);

  data   = signal<FooDto[]>([]);
  filter = signal('');
  list   = computed(() => this.data().filter(d => d.name.includes(this.filter())));
}
```

### Service injection

Always use `inject()` — never constructor injection:
```typescript
private api  = inject(ApiService);
private auth = inject(AuthService);
```

### Routing

All routes are lazy-loaded via `loadComponent`:
```typescript
{ path: 'metrics', loadComponent: () => import('./pages/metrics/metrics').then(m => m.MetricsComponent) }
```
Protected routes use `canActivate: [authGuard]`.

### API service patterns

```typescript
// SSE stream — wrapped in Observable<T>
streamEvents(params): Observable<EventDto>

// Standard REST
getMetricNames(prefix?): Observable<string[]>
queryMetric(name, from?, to?, step?): Observable<MetricSeriesDto[]>
//   step is integer seconds as string, e.g. '300' (5 min)
//   server param: int? step → TimeSpan.FromSeconds(step.Value)
```

Token for SSE: `?access_token=<jwt>` query param (interceptor adds `Authorization` header for HTTP).

### CSS conventions

- Dark-theme CSS vars: `--bg-main: #0B0F17`, `--bg-card: #161E2E`, `--bg-elevated`, `--border`, `--text-primary`, `--text-secondary`, `--text-muted`, `--accent: #F59E0B`, `--success: #22C55E`, `--error: #EF4444`, `--info: #38BDF8`
- Icons: `lucide-angular` — import `LucideAngularModule` and reference by name string
- Charts: `chart.js` with `Chart.register(...registerables)` at module level
- SCSS follows BEM-style naming; prefer component-scoped styles, avoid global overrides

### Key models (`client/src/app/core/models/`)

| File | Exported types |
|------|---------------|
| `event.model.ts` | `EventDto`, `EventQueryParams`, `StatsDto` |
| `span.model.ts` | `SpanDto`, `TraceRowDto`, `TraceStatsDto` |
| `metric.model.ts` | `MetricSeriesDto`, `MetricPointDto` |
| `alert.model.ts` | `AlertRule`, `AlertRuleUpsertRequest` |
| `auth.model.ts` | `UserDto`, `ApiKeyDto`, `CreatedApiKeyDto` |

---

## Build & Test

```bash
# Backend
dotnet build                              # all projects
dotnet test tests/Rd.Log.Core.Tests
dotnet test tests/Rd.Log.Storage.Tests
dotnet test tests/Rd.Log.Query.Tests
dotnet test tests/Rd.Log.Integration.Tests
dotnet run -c Release --project tests/Rd.Log.Perf  # benchmarks

# Frontend
cd client && npm install && npm run build
npm test                                  # Vitest

# Combined publish (build + embed Angular into wwwroot)
./publish.ps1                            # interactive
./publish.ps1 -Restart                   # build + auto-restart server
```

Tests use **xUnit** (`[Fact]` / `[Theory]`). Perf tests use **BenchmarkDotNet**.

---

## Conventions to Follow

1. **No controllers** — always add endpoints in `*EndpointMapper.cs`
2. **No new base classes** — use interfaces + DI composition
3. **New module** → create `FooServiceExtensions.AddRdLogFoo()` + call it in `Program.cs` in the correct order
4. **Angular component** → one `.ts` / `.html` / `.scss` triple; standalone; OnPush
5. **Signals not subjects** for new Angular state; keep RxJS only for streams
6. **No inline styles** in Angular templates — use component SCSS or CSS vars
7. **Filter expressions** follow Seq/CLEF syntax — see [docs/FILTER_EXPRESSIONS.md](../docs/FILTER_EXPRESSIONS.md)
8. **Configuration** uses YAML + env overrides (`RdLog__DataDirectory`); see [docs/CONFIGURATION.md](../docs/CONFIGURATION.md)
