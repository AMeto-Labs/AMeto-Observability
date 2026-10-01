# Project guidance

## Stack
- Angular 21 client in `client/`: zoneless, standalone, state in `@ngrx/signals`, no SSR.
- Before writing or reviewing backend C#, load the `aspnetcore-developer` skill.
- Before writing or reviewing code in `client/`, load the `angular-developer` skill.

## Code style
- Zero-allocation rules apply to hot paths only: Ameto.Ingestion, Ameto.Otel decoders, Ameto.Storage, Ameto.Indexing, Ameto.Query, SSE writers.
- Outside hot paths (alerts, auth, settings and admin endpoints) plain C# is fine, including LINQ and anonymous-type `Results.*` responses.
- Hot or streaming JSON payloads use a source-generated `JsonSerializerContext`.

## Tests
- Run tests as CI does: `dotnet test -c Debug` at the repo root (all six projects, Ameto.Perf included).

## Prohibitions
- Do not change the GC settings in `src/Ameto.Server/Ameto.Server.csproj`: Workstation GC is deliberate for small hosts.
- No MVC controllers: new endpoints go into a `*EndpointMapper.cs`.
