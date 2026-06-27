# Project guidance

## Skills — always use

- **angular-developer** — invoke for ANY Angular work (components, services, routing, forms, templates, SSR, testing, build config). Always follow its modern-Angular conventions (signals, standalone, zoneless, `@if`/`@for`, `inject()`).
- **aspnetcore-developer** — invoke for ANY ASP.NET Core / backend C# work. Always follow its zero-allocation / GC-free architecture rules (spans, `ValueTask`, `ArrayPool`, Minimal APIs, source-generated JSON).

When a task touches Angular frontend or ASP.NET Core backend code, load and apply the matching skill before writing or reviewing code.
