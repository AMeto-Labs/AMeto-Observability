# syntax=docker/dockerfile:1
# BuildKit is required (default in modern Docker) for the --mount=type=cache
# package caches and COPY --chmod below.

# ── Stage 1: Build Angular SPA ────────────────────────────────────────────────
FROM node:22-alpine AS client-build

WORKDIR /build/client

# Install dependencies (cached layer). The npm cache is a BuildKit cache mount,
# so a changed lockfile re-links from cache instead of re-downloading the world.
COPY client/package*.json ./
RUN --mount=type=cache,target=/root/.npm \
    npm ci --prefer-offline --no-audit --no-fund

# Build production bundle
COPY client/ ./
RUN npx ng build --configuration production --output-path /build/wwwroot

# Angular 17+ outputs assets to browser/ — flatten to wwwroot root
RUN if [ -d /build/wwwroot/browser ]; then \
        mv /build/wwwroot/browser/* /build/wwwroot/ && \
        rmdir /build/wwwroot/browser; \
    fi

# ── Stage 2: Build .NET Server ────────────────────────────────────────────────
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS server-build

ENV DOTNET_NOLOGO=1 \
    DOTNET_CLI_TELEMETRY_OPTOUT=1

WORKDIR /build

# Restore first, from the project graph ONLY. Copying just the .slnx + every
# .csproj reachable from Ameto.Server (before any .cs) means the expensive
# `dotnet restore` layer is cached and only re-runs when a project file changes —
# editing source no longer re-downloads NuGet. Keep this list in sync with the
# Ameto.Server project reference graph.
COPY Ameto.slnx ./
COPY src/Ameto.Core/Ameto.Core.csproj             src/Ameto.Core/
COPY src/Ameto.Storage/Ameto.Storage.csproj       src/Ameto.Storage/
COPY src/Ameto.Indexing/Ameto.Indexing.csproj     src/Ameto.Indexing/
COPY src/Ameto.Query/Ameto.Query.csproj           src/Ameto.Query/
COPY src/Ameto.Ingestion/Ameto.Ingestion.csproj   src/Ameto.Ingestion/
COPY src/Ameto.Metrics/Ameto.Metrics.csproj       src/Ameto.Metrics/
COPY src/Ameto.Tracing/Ameto.Tracing.csproj       src/Ameto.Tracing/
COPY src/Ameto.Otel/Ameto.Otel.csproj             src/Ameto.Otel/
COPY src/Ameto.Alerts/Ameto.Alerts.csproj         src/Ameto.Alerts/
COPY src/Ameto.Replication/Ameto.Replication.csproj src/Ameto.Replication/
COPY src/Ameto.Server/Ameto.Server.csproj         src/Ameto.Server/

# The NuGet global-packages folder is a BuildKit cache mount — persisted across
# builds, so restore is near-instant once warm. It must be declared on BOTH the
# restore and the publish RUN (publish --no-restore reads packages from it).
RUN --mount=type=cache,target=/root/.nuget/packages \
    dotnet restore src/Ameto.Server/Ameto.Server.csproj \
        --runtime linux-musl-x64

# Now bring in the actual sources + the built SPA (these layers change often, but
# they sit AFTER the cached restore).
COPY src/ ./src/
COPY --from=client-build /build/wwwroot ./src/Ameto.Server/wwwroot/

# linux-musl-x64        → Alpine runtime base (tiny vs Debian)
# InvariantGlobalization → drop the ~28 MB ICU dependency (app parses invariantly)
# The residual .pdb/.xml/web.config/locale strip runs in the same layer so it
# never lands in an intermediate layer.
RUN --mount=type=cache,target=/root/.nuget/packages \
    dotnet publish src/Ameto.Server/Ameto.Server.csproj \
        --configuration Release \
        --output /publish \
        --runtime linux-musl-x64 \
        --self-contained true \
        /p:DebugType=None \
        /p:DebugSymbols=false \
        /p:GenerateDocumentationFile=false \
        /p:InvariantGlobalization=true \
        --no-restore \
 && find /publish -name '*.pdb' -delete \
 && find /publish -name '*.xml' -delete \
 && find /publish -name 'web.config' -delete \
 && find /publish -maxdepth 1 -type d -regextype posix-extended \
        -regex '.*/[a-z]{2}(-[A-Z]{2})?$' -exec rm -rf {} + 2>/dev/null || true

# ── Stage 3: Runtime image ────────────────────────────────────────────────────
# Alpine runtime-deps: musl libc + only the native libs a self-contained .NET
# binary needs, on a ~12 MB base (vs ~190 MB Debian). No ICU is shipped — the app
# is published with InvariantGlobalization so it doesn't need it.
FROM mcr.microsoft.com/dotnet/runtime-deps:10.0-alpine

LABEL org.opencontainers.image.title="Ameto Server" \
      org.opencontainers.image.description="High-performance self-hosted structured log server" \
      org.opencontainers.image.licenses="MIT"

# One layer for all image prep:
#   jemalloc  — replaces musl's allocator to keep ingestion-path RSS in check
#               (high-churn small allocations; purges aggressively). wget for the
#               HEALTHCHECK is already provided by BusyBox.
#   ameto user — dedicated non-root identity, uid pinned to 999 to match data
#               volumes created by the prior (Debian/Ubuntu) image whose system
#               user was uid 999. The /data files are owner-writable, so matching
#               the OWNER uid is what matters; gid 999 is already taken by 'ping'
#               on Alpine so the group keeps an auto-assigned id. Without the pin,
#               BusyBox picks uid 100 and the process can't write existing files
#               → SQLite "attempt to write a readonly database".
#   /data     — persisted volume for log/metric/trace segments + SQLite (Ameto.db).
RUN apk add --no-cache jemalloc \
 && addgroup ameto \
 && adduser -D -H -u 999 -G ameto ameto \
 && mkdir -p /data && chown ameto:ameto /data

WORKDIR /app
# --chmod makes Ameto.Server executable at copy time (no extra chmod layer).
COPY --from=server-build --chmod=755 /publish ./

VOLUME ["/data"]

# ── Configuration via environment variables ───────────────────────────────────
# Program.cs layers config sources as: config.yml  ->  env vars  ->  CLI args,
# so any baked-in YAML value is overridden by an env var using the standard
# .NET double-underscore convention:  Ameto:HttpPort  ==  Ameto__HttpPort
#
# These defaults make the image work out-of-the-box on the /data volume; override
# any of them (and the many others — see docker-compose.yml) at `docker run`/compose time.
ENV Ameto__DataDirectory=/data \
    Ameto__HttpPort=5341 \
    DOTNET_RUNNING_IN_CONTAINER=true \
    ASPNETCORE_ENVIRONMENT=Production \
    # Belt-and-suspenders with the publish-time InvariantGlobalization flag: the
    # Alpine base ships no ICU, so globalization must stay invariant or startup fails.
    DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=true \
    # Trade a little CPU for a tighter managed heap: the GC compacts/returns memory
    # more eagerly instead of letting the Workstation heap grow to its lazy working set.
    DOTNET_GCConserveMemory=5 \
    # ── Native allocator ──────────────────────────────────────────────────────
    # Preload jemalloc (Alpine path): the ingestion path's high-churn small
    # allocations fragment the default allocator and don't return freed memory to
    # the OS; jemalloc purges aggressively, keeping resident anon memory in check.
    LD_PRELOAD=/usr/lib/libjemalloc.so.2 \
    # Return dirty/muzzy pages to the OS within ~5 s; run a background purge thread.
    MALLOC_CONF=background_thread:true,dirty_decay_ms:5000,muzzy_decay_ms:5000

EXPOSE 5341

# BusyBox wget; shell form expands ${Ameto__HttpPort} so a runtime port override is honoured.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -q -O /dev/null "http://localhost:${Ameto__HttpPort:-5341}/health" || exit 1

USER ameto

ENTRYPOINT ["./Ameto.Server"]
