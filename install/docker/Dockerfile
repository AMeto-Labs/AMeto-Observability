# ── Stage 1: Build Angular SPA ────────────────────────────────────────────────
FROM node:22-alpine AS client-build

WORKDIR /build/client

# Install dependencies (cached layer)
COPY client/package*.json ./
RUN npm ci --prefer-offline --no-audit --no-fund

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

WORKDIR /build

# Restore NuGet packages first (cached layer — only re-runs when csproj files change)
COPY Ameto.slnx ./
COPY src/ ./src/

RUN dotnet restore src/Ameto.Server/Ameto.Server.csproj \
        --runtime linux-musl-x64

# Copy Angular output into wwwroot before publish
COPY --from=client-build /build/wwwroot ./src/Ameto.Server/wwwroot/

# linux-musl-x64        → Alpine runtime base (tiny vs Debian)
# InvariantGlobalization → drop the ~28 MB ICU dependency (app parses invariantly)
RUN dotnet publish src/Ameto.Server/Ameto.Server.csproj \
        --configuration Release \
        --output /publish \
        --runtime linux-musl-x64 \
        --self-contained true \
        /p:DebugType=None \
        /p:DebugSymbols=false \
        /p:GenerateDocumentationFile=false \
        /p:InvariantGlobalization=true \
        --no-restore

# Strip residual debug / doc files
RUN find /publish -name '*.pdb' -delete && \
    find /publish -name '*.xml' -delete && \
    find /publish -name 'web.config' -delete && \
    find /publish -maxdepth 1 -type d -regextype posix-extended \
         -regex '.*/[a-z]{2}(-[A-Z]{2})?$' -exec rm -rf {} + 2>/dev/null || true

# ── Stage 3: Runtime image ────────────────────────────────────────────────────
# Alpine runtime-deps: musl libc + only the native libs a self-contained .NET
# binary needs, on a ~12 MB base (vs ~190 MB Debian). No ICU is shipped — the app
# is published with InvariantGlobalization so it doesn't need it.
FROM mcr.microsoft.com/dotnet/runtime-deps:10.0-alpine

LABEL org.opencontainers.image.title="Ameto Server" \
      org.opencontainers.image.description="High-performance self-hosted structured log server" \
      org.opencontainers.image.licenses="MIT"

# jemalloc — replaces musl's allocator to keep ingestion-path RSS in check.
# wget for the HEALTHCHECK is already provided by BusyBox (no install needed).
RUN apk add --no-cache jemalloc

# Dedicated non-root user — pin uid 999 to match data volumes created by the prior
# (Debian/Ubuntu) image, whose system user was uid 999. The /data files are owner-
# writable (-rw-r--r--), so matching the OWNER uid is what matters; gid 999 is already
# taken by 'ping' on Alpine, so the group keeps an auto-assigned id. Without the pin,
# BusyBox picks uid 100 and the process can't write existing files → SQLite
# "attempt to write a readonly database".
RUN addgroup ameto \
 && adduser -D -H -u 999 -G ameto ameto

WORKDIR /app
COPY --from=server-build /publish ./
RUN chmod +x ./Ameto.Server

# Data volume — persists log/metric/trace segments + SQLite (Ameto.db) across restarts.
RUN mkdir -p /data && chown ameto:ameto /data
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
