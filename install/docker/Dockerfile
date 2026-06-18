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
        --runtime linux-x64

# Copy Angular output into wwwroot before publish
COPY --from=client-build /build/wwwroot ./src/Ameto.Server/wwwroot/

RUN dotnet publish src/Ameto.Server/Ameto.Server.csproj \
        --configuration Release \
        --output /publish \
        --runtime linux-x64 \
        --self-contained true \
        /p:DebugType=None \
        /p:DebugSymbols=false \
        /p:GenerateDocumentationFile=false \
        --no-restore

# Strip residual debug / doc files
RUN find /publish -name '*.pdb' -delete && \
    find /publish -name '*.xml' -delete && \
    find /publish -name 'web.config' -delete && \
    find /publish -maxdepth 1 -type d -regextype posix-extended \
         -regex '.*/[a-z]{2}(-[A-Z]{2})?$' -exec rm -rf {} + 2>/dev/null || true

# ── Stage 3: Runtime image ────────────────────────────────────────────────────
# mcr.microsoft.com/dotnet/runtime-deps provides the exact native libs that
# self-contained .NET binaries need (libssl, libicu, libstdc++, etc.) without
# bundling the managed .NET runtime — the binary already contains it.
FROM mcr.microsoft.com/dotnet/runtime-deps:10.0

LABEL org.opencontainers.image.title="Ameto Server" \
      org.opencontainers.image.description="High-performance self-hosted structured log server" \
      org.opencontainers.image.licenses="MIT"

# Dedicated non-root user
RUN groupadd --system ameto && useradd --system --no-create-home --gid ameto ameto

WORKDIR /app
COPY --from=server-build /publish ./
RUN chmod +x ./Ameto.Server

# Data volume — persists log segments across container restarts
RUN mkdir -p /data && chown ameto:ameto /data
VOLUME ["/data"]

# Default config — mounts over the baked-in config.yml at runtime
# (see docker-compose.yml for how to supply your own config.yml)

EXPOSE 5341

USER ameto

ENTRYPOINT ["./Ameto.Server"]
