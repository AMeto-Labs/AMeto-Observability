#!/usr/bin/env bash
# Ameto — one-line Linux installer.
# Downloads the latest linux-x64 release and runs the bundled systemd installer.
#
#   curl -fsSL https://raw.githubusercontent.com/AMeto-Observability/AMeto-Observability/main/install/linux/bootstrap.sh | sudo bash
#
# Extra install.sh flags pass straight through, e.g.:
#   ... | sudo bash -s -- --port 8080 --data /srv/ameto
set -euo pipefail

REPO="AMeto-Observability/AMeto-Observability"
# Override the version by exporting AMETO_VERSION=v0.1.0 before running.
VERSION="${AMETO_VERSION:-}"

command -v curl >/dev/null || { echo "curl is required" >&2; exit 1; }
command -v tar  >/dev/null || { echo "tar is required"  >&2; exit 1; }

if [[ -z "$VERSION" ]]; then
  echo ">> Resolving latest release ..."
  VERSION="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
             | grep -oP '"tag_name":\s*"\K[^"]+')"
fi
[[ -n "$VERSION" ]] || { echo "Could not resolve a release version" >&2; exit 1; }

ASSET="ameto-${VERSION}-linux-x64.tar.gz"
URL="https://github.com/${REPO}/releases/download/${VERSION}/${ASSET}"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo ">> Downloading ${ASSET} ..."
curl -fSL "$URL" -o "$TMP/${ASSET}"
tar -xzf "$TMP/${ASSET}" -C "$TMP"

chmod +x "$TMP/install.sh" "$TMP/Ameto.Server" 2>/dev/null || true

echo ">> Running installer ..."
# The systemd installer needs root; re-invoke with sudo if necessary.
if [[ $EUID -ne 0 ]]; then
  sudo bash "$TMP/install.sh" --binary "$TMP/Ameto.Server" "$@"
else
  bash "$TMP/install.sh" --binary "$TMP/Ameto.Server" "$@"
fi
