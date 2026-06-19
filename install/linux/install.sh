#!/usr/bin/env bash
# Ameto — Linux installer (systemd service)
# Usage:
#   sudo ./install.sh                            # interactive
#   sudo ./install.sh --binary /path/to/Ameto.Server
#   sudo ./install.sh --port 5341 --data /var/lib/ameto/data
#   sudo ./install.sh --uninstall
set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
BINARY_PATH=""
INSTALL_DIR="/opt/ameto"
DATA_DIR="/var/lib/ameto/data"
HTTP_PORT=5341
SERVICE_NAME="ameto"
SERVICE_USER="ameto"
UNINSTALL=0

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --binary)    BINARY_PATH="$2";  shift 2 ;;
        --install-dir) INSTALL_DIR="$2"; shift 2 ;;
        --data)      DATA_DIR="$2";     shift 2 ;;
        --port)      HTTP_PORT="$2";    shift 2 ;;
        --service)   SERVICE_NAME="$2"; shift 2 ;;
        --uninstall) UNINSTALL=1;       shift   ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ── Color helpers ─────────────────────────────────────────────────────────────
CYAN='\033[0;36m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
step()  { echo -e "${CYAN}  >> $*${NC}"; }
ok()    { echo -e "${GREEN}     $*${NC}"; }
warn()  { echo -e "${YELLOW}     $*${NC}"; }
error() { echo -e "${RED}  ERROR: $*${NC}"; exit 1; }

echo ""
echo -e "${CYAN}═══════════════════════════════════════════${NC}"
echo -e "${CYAN}  Ameto  ·  Linux Installer (systemd)${NC}"
echo -e "${CYAN}═══════════════════════════════════════════${NC}"
echo ""

[[ $EUID -ne 0 ]] && error "This script must be run as root (sudo)."

# ── Uninstall ─────────────────────────────────────────────────────────────────
if [[ $UNINSTALL -eq 1 ]]; then
    step "Stopping and disabling service '$SERVICE_NAME' ..."
    systemctl stop    "$SERVICE_NAME" 2>/dev/null || true
    systemctl disable "$SERVICE_NAME" 2>/dev/null || true

    step "Removing service unit file ..."
    rm -f "/etc/systemd/system/${SERVICE_NAME}.service"
    systemctl daemon-reload

    step "Removing install directory $INSTALL_DIR ..."
    rm -rf "$INSTALL_DIR"

    read -rp "  Remove data directory '$DATA_DIR'? [y/N] " yn
    if [[ "$yn" =~ ^[Yy]$ ]]; then
        rm -rf "$DATA_DIR"
        ok "Data directory removed."
    fi

    step "Removing service user '$SERVICE_USER' ..."
    userdel "$SERVICE_USER" 2>/dev/null || warn "User '$SERVICE_USER' not found."

    echo ""
    ok "Uninstall complete."
    echo ""
    exit 0
fi

# ── Locate binary ─────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -z "$BINARY_PATH" ]]; then
    for candidate in \
        "$SCRIPT_DIR/Ameto.Server" \
        "$(dirname "$SCRIPT_DIR")/Ameto.Server" \
        "$(dirname "$(dirname "$SCRIPT_DIR")")/publish/Ameto.Server"; do
        if [[ -f "$candidate" ]]; then
            BINARY_PATH="$candidate"
            break
        fi
    done
fi

[[ -z "$BINARY_PATH" || ! -f "$BINARY_PATH" ]] && \
    error "Ameto.Server binary not found. Specify with --binary <path>."

ok "Binary: $BINARY_PATH"

# ── Create service user ───────────────────────────────────────────────────────
step "Creating system user '$SERVICE_USER' ..."
if id "$SERVICE_USER" &>/dev/null; then
    warn "User '$SERVICE_USER' already exists — skipping."
else
    useradd --system --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
    ok "User '$SERVICE_USER' created."
fi

# ── Install files ─────────────────────────────────────────────────────────────
step "Installing to $INSTALL_DIR ..."
mkdir -p "$INSTALL_DIR"
SOURCE_DIR="$(dirname "$BINARY_PATH")"
cp -r "$SOURCE_DIR/." "$INSTALL_DIR/"
chmod +x "$INSTALL_DIR/Ameto.Server"

# ── Create data directory ─────────────────────────────────────────────────────
step "Creating data directory: $DATA_DIR"
mkdir -p "$DATA_DIR"
chown -R "$SERVICE_USER:$SERVICE_USER" "$DATA_DIR"

# ── Write config.yml ──────────────────────────────────────────────────────────
CONFIG_PATH="$INSTALL_DIR/config.yml"
if [[ ! -f "$CONFIG_PATH" ]]; then
    step "Writing default config.yml ..."
    cat > "$CONFIG_PATH" <<EOF
Ameto:
  NodeId: 0
  DataDirectory: $DATA_DIR
  HttpPort: $HTTP_PORT

  SslCertPath: ""
  SslCertPassword: ""

  RamTargetPercent: 99

  HotTier:
    MaxSizeBytes: 268435456  # 256 MB
    MaxAge: "00:05:00"

  Indexing:
    MaxPropertyFlattenDepth: 5

  Retention:
    VerboseDays: 3
    DebugDays: 3
    InformationDays: 90
    WarningDays: 90
    ErrorDays: 90
    FatalDays: 90

  Replication:
    Enabled: false
    SeedNodes: []
EOF
    ok "Config written to $CONFIG_PATH"
else
    warn "config.yml already exists — skipping (edit manually if needed)."
fi

chown -R "$SERVICE_USER:$SERVICE_USER" "$INSTALL_DIR"

# ── Install systemd service ───────────────────────────────────────────────────
UNIT_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
step "Writing systemd unit: $UNIT_FILE ..."
cat > "$UNIT_FILE" <<EOF
[Unit]
Description=Ameto — high-performance structured log server
Documentation=https://github.com/AMeto-Observability/AMeto-Observability
After=network-online.target
Wants=network-online.target

[Service]
Type=exec
User=$SERVICE_USER
Group=$SERVICE_USER
WorkingDirectory=$INSTALL_DIR
ExecStart=$INSTALL_DIR/Ameto.Server
Restart=on-failure
RestartSec=5s

# Hardening
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=$DATA_DIR $INSTALL_DIR
PrivateTmp=true

# Ensure the server can bind low ports if needed (not required for 5341)
# AmbientCapabilities=CAP_NET_BIND_SERVICE

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "$SERVICE_NAME"

# ── Start service ─────────────────────────────────────────────────────────────
step "Starting $SERVICE_NAME ..."
systemctl start "$SERVICE_NAME"
sleep 1
systemctl is-active --quiet "$SERVICE_NAME" && ok "Service is running." || \
    { warn "Service did not start cleanly. Check: journalctl -u $SERVICE_NAME -n 50"; }

echo ""
echo -e "${GREEN}═══════════════════════════════════════════${NC}"
echo -e "${GREEN}  Installation complete!${NC}"
echo -e "${GREEN}═══════════════════════════════════════════${NC}"
echo ""
echo -e "${CYAN}  UI:      http://localhost:${HTTP_PORT}${NC}"
echo -e "${YELLOW}  Login:   admin / 123123  (change immediately!)${NC}"
echo -e "${CYAN}  Config:  ${CONFIG_PATH}${NC}"
echo -e "${CYAN}  Data:    ${DATA_DIR}${NC}"
echo ""
echo "  Manage:"
echo "    Status:    systemctl status $SERVICE_NAME"
echo "    Logs:      journalctl -u $SERVICE_NAME -f"
echo "    Stop:      systemctl stop $SERVICE_NAME"
echo "    Restart:   systemctl restart $SERVICE_NAME"
echo "    Uninstall: sudo ./install.sh --uninstall"
echo ""
