#!/bin/bash
# pgshadow deployment script
# Usage: sudo ./deploy.sh [--skip-build]
set -euo pipefail

INSTALL_DIR="/opt/pgshadow"
LOG_DIR="/var/log/pgshadow"
BIN_NAME="pgshadow"
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "=== pgshadow deployment ==="

# Parse arguments
SKIP_BUILD=false
for arg in "$@"; do
    case $arg in
        --skip-build) SKIP_BUILD=true ;;
    esac
done

# Step 0: Build binary
if [ "$SKIP_BUILD" = false ]; then
    echo "Building pgshadow binary..."
    cd "$SCRIPT_DIR"
    go build -tags "pcap ebpf" -o "$BIN_NAME" ./cmd/pgshadow/
    echo "Build complete: $(ls -lh $BIN_NAME | awk '{print $5}') binary"
else
    echo "Skipping build (--skip-build)"
fi

cd "$SCRIPT_DIR"

# Create directories
mkdir -p "$INSTALL_DIR" "$LOG_DIR"

# Copy binary
cp -f "$BIN_NAME" "$INSTALL_DIR/$BIN_NAME"
chmod 755 "$INSTALL_DIR/$BIN_NAME"

# Copy config (don't overwrite existing)
if [ ! -f "$INSTALL_DIR/config.yaml" ]; then
    cp config.yaml "$INSTALL_DIR/config.yaml"
    echo "Copied config.yaml → $INSTALL_DIR/config.yaml"
else
    echo "Config already exists, skipping (use 'cp -f config.yaml $INSTALL_DIR/' to force)"
fi

# Create env file template if not exists
if [ ! -f "$INSTALL_DIR/env" ]; then
    cat > "$INSTALL_DIR/env" << 'EOF'
# pgshadow environment variables (secrets)
# This file is read by systemd EnvironmentFile directive
PGSHADOW_DB_PASSWORD=changeme
# KAFKA_PASSWORD=changeme
EOF
    chmod 600 "$INSTALL_DIR/env"
    echo "Created $INSTALL_DIR/env (edit with actual passwords)"
fi

# Install systemd service
cp deploy/pgshadow.service /etc/systemd/system/pgshadow.service
systemctl daemon-reload

# Setup log rotation (for any file-based logs)
cat > /etc/logrotate.d/pgshadow << 'EOF'
/var/log/pgshadow/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
    maxsize 100M
}
EOF

echo ""
echo "=== Deployment complete ==="
echo ""
echo "Next steps:"
echo "  1. Edit passwords:    sudo vi $INSTALL_DIR/env"
echo "  2. Edit config:       sudo vi $INSTALL_DIR/config.yaml"
echo "  3. Start service:     sudo systemctl start pgshadow"
echo "  4. Enable on boot:    sudo systemctl enable pgshadow"
echo "  5. Check status:      sudo systemctl status pgshadow"
echo "  6. View logs:         sudo journalctl -u pgshadow -f"
echo ""
