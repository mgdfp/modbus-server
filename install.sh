#!/usr/bin/env bash
# Install and enable the modbus-server systemd user service.
# Run once after cloning. Re-run after changing the unit file.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SYSTEMD_USER_DIR="$HOME/.config/systemd/user"

mkdir -p "$SYSTEMD_USER_DIR"

# Enable linger so user units run without an active login session
loginctl enable-linger "$USER"

ln -sf "$REPO_DIR/systemd/modbus-server.service" "$SYSTEMD_USER_DIR/modbus-server.service"
echo "Linked modbus-server.service"

systemctl --user daemon-reload
systemctl --user enable --now modbus-server.service

echo ""
echo "Done. Service status:"
systemctl --user status modbus-server.service
