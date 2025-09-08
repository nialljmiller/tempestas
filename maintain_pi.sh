#!/bin/sh
# Debian 12 (bookworm) Raspberry Pi maintenance: update, clean, optimize (safe).
# Run as root (or with sudo).

set -eu

# ---- Tunables (safe defaults) ----
JOURNAL_VACUUM_DAYS="${JOURNAL_VACUUM_DAYS:-14}"   # delete journal logs older than N days
ENABLE_ZRAM="${ENABLE_ZRAM:-1}"                    # 1 = enable safe zram swap (skips if dphys-swapfile active)
ZRAM_PERCENT="${ZRAM_PERCENT:-50}"                 # zram size as % of RAM (50% is conservative/safe)
APT_FLAGS="-y -o Dpkg::Options::=--force-confdef -o Dpkg::Options::=--force-confold"
export DEBIAN_FRONTEND=noninteractive
export NEEDRESTART_MODE=a   # auto-restart services as needed, non-interactive

need_cmd() { command -v "$1" >/dev/null 2>&1 || { echo "Missing: $1"; exit 1; }; }
run() { echo "+ $*"; "$@"; }

require_root() {
  if [ "$(id -u)" != "0" ]; then
    echo "Run as root: sudo $0"
    exit 1
  fi
}

preflight() {
  echo "==> Preflight"
  need_cmd apt-get
  run dpkg --configure -a
  run apt-get ${APT_FLAGS} -f install || true
}

apt_update_upgrade() {
  echo "==> APT update & upgrade (safe)"
  run apt-get update
  # --with-new-pkgs installs new dependencies when required, without removing packages
  run apt-get ${APT_FLAGS} upgrade --with-new-pkgs
}

apt_cleanup() {
  echo "==> Cleaning APT caches and orphans"
  run apt-get ${APT_FLAGS} autoremove --purge
  run apt-get autoclean
  run apt-get clean
}

logs_cleanup() {
  echo "==> Rotating and trimming logs"
  if command -v logrotate >/dev/null 2>&1; then
    # Force a rotation per current policy (safe)
    run logrotate -f /etc/logrotate.conf || true
  fi
  if command -v journalctl >/dev/null 2>&1; then
    # Delete journal entries older than N days (safe)
    run journalctl --vacuum-time="${JOURNAL_VACUUM_DAYS}d" || true
  fi
  # Clean tmp according to tmpfiles.d policies (safe)
  if command -v systemd-tmpfiles >/dev/null 2>&1; then
    run systemd-tmpfiles --clean || true
  fi
}

maybe_enable_zram() {
  [ "$ENABLE_ZRAM" = "1" ] || { echo "==> ZRAM: skipped (ENABLE_ZRAM=0)"; return; }

  # If dphys-swapfile is active, do not add zram (avoid double swap surprises)
  if command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet dphys-swapfile 2>/dev/null; then
    echo "==> ZRAM: dphys-swapfile active; skipping zram setup."
    return
  fi

  echo "==> ZRAM: installing and enabling (safe, reversible)"
  if ! dpkg -s zram-tools >/dev/null 2>&1; then
    run apt-get ${APT_FLAGS} install zram-tools
  fi

  cfg="/etc/default/zramswap"
  if [ -f "$cfg" ]; then
    echo "==> ZRAM: config exists; leaving as-is ($cfg)"
  else
    echo "==> ZRAM: creating $cfg (PERCENT=${ZRAM_PERCENT})"
    umask 022
    cat >/tmp.zramswap.$$ <<EOF
# Managed by maintain_pi.sh (safe defaults for 1GB RAM)
PERCENT=${ZRAM_PERCENT}
PRIORITY=100
# ALGO left default (kernel chooses). No compression level pinning to keep it safe.
EOF
    run install -m 0644 /tmp.zramswap.$$ "$cfg"
    rm -f /tmp.zramswap.$$
  fi

  if command -v systemctl >/dev/null 2>&1; then
    run systemctl enable --now zramswap
  else
    echo "==> ZRAM: systemd not available; start zramswap service manually if needed."
  fi

  echo "==> Swap status after ZRAM:"
  run swapon --show || true
}

flatpak_cleanup_if_present() {
  if command -v flatpak >/dev/null 2>&1; then
    echo "==> Flatpak: uninstalling unused runtimes (if any)"
    run flatpak uninstall --unused -y || true
  fi
}

post_status() {
  echo "==> System status summary"
  run uname -a
  echo "-- Disk usage --"
  run df -h /
  echo "-- Memory --"
  run free -h || true
  echo "-- Swap --"
  run swapon --show || true
  echo "-- Failed systemd services (if any) --"
  if command -v systemctl >/dev/null 2>&1; then
    systemctl --failed --no-legend || true
  fi
  if [ -f /var/run/reboot-required ]; then
    echo "==> Reboot recommended: kernel/libc or similar updated."
  fi
}

main() {
  require_root
  preflight
  apt_update_upgrade
  apt_cleanup
  logs_cleanup
  maybe_enable_zram
  flatpak_cleanup_if_present
  post_status
  echo "==> Done."
}

main "$@"
