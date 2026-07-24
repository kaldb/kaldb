#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Install Linux dependencies needed to build and run KalDB locally.

Usage:
  scripts/setup-linux-deps.sh [--no-docker] [--help]

Options:
  --no-docker  Install only the local Java/Maven build toolchain.
  --help       Show this help message.

This script supports Debian and Ubuntu systems with apt-get.
USAGE
}

require_command() {
  local command_name=$1
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Missing required command: $command_name" >&2
    exit 1
  fi
}

run_sudo() {
  if [ "$(id -u)" -eq 0 ]; then
    "$@"
  else
    sudo "$@"
  fi
}

install_packages() {
  run_sudo apt-get update
  run_sudo apt-get install -y "$@"
}

ensure_docker_group() {
  local current_user
  current_user=${SUDO_USER:-${USER:-}}

  if [ -z "$current_user" ] || [ "$(id -u "$current_user")" -eq 0 ]; then
    return
  fi

  if id -nG "$current_user" | tr ' ' '\n' | grep -qx docker; then
    return
  fi

  run_sudo usermod -aG docker "$current_user"
  echo "Added $current_user to the docker group. Log out and back in before running Docker without sudo."
}

verify_installation() {
  java -version
  mvn -version

  if [ "$INSTALL_DOCKER" = true ]; then
    docker --version
    docker compose version
  fi
}

INSTALL_DOCKER=true

for arg in "$@"; do
  case "$arg" in
    --no-docker)
      INSTALL_DOCKER=false
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [ ! -f /etc/os-release ]; then
  echo "Cannot determine Linux distribution; /etc/os-release is missing." >&2
  exit 1
fi

. /etc/os-release

case "${ID:-}" in
  debian | ubuntu)
    ;;
  *)
    if ! echo "${ID_LIKE:-}" | tr ' ' '\n' | grep -Eq '^(debian|ubuntu)$'; then
      echo "Unsupported Linux distribution: ${PRETTY_NAME:-unknown}" >&2
      echo "Install JDK 21, Maven, Docker, and Docker Compose using your system package manager." >&2
      exit 1
    fi
    ;;
esac

require_command apt-get

packages=(ca-certificates curl openjdk-21-jdk maven)

if [ "$INSTALL_DOCKER" = true ]; then
  packages+=(docker.io docker-compose-v2)
fi

install_packages "${packages[@]}"

if [ "$INSTALL_DOCKER" = true ]; then
  run_sudo systemctl enable --now docker
  ensure_docker_group
fi

verify_installation

echo "KalDB Linux dependencies are installed."
