#!/usr/bin/env bash
set -euo pipefail

ENV_EXAMPLE=".env.example"
ENV_FILE=".env"
KAFKA_IMAGE="confluentinc/cp-kafka:7.5.3"

# ── Colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

info()    { echo -e "${CYAN}[info]${NC}  $*"; }
success() { echo -e "${GREEN}[ok]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[warn]${NC}  $*"; }
error()   { echo -e "${RED}[error]${NC} $*" >&2; }

# ── Checks ────────────────────────────────────────────────────────────────────
if ! command -v docker &>/dev/null; then
  error "Docker is not installed or not in PATH. Please install Docker first."
  exit 1
fi

if ! docker info &>/dev/null; then
  error "Docker is not running. Please start Docker and try again."
  exit 1
fi

if [[ ! -f "$ENV_EXAMPLE" ]]; then
  error "$ENV_EXAMPLE not found. Make sure you are running this script from the repo root."
  exit 1
fi

# ── Copy .env ─────────────────────────────────────────────────────────────────
if [[ -f "$ENV_FILE" ]]; then
  warn "$ENV_FILE already exists — skipping copy."
else
  cp "$ENV_EXAMPLE" "$ENV_FILE"
  success "Copied $ENV_EXAMPLE → $ENV_FILE"
fi

# ── Generate Kafka cluster ID ─────────────────────────────────────────────────
info "Generating Kafka cluster ID (pulling $KAFKA_IMAGE if needed)..."

KAFKA_CLUSTER_ID=$(docker run --rm "$KAFKA_IMAGE" kafka-storage random-uuid)

if [[ -z "$KAFKA_CLUSTER_ID" ]]; then
  error "Failed to generate Kafka cluster ID."
  exit 1
fi

success "Generated cluster ID: $KAFKA_CLUSTER_ID"

# ── Inject into .env ──────────────────────────────────────────────────────────
if grep -q "^KAFKA_CLUSTER_ID=" "$ENV_FILE"; then
  # Replace existing key
  sed -i.bak "s|^KAFKA_CLUSTER_ID=.*|KAFKA_CLUSTER_ID=$KAFKA_CLUSTER_ID|" "$ENV_FILE"
  rm -f "$ENV_FILE.bak"
  success "Updated KAFKA_CLUSTER_ID in $ENV_FILE"
else
  # Append if key is missing
  echo "KAFKA_CLUSTER_ID=$KAFKA_CLUSTER_ID" >> "$ENV_FILE"
  success "Appended KAFKA_CLUSTER_ID to $ENV_FILE"
fi

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
success "Setup complete."
