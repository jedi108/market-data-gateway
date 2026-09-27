# =============================================================================
# Market Data Gateway — public build / verify Makefile
#
# This is the standalone public surface of the gateway: build, tests, static
# analysis, and loopback-only smokes against the fake provider. Everything
# that touches real hosts, remote operations or private infrastructure lives
# in the private operator repository next to the deployment assets — never
# here.
#
# Smokes bind client listeners on loopback only and use synthetic node
# tokens (no real credentials exist in any smoke flow). Multi-node smokes
# additionally bind the peer listener on a discovered private IPv4 of the
# local machine, because config validation rejects loopback peers for
# multi-node membership.
# =============================================================================
SHELL := /bin/bash

BUILD_DIR := build
BIN_PATH  := $(BUILD_DIR)/gateway
LINUX_BIN := $(BUILD_DIR)/gateway-linux-amd64
SMOKE_DIR := scripts/smoke

VERSION ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.build=$(VERSION)

.PHONY: help build build-linux test test-race vet fmt-check verify \
        smoke smoke-cluster2 smoke-cluster3 smoke-loglevel smoke-packaging

.DEFAULT_GOAL := help

help: ## Show this help
	@echo "Market Data Gateway (public) | version: $(VERSION)"
	@echo ""
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' | sort
	@echo ""
	@echo "Local verification:"
	@echo "  make verify          — fmt-check + vet + test + single-node smoke"
	@echo "  make smoke-cluster2  — two-node fake-provider cluster smoke"
	@echo "  make smoke-cluster3  — three-node routing/version-switch smoke"
	@echo "  make smoke-loglevel  — no cache-hit events at INFO log level"
	@echo "  make smoke-packaging — linux/amd64 static packaging checks"

# =============================================================================
# Build
# =============================================================================

build: ## Build the static host binary (CGO_ENABLED=0) -> build/gateway
	CGO_ENABLED=0 go build -trimpath \
	  -ldflags '$(LDFLAGS)' -o $(BIN_PATH) ./cmd/gateway
	@echo "built $(BIN_PATH) (version $(VERSION))"

build-linux: ## Cross-compile the static linux/amd64 binary -> build/gateway-linux-amd64
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath \
	  -ldflags '$(LDFLAGS)' -o $(LINUX_BIN) ./cmd/gateway
	@echo "built $(LINUX_BIN) (version $(VERSION))"

# =============================================================================
# Static analysis and tests
# =============================================================================

test: ## Run all Go tests (CGO_ENABLED=0)
	CGO_ENABLED=0 go test ./... -count=1

test-race: ## Run all Go tests with -race (test-only; production is CGO-free)
	CGO_ENABLED=1 go test -race ./... -count=1

vet: ## go vet ./...
	go vet ./...

fmt-check: ## gofmt check (every listed file is a failure)
	@unformatted=$$(gofmt -l . 2>/dev/null); \
	  if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi; \
	  echo "gofmt clean"

verify: fmt-check vet test smoke ## Full local verification suite (fmt, vet, test, single-node smoke)

# =============================================================================
# Smokes (fake provider, loopback clients, synthetic tokens)
# =============================================================================

smoke: ## Local single-node fake-provider smoke (loopback only, config/gateway.local.example.yaml)
	bash $(SMOKE_DIR)/local_smoke.sh

smoke-cluster2: ## Two-node cluster smoke (client loopback, peer binds on a discovered private IPv4)
	bash $(SMOKE_DIR)/cluster2_smoke.sh

smoke-cluster3: ## Three-node cluster smoke (routing hash, version switch and deterministic rollback)
	bash $(SMOKE_DIR)/cluster3_smoke.sh

smoke-loglevel: ## INFO log-level contract: no cache-hit events at INFO
	bash $(SMOKE_DIR)/loglevel_check.sh

smoke-packaging: ## Packaging checks: linux/amd64 ELF, static linking, no private strings in the binary
	bash $(SMOKE_DIR)/packaging_smoke.sh
