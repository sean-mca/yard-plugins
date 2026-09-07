# yard-plugins root Makefile -- local ministack test harness (Phase 8).
#
# Container lifecycle for the ministack AWS emulator lives here and in
# docker-compose.yml, never in Rust test code: no testcontainers crate and no
# docker CLI calls from the test harness. This Makefile is the seam a future CI
# phase calls into.
#
# Two suites, deliberately separate:
#   make test              offline, no Docker, always green
#   make test-integration  opt-in, needs a reachable ministack gateway
.PHONY: ministack-up ministack-down test test-integration lint

SHELL := /usr/bin/env bash

# Starts the pinned ministack container in the background, then polls the
# gateway from the host until it answers. Readiness is checked here rather than
# by a compose healthcheck because the presence of curl inside the image is
# unverified.
#
# This target is independently runnable, and test-integration deliberately does
# NOT invoke it -- the container stays under the developer's control, so a
# ministack already running from elsewhere is never restarted or torn down by a
# test run.
ministack-up:
	docker compose up -d
	@echo "==> polling the ministack gateway at http://127.0.0.1:4566 ..."
	@attempt=0; \
	until curl -fsS -o /dev/null http://127.0.0.1:4566/_ministack/health 2>/dev/null; do \
		attempt=$$((attempt + 1)); \
		if [ "$$attempt" -ge 30 ]; then \
			echo "ERROR: the ministack gateway never answered after 30 seconds." >&2; \
			echo "       Inspect it with: docker compose logs ministack" >&2; \
			exit 1; \
		fi; \
		sleep 1; \
	done; \
	echo "==> ministack is ready"

# Stops and removes the container and its volumes. Suffixed with `|| true` so a
# second invocation, or one with nothing running, still exits 0.
ministack-down:
	docker compose down -v || true

# The offline suite. Requires no Docker and no network, and must stay green at
# all times -- the ministack lifecycle tests skip themselves when the opt-in
# variable below is unset, so they never make this target conditional on a
# running container.
test:
	cargo test --workspace

# The opt-in ministack suite.
#
# YARD_TEST_AWS_ENDPOINT is the gate the lifecycle tests read: unset, they skip
# cleanly and report why; set, they exercise real deploy/verify/destroy calls
# against the emulator named by its value.
#
# The value is written as an IP literal rather than `localhost`. This diverges
# on purpose from ../yard-infra/scripts/lib/ministack-common.sh: an IP-literal
# endpoint makes the S3 endpoint ruleset select path-style addressing, whereas a
# hostname yields <bucket>.localhost:4566 virtual-host URLs whose DNS resolution
# is not portable across the four target platforms.
#
# The variable may be overridden to point at ANY already-running ministack:
#     YARD_TEST_AWS_ENDPOINT=http://127.0.0.1:4566 make test-integration
# That makes `make ministack-up` optional, and is the way out of a port-4566
# bind conflict with a container started outside this repo.
test-integration: export YARD_TEST_AWS_ENDPOINT := http://127.0.0.1:4566
test-integration:
	cargo test -p yard-plugin-glue

lint:
	cargo clippy --workspace --all-targets -- -D warnings
