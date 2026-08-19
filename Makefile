PRISM_STORE ?= ./data
API_ADDR ?= 127.0.0.1:3000
OTLP_ADDR ?= 0.0.0.0:4318

# The OpenTelemetry demo: a webstore of fifteen services with a load generator
# driving it. Cloned rather than vendored, so it stays upstream's to change.
DEMO_REPO ?= https://github.com/open-telemetry/opentelemetry-demo.git
DEMO_DIR ?= .demo
DEMO_SECONDS ?= 120

# Simulated shoppers. The demo ships five, which is a trickle; this is enough
# to fill a store in a couple of minutes.
DEMO_USERS ?= 50

export PRISM_STORE

store:
	@mkdir -p $(PRISM_STORE)

api: store
	cargo run -q -- api --addr $(API_ADDR)

otlp: store
	cargo run -q -- otlp --addr $(OTLP_ADDR)

compact: store
	cargo run -q -- compact

$(DEMO_DIR):
	git clone --depth 1 $(DEMO_REPO) $(DEMO_DIR)

# Slow once, cached after, so it stays out of the timed run.
demo-pull: $(DEMO_DIR)
	cd $(DEMO_DIR) && docker compose pull

demo-down:
	@cd $(DEMO_DIR) && docker compose down --remove-orphans

# Up and down around a wait, so what lands in the store is a bounded,
# repeatable slice rather than whatever was left running.
demo: $(DEMO_DIR)
	@nc -z 127.0.0.1 $(lastword $(subst :, ,$(OTLP_ADDR))) \
	  || { echo "nothing is listening on $(OTLP_ADDR) — run 'make otlp' first"; exit 1; }
	@cd $(DEMO_DIR) \
	  && export OTEL_COLLECTOR_CONFIG_EXTRAS=$(CURDIR)/otel-collector-config.yml \
	  && export LOAD_GENERATOR_VUS=$(DEMO_USERS) \
	  && docker compose up -d --no-build --quiet-pull \
	  && trap 'docker compose down --remove-orphans' EXIT INT TERM \
	  && echo "generating for $(DEMO_SECONDS)s — the shop is at http://localhost:8080" \
	  && sleep $(DEMO_SECONDS)

.PHONY: store api otlp compact demo demo-down demo-pull
