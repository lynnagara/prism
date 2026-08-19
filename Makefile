PRISM_STORE ?= ./data
API_ADDR ?= 127.0.0.1:3000
OTLP_ADDR ?= 127.0.0.1:4318

export PRISM_STORE

store:
	@mkdir -p $(PRISM_STORE)

api: store
	cargo run -q -- api --addr $(API_ADDR)

receive-otlp: store
	cargo run -q -- otlp --addr $(OTLP_ADDR)

compact: store
	cargo run -q -- compact

.PHONY: store api receive-otlp compact
