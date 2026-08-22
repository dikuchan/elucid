NPM ?= npm
CARGO ?= cargo
NPM_CACHE ?= $(CURDIR)/.elucid/npm-cache

.PHONY: build ui-assets

build: ui-assets
	$(CARGO) build --manifest-path elucid/Cargo.toml --locked --release --package elucid-cli

ui-assets:
	npm_config_cache="$(NPM_CACHE)" $(NPM) --prefix ui ci
	npm_config_cache="$(NPM_CACHE)" $(NPM) --prefix ui run build
