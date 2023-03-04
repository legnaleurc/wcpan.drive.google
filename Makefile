PYTHON := poetry run -- python3
BLACK := poetry run -- black

PKG_FILES := pyproject.toml poetry.lock
PKG_DIR := .venv
BLD_LOCK := $(PKG_DIR)/pyvenv.cfg

.PHONY: all format lint clean purge test build publish venv

all: venv

format: venv
	$(BLACK) tests wcpan

lint: venv
	$(BLACK) --check tests wcpan

clean:
	rm -rf ./dist ./build ./*.egg-info

purge: clean
	rm -rf $(PKG_DIR)

test: venv
	$(PYTHON) -m compileall wcpan
	$(PYTHON) -m unittest

build: clean venv
	poetry build

publish: venv
	poetry publish

venv: $(BLD_LOCK)

$(BLD_LOCK): $(PKG_FILES)
	poetry install
	touch $@
