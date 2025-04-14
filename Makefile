RM := rm -rf
PYTHON := poetry run -- python3
RUFF := poetry run -- ruff

PKG_FILES := pyproject.toml
PKG_LOCK := poetry.lock
ENV_DIR := .venv
ENV_LOCK := $(ENV_DIR)/pyvenv.cfg

.PHONY: all format lint clean purge test build publish venv

all: venv

format: venv
	$(RUFF) check --fix tests wcpan
	$(RUFF) format tests wcpan

lint: venv
	$(RUFF) check tests wcpan
	$(RUFF) format --check tests wcpan

clean:
	$(RM) ./dist ./build ./*.egg-info

purge: clean
	$(RM) -rf $(ENV_DIR)

test: venv
	$(PYTHON) -m compileall wcpan
	$(PYTHON) -m unittest

build: clean venv
	poetry build

publish: venv
	poetry publish

venv: $(ENV_LOCK)

$(ENV_LOCK): $(PKG_LOCK)
	poetry install
	touch $@

$(PKG_LOCK): $(PKG_FILES)
	poetry lock
	touch $@
