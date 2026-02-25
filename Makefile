PY?=python3
VENV=.venv
PIP=$(VENV)/bin/pip
PYV=$(VENV)/bin/python
RUFF=$(VENV)/bin/ruff
PYTEST=$(VENV)/bin/pytest

.PHONY: setup lint test run clean

setup:
	$(PY) -m pip install -U pip virtualenv
	$(PY) -m virtualenv $(VENV)
	$(PIP) install -r requirements.txt

lint:
	$(RUFF) check src tests

test:
	PYTHONPATH=src $(PYTEST) -q

run:
	PYTHONPATH=src $(VENV)/bin/uvicorn wzcore_sandbox.app:app --host 127.0.0.1 --port 8080

clean:
	rm -rf $(VENV) .pytest_cache
