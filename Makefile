.PHONY: help venv-install db-up db-down pipeline pipeline-ddl pipeline-strict dashboard test lint format

help:
	@echo "Targets:"
	@echo "  venv-install    Install Python deps into .venv"
	@echo "  db-up           Start PostGIS + pgAdmin"
	@echo "  db-down         Stop containers"
	@echo "  pipeline        Run ingest->load->transform->checks"
	@echo "  pipeline-ddl    Run DDL then pipeline"
	@echo "  pipeline-strict Run pipeline with strict checks"
	@echo "  dashboard       Run Streamlit dashboard"
	@echo "  test            Run pytest"
	@echo "  lint            Run ruff"
	@echo "  format          Run black"

venv-install:
	python3 -m venv .venv
	. .venv/bin/activate && python -m pip install -r requirements.txt

db-up:
	docker compose up -d

db-down:
	docker compose down

pipeline:
	. .venv/bin/activate && python src/orchestration/run_pipeline.py --with-docker

pipeline-ddl:
	. .venv/bin/activate && python src/orchestration/run_pipeline.py --with-docker --with-ddl

pipeline-strict:
	. .venv/bin/activate && python src/orchestration/run_pipeline.py --with-docker --strict-checks

dashboard:
	. .venv/bin/activate && streamlit run dashboards/streamlit_app.py

test:
	. .venv/bin/activate && python -m pytest

lint:
	. .venv/bin/activate && python -m ruff check src tests

format:
	. .venv/bin/activate && python -m black .
