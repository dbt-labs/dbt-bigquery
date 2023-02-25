.DEFAULT_GOAL:=help

.PHONY: dev
dev: ## Installs adapter in develop mode along with development depedencies
	@\
	pip install -e . -r dev-requirements.txt && pre-commit install

.PHONY: ubuntu-py311
ubuntu-py311:
	docker build -f docker/ubuntu-py311.Dockerfile -t dbt-bigquery-ubuntu-py311 .
	docker run --rm -it --name dbt-bigquery-ubuntu-py311 -v $(shell pwd):/opt/code dbt-bigquery-ubuntu-py311
