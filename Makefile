.DEFAULT_GOAL:=help

.PHONY: dev
dev: ## Installs adapter in develop mode along with development dependencies
	@\
	pip install -e . -r dev-requirements.txt && pre-commit install

.PHONY: dev-uninstall
dev-uninstall: ## Uninstalls all packages while maintaining the virtual environment
               ## Useful when updating versions, or if you accidentally installed into the system interpreter
	pip freeze | grep -v "^-e" | cut -d "@" -f1 | xargs pip uninstall -y
	pip uninstall -y dbt-bigquery

.PHONY: docker-dev
docker-dev:
	docker build -f docker/dev.Dockerfile -t dbt-bigquery-dev .
	docker run --rm -it --name dbt-bigquery-dev -v $(shell pwd):/opt/code dbt-bigquery-dev

.PHONY: docker-prod
docker-prod:
	docker build -f docker/Dockerfile -t dbt-bigquery .
