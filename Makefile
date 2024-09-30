build:
	poetry build
	pip install dist/*.tar.gz

build-docs:
	sphinx-build --builder html docs build-docs

create-dev:
	pre-commit install
	rm -rf env
	python3.10 -m venv env
	( \
		. env/bin/activate; \
		pip install -r requirements.txt; \
		poetry install; \
		deactivate; \
	)

create-docs:
	sphinx-apidoc src --output-dir docs --maxdepth 100 --separate
