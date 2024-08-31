# Python Template Repository

> A template repository that Python projects can inherit from to ensure tooling
> consistency

## Table of Contents

- [Python Template Repository](#python-template-repository)
  - [Table of Contents](#table-of-contents)
  - [About](#about)
  - [Supported Tooling](#supported-tooling)

## About

This is a template repository that is intended to be inherited by other template
repositories *to ensure consistent common tool deployment across languages*.

This will also support *optional* tooling that services like GitHub offer in
order to provide repository owners access to these features without them having
to discover it themselves.

## Supported Tooling

The following tooling is supported:

- [Base Template Tooling](https://github.com/NicholasSynovic/template_base)
- [Python .gitignore](.gitignore)
- [Pypi Requirements](requirements.txt)
  - [Poetry](.pyproject.toml)
  - Sphinx
- [Python pre-commit hooks](.pre-commit-config.yaml)
  - Pyroma
  - [isort](.isort.cfg)
  - Black
  - Flake8
  - Bandit
