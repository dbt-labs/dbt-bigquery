# Contributing to `dbt-bigquery`

1. [About this document](#about-this-document)
3. [Getting the code](#getting-the-code)
5. [Running `dbt-bigquery` in development](#running-dbt-bigquery-in-development)
6. [Testing](#testing)
7. [Updating Docs](#updating-docs)
7. [Submitting a Pull Request](#submitting-a-pull-request)

## About this document
This document is a guide intended for folks interested in contributing to `dbt-bigquery`. Below, we document the process by which members of the community should create issues and submit pull requests (PRs) in this repository. It is not intended as a guide for using `dbt-bigquery`, and it assumes a certain level of familiarity with Python concepts such as virtualenvs, `pip`, python modules, filesystems, and so on. This guide assumes you are using macOS or Linux and are comfortable with the command line.

For those wishing to contribute we highly suggest reading the [dbt-core](https://github.com/dbt-labs/dbt-core/blob/main/CONTRIBUTING.md), if you haven't already. Almost all of the information there is applicable to contributing here, too!

### Signing the CLA

Please note that all contributors to `dbt-bigquery` must sign the [Contributor License Agreement](https://docs.getdbt.com/docs/contributor-license-agreements) to have their Pull Request merged into an `dbt-bigquery` codebase. If you are unable to sign the CLA, then the `dbt-bigquery` maintainers will unfortunately be unable to merge your Pull Request. You are, however, welcome to open issues and comment on existing ones.


## Getting the code

You will need `git` in order to download and modify the `dbt-bigquery` source code. You can find direction [here](https://github.com/git-guides/install-git) on how to install `git`.

### External contributors

If you are not a member of the `dbt-labs` GitHub organization, you can contribute to `dbt-bigquery` by forking the `dbt-bigquery` repository. For a detailed overview on forking, check out the [GitHub docs on forking](https://help.github.com/en/articles/fork-a-repo). In short, you will need to:

1. fork the `dbt-bigquery` repository
2. clone your fork locally
3. check out a new branch for your proposed changes
4. push changes to your fork
5. open a pull request against `dbt-labs/dbt-bigquery` from your forked repository

### dbt Labs contributors

If you are a member of the `dbt Labs` GitHub organization, you will have push access to the `dbt-bigquery` repo. Rather than forking `dbt-bigquery` to make your changes, just clone the repository, check out a new branch, and push directly to that branch.


## Running `dbt-bigquery` in development

### Installation

First make sure that you set up your `virtualenv` as described in [Setting up an environment](https://github.com/dbt-labs/dbt-core/blob/HEAD/CONTRIBUTING.md#setting-up-an-environment).  Ensure you have the latest version of pip installed with `pip install --upgrade pip`. Next, install `dbt-bigquery` latest dependencies:

```sh
pip install -e . -r dev-requirements.txt
```

When `dbt-bigquery` is installed this way, any changes you make to the `dbt-bigquery` source code will be reflected immediately in your next `dbt-bigquery` run.

To confirm you have the correct version of `dbt-core` installed please run `dbt --version` and `which dbt`.

## Testing

### Initial Setup

`dbt-bigquery` contains [unit](https://github.com/dbt-labs/dbt-bigquery/tree/main/tests/unit) and [integration](https://github.com/dbt-labs/dbt-bigquery/tree/main/tests/integration) tests. Integration tests require testing against an actual BigQuery warehouse. We have CI set up to test against a BigQuery warehouse. In order to run integration tests locally, you will need a `test.env` file in the root of the repository that contains credentials for BigQuery.

Note: This `test.env` file is git-ignored, but please be _extra_ careful to never check in credentials or other sensitive information when developing. To create your `test.env` file, copy the provided example file, then supply your relevant credentials.

```
cp test.env.example test.env
$EDITOR test.env
```

### Test commands
There are a few methods for running tests locally.

#### `tox`
`tox` takes care of managing Python virtualenvs and installing dependencies in order to run tests. You can also run tests in parallel, for example you can run unit tests for Python 3.8, Python 3.9, and `flake8` checks in parallel with `tox -p`. Also, you can run unit tests for specific python versions with `tox -e py38`. The configuration of these tests are located in `tox.ini`.

#### `pytest`
Finally, you can also run a specific test or group of tests using `pytest` directly. With a Python virtualenv active and dev dependencies installed you can do things like:

```sh
# run specific bigquery functional tests
python -m pytest -m profile_bigquery tests/functional/adapter/test_aliases.py::TestSameTestSameAliasDifferentDatabasesBigQuery
# run all unit tests in a file
python -m pytest tests/unit/test_bigquery_adapter.py
# run a specific unit test
python -m pytest tests/unit/test_bigquery_adapter.py::TestBigQueryAdapter::test_copy_table_materialization_table
```
## Updating Docs

Many changes will require and update to the `dbt-bigquery` docs here are some useful resources.

- Docs are [here](https://docs.getdbt.com/).
- The docs repo for making changes is located [here]( https://github.com/dbt-labs/docs.getdbt.com).
- The changes made are likely to impact one or both of [BigQuery Profile](https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile), or [BigQuery Configs](https://docs.getdbt.com/reference/resource-configs/bigquery-configs).
- We ask every community member who makes a user-facing change to open an issue or PR regarding doc changes.

## Adding CHANGELOG Entry

We use [changie](https://changie.dev) to generate `CHANGELOG` entries. **Note:** Do not edit the `CHANGELOG.md` directly. Your modifications will be lost.

Follow the steps to [install `changie`](https://changie.dev/guide/installation/) for your system.

Once changie is installed and your PR is created, simply run `changie new` and changie will walk you through the process of creating a changelog entry.  Commit the file that's created and your changelog entry is complete!

You don't need to worry about which `dbt-bigquery` version your change will go into. Just create the changelog entry with `changie`, and open your PR against the `main` branch. All merged changes will be included in the next minor version of `dbt-bigquery`. The Core maintainers _may_ choose to "backport" specific changes in order to patch older minor versions. In that case, a maintainer will take care of that backport after merging your PR, before releasing the new version of `dbt-bigquery`.


## Submitting a Pull Request

dbt Labs provides a CI environment to test changes to the `dbt-bigquery` adapter and periodic checks against the development version of `dbt-core` through Github Actions.

A `dbt-bigquery` maintainer will review your PR. They may suggest code revision for style or clarity, or request that you add unit or integration test(s). These are good things! We believe that, with a little bit of help, anyone can contribute high-quality code.

Once all tests are passing, you have updated the changelog to reflect and tag your issue/pr for reference with a small description of the change, and your PR has been approved, a `dbt-bigquery` maintainer will merge your changes into the active development branch. And that's it! Happy developing :tada:
