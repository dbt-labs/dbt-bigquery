 <!-- 1.look to see if we can make it smaller, lots of duplicated reading from dbt-core
      2.have correct examples of tests for each adapter
      3.point to and succinctly describe a process for updating docs
  -->
# Contributing to `dbt-bigquery`

1. [About this document](#about-this-document)
3. [Getting the code](#getting-the-code)
5. [Running `dbt-bigquery` in development](#running-dbt-bigquery-in-development)
6. [Testing](#testing)
7. [Updating Docs](#updating-docs)
7. [Submitting a Pull Request](#submitting-a-pull-request)

## About this document
This document is a guide intended for folks interested in contributing to `dbt-bigquery`. Below, we document the process by which members of the community should create issues and submit pull requests (PRs) in this repository. It is not intended as a guide for using `dbt-bigquery`, and it assumes a certain level of familiarity with Python concepts such as virtualenvs, `pip`, python modules, filesystems, and so on. This guide assumes you are using macOS or Linux and are comfortable with the command line.

For those wishing to contribute we highly suggest reading the [dbt-core](https://github.com/dbt-labs/dbt-core/blob/main/CONTRIBUTING.md). if you haven't already. Almost all of the information there is applicable to contributing here, too!

### Signing the CLA

Please note that all contributors to `dbt-bigquery` must sign the [Contributor License Agreement](https://docs.getdbt.com/docs/contributor-license-agreements) to have their Pull Request merged into an `dbt-bigquery` codebase. If you are unable to sign the CLA, then the `dbt-bigquery` maintainers will unfortunately be unable to merge your Pull Request. You are, however, welcome to open issues and comment on existing ones.


## Getting the code 

You will need `git` in order to download and modify the `dbt-bigquery` source code. On macOS, the best way to download git is to just install [Xcode](https://developer.apple.com/support/xcode/).

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

First make sure that you set up your `virtualenv` as described in [Setting up an environment](#setting-up-an-environment).  Also ensure you have the latest version of pip installed with `pip install --upgrade pip`. Next, install `dbt-biguery` latest dependencies:

```sh
pip install -e . -r dev-requirements.txt
```

When `dbt-bigquery` is installed this way, any changes you make to the `dbt-bigquery` source code will be reflected immediately in your next `dbt-bigquery` run.

To confirm you have correct version of `dbt-core` installed please run `dbt --version` and `which dbt`.

## Initial Setup

`dbt-bigquery` uses test credentials specified in a `test.env` file in the root of the repository for non-Postgres databases. This `test.env` file is git-ignored, but please be _extra_ careful to never check in credentials or other sensitive information when developing against `dbt-core`. To create your `test.env` file, copy the provided sample file, then supply your relevant credentials. This step is only required to use non-Postgres databases.

```
cp test.env.sample test.env
$EDITOR test.env
```

> In general, it's most important to have successful unit and Postgres tests. Once you open a PR, `dbt-core` will automatically run integration tests for the other three core database adapters. Of course, if you are a BigQuery user, contributing a BigQuery-only feature, it's important to run BigQuery tests as well.

## Testing

`dbt-core` uses test credentials specified in a `test.env` file in the root of the repository for non-Postgres databases. This `test.env` file is git-ignored, but please be _extra_ careful to never check in credentials or other sensitive information when developing against `dbt-core`. To create your `test.env` file, copy the provided sample file, then supply your relevant credentials. This step is only required to use non-Postgres databases.

```
cp test.env.sample test.env
$EDITOR test.env
```

> In general, it's most important to have successful unit and Postgres tests. Once you open a PR, `dbt-core` will automatically run integration tests for the other three core database adapters. Of course, if you are a BigQuery user, contributing a BigQuery-only feature, it's important to run BigQuery tests as well.

### Test commands
There are a few mehtods for running tests locally.

####`tox`
`tox` takes care of managing virtualenvs and install dependencies in order to run tests. You can also run tests in parallel, for example you can run unit tests for Python 3.7, Python 3.8, Python 3.9 `flake8` checks in parallel with `tox -p`. Also you  can run unit tests for specific python versions with `tox -e py37`. The configuration of theses tests are located in `tox.ini`.

####`pyteest`
Finally, you can also run  a specifiic test or group of tests using `pytest` directlly. With virtualenv activa and dev dependencies installed you can do things like:
```sh
# run specific postgres integration tests
python -m pytest -m profile_bigquery test/integration/001_simple_copy_test
# run all unit tests in a file
python -m pytest test/unit/test_bigquery_adapter.py
# run a specific unit test
python -m pytest test/unit/test_bigquery_adapter.py::TestBigQueryAdapter::test_copy_table_materialization_table
```
## Updating Docs

Many changes will require and update to the `dbt-bigquery` docs here are some useful resources.

- Docs are [here](https://docs.getdbt.com/).
- The docs repo for making changes is located [here]( https://github.com/dbt-labs/docs.getdbt.com).
- The changes made are likely to impact one or both of [BigQuery profile](https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile), or [BigQuery configs](https://docs.getdbt.com/reference/resource-configs/bigquery-configs).
- We as every community member who makes a user-facing change to open and issue for their change in that repo.
- If the contributor feels up to they should contribute to the PR for the docs update as well.


## Submitting a Pull Request

dbt Labs provides a CI environment to test changes to  the `dbt-bigquery` adapter, and periodic maintenance checks of `dbt-core` through Github Actions. 

A `dbt-bigquery` maintainer will review your PR. They may suggest code revision for style or clarity, or request that you add unit or integration test(s). These are good things! We believe that, with a little bit of help, anyone can contribute high-quality code.

Once all tests are passing and your PR has been approved, a `dbt-bigquery` maintainer will merge your changes into the active development branch. And that's it! Happy developing :tada:
