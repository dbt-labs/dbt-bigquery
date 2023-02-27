#!/bin/bash
# Set TOXENV environment variable for subsequent steps
echo "TOXENV=integration-bigquery" >> $GITHUB_ENV
# Set INTEGRATION_TESTS_SECRETS_PREFIX environment variable for subsequent steps
# All GH secrets that have this prefix will be set as environment variables
echo "INTEGRATION_TESTS_SECRETS_PREFIX=BIGQUERY_TEST" >> $GITHUB_ENV
# Set environment variables required for integration tests
echo "DBT_TEST_USER_1=group:buildbot@dbtlabs.com" >> $GITHUB_ENV
echo "DBT_TEST_USER_2=group:dev-core@dbtlabs.com" >> $GITHUB_ENV
echo "DBT_TEST_USER_3=serviceAccount:dbt-integration-test-user@dbt-test-env.iam.gserviceaccount.com" >> $GITHUB_ENV
echo "DATAPROC_REGION=us-central1" >> $GITHUB_ENV
echo "DATAPROC_CLUSTER_NAME=dbt-test-1" >> $GITHUB_ENV
echo "GCS_BUCKET=dbt-ci" >> $GITHUB_ENV
