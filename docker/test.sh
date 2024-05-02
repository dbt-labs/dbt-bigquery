# - VERY rudimentary test script to run latest + specific branch image builds and test them all by running `--version`
# TODO: create a real test suite
set -e

echo "\n\n"
echo "#######################################"
echo "##### Testing dbt-bigquery latest #####"
echo "#######################################"

docker build --tag dbt-bigquery --target dbt-bigquery docker
docker run dbt-bigquery --version

echo "\n\n"
echo "########################################"
echo "##### Testing dbt-bigquery-1.0.0b1 #####"
echo "########################################"

docker build --tag dbt-bigquery-1.0.0b1 --target dbt-bigquery --build-arg commit_ref=v1.0.0b1 docker
docker run dbt-bigquery-1.0.0b1 --version
