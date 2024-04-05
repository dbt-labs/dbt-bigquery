# - VERY rudimentary test script to run latest + specific branch image builds and test them all by running `--version`
# TODO: create a real test suite

clear \
&& echo "\n\n"\
"########################################\n"\
"##### Testing dbt-bigquery latest #####\n"\
"########################################\n"\
&& docker build --tag dbt-bigquery \
  --target dbt-bigquery \
  docker \
&& docker run dbt-bigquery --version \
\
&& echo "\n\n"\
"#########################################\n"\
"##### Testing dbt-bigquery-1.0.0b1 #####\n"\
"#########################################\n"\
&& docker build --tag dbt-bigquery-1.0.0b1 \
  --target dbt-bigquery \
  --build-arg dbt_bigquery_ref=dbt-bigquery@v1.0.0b1 \
  docker \
&& docker run dbt-bigquery-1.0.0b1 --version
