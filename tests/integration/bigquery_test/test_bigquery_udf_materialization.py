from tests.integration.base import DBTIntegrationTest, use_profile

class TestBigQueryUDFMaterialization(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test"

    @property
    def models(self):
        return "udf-models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'vars': {
                'udf_description': self.udf_description
            }
        }

    @property
    def udf_description(self):
        return 'this is a UDF'

    @use_profile('bigquery')
    def test__bigquery_materialize_udf_no_args(self):
        results = self.run_dbt(['run', '--select', 'udf_no_args'])
        self.assertEqual(len(results), 1)

        with self.get_connection() as conn:
            client = conn.handle

            udf = client.get_routine(
                self.adapter.connections.get_bq_routine(
                    self.default_database, self.unique_schema(), 'udf_no_args' 
                )
            )

            self.assertEqual(len(udf.arguments), 0)
            self.assertEqual(udf.return_type, 'INT64')
            # No description specified, so should be None
            self.assertIsNone(udf.description)

    @use_profile('bigquery')
    def test__bigquery_materialize_udf_with_args(self):
        results = self.run_dbt(['run', '--select', 'udf_with_args'])
        self.assertEqual(len(results), 1)

        with self.get_connection() as conn:
            client = conn.handle

            udf = client.get_routine(
                self.adapter.connections.get_bq_routine(
                    self.default_database, self.unique_schema(), 'udf_no_args' 
                )
            )
            
            # Make sure the UDF has two args as specified in model config
            self.assertEqual(len(udf.arguments), 2)

            # Check the name & type of the first arg
            self.assertEqual(udf.arguments[0].name, 'color')
            self.assertEqual(udf.arguments[0].data_type.type_kind.name, 'STRING')

            # Check the name & type of the second arg
            self.assertEqual(udf.arguments[1].name, 'is_pretty')
            self.assertEqual(udf.arguments[1].data_type.type_kind.name, 'BOOL')
            
            # Model config did not specify return_type, so should be None
            self.assertIsNone(udf.return_type)
            # No description specified, so should be None
            self.assertIsNone(udf.description)


    @use_profile('bigquery')
    def test__bigquery_materialize_udf_with_description(self):
        results = self.run_dbt(['run', '--select', 'udf_with_description'])
        self.assertEqual(len(results), 1)

        with self.get_connection() as conn:
            client = conn.handle

            udf = client.get_routine(
                self.adapter.connections.get_bq_routine(
                    self.default_database, self.unique_schema(), 'udf_with_description' 
                )
            )
            # Check that the description persisted
            self.assertEqual(udf.description, self.udf_description)

            self.assertEqual(len(udf.arguments), 0)
            self.assertIsNone(udf.return_type)
