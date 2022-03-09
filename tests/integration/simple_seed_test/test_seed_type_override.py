from tests.integration.base import DBTIntegrationTest, use_profile


class TestSimpleSeed(DBTIntegrationTest):

    @property
    def schema(self):
        return "simple_seed"

    @property
    def models(self):
        return "models-bq"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seed-paths': ['seeds-config'],
            'macro-paths': ['macros'],
            'seeds': {
                'test': {
                    'enabled': False,
                    'quote_columns': True,
                    'seed_enabled': {
                        'enabled': True,
                        '+column_types': self.seed_enabled_types(),
                    },
                    'seed_tricky': {
                        'enabled': True,
                        '+column_types': self.seed_tricky_types(),
                    },
                    'seed_labels': {
                        'enabled': True,
                    },
                },
            },
        }

    def seed_enabled_types(self):
        return {
            "id": "FLOAT64",
            "birthday": "STRING",
        }

    def seed_tricky_types(self):
        return {
            'id_str': 'STRING',
            'looks_like_a_bool': 'STRING',
            'looks_like_a_date': 'STRING',
        }

    @property
    def profile_config(self):
        return self.bigquery_profile()


class TestSimpleSeedColumnOverrideBQ(TestSimpleSeed):

    @use_profile('bigquery')
    def test_bigquery_simple_seed_with_column_override_bigquery(self):
        results = self.run_dbt(["seed", "--show"])
        self.assertEqual(len(results),  3)
        results = self.run_dbt(["test"])
        self.assertEqual(len(results),  10)


class TestSimpleSeedConfigLabelsBQ(TestSimpleSeed):

    @property
    def table_labels(self):
        return {
            'contains_pii': 'yes',
            'contains_pie': 'no'
        }

    @use_profile('bigquery')
    def test__bigquery_seed_table_with_labels(self):
        _ = self.run_dbt(["seed", "--show"])
        with self.get_connection() as conn:
            client = conn.handle

            table = client.get_table(
                self.adapter.connections.get_bq_table(
                    self.default_database, self.unique_schema(), 'seed_labels')
            )

            self.assertTrue(table.labels)
            self.assertEquals(table.labels, self.table_labels)
