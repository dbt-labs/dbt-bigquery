import pytest

from dbt.tests.util import run_dbt

from dbt.tests.adapter.simple_snapshot.test_snapshot import BaseSimpleSnapshotBase, BaseSnapshotCheck
from tests.functional.adapter.simple_snapshot import snapshots


class TestSnapshot(BaseSimpleSnapshotBase):
    # Not importing the base case because the test_updates* tests need modification for updating intervals
    @pytest.fixture(scope="class")
    def snapshots(self):
        # Using the snapshot defined the adapter itself rather than the base case
        # Reason: dbt-bigquery:#3710: UNION ALL issue when running snapshots with invalidate_hard_deletes=True
        return {"snapshot.sql": snapshots.SNAPSHOT_TIMESTAMP_SQL}

    def test_updates_are_captured_by_snapshot(self, project):
        """
        Update the last 5 records. Show that all ids are current, but the last 5 reflect updates.
        """
        dt_add_type = "date_add(updated_at, interval 1 day)"
        self.update_fact_records(
            {
                "updated_at": dt_add_type
            },
            "id between 16 and 20"
        )
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 21),
            ids_with_closed_out_snapshot_records=range(16, 21),
        )

    def test_inserts_are_captured_by_snapshot(self, project):
        """
        Insert 10 records. Show that there are 30 records in `snapshot`, all of which are current.
        """
        self.insert_fact_records("id between 21 and 30")
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 31), ids_with_closed_out_snapshot_records=[]
        )

    def test_deletes_are_captured_by_snapshot(self, project):
        """
        Hard delete the last five records. Show that there are now only 15 current records and 5 expired records.
        """
        self.delete_fact_records("id between 16 and 20")
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 16),
            ids_with_closed_out_snapshot_records=range(16, 21),
        )

    def test_revives_are_captured_by_snapshot(self, project):
        """
        Delete the last five records and run snapshot to collect that information, then revive 3 of those records.
        Show that there are now 18 current records and 5 expired records.
        """
        self.delete_fact_records("id between 16 and 20")
        run_dbt(["snapshot"])
        self.insert_fact_records("id between 16 and 18")
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 19),
            ids_with_closed_out_snapshot_records=range(16, 21),
        )

    def test_new_column_captured_by_snapshot(self, project):
        """
        Add a column to `fact` and populate the last 10 records with a non-null value.
        Show that all ids are current, but the last 10 reflect updates and the first 10 don't
        i.e. if the column is added, but not updated, the record doesn't reflect that it's updated
        """
        self.add_fact_column("full_name", "varchar(200) default null")
        dt_add_type = "date_add(date(updated_at), interval 1 day)"
        self.update_fact_records(
            {
                "full_name": "first_name || ' ' || last_name",
                "updated_at": dt_add_type,
            },
            "id between 11 and 20",
        )
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 21),
            ids_with_closed_out_snapshot_records=range(11, 21),
        )


class TestSnapshotCheck(BaseSnapshotCheck):
    pass
