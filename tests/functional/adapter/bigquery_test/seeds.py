seed_data_csv = """
id,dupe
1,a
2,a
3,a
4,a
""".lstrip()

seed_incremental_overwrite_date_expected_csv = """
id,date_day
10,2020-01-01
20,2020-01-01
30,2020-01-02
40,2020-01-02
""".lstrip()

seed_incremental_overwrite_day_expected_csv = """
id,date_time
10,2020-01-01 00:00:00
20,2020-01-01 00:00:00
30,2020-01-02 00:00:00
40,2020-01-02 00:00:00
""".lstrip()

seed_incremental_overwrite_range_expected_csv = """
id,date_int
10,20200101
20,20200101
30,20200102
40,20200102
""".lstrip()

seed_incremental_overwrite_time_expected_csv = """
id,date_hour
10,2020-01-01 01:00:00
20,2020-01-01 01:00:00
30,2020-01-01 02:00:00
40,2020-01-01 02:00:00
""".lstrip()

seed_merge_expected_csv = """
id,date_time
1,2020-01-01 00:00:00
2,2020-01-01 00:00:00
3,2020-01-01 00:00:00
4,2020-01-02 00:00:00
5,2020-01-02 00:00:00
6,2020-01-02 00:00:00
""".lstrip()
