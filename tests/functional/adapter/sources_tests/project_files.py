SCHEMA__YML = """
version: 2

sources:
  - name: test_source
    loader: custom
    freshness:
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}
    schema: "{{ var('test_run_schema') }}"
    tables:
      - name: source_with_loaded_at_field
        identifier: source_with_loaded_at_field
        loaded_at_field: "{{ var('test_loaded_at') | as_text }}"
      - name: source_without_loaded_at_field
        identifier: source_without_loaded_at_field
"""


MODEL__WITH_LOADED_AT_FIELD__SQL = """
select * from {{ source('test_source', 'source_with_loaded_at_field') }}
"""


MODEL__WITHOUT_LOADED_AT_FIELD__SQL = """
select * from {{ source('test_source', 'source_without_loaded_at_field') }}
"""


SEED__SOURCE__CSV = """id,first_name,email,ip_address,updated_at
1,Larry,lking0@miitbeian.gov.cn,'69.135.206.194',2008-09-12 19:08:31
2,Larry,lperkins1@toplist.cz,'64.210.133.162',1978-05-09 04:15:14
3,Anna,amontgomery2@miitbeian.gov.cn,'168.104.64.114',2011-10-16 04:07:57
4,Sandra,sgeorge3@livejournal.com,'229.235.252.98',1973-07-19 10:52:43
5,Fred,fwoods4@google.cn,'78.229.170.124',2012-09-30 16:38:29
6,Stephen,shanson5@livejournal.com,'182.227.157.105',1995-11-07 21:40:50
7,William,wmartinez6@upenn.edu,'135.139.249.50',1982-09-05 03:11:59
""".lstrip()
