select * from {{ ref('incremental_time_ingestion_partitioning_target') }} where false
