select * from {{ ref('incremental_time_ingestion_partitioning') }} where false
