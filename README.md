# UpsertUsingIndex

Capturing the incoming data and keeping only current snapshot of it using index table.
it can be done other way also but doing this won't impact the existing table if there are some views on top of base table.
Steps that are involved:- 

1. In case of batch load, load the data directly in base table with one extra column (verison) it can be anything but in increasing order and load the primary key and verison column in index table.
2. Index table is partitioned on run_id(how many times it has ran) and base table is partitioned on business key and version.
3. In case of delta load, load data into base table with new verison.
4. pull latest partitioned data from index table.
5. perform union with new coming data and remove duplicate data and keep the latest key in index table within new run_id partitioned.
6. After some period compaction Job will run and compact all the partition's data of base table and remove duplication and store in new partition.
7. delete the older one. Same process is done with index table too.
8. Views will be pointing to base table with the help of index table, so they always be pointing to latest partitions.
