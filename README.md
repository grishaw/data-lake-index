# data-lake-index
This is a proof-of-concept implementation of the ideas presented in the "Needle in a haystack queries in cloud data lakes" paper.

## Installation
To build the project, run 
```
mvn clean package
```

## Setup
To create data lake index on a particular column, run 
```
spark-submit --deploy-mode <deploy-mode> --class index.Main <path-to-jar> <path-to-datalake> <path-to-index> <column-name> <chunk-size-in-mb> <num-of-index-files>
```

## Benchmark
For the benchmark execution, run 
```
spark-submit --deploy-mode <deploy-mode> --class benchmark.Main <path-to-jar> <path-to-datalake> <path-to-index> <column-name> <cassandra-keyspace> <cassandra-table>
```

## Benchmark Results

We ran 10 random queries on two different columns - event_id and record_id (pk) with and without index.

### The results of 10 random queries on record-id (in ms)

|    #    |  no-index-spark  |   no-index-athena   | index-s3-spark | index-cassandra-spark| 
|---     |    ---  | ---   |   ---  | ---   | 
|1       |  64453 | 43850 | 4178 | 4817 |
|2       |  63558 | 39470 | 3905 | 3012 | 
|3       |  63367 | 42640 | 3916 | 3076 | 
|4       |  62457 | 41350 | 3611 | 3150 | 
|5       |  62188 | 41160 | 4071 | 3022 | 
|6       |  61811 | 44110 | 3913 | 3046 | 
|7       |  62783 | 41840 | 3932 | 3065 | 
|8       |  61926 | 38020 | 3931 | 3468 | 
|9       |  62308 | 43550 | 3972 | 3117 | 
|10      |  62360 | 41030 | 3846 | 3126 | 

### The results of 10 random queries on event-id (in ms)

|    #    |  no-index-spark  |   no-index-athena   | index-s3-spark | index-cassandra-spark| 
|---     |    ---  | ---   |   ---  | ---   | 
|1       |  67302 | 40980 | 8158 | 7572 |
|2       |  65585 | 41190 | 7184 | 6101 | 
|3       |  65024 | 40790 | 9715 | 6181 | 
|4       |  64431 | 40600 | 9441 | 8712 | 
|5       |  63987 | 42040 | 9773 | 3629 | 
|6       |  64360 | 41540 | 7323 | 6200 | 
|7       |  64461 | 42990 | 6348 | 6893 | 
|8       |  143607 | 39820 | 6565 | 6515 | 
|9       |  95804 | 44150 | 6703 | 6976 | 
|10      |  64484 | 40580 | 6802 | 6211 | 