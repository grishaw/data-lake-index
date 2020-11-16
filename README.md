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
