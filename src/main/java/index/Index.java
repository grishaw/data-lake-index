package index;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

public class Index {

    final static String indexSuffix = "/index";
    public final static String rootIndexSuffix = "/root-index";

    public static void createIndex(SparkSession sparkSession, String dataLakePath, String indexPath, String colName, int chunkSizeMb, int numOfFiles){
        sparkSession.sparkContext().hadoopConfiguration().setInt("parquet.block.size", 1024 * 1024 * chunkSizeMb);

        Dataset dataLake = sparkSession.read().parquet(dataLakePath);

        // create index files
        dataLake = dataLake.withColumn("fileName", functions.input_file_name());
        Dataset index = dataLake.groupBy(colName).agg(functions.collect_set("fileName").cast(DataTypes.StringType).as("file-names"));
        index = index.orderBy(colName);
        index.coalesce(numOfFiles).write().mode(SaveMode.Overwrite).parquet(indexPath + indexSuffix);

        // create root index
        Dataset indexStored = sparkSession.read().parquet(indexPath + indexSuffix);
        Dataset rootIndex = indexStored.groupBy(functions.input_file_name().as("file")).agg(functions.min(colName).as("min"), functions.max(colName).as("max"));
        rootIndex.coalesce(1).write().mode(SaveMode.Overwrite).json(indexPath + rootIndexSuffix);
    }

    public static String[] getFileNamesByIndexedColumnValueViaIndexDir(SparkSession sparkSession, String indexPath, String colName, String colValue){

        Dataset rootIndex = sparkSession.read().json(indexPath + rootIndexSuffix);

        return getFileNamesByIndexedColumnValueViaIndexRoot(sparkSession, rootIndex, colName, colValue);
    }

    public static String[] getFileNamesByIndexedColumnValueViaIndexRoot(SparkSession sparkSession, Dataset rootIndex, String colName, String colValue){

        String indexFilePath = (String) rootIndex
                .where(functions.col("min").leq(functions.lit(colValue)).and(functions.col("max").geq(functions.lit(colValue))))
                .select("file")
                .as(Encoders.STRING())
                .collectAsList().get(0);

        return getFileNamesByIndexedColumnValueViaIndexFile(sparkSession, indexFilePath, colName, colValue);
    }

    public static String[] getFileNamesByIndexedColumnValueViaIndexFile(SparkSession sparkSession, String indexPath, String colName, String colValue){

        Dataset index = sparkSession.read().parquet(indexPath);

        String fileNamesAsString = (String) index
                .where(functions.col(colName).equalTo(colValue))
                .select("file-names")
                .as(Encoders.STRING())
                .collectAsList().get(0);

        fileNamesAsString = fileNamesAsString.replaceAll("[\\[\\]\\s]", "");

        return fileNamesAsString.split(",");
    }

    public static String[] getFileNamesByIndexedColumnValueViaCassandra(SparkSession sparkSession, String colName, String colValue, String keyspace, String table){

        CassandraConnector cassandraConnector = CassandraConnector.apply(new SparkConf().setAll(sparkSession.conf().getAll()));
        String query = "select file from " + keyspace + "." + table + " where column_name = '" + colName + "' and column_value = '" + colValue + "'";
        try(Session session = cassandraConnector.openSession()){
            return session.execute(query).one().getString(0).replaceAll("[\\[\\]\\s]", "").split(",");
        }
    }

    public static void createIndexCassandra(SparkSession sparkSession, String dataLakePath, String colName, String keyspaceName, String tableName){

        Dataset dataLake = sparkSession.read().parquet(dataLakePath);

        // create index files
        dataLake = dataLake.withColumn("fileName", functions.input_file_name());
        Dataset index = dataLake.groupBy(colName).agg(functions.collect_set("fileName").cast(DataTypes.StringType).as("file"));

        index = index
                .withColumn("column_name", functions.lit(colName))
                .withColumn("column_value", functions.col(colName))
                .drop(colName);

        index
                .write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", keyspaceName)
                .option("table", tableName)
                .mode(SaveMode.Append)
                .save();

    }

}
