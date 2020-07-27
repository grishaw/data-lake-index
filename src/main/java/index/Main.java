package index;

import org.apache.spark.sql.SparkSession;

public class Main {

    public static void main(String[] args) {

        String dataLakePath = args[0];
        String indexPath = args[1];
        String columnName = args[2];
        int chunkSizeMb = Integer.parseInt(args[3]);
        int numOfFiles = Integer.parseInt(args[4]);
        boolean isCassandra = Boolean.parseBoolean(args[5]);
        String keyspace = args[6];
        String table = args[7];

        SparkSession sparkSession = SparkSession.builder().appName("data-lake-index-create").getOrCreate();

        if (isCassandra) {
            Index.createIndexCassandra(sparkSession, dataLakePath, columnName, keyspace, table);
        }else{
            Index.createIndex(sparkSession, dataLakePath, indexPath, columnName, chunkSizeMb, numOfFiles);
        }

    }
}
