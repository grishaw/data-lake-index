package benchmark;

import index.Index;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.*;

import static index.Index.rootIndexSuffix;

public class Main {


    public static void main(String[] args) {
        String dataLakePath = args[0];
        String indexPath = args[1];
        String columnName = args[2];
        String keyspace = args[3];
        String table = args[4];

        SparkSession sparkSession = SparkSession.builder().appName("data-lake-index-benchmark").getOrCreate();

        Dataset datalake = sparkSession.read().parquet(dataLakePath);

        // Test no index
        testNoIndex(datalake, columnName);

        // Test with index no cache
        testIndexNoCache(datalake, columnName, indexPath);

        // Test with index + root cache
        testIndexWithCache(datalake, columnName, indexPath);

        //Test with cassandra index
        testIndexCassandra(datalake, columnName, keyspace, table);

    }

    private static List<String> getRandomValuesForColumn(Dataset datalake, String columnName){
        return datalake
                .orderBy(functions.rand())
                .limit(10)
                .select(columnName)
                .as(Encoders.STRING())
                .collectAsList();
    }

    private static void testNoIndex(Dataset datalake, String columnName){
        System.out.println("running testNoIndex ******************");
        List<String> sampleValues = getRandomValuesForColumn(datalake, columnName);
        Map<String, Long> latencyResults = new LinkedHashMap<>();

        for (String value : sampleValues){
            long start = System.currentTimeMillis();
            System.out.println("going to get value " + value);

            datalake
                    .where(functions.col(columnName).equalTo(value))
                    .show(1000);

            long end = System.currentTimeMillis();
            long took = end-start;
            System.out.println("took :" + took);
            latencyResults.put(value, took);
        }

        System.out.println(latencyResults);
        System.out.println("------------------------------");
        System.out.println("finished testNoIndex ******************");
    }

    private static void testIndexNoCache(Dataset datalake, String columnName, String indexPath){
        System.out.println("running testIndexNoCache ******************");
        Map<String, Long> indexLatencyResults = new LinkedHashMap<>();
        Map<String, Long> latencyResults = new LinkedHashMap<>();

        // get random values
        List<String> sampleValues = getRandomValuesForColumn(datalake, columnName);

        // get value and print
        for (String value : sampleValues){
            long start = System.currentTimeMillis();
            System.out.println("going to get file names from index ");

            String[] files = Index.getFileNamesByIndexedColumnValueViaIndexDir(datalake.sparkSession(), indexPath, columnName, value);
            long endIndex = System.currentTimeMillis();
            long indexTook = endIndex - start;
            System.out.println("index read took:" + indexTook);
            indexLatencyResults.put(value, indexTook);

            System.out.println("going to get value " + value);
            datalake.sparkSession().read().parquet(files)
                    .where(functions.col(columnName).equalTo(value))
                    .show(1000);
            long end = System.currentTimeMillis();
            long took = end-start;

            System.out.println("took :" + took);
            latencyResults.put(value, took);
        }

        System.out.println(indexLatencyResults);
        System.out.println(latencyResults);
        System.out.println("------------------------------");
        System.out.println("finished testIndexNoCache ******************");

    }

    private static void testIndexWithCache(Dataset datalake, String columnName, String indexPath){
        System.out.println("running testIndexWithCache ******************");

        Map<String, Long> indexLatencyResults = new LinkedHashMap<>();
        Map<String, Long> latencyResults = new LinkedHashMap<>();

        // get random values
        List<String> sampleValues = getRandomValuesForColumn(datalake, columnName);

        // get root index
        Dataset rootIndex = datalake.sparkSession().read().json(indexPath + rootIndexSuffix).cache();

        rootIndex.show();

        // get value and print
        for (String value : sampleValues){
            long start = System.currentTimeMillis();
            System.out.println("going to get file names from index ");

            String[] files = Index.getFileNamesByIndexedColumnValueViaIndexRoot(datalake.sparkSession(), rootIndex, columnName, value);
            long endIndex = System.currentTimeMillis();
            long indexTook = endIndex - start;
            System.out.println("index read took:" + indexTook);
            indexLatencyResults.put(value, indexTook);

            System.out.println("going to get value " + value);
            datalake.sparkSession().read().parquet(files)
                    .where(functions.col(columnName).equalTo(value))
                    .show(1000);
            long end = System.currentTimeMillis();
            long took = end-start;

            System.out.println("took :" + took);
            latencyResults.put(value, took);
        }

        System.out.println(indexLatencyResults);
        System.out.println(latencyResults);
        System.out.println("------------------------------");
        System.out.println("finished testIndexWithCache ******************");
    }

    private static void testIndexCassandra(Dataset datalake, String columnName, String keyspace, String table){
        System.out.println("running testIndexCassandra ******************");

        Map<String, Long> indexLatencyResults = new LinkedHashMap<>();
        Map<String, Long> latencyResults = new LinkedHashMap<>();

        // get random values
        List<String> sampleValues = getRandomValuesForColumn(datalake, columnName);

        // get value and print
        for (String value : sampleValues){
            long start = System.currentTimeMillis();
            System.out.println("going to get file names from index ");

            String[] files = Index.getFileNamesByIndexedColumnValueViaCassandra(datalake.sparkSession(), columnName, value, keyspace, table);
            long endIndex = System.currentTimeMillis();
            long indexTook = endIndex - start;
            System.out.println("index read took:" + indexTook);
            indexLatencyResults.put(value, indexTook);

            System.out.println("going to get value " + value);
            datalake.sparkSession().read().parquet(files)
                    .where(functions.col(columnName).equalTo(value))
                    .show(1000);
            long end = System.currentTimeMillis();
            long took = end-start;

            System.out.println("took :" + took);
            latencyResults.put(value, took);
        }

        System.out.println(indexLatencyResults);
        System.out.println(latencyResults);
        System.out.println("------------------------------");
        System.out.println("finished testIndexCassandra ******************");
    }
}
