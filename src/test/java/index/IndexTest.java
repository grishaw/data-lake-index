package index;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static index.Index.indexSuffix;
import static index.Index.rootIndexSuffix;


public class IndexTest {

    private static final String DATA_LAKE_PATH = "src/test/resources/sample_data_lake";

    @Test
    public void createIndexTest(){
        SparkSession sparkSession = initTestSparkSession("createIndexTest");
        Dataset dataLake = sparkSession.read().parquet(DATA_LAKE_PATH);


        // test column with unique value
        String colName = "id";
        String indexPath = "target/sample_data_lake_index_1";

        Index.createIndex(sparkSession, DATA_LAKE_PATH, indexPath, colName, 1, 10);
        Dataset myIndex = sparkSession.read().parquet(indexPath + indexSuffix);

        Assert.assertEquals(dataLake.select(colName).distinct().count(), myIndex.count());

        // test column with non-unique value
        colName = "date";
        indexPath = "target/sample_data_lake_index_2";

        Index.createIndex(sparkSession, DATA_LAKE_PATH, indexPath, colName, 1, 10);
        myIndex = sparkSession.read().parquet(indexPath + indexSuffix);
        Assert.assertEquals(dataLake.select(colName).distinct().count(), myIndex.count());
        Assert.assertTrue(myIndex.where(functions.col("file-names").contains(",")).count()>0);
    }

    @Test
    public void createIndexCassandraTest() throws Exception {
        try {
            SparkSession sparkSession = initTestSparkSession("createIndexCassandraTest");

            // start cassandra docker
            Runtime.getRuntime().exec("docker run --name test_cassandra -p 9042:9042 cassandra").waitFor(30, TimeUnit.SECONDS);

            System.out.println("started cassandra");

            String keyspace = "test";
            String table = "index_test";
            CassandraConnector connector = CassandraConnector.apply(sparkSession.sparkContext());
            try (Session session = connector.openSession()) {
                session.execute("CREATE KEYSPACE test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
                session.execute("CREATE TABLE " + keyspace + "." + table +"(column_name text, column_value text, file text, PRIMARY KEY ((column_name, column_value)));");
            }

            Index.createIndexCassandra(sparkSession, DATA_LAKE_PATH, "id", "test", "index_test");

            Dataset result = sparkSession
                    .read()
                    .format("org.apache.spark.sql.cassandra")
                    .option("keyspace", keyspace)
                    .option("table", table)
                    .load();

            Dataset dataLake = sparkSession.read().parquet(DATA_LAKE_PATH);
            Assert.assertEquals(dataLake.select("id").distinct().count(), result.count());

            // verify content
            String colName = "id";
            String colValue = String.valueOf(ThreadLocalRandom.current().nextInt(100_000));
            String expectedFileName = sparkSession
                    .read()
                    .parquet(DATA_LAKE_PATH)
                    .where(functions.col(colName).equalTo(colValue))
                    .select(functions.input_file_name())
                    .as(Encoders.STRING())
                    .collectAsList()
                    .get(0);

            Assert.assertEquals(expectedFileName, Index.getFileNamesByIndexedColumnValueViaCassandra(sparkSession, colName, colValue, keyspace, table)[0]);

        }finally{
            Runtime.getRuntime().exec("docker rm --force test_cassandra");
        }
    }

    @Test
    public void getFileNamesByIndexedColumnValueTest(){
        SparkSession sparkSession = initTestSparkSession("getFileNamesByIndexedColumnValueTest");
        String indexPath = "target/sample_data_lake_index";
        String colName = "id";
        String colValue = String.valueOf(ThreadLocalRandom.current().nextInt(100_000));

        String expectedFileName = sparkSession
                .read()
                .parquet(DATA_LAKE_PATH)
                .where(functions.col(colName).equalTo(colValue))
                .select(functions.input_file_name())
                .as(Encoders.STRING())
                .collectAsList()
                .get(0);

        Index.createIndex(sparkSession, DATA_LAKE_PATH, indexPath, colName, 1,10);

        String[] fileNames = Index.getFileNamesByIndexedColumnValueViaIndexDir(sparkSession, indexPath, colName, colValue);

        Assert.assertEquals(expectedFileName, fileNames[0]);
    }

    @Test
    public void getFileNamesByRootTest(){
        SparkSession sparkSession = initTestSparkSession("getFileNamesByRootTest");
        String indexPath = "target/sample_data_lake_index_root_test";

        String colName = "id";
        String colValue = String.valueOf(ThreadLocalRandom.current().nextInt(100_000));

        String expectedFileName = sparkSession
                .read()
                .parquet(DATA_LAKE_PATH)
                .where(functions.col(colName).equalTo(colValue))
                .select(functions.input_file_name())
                .as(Encoders.STRING())
                .collectAsList()
                .get(0);

        Index.createIndex(sparkSession, DATA_LAKE_PATH, indexPath, colName, 1,10);

        Dataset rootIndex = sparkSession.read().json(indexPath + rootIndexSuffix);

        NavigableMap <Long, String> myMap = new TreeMap<>();

        List <GenericRowWithSchema> rootAsList = rootIndex.collectAsList();
        for(GenericRowWithSchema row : rootAsList){
            myMap.put((Long)row.values()[2], (String)row.values()[0]);
        }

        String[] fileNames = Index.getFileNamesByIndexedColumnValueViaIndexFile(sparkSession, myMap.floorEntry(Long.valueOf(colValue)).getValue(), colName, colValue);

        Assert.assertEquals(expectedFileName, fileNames[0]);
    }

    // generates "sample_data_lake" from "sample_data_set"
    // sample data set is taken from https://data.world/associatedpress/johns-hopkins-coronavirus-case-tracker
    private static void generateLake(){
        SparkSession sparkSession = initTestSparkSession("getFileNamesByIndexedColumnValueTest");

        Dataset df = sparkSession.read().option("header", "true").csv("src/test/resources/sample_data_set");

        df = df.withColumn("id", functions.monotonicallyIncreasingId());

        df.repartition(functions.col("state")).write().parquet(DATA_LAKE_PATH);
    }

    public static SparkSession initTestSparkSession(String appName) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf()
                .setAppName(appName)
                .setMaster("local[*, 2]")
                .set("spark.driver.host", "localhost")
                .set("spark.sql.shuffle.partitions", "5")
                .set("spark.default.parallelism", "5")
                .set("spark.sql.autoBroadcastJoinThreshold", "-1")
                ;

        return SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
    }
}
