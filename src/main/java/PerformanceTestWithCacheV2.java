import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Measuring performance without cache, with cache, and with checkpoint.
 *
 * @author jgp
 */
public class PerformanceTestWithCacheV2 {
    enum Mode {
        NO_CACHE_NO_CHECKPOINT, CACHE, CHECKPOINT, CHECKPOINT_NON_EAGER
    }

    private SparkSession spark;

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        PerformanceTestWithCacheV2 app = new PerformanceTestWithCacheV2();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        // Creates a session on a local master
        this.spark = SparkSession.builder()
                .appName("Example of cache and checkpoint")
                .master("yarn")
                .getOrCreate();
        SparkContext sc = spark.sparkContext();
        sc.setLogLevel("Error");

        sc.setCheckpointDir("/home/patel_sandeep81/tmp");

        // Specify the number of records to generate
        int recordCount = 1000;

        // Create and process the records without cache or checkpoint
        long t0 = processDataframe(recordCount, Mode.NO_CACHE_NO_CHECKPOINT);

        // Create and process the records with cache
        long t1 = processDataframe(recordCount, Mode.CACHE);

        // Create and process the records with a checkpoint
        long t2 = processDataframe(recordCount, Mode.CHECKPOINT);

        // Create and process the records with a checkpoint
        long t3 = processDataframe(recordCount, Mode.CHECKPOINT_NON_EAGER);

        System.out.println("\nProcessing times");
        System.out.println("Without cache ............... " + t0 + " ms");
        System.out.println("With cache .................. " + t1 + " ms");
        System.out.println("With checkpoint ............. " + t2 + " ms");
    }

    /**
     *
     * @param recordCount
     * @param mode
     * @return
     */
    private long processDataframe(int recordCount, Mode mode) {
        Dataset<Row> df =
                RecordGeneratorUtils.createDataframe(this.spark, recordCount);

        long t0 = System.currentTimeMillis();
        Dataset<Row> topDf = df.filter(col("rating").equalTo(5));
        switch (mode) {
            case CACHE:
                topDf = topDf.cache();
                break;

            case CHECKPOINT:
                topDf = topDf.checkpoint();
                break;

            case CHECKPOINT_NON_EAGER:
                topDf = topDf.checkpoint(false);
                break;

        }

        List<Row> langDf =
                topDf.groupBy("lang").count().orderBy("lang").collectAsList();
        List<Row> yearDf =
                topDf.groupBy("year").count().orderBy(col("year").desc()).collectAsList();
        long t1 = System.currentTimeMillis();

        System.out.println("Processing took " + (t1 - t0) + " ms.");

        System.out.println("Five-star publications per language");
        for (Row r : langDf) {
            System.out.println(r.getString(0) + " ... " + r.getLong(1));
        }

        System.out.println("\nFive-star publications per year");
        for (Row r : yearDf) {
            System.out.println(r.getInt(0) + " ... " + r.getLong(1));
        }

        return t1 - t0;
    }
}
