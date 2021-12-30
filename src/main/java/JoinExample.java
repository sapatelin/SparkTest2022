import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JoinExample {
    public static void main(String[] args) {
        JoinExample joinExample = new JoinExample();
        joinExample.start();
    }

    public void start(){

        SparkSession spark= SparkSession.builder().appName("JoinExample").master("local").getOrCreate();
        spark.sparkContext().setLogLevel("Error");
        System.out.println("shuffle.partitions-------->"+spark.conf().get("spark.sql.shuffle.partitions"));

       // :Loading Emp Master Data
        Dataset<Row> empMasterDS=spark.read()
                                    .format("csv")
                                    .option("inferSchema",true)
                                    .option("header",true)
                                    .option("sep",";")
                                    .load("dataset/emp_master_reco.csv");
        System.out.println("---------Employee Master Data Print---------");
        empMasterDS.printSchema();
        empMasterDS.show();

        empMasterDS=empMasterDS.repartition(4);

       // Loading Emp Sal Data
        Dataset<Row> empSalDS=spark.read()
                .format("csv")
                .option("inferSchema",true)
                .option("header",true)
                .option("sep",";")
                .load("dataset/emp_sal.csv");
        System.out.println("---------Employee Sal Data Print---------");
        empSalDS.printSchema();
        empSalDS.show();
        empSalDS=empSalDS.repartition(4);

      System.out.println("---------Inner Join Data Print---------");
        Dataset<Row> empMasterSalInnerJoinDS=
                empMasterDS.join(empSalDS,
                                empMasterDS.col("emp_id")
                                        .equalTo(empSalDS.col("emp_id")),
                                "inner");
        System.out.println("empMasterSalInnerJoinDS.count()=="+empMasterSalInnerJoinDS.count());
        System.out.println("---------empMasterSalInnerJoinDS getNumPartitions---------"+empMasterSalInnerJoinDS.rdd().getNumPartitions());
        empMasterSalInnerJoinDS.show();

    }
}

