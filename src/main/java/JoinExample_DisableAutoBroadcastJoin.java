import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JoinExample_DisableAutoBroadcastJoin {
    public static void main(String[] args) {
        JoinExample_DisableAutoBroadcastJoin joinExample = new JoinExample_DisableAutoBroadcastJoin();
        joinExample.start();
    }

    public void start(){

        SparkSession spark= SparkSession.builder().appName("JoinExample").master("yarn").getOrCreate();
        spark.sparkContext().setLogLevel("Error");
        System.out.println("-----------------------------------------------------------------------------------------");
        System.out.println("spark.sql.shuffle.partitions            -------->"+spark.conf().get("spark.sql.shuffle.partitions"));
        System.out.println("spark.conf.autoBroadcastJoinThreshold-------->"+spark.conf().get("spark.sql.autoBroadcastJoinThreshold"));

        System.out.println("-----------------------------------------------------------------------------------------");

        //Disable autoBroadcastJoinThreshold join
        spark.conf().set("spark.sql.autoBroadcastJoinThreshold",-1);
        System.out.println("spark.conf.autoBroadcastJoinThreshold (After Disable)-------->"+spark.conf().get("spark.sql.autoBroadcastJoinThreshold"));

       // :Loading Emp Master Data
        Dataset<Row> empMasterDS=spark.read()
                                    .format("csv")
                                    .option("inferSchema",true)
                                    .option("header",true)
                                    .option("sep",",")
                                    .load("hdfs://sandeep-demo-clstr-m:8020/home/dataset/emp_master.csv");

        System.out.println("---------Employee Master Data Print---------");
        empMasterDS.printSchema();
        empMasterDS.show();
        empMasterDS=empMasterDS.repartition(10);

       // Loading Emp Sal Data
        Dataset<Row> empSalDS=spark.read()
                .format("csv")
                .option("inferSchema",true)
                .option("header",true)
                .option("sep",",")
                .load("hdfs://sandeep-demo-clstr-m:8020/home/dataset/emp_sal.csv");
        System.out.println("---------Employee Sal Data Print---------");
        empSalDS.printSchema();
        empSalDS.show();
        empSalDS=empSalDS.repartition(10);

      System.out.println("---------Inner Join Data Print---------");
        Dataset<Row> empMasterSalInnerJoinDS=
                empMasterDS.join(empSalDS,
                                empMasterDS.col("id")
                                        .equalTo(empSalDS.col("id")),
                                "inner").drop(empSalDS.col("id"));
        System.out.println("empMasterSalInnerJoinDS.count()-->"+empMasterSalInnerJoinDS.count());
        System.out.println("---------empMasterSalInnerJoinDS getNumPartitions---------"+empMasterSalInnerJoinDS.rdd().getNumPartitions());
        empMasterSalInnerJoinDS.show(10);

    }
}

