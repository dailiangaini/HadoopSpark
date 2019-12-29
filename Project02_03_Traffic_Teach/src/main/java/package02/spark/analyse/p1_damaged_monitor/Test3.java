package package02.spark.analyse.p1_damaged_monitor;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/12/27 19:05
 */

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 */
public class Test3 {
    public static void main(String[] args) {
        String startDate = "2019-12-01";
        String endDate = "2019-12-31";
        // 构建Spark运行时的环境参数
        SparkConf sparkConf = new SparkConf().setAppName("Java Spark Hive")
                .setMaster("spark://localhost:7077");

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();
        /**
         * 设置jar路径
         * sc.addJar("");
         */

        spark.sql("show databases").show();
        spark.sql("show tables").show();

        spark.sql("use traffic");
        String sql = String.format("SELECT * FROM monitor_flow_action WHERE date>='%s' AND date<='%s'", startDate, endDate);
        Dataset<Row> dataset = spark.sql(sql + " limit 10");
        dataset.show();

    }
}
