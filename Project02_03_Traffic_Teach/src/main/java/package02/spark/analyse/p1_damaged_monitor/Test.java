package package02.spark.analyse.p1_damaged_monitor;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/12/27 19:05
 */

import org.apache.spark.sql.SparkSession;

/**
 *
 */
public class Test {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .master("local")
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("show databases").show();
        spark.sql("show tables").show();
    }
}
