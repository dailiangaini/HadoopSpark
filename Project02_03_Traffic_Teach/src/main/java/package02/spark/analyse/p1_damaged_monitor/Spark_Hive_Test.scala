package package02.spark.analyse.p1_damaged_monitor

import org.apache.spark.sql.SparkSession

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/30 10:18
 */
object Spark_Hive_Test {
  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder().appName("Spark_Hive_Test").master("local")
      .enableHiveSupport().getOrCreate()
    sparkSession.sql("show databases").show()
  }
}
