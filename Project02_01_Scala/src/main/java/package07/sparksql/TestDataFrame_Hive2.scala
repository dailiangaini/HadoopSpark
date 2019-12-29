package package07.sparksql

import org.apache.spark.sql.SparkSession

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/30 10:18
 */
object TestDataFrame_Hive2 {
  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder().appName("TestDataFrame_Hive2").master("local")
      .enableHiveSupport().getOrCreate()
    sparkSession.sql("show databases").show()
  }

  /**
   * 需要在resource文件夹中，添加hive-site.xml
   * +-----+-------+---------+---+----------+------+----+------+
   * |empno|empname|      job|mgr|  hiredate|salary|comm|deptno|
   * +-----+-------+---------+---+----------+------+----+------+
   * |  101| 'duan'|     'it'|  1|'hiredate'| 100.0|10.0|     1|
   * |  102|'duan2'|'product'|  1|    '2018'| 200.0|20.0|     1|
   * +-----+-------+---------+---+----------+------+----+------+
   */

}
