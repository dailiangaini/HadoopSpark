package package07.sparksql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/30 10:18
 */
object TestDataFrame_MySql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestMysql").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val url = "jdbc:mysql://localhost:3306/asmp"
    val table = "TB_APP"
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")
    //需要传入Mysql的URL、表明、properties（连接数据库的用户名密码）
    val df = sqlContext.read.jdbc(url,table,properties)
    df.createOrReplaceTempView("TB_APP")
    sqlContext.sql("select APP_ID, APP_NO, APP_NAME from TB_APP").show()
  }

}
