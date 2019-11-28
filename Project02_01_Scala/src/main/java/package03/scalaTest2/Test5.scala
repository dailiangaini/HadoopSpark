package package03.scalaTest2

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/28 13:00
 */
object Test5 {
  def main(args: Array[String]): Unit = {
    val t = (1, 3.14, "Red")

    val t2 = new Tuple3(1, 3.14, "Red")

    val t3 = new Tuple3(1, 3.14, "Red")


    val sum = t3._1 + t3._2
    println(sum)
  }
}
