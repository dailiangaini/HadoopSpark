package package03.scalaTest2

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/28 13:00
 */
object Test4 {
  def main(args: Array[String]): Unit = {
    val colors = Map(
      "read" ->"1",
      "azure" ->"2",
      "peru" ->"3"
    )

    val nums: Map[Int, Int] = Map()

    println(colors.keys)
    println(colors.keySet)
    println(colors.values)
    println()
    colors.foreach(item => println(item))
  }
}
