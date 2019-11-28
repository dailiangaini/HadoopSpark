package package04.scalaTest3

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/28 14:27
 */
object Test4 {
  def main(args: Array[String]): Unit = {
    val p1 = new MyPoint(2,3)
    val p2 = new MyPoint(2,3)
    val p3 = new MyPoint(2,4)

    println(p1.isEqual(p2))
    println(p1.isNotEqual(p3))
  }
}
