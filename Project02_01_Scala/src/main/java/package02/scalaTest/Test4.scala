package package02.scalaTest

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/27 17:24
 */
object Test4 {


  def main(args: Array[String]): Unit = {
    val test3: Test_3 = new Test_3()
    test3.printMe(test3.addInt(3, 5))
  }

}
