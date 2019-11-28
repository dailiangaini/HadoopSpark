package package03.scalaTest2

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/28 12:58
 */
object Test3 {
  def main(args: Array[String]): Unit = {
    val site = Set("google", "baidu")
    val nums: Set[Int] = Set()

    print(site.head)
    print(site.tail)
    for (elem <- site) {
      print(elem)
    }
  }
}
