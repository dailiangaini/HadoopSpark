package package02.scalaTest

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/27 17:24
 */
object Test2 {

  def main(args: Array[String]): Unit = {
    val a = Array(11, 9)
    for (elem <- a) {
      println(elem)
    }
    println(a(0), " ", a(1))

    /**
     * 添加前缀s，则字符串中可以使用表达式
     */
    println(s"${a(0)}")

    /**
     * 添加前缀f, 则可以对字符串进行格式化
     */

     println(f"power of 5: ${math.pow(5, 2)}%1.0f")

     println(f"Square root of 122 : ${math.sqrt(122)}%1.4f")

    /**
     * 添加前缀row, 忽略特殊字符
     */
    println("New Line \n Create")
    println(raw"New Line \n Create")

    /**
     * 三个引号，可以使字符串跨多行，并包含引号
     */
    var html ="""
      <<hello>>
      <p>"aa"</p>
    """
    println(html)
  }

}
