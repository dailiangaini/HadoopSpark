package package02.scalaTest


/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/27 17:24
 */
object Test1 {

  def main(args: Array[String]): Unit = {
    println("aabb")
    val x = 10

    /**
     * 错误，对val声明的变量，不能重新赋值
     */
    // x =20
    var y = 20
    y = 10
    println(y)

    val z: Int = 10
    val a: Double = 1.0
    val b: Double = 10.0
    println(true)
    println(false)

    val hello_world: String = "hello world"
    println(hello_world.length)
    println(hello_world.take(5))
    println(hello_world.substring(2,6))
    println(hello_world.replace("o", "123"))
    println(hello_world.drop(2))

    val n = 45
    println(s"We have $n apples")

  }

}
