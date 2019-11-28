package package02.scalaTest

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/27 17:24
 */
object Test6 {


  def main(args: Array[String]): Unit = {
    var a = 0
    for(a <- 1 to 10){
      print(a)
    }
    println()

    var x = 10
    if(1==x) println("yeah")
    if(10==x) println("yeah10")

    println(if(101==x) "yeah" else "haha")

  }


}
