package package02.scalaTest

import scala.util.control.Breaks

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/27 17:24
 */
object Test7 {


  def main(args: Array[String]): Unit = {
    var a = 0
    var numList = (1 to 10).toList

    var loop = new Breaks
    loop.breakable{
      for(a <- numList){
        println("a.value="+ a)
        if(4 == a){
          loop.break()
        }
      }
    }

  }


}
