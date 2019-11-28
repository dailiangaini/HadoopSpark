package package02.scalaTest

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/27 17:24
 */
object Test5 {


  def main(args: Array[String]): Unit = {
    val r = 1 to 5
    r foreach print
    println()
    r.foreach(print)
    println()
    r.foreach(f=>print(f))
    println()
    r.foreach(f=>{print(f)})
    println()
    (5 to 1 by -1) foreach(print)
    println()
    (5 to 1 by -1).foreach(print)
    println()


    var i = 0
    while (i<10){
      print("i= " + i + " ")
      i += 1
    }

    println()

    var x = 0
    do{
      print("x= " + x + " ")
      x += 1
    }while(x < 10)



  }


}
