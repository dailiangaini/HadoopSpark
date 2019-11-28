package package03.scalaTest2

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/28 11:13
 */
object Test1{

  def main(args: Array[String]): Unit = {
    var z: Array[String] = new Array[String](3)
    var zz = new Array[String](3)

    val myList = Array(1.9, 2.9, 3.4, 3.5)
    for(x <- myList){
      print(x + " ")
    }

    var total:Double = 0

    for(i <- myList.indices){
      total += myList(i)
    }

    for(i <- 0 until myList.length){
      total += myList(i)
    }

    for(i <- 0 to myList.length-1 ){
      total += myList(i)
    }
    print(total)
  }
}
