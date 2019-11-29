package package06.actions

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/28 16:34
 */
object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TTT222")
      .set("spark.dynamicAllocation.enabled", "false")
    val sc = new SparkContext(conf)

    /**
     * 1.foreach
     * for循环
     */
    def testForeach(): Unit = {
      val c = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu"))
      c.foreach(x => println(x + "s are yummy") )
      /**
       * testForeach
       */
    }
    testForeach

    /**
     * 2.saveAsTextFile
     * 保存结果到HDFS
     */
    def testSaveAsTextFile(): Unit = {
      val rdd: RDD[Int] = sc.parallelize(1 to 1000, 3)
      val file = new File("/tmp/dailiang/test2")
      if(file.exists()){
        if(file.isDirectory){
          file.listFiles().foreach(item => item.delete())
        }
        file.delete()
      }
      rdd.saveAsTextFile("/tmp/dailiang/test2")
    }


    /**
     * 3.saveAsObjectFile
     * saveAsObjectFile用于将RDD中的元素序列化成对象，存储到文件中。对于HDFS，默认采用SequenceFile保存。
     */
    def testSaveAsObjectFile(): Unit = {
      val rdd: RDD[Int] = sc.parallelize(1 to 10, 3)
      val file = new File("/tmp/dailiang/test2")
      if(file.exists()){
        if(file.isDirectory){
          file.listFiles().foreach(item => item.delete())
        }
        file.delete()
      }
      rdd.saveAsObjectFile("/tmp/dailiang/test2")
      val rdd2: RDD[Int] = sc.objectFile[Int]("/tmp/dailiang/test2")
      val ints: Array[Int] = rdd2.collect()
      ints.foreach(item=>print(item + " "))

      /**
       * 7 8 9 10 4 5 6 1 2 3
       */
    }

    /**
     * 4.collect
     * 将RDD中数据收集起来，变成一个Array，仅限数据量比较小的时候。
     */
    def testCollect(): Unit = {
      val c = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu"))
      val strings: Array[String] = c.collect()
      strings.foreach(item=>print(item + " "))

      /**
       * Gnu Cat Rat Dog Gnu
       */
    }

    /**
     * 5.collectAsMap
     * 返回hashMap包含所有RDD中的分片，key如果重复，后边的元素会覆盖前面的元素。
     * zip函数用于将两个RDD组合成key/value形式的RDD。
     */
    def testCollectAsMap(): Unit = {
      val a = sc.parallelize(List(1,2,1,3))
      val b = a.zip(a)
      val intToInt: collection.Map[Int, Int] = b.collectAsMap()
      intToInt.foreach(item=>print(item + " "))

      /**
       * (2,2) (1,1) (3,3)
       */
    }


    /**
     * 6.reduceByKeyLocally
     * 先执行reduce，然后再执行collectAsMap
     */
    def testReduceByKeyLocally(): Unit = {
      val a = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu"))
      val b: RDD[(Int, String)] = a.map(x => ( x.length,x))
      val tuples: Array[(Int, String)] = b.reduceByKey(_+_).collect()
      tuples.foreach(item=>print(item + " "))

      /**
       * (3,GnuCatRatDogGnu)
       */
    }

    /**
     * 7.lookup
     * 查找，针对key-value类型的RDD
     */
    def testLookup(): Unit = {
      val a = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu"))
      val b: RDD[(Int, String)] = a.map(x => ( x.length,x))
      val tuples: Array[(Int, String)] = b.collect()
      val strings: Seq[String] = b.lookup(3)
      tuples.foreach(item=>print(item + " "))
      println()
      println("____")
      strings.foreach(item=>print(item + " "))

      /**
       * (3,Gnu) (3,Cat) (3,Rat) (3,Dog) (3,Gnu)
       * ____
       * Gnu Cat Rat Dog Gnu
       */
    }
    /**
     * 8.count
     * 总数
     */
    def testCount(): Unit = {
      val a = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu"))
      val num: Long = a.count()
      print(num)

      /**
       * 5
       */
    }

    /**
     * 9.top
     * 返回最大的K个元素。
     */
    def testTop(): Unit = {
      val c = sc.parallelize(List(6,9, 4, 7,5,8),2)
      val ints: Array[Int] = c.top(2)
      ints.foreach(item=>print(item + " "))

      /**
       * 9 8
       */
    }

    /**
     * 10.reduce
     * 相对于对RDD中的元素进行reduceLeft操作
     */
    def testReduce(): Unit = {
      val a = sc.parallelize(1 to 100, 3)
      val unit: Int = a.reduce(_+_)
      println(unit)

      /**
       * 5050
       */
    }

    /**
     * 11.fold
     * fold与reduce类似，接收与reduce接收的函数签名相同的函数，另外再加一个初始值作为第一次调用的结果。
     * 结果为：(区+1)*(初始值)+list(值)
     */
    def testFold(): Unit = {
      var a = sc.parallelize(List(1,2,3), 3)
      val unit: Int = a.fold(0)(_+_)
      println(unit)

      /**
       * 6
       */
    }

    /**
     * 12.aggregate
     * aggregate先对对个分区的所有元素进行aggregate操作，再对分区的结果进行fold操作。
     */
    def testAggregate(): Unit = {
      var a = sc.parallelize(List(1,2,3,4,5,6), 2)
      val unit: Int = a.aggregate(0)(math.max(_,_), _+_)
      println(unit)
      /**
       * 9
       */
    }

  }
}
