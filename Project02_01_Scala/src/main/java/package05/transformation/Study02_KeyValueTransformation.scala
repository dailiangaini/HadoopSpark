package package05.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/28 16:30
 */
object Study02_KeyValueTransformation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("StudySpark")
    val sc = new SparkContext(conf)

    /**
     * 1.mapValues
     * mapValues是针对[K,V]中V值进行map操作
     */
    def testMapValues(): Unit = {
      val a =  sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu"),  2)
      val b = a.map(x => (x.length,x))
      b.mapValues("x"+_+"x").collect()
    }

    /**
     * 2.combineByKey
     * 使用用户设置好的聚合函数对每个key中的value进行组合（combine）,可以将输入类型为RDD[(k,V)]转换成RDD[(k,c)]
     */
    def testCombineByKey(): Unit = {
      val a =  sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu"),  3)
      val b = sc.parallelize(List(1,1,2,2,3), 3)
      val c: RDD[(String, Int)] = a.zip(b)
//      c.combineByKey(List(_), (x: List[String], y: Int) => y::x, (x: Int, y: Int) => x +y )
//
//      val d = c.combineByKey(List(_), (x: List[String], y: String) => y::x, (x: List[String], y: String) => x:::y)
    }


    /**
     * 3.reduceByKey
     * 对元素为KV对的RDD中key相同的value进行value进行binary_function的reduce操作，
     * 因此，key相同的多个元素的值被reduce为一个值，然后与原RDD中的key组合成一个新的KV对。
     */
    def testReduceByKey(): Unit = {
      val a =  sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu"),  3)
      val b: RDD[(Int, String)] = a.map(x => (x.length, x))
      val result: RDD[(Int, String)] = b.reduceByKey(_+_)
      val result2: RDD[(Int, String)] = b.reduceByKey((x:String, y: String) => x +y)
    }

    /**
     * 4.PartitionBy
     * 对RDD进行分区操作。
     */
    def testPartitionBy(): Unit = {
      val a =  sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu"))
      a.map(x => (x, x.length)).partitionBy(new HashPartitioner(2))
    }

    /**
     * 5.cogroup
     * cogroup指对两个RDD中的KV元素，每个RDD中相同的key中的元素分别聚合成一个集合。
     */
    def testCogroup(): Unit = {
      val rdd: RDD[Int] = sc.parallelize(List(1,2,1,3))
      var rdd2 = rdd.map((_, "b"))
      var rdd3 = rdd.map((_, "c"))
      rdd2.cogroup(rdd3).collect()
    }

    /**
     * 6.join
     * 对两个需要连接的RDD进行cogroup函数操作。
     */
    def testJoin(): Unit = {
      val a =  sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu"),  2)
      val b: RDD[(Int, String)] = a.keyBy(_.length)

      val c: RDD[String] = sc.parallelize(List("dog", "salmon", "pig"), 2)
      val d: RDD[(Int, String)] = c.keyBy(_.length)
      b.join(d).collect()
    }

    /**
     * 7.leftOutJoin
     */
    def testLeftOutJoin(): Unit = {
      val a =  sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu"),  2)
      val b: RDD[(Int, String)] = a.keyBy(_.length)

      val c: RDD[String] = sc.parallelize(List("dog", "salmon", "pig"), 2)
      val d: RDD[(Int, String)] = c.keyBy(_.length)
      b.leftOuterJoin(d).collect()
    }

    /**
     * 8.rightOutJoin
     */
    def testRightOutJoin(): Unit = {
      val a =  sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu"),  2)
      val b: RDD[(Int, String)] = a.keyBy(_.length)

      val c: RDD[String] = sc.parallelize(List("dog", "salmon", "pig"), 2)
      val d: RDD[(Int, String)] = c.keyBy(_.length)
      b.leftOuterJoin(d).collect()
    }
  }
}
