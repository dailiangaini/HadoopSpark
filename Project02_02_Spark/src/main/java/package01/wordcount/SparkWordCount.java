package package01.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/21 15:40
 */
public class SparkWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("testWordCount").setMaster("spark://localhost:7077")
                .set("spark.dynamicAllocation.enabled", "false")
                .set("spark.executor.memory", "4g");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.addJar("/Users/dailiang/Documents/Code/StudyBigData/HadoopSpark/out/artifacts/HadoopSpark_jar/HadoopSpark.jar");
//        JavaRDD<String> lines = sc.textFile("/Users/dailiang/Documents/Code/StudyBigData/HadoopSpark/Project01_01_Hadoop/input/11");
        JavaRDD<String> lines = sc.textFile("hdfs://localhost:9000/opt/test/11");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 9166467038300894254L;

            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.replace(" ", "").split(" ")).iterator();
            }

        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = -2575877317483490894L;

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String,Integer>(s,1);
            }

        });

        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1995267764597335108L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {

                return v1+v2;
            }
        });

        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 7339200658105284851L;

            @Override
            public void call(Tuple2<String, Integer> wordAndCount) throws Exception {
                System.out.println(wordAndCount._1+"  "+ wordAndCount._2);
            }
        });

        sc.close();

    }
}
