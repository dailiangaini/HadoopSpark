package package02.spark.analyse.p9_real_time_road_congestion;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import package02.spark.conf.ConfigurationManager;
import package02.spark.constant.Constants;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/12/30 16:32
 */
public class Real_time_road_congestion {

    public static void main(String[] args) throws InterruptedException {
        String startDate = "2019-12-01";
        String endDate = "2019-12-31";

        // 构建Spark运行时的环境参数
        SparkConf sparkConf = new SparkConf().setAppName("Sample car")
                .setMaster("local");

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();

        SparkContext sparkContext = spark.sparkContext();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(5));
        jssc.sparkContext().setLogLevel("WARN");
        jssc.checkpoint("./checkpoint");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "my-group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        // 构建topic set
        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[] kafkaTopicsSplited = kafkaTopics.split(",");
        Collection<String> topics = Arrays.asList(kafkaTopicsSplited);

        JavaInputDStream<ConsumerRecord<String, String>> carRealTimeLogDStream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        /**
         * 拿到车辆的信息了
         * <monitorId,speed>
         */
        JavaPairDStream<String, Integer> mapToPair = carRealTimeLogDStream.mapToPair(new PairFunction<ConsumerRecord<String, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(ConsumerRecord<String, String> record) throws Exception {
                String key = record.key();
                String log = record.value();
                String[] split = log.split("\t");
                // Tuple2(monitorId,speed)
                return new Tuple2<>(split[1], Integer.parseInt(split[5]));
            }
        });
        /**
         * <monitorId,<speed,1>>
         */
        JavaPairDStream<String, Tuple2<Integer, Integer>> monitorId2SpeedDStream =
            mapToPair.mapValues(new Function<Integer, Tuple2<Integer,Integer>>() {

            /**
             * <monitorId,<speed,1>>
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<Integer, Integer> call(Integer v1) throws Exception {
                return new Tuple2<Integer, Integer>(v1, 1);
            }
        });

        /**
         * 用优化的方式统计速度，返回的是tuple2(monitorId,(总速度，当前卡口通过的车辆总个数))
         */
        JavaPairDStream<String, Tuple2<Integer, Integer>> resultDStream =
                monitorId2SpeedDStream.reduceByKeyAndWindow(new Function2<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>, Tuple2<Integer,Integer>>() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                return new Tuple2<Integer, Integer>(v1._1+v2._1, v1._2+v2._2);
            }
        }, new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {

                return new Tuple2<Integer, Integer>(v1._1 - v2._1,v2._2 - v2._2);
            }
        }, Durations.minutes(5), Durations.seconds(5));

        /**
         * 使用reduceByKeyAndWindow  窗口大小是1分钟，如果你的application还有其他的功能点的话，另外一个功能点不能忍受这个长的延迟。权衡一下还是使用reduceByKeyAndWindow。
         */

        resultDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Tuple2<Integer, Integer>>>() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(JavaPairRDD<String, Tuple2<Integer, Integer>> rdd) throws Exception {
                final SimpleDateFormat secondFormate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

                System.out.println("==========================================");

                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Tuple2<Integer,Integer>>>>() {
                    /**
                     *
                     */
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(Iterator<Tuple2<String, Tuple2<Integer, Integer>>> iterator) throws Exception {
                        while (iterator.hasNext()) {
                            Tuple2<String, Tuple2<Integer, Integer>> tuple = iterator.next();
                            String monitor = tuple._1;
                            int speedCount = tuple._2._1;
                            int carCount = tuple._2._2;
                            System.out.println("当前时间："+secondFormate.format(Calendar.getInstance().getTime())+
                                    "卡扣编号："+monitor + "车辆总数："+carCount + "速度总数：" + speedCount+" 平均速度："+(speedCount/carCount));
                        }
                    }
                });
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
