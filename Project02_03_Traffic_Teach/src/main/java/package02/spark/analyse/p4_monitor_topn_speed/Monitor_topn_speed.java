package package02.spark.analyse.p4_monitor_topn_speed;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/12/27 19:05
 */

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import package02.spark.constant.Constants;
import package02.spark.skynet.SpeedSortKey;
import package02.spark.util.StringUtils;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class Monitor_topn_speed {
    
    public static void main(String[] args) {
        String startDate = "2019-12-01";
        String endDate = "2019-12-31";
        // 构建Spark运行时的环境参数
        SparkConf sparkConf = new SparkConf().setAppName("Java Spark Hive")
                .setMaster("local");

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();
        spark.sql("show databases").show();
        spark.sql("show tables").show();

        SparkContext sparkContext = spark.sparkContext();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        /**
         * 设置jar路径
         * sc.addJar("");
         */

        spark.sql("show databases").show();
        spark.sql("show tables").show();

        spark.sql("use traffic");
        String sql = String.format("SELECT * FROM monitor_flow_action WHERE date>='%s' AND date<='%s'", startDate, endDate);

        /**
         * 获取指定日期内检测的monitor_flow_action中车流量数据，返回JavaRDD<Row>
         */
        JavaRDD<Row> cameraRDD = spark.sql(sql).javaRDD();
        /**
         * 持久化
         */
        cameraRDD = cameraRDD.cache();

        /**
         * 将row类型的RDD 转换成kv格式的RDD   k:monitor_id  v:row
         */

        /**
         * 将row类型的RDD 转换成kv格式的RDD   k:monitor_id  v:row
         */
        JavaPairRDD<String, Row> monitor2DetailRDD = cameraRDD.mapToPair(new PairFunction<Row, String, Row>() {
            /**
             * row.getString(1) 是得到monitor_id 。
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(1),row);
            }
        });
        /**
         * monitor2DetailRDD进行了持久化
         */
        monitor2DetailRDD = monitor2DetailRDD.cache();

        /**
         * 按照卡扣号分组，对应的数据是：每个卡扣号(monitor)对应的Row信息
         * 由于一共有9个卡扣号，这里groupByKey后一共有9组数据。
         */
        JavaPairRDD<String, Iterable<Row>> monitorId2RowsRDD = monitor2DetailRDD.groupByKey();

        monitorId2RowsRDD = monitorId2RowsRDD.cache();

        /**
         * key:自定义的类  value：卡扣ID
         */
        JavaPairRDD<SpeedSortKey, String> speedSortKey2MonitorId = monitorId2RowsRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, SpeedSortKey, String>() {
            @Override
            public Tuple2<SpeedSortKey, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                String monitorId = tuple._1;
                Iterator<Row> speedIterator = tuple._2.iterator();

                /**
                 * 这四个遍历 来统计这个卡扣下 高速 中速 正常 以及低速通过的车辆数
                 */
                long lowSpeed = 0;
                long normalSpeed = 0;
                long mediumSpeed = 0;
                long highSpeed = 0;

                while(speedIterator.hasNext()){
                    int speed = StringUtils.convertStringtoInt(speedIterator.next().getString(5));
                    if(speed >= 0 && speed < 60){
                        lowSpeed ++;
                    }else if (speed >= 60 && speed < 90) {
                        normalSpeed ++;
                    }else if (speed >= 90 && speed < 120) {
                        mediumSpeed ++;
                    }else if (speed >= 120) {
                        highSpeed ++;
                    }
                }
                SpeedSortKey speedSortKey = new SpeedSortKey(lowSpeed,normalSpeed,mediumSpeed,highSpeed);
                return new Tuple2<SpeedSortKey, String>(speedSortKey, monitorId);
            }
        });

        /**
         * key:自定义的类  value：卡扣ID
         */
        JavaPairRDD<SpeedSortKey, String> sortBySpeedCount = speedSortKey2MonitorId.sortByKey(false);

        /**
         * 硬编码问题
         * 取出前3个经常速度高的卡扣
         */
        List<Tuple2<SpeedSortKey, String>> take = sortBySpeedCount.take(3);

        for (Tuple2<SpeedSortKey, String> tuple : take) {
            System.out.println("monitor_id = "+tuple._2+"-----"+tuple._1.toString());
        }
    }
}
