package package02.spark.analyse.p8_monitor_flow_conversion_rate;

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
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import package02.spark.util.DateUtils;
import package02.spark.util.NumberUtils;
import scala.Tuple2;

import java.util.*;

/**
 *
 */
public class Monitor_flow_conversion_rate {
    public static void main(String[] args) {
        String startDate = "2019-12-01";
        String endDate = "2019-12-31";
        String roadFlow = "0001,0002,0003,0004,0005";

        // 构建Spark运行时的环境参数
        SparkConf sparkConf = new SparkConf().setAppName("Sample car")
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

        final Broadcast<String> roadFlowBroadcast = sc.broadcast(roadFlow);

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
         * 将rowRDDByDateRange 变成key-value对的形式，key car value 详细信息
         * （key,row）
         * 为什么要变成k v对的形式？
         * 因为下面要对car 按照时间排序，绘制出这辆车的轨迹。
         */
        JavaPairRDD<String, Row> car2RowRDD = cameraRDD.mapToPair(new PairFunction<Row, String, Row>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(3), row);
            }
        });

        /**
         * 计算这一辆车，有多少次匹配到我们指定的卡扣流
         *
         * 先拿到车辆的轨迹，比如一辆车轨迹：0001,0002,0003,0004,0001,0002,0003,0001,0004
         * 返回一个二元组（切分的片段，该片段对应的该车辆轨迹中匹配上的次数）
         * ("0001",3)
         * ("0001,0002",2)
         * ("0001,0002,0003",2)
         * ("0001,0002,0003,0004",1)
         * ("0001,0002,0003,0004,0005",0)
         * ... ...
         * ("0001",13)
         * ("0001,0002",12)
         * ("0001,0002,0003",11)
         * ("0001,0002,0003,0004",11)
         * ("0001,0002,0003,0004,0005",10)
         */
        JavaPairRDD<String, Long> roadSplitRDD = car2RowRDD.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Long>() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<Tuple2<String, Long>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                Iterator<Row> iterator = tuple._2.iterator();
                List<Tuple2<String, Long>> list = new ArrayList<>();

                List<Row> rows = new ArrayList<>();
                /**
                 * 遍历的这一辆车的所有的详细信息，然后将详细信息放入到rows集合中
                 */
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    rows.add(row);
                }

                /**
                 * 对这个rows集合 按照车辆通过卡扣的时间排序
                 */
                Collections.sort(rows, new Comparator<Row>() {

                    @Override
                    public int compare(Row row1, Row row2) {
                        String actionTime1 = row1.getString(4);
                        String actionTime2 = row2.getString(4);
                        if (DateUtils.after(actionTime1, actionTime2)) {
                            return 1;
                        } else {
                            return -1;
                        }
                    }
                });

                /**
                 * roadFlowBuilder保存到是本次车辆的轨迹是一组逗号分开的卡扣id，组合起来就是这辆车的运行轨迹
                 */
                StringBuilder roadFlowBuilder = new StringBuilder();

                /**
                 * roadFlowBuilder怎么拼起来的？  rows是由顺序了，直接遍历然后追加到roadFlowBuilder就可以了吧。
                 * row.getString(1) ---- monitor_id
                 */
                for (Row row : rows) {
                    roadFlowBuilder.append("," + row.getString(1));
                }
                /**
                 * roadFlowBuilder这里面的开头有一个逗号， 去掉逗号。
                 * roadFlow是本次车辆的轨迹
                 */
                String carTracker = roadFlowBuilder.toString().substring(1);
                /**
                 *  从广播变量中获取指定的卡扣流参数
                 *  0001,0002,0003,0004,0005
                 */
                String standardRoadFlow = roadFlowBroadcast.value();

                /**
                 * 对指定的卡扣流参数分割
                 */
                String[] split = standardRoadFlow.split(",");

                /**
                 * [0001,0002,0003,0004,0005]
                 * 1 2 3 4 5
                 * 遍历分割完成的数组
                 */
                for (int i = 1; i <= split.length; i++) {
                    //临时组成的卡扣切片  1,2 1,2,3
                    String tmpRoadFlow = "";
                    /**
                     * 第一次进来：,0001
                     * 第二次进来：,0001,0002
                     */
                    for (int j = 0; j < i; j++) {
                        tmpRoadFlow += "," + split[j];//,0001
                    }
                    tmpRoadFlow = tmpRoadFlow.substring(1);//去掉前面的逗号     0001

                    //indexOf 从哪个位置开始查找
                    int index = 0;
                    //这辆车有多少次匹配到这个卡扣切片的次数
                    Long count = 0L;

                    while (carTracker.indexOf(tmpRoadFlow, index) != -1) {
                        index = carTracker.indexOf(tmpRoadFlow, index) + 1;
                        count++;
                    }
                    list.add(new Tuple2<String, Long>(tmpRoadFlow, count));
                }
                return list.iterator();
            }
        });

        /**
         * roadSplitRDD
         * 所有的相同的key先聚合得到总数
         * ("0001",100)
         * ("0001,0002",200)
         * ("0001,0002,0003",300)
         * ("0001,0002,0003,0004",400)
         * ("0001,0002,0003,0004,0005",500)
         * 变成了一个 K,V格式的map
         * ("0001",500)
         * ("0001,0002",400)
         * ("0001,0002,0003",300)
         * ("0001,0002,0003,0004",200)
         * ("0001,0002,0003,0004,0005",100)
         */
        JavaPairRDD<String, Long> sumByKey = roadSplitRDD.reduceByKey(new Function2<Long, Long, Long>() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });

        Map<String, Long> splitCountMap = sumByKey.collectAsMap();

        String[] split = roadFlow.split(",");
        /**
         * 存放卡扣切面的转换率
         * "0001,0002" 0.16
         */
        Map<String, Double> rateMap = new HashMap<>();
        long lastMonitorCarCount = 0L;
        String tmpRoadFlow = "";
        for (int i = 0; i < split.length; i++) {
            tmpRoadFlow += "," + split[i];
            Long count = splitCountMap.get(tmpRoadFlow.substring(1));
            if(count != 0L){
                /**
                 * 1_2
                 * lastMonitorCarCount      1 count
                 */
                if(i != 0 && lastMonitorCarCount != 0L){
                    double rate = NumberUtils.formatDouble((double)count/(double)lastMonitorCarCount,2);
                    rateMap.put(tmpRoadFlow.substring(1), rate);
                }
                lastMonitorCarCount = count;
            }
        }

        for (Map.Entry<String, Double> entry : rateMap.entrySet()) {
            System.out.println(entry.getKey()+"="+entry.getValue());
        }

    }
}
