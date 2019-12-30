package package02.spark.analyse.p6_one_monitor_all_car_trace;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/12/27 19:05
 */

import com.google.inject.internal.asm.$FieldVisitor;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import package02.spark.constant.Constants;
import package02.spark.skynet.MonitorAndCameraStateAccumulator;
import package02.spark.util.DateUtils;
import package02.spark.util.StringUtils;
import scala.Tuple2;

import java.util.*;

/**
 *
 */
public class One_onitor_all_car_trace {
    
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

        /**
         * 创建了一个自定义的累加器
         */
        Accumulator<String> monitorAndCameraStateAccumulator = sparkContext.accumulator("", new MonitorAndCameraStateAccumulator());

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
         * 获取通过卡口0001下所有车的轨迹
         */
        JavaRDD<Row> filterRdd = cameraRDD.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                return "0001".equals(v1.getAs("monitor_id"));
            }
        });
        JavaRDD<String> carRdd = filterRdd.map(new Function<Row, String>() {
            @Override
            public String call(Row v1) throws Exception {
                return v1.getAs("car");
            }
        });
        List<String> cars = carRdd.distinct().take(3);

        /**
         * top3MonitorIds这个集合里面都是monitor_id
         */
        final Broadcast<List<String>> top3MonitorIdsBroadcast = sc.broadcast(cars);

        JavaRDD<Row> rowJavaRDD = cameraRDD.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                return top3MonitorIdsBroadcast.value().contains(v1.getAs("car"));
            }
        });
        JavaPairRDD<String, Row> carRow = rowJavaRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getAs("car"), row);
            }
        });

        JavaPairRDD<String, Iterable<Row>> carRows = carRow.groupByKey();

        carRows.foreach(new VoidFunction<Tuple2<String, Iterable<Row>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                String car = tuple._1;
                Iterable<Row> rows = tuple._2;
                Iterator<Row> iterator = rows.iterator();

                List<Row> list = new ArrayList<>();
                while (iterator.hasNext()){
                    list.add(iterator.next());
                }
                Collections.sort(list, new Comparator<Row>() {
                    @Override
                    public int compare(Row o1, Row o2) {
                        return DateUtils.after(o1.getAs("action_time"), o2.getAs("action_time")) ? 1 : -1;
                    }
                });

                StringBuffer tracker = new StringBuffer();
                for (Row row : list) {
                    tracker.append(row.getAs("monitor_id")+"->");
                }
                System.out.println("car="+ car+",tracker= "+ tracker.substring(0, tracker.length()-2));
            }
        });

        spark.close();

        /**
         * car=京F51862,tracker= 0003->0007->0007->0006->0003->0007->0008->0002->0003->0003->0007->0008->0003->0006->0004->0006->0004->0001->0005->0003->0000
         * car=京G16765,tracker= 0004->0007->0004->0003->0003->0004->0008->0003->0004->0004->0008->0008->0007->0001->0000->0003->0001->0008->0002->0003
         * car=京D95490,tracker= 0000->0003->0008->0003->0005->0004->0001->0007->0005->0001->0007->0002->0001
         */

    }

}
