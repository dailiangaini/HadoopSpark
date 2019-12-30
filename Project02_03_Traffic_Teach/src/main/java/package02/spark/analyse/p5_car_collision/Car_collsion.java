package package02.spark.analyse.p5_car_collision;

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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import package02.spark.skynet.SpeedSortKey;
import package02.spark.util.StringUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class Car_collsion {
    
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
        /**
         * 卡扣碰撞分析
         * 假设数据如下：
         * area1卡扣:["0000", "0001", "0002", "0003"]
         * area2卡扣:["0004", "0005", "0006", "0007"]
         */
        List<String> monitorIds1 = Arrays.asList("0000", "0001", "0002", "0003");
        List<String> monitorIds2 = Arrays.asList("0004", "0005", "0006", "0007");
        // 通过两堆卡扣号，分别取数据库（本地模拟的两张表）中查询数据
        JavaRDD<Row> areaRDD1 = getAreaRDDByMonitorIds(spark, startDate, endDate, monitorIds1);
        JavaRDD<Row> areaRDD2 = getAreaRDDByMonitorIds(spark, startDate, endDate, monitorIds2);

        JavaRDD<String> area1Cars = areaRDD1.map(new Function<Row, String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public String call(Row row) throws Exception {
                return row.getAs("car")+"";
            }
        }).distinct();
        JavaRDD<String> area2Cars = areaRDD2.map(new Function<Row, String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public String call(Row row) throws Exception {
                return row.getAs("car")+"";
            }
        }).distinct();

        JavaRDD<String> intersection = area1Cars.intersection(area2Cars);
        intersection.foreach(new VoidFunction<String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(String car) throws Exception {
                System.out.println("area1 与 area2 共同车辆="+car);
            }
        });



    }

    private static JavaRDD<Row> getAreaRDDByMonitorIds(SparkSession sparkSession,String startTime, String endTime, List<String> monitorId1) {
        String sql = "SELECT * "
                + "FROM monitor_flow_action"
                + " WHERE date >='" + startTime + "' "
                + " AND date <= '" + endTime+ "' "
                + " AND monitor_id in (";

        for(int i = 0 ; i < monitorId1.size() ; i++){
            sql += "'"+monitorId1.get(i) + "'";

            if( i  < monitorId1.size() - 1 ){
                sql += ",";
            }
        }

        sql += ")";
        return sparkSession.sql(sql).javaRDD();
    }
}
