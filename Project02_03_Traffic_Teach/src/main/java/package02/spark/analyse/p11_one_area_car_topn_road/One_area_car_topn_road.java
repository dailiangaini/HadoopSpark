package package02.spark.analyse.p11_one_area_car_topn_road;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import package02.spark.conf.ConfigurationManager;
import package02.spark.constant.Constants;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/12/30 19:52
 */
public class One_area_car_topn_road {
    public static void main(String[] args) {
        String startDate = "2019-12-01";
        String endDate = "2019-12-31";
        // 构建Spark运行时的环境参数
        SparkConf sparkConf = new SparkConf().setAppName("One_area_car_topn_road")
                .setMaster("local");

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();

        SparkContext sparkContext = spark.sparkContext();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // 注册自定义函数
        spark.udf().register("concat_String_string", new ConcatStringStringUDF(), DataTypes.StringType);
        spark.udf().register("random_prefix", new RandomPrefixUDF(), DataTypes.StringType);
        spark.udf().register("remove_random_prefix", new RemoveRandomPrefixUDF(), DataTypes.StringType);
        spark.udf().register("group_concat_distinct",new GroupConcatDistinctUDAF());

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
                String areaId = row.getString(7);
                return new Tuple2<String, Row>(areaId,row);
            }
        });
        /**
         * monitor2DetailRDD进行了持久化
         */
        monitor2DetailRDD = monitor2DetailRDD.cache();


        monitor2DetailRDD.take(10).forEach(tuple ->{
            System.out.println(tuple._1 + "~" + tuple._2);
        });


        /**
         * 从异构数据源MySQL中获取区域信息
         * area_id	area_name
         * areaId2AreaInfoRDD<area_id,area_name:区域名称>
         */
        JavaPairRDD<String, String> areaId2AreaInfoRDD = getAreaId2AreaInfoRDD(spark);

        /**
         * 补全区域信息    添加区域名称
         * 	monitor_id car road_id	area_id	area_name
         * 生成基础临时信息表
         * 	tmp_car_flow_basic
         *
         * 将符合条件的所有的数据得到对应的中文区域名称 ，然后动态创建Schema的方式，将这些数据注册成临时表tmp_car_flow_basic
         */
        generateTempRoadFlowBasicTable(spark,monitor2DetailRDD,areaId2AreaInfoRDD);

        /**
         * 统计各个区域各个路段车流量的临时表
         *
         * area_name  road_id    car_count      monitor_infos
         *   海淀区		  01		 100	  0001=20|0002=30|0003=50
         *
         * 注册成临时表tmp_area_road_flow_count
         */
        generateTempAreaRoadFlowTable(spark);

        /**
         *  area_name
         *	 road_id
         *	 count(*) car_count
         *	 monitor_infos
         * 使用开窗函数  获取每一个区域的topN路段
         */
        getAreaTop3RoadFolwRDD(spark);
        System.out.println("***********ALL FINISHED*************");
        sc.close();
    }

    private static JavaPairRDD<String, String> getAreaId2AreaInfoRDD(SparkSession sparkSession) {
        String url = null;
        String user = null;
        String password = null;
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        //获取Mysql数据库的url,user,password信息
        if(local) {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
        } else {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
        }

        Map<String, String> options = new HashMap<String, String>();
        options.put("url", url);
        options.put("user", user);
        options.put("password", password);
        options.put("dbtable", "area_info");

        // 通过SQLContext去从MySQL中查询数据
        Dataset<Row> areaInfoDF = sparkSession.read().format("jdbc").options(options).load();
        System.out.println("------------Mysql数据库中的表area_info数据为------------");
        areaInfoDF.show();
        // 返回RDD
        JavaRDD<Row> areaInfoRDD = areaInfoDF.javaRDD();

        JavaPairRDD<String, String> areaid2areaInfoRDD = areaInfoRDD.mapToPair(
                new PairFunction<Row, String, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, String> call(Row row) throws Exception {
                        String areaid = String.valueOf(row.get(0));
                        String areaname = String.valueOf(row.get(1));
                        return new Tuple2<String, String>(areaid, areaname);
                    }
                });

        return areaid2areaInfoRDD;
    }

    /**
     * 获取符合条件数据对应的区域名称，并将这些信息注册成临时表 tmp_car_flow_basic
     * @param sparkSessions
     * @param areaId2DetailInfos
     * @param areaId2AreaInfoRDD
     */
    private static void generateTempRoadFlowBasicTable(SparkSession sparkSessions,
                                                       JavaPairRDD<String, Row> areaId2DetailInfos, JavaPairRDD<String, String> areaId2AreaInfoRDD) {

        JavaRDD<Row> tmpRowRDD = areaId2DetailInfos.join(areaId2AreaInfoRDD).map(
                new Function<Tuple2<String,Tuple2<Row,String>>, Row>() {

                    /**
                     *
                     */
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Row call(Tuple2<String, Tuple2<Row, String>> tuple) throws Exception {
                        String areaId = tuple._1;
                        Row carFlowDetailRow = tuple._2._1;
                        String areaName = tuple._2._2;

                        String roadId = carFlowDetailRow.getString(2);
                        String monitorId = carFlowDetailRow.getString(0);
                        String car = carFlowDetailRow.getString(1);


                        return RowFactory.create(areaId, areaName, roadId, monitorId,car);
                    }
                });

        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("area_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("area_name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("road_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("monitor_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("car", DataTypes.StringType, true));


        StructType schema = DataTypes.createStructType(structFields);

        Dataset<Row> df = sparkSessions.createDataFrame(tmpRowRDD, schema);
        System.out.println("------ generateTempRoadFlowBasicTable result ------");
        df.show();
        df.createOrReplaceTempView("tmp_car_flow_basic");

    }

    private static void generateTempAreaRoadFlowTable(SparkSession sparkSession) {

        /**
         * 	structFields.add(DataTypes.createStructField("area_id", DataTypes.StringType, true));
         *	structFields.add(DataTypes.createStructField("area_name", DataTypes.StringType, true));
         *	structFields.add(DataTypes.createStructField("road_id", DataTypes.StringType, true));
         *	structFields.add(DataTypes.createStructField("monitor_id", DataTypes.StringType, true));
         *	structFields.add(DataTypes.createStructField("car", DataTypes.StringType, true));
         */
        String sql =
                "SELECT "
                        + "area_name,"
                        + "road_id,"
                        + "count(*) car_count,"
                        //group_concat_distinct 统计每一条道路中每一个卡扣下的车流量
                        + "group_concat_distinct(monitor_id) monitor_infos "//0001=20|0002=30
                        + "FROM tmp_car_flow_basic "
                        + "GROUP BY area_name,road_id";
        /**
         * 下面是当遇到区域下某个道路车辆特别多的时候，会有数据倾斜，怎么处理？random
         */
        String sqlText = ""
                + "SELECT "
                + "area_name_road_id,"
                + "sum(car_count),"
                + "group_concat_distinct(monitor_infos) monitor_infoss "
                + "FROM ("
                + "SELECT "
                + "remove_random_prefix(prefix_area_name_road_id) area_name_road_id,"
                + "car_count,"
                + "monitor_infos "
                + "FROM ("
                + "SELECT "
                + "prefix_area_name_road_id,"//1_海淀区:49
                + "count(*) car_count,"
                + "group_concat_distinct(monitor_id) monitor_infos "
                + "FROM ("
                + "SELECT "
                + "monitor_id,"
                + "car,"
                + "random_prefix(concat_String_string(area_name,road_id,':'),10) prefix_area_name_road_id "
                + "FROM tmp_car_flow_basic "
                + ") t1 "
                + "GROUP BY prefix_area_name_road_id "
                + ") t2 "
                + ") t3 "
                + "GROUP BY area_name_road_id";


        Dataset<Row> df = sparkSession.sql(sql);

        // df.show();
        df.createOrReplaceTempView("tmp_area_road_flow_count");
    }

    private static JavaRDD<Row> getAreaTop3RoadFolwRDD(SparkSession sparkSession) {
        /**
         * tmp_area_road_flow_count表：
         * 		area_name
         * 		road_id
         * 		car_count
         * 		monitor_infos
         */
        String sql = ""
                + "SELECT "
                + "area_name,"
                + "road_id,"
                + "car_count,"
                + "monitor_infos, "
                + "CASE "
                + "WHEN car_count > 170 THEN 'A LEVEL' "
                + "WHEN car_count > 160 AND car_count <= 170 THEN 'B LEVEL' "
                + "WHEN car_count > 150 AND car_count <= 160 THEN 'C LEVEL' "
                + "ELSE 'D LEVEL' "
                +"END flow_level "
                + "FROM ("
                + "SELECT "
                + "area_name,"
                + "road_id,"
                + "car_count,"
                + "monitor_infos,"
                + "row_number() OVER (PARTITION BY area_name ORDER BY car_count DESC) rn "
                + "FROM tmp_area_road_flow_count "
                + ") tmp "
                + "WHERE rn <=3";
        sparkSession.sql("use traffic");
        Dataset<Row> df = sparkSession.sql(sql);
        System.out.println("--------最终的结果-------");
        df.show();
        // 存入Hive中，要有result 这个database库
        // sparkSession.sql("drop table if exists areaTop3Road");
        // df.write().saveAsTable("areaTop3Road");
        return df.javaRDD();
    }

}
