package package02.spark.analyse.p3_monitor_topn_car_info;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/12/27 19:05
 */

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
import package02.spark.util.StringUtils;
import scala.Tuple2;
import shapeless.ops.tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class Monitor_topn_car_info {
    
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
         * 遍历分组后的RDD，拼接字符串
         * 数据中一共就有9个monitorId信息，那么聚合之后的信息也是9条
         * monitor_id=|cameraIds=|area_id=|camera_count=|carCount=
         * 例如:
         * ("0005","monitorId=0005|areaId=02|camearIds=09200,03243,02435,03232|cameraCount=4|carCount=100")
         *
         */

        JavaPairRDD<String, String> monitorId2CameraCountRDD = monitorId2RowsRDD.mapToPair(
                new PairFunction<Tuple2<String,Iterable<Row>>,String, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        String monitorId = tuple._1;
                        Iterator<Row> rowIterator = tuple._2.iterator();

                        List<String> list = new ArrayList<>();//同一个monitorId下，对应的所有的不同的cameraId,list.count方便知道此monitor下对应多少个cameraId

                        StringBuilder tmpInfos = new StringBuilder();//同一个monitorId下，对应的所有的不同的camearId信息

                        int count = 0;//统计车辆数的count
                        String areaId = "";
                        /**
                         * 这个while循环  代表的是当前的这个卡扣一共经过了多少辆车，   一辆车的信息就是一个row
                         */
                        while(rowIterator.hasNext()){
                            Row row = rowIterator.next();
                            areaId = row.getString(7);
                            String cameraId = row.getString(2);
                            if(!list.contains(cameraId)){
                                list.add(cameraId);
                            }
                            //针对同一个卡扣 monitor，append不同的cameraId信息
                            if(!tmpInfos.toString().contains(cameraId)){
                                tmpInfos.append(","+cameraId);
                            }
                            //这里的count就代表的车辆数，一个row一辆车
                            count++;
                        }
                        /**
                         * camera_count
                         */
                        int cameraCount = list.size();
                        //monitorId=0001|areaId=03|cameraIds=00001,00002,00003|cameraCount=3|carCount=100
                        String infos =  Constants.FIELD_MONITOR_ID+"="+monitorId+"|"
                                +Constants.FIELD_AREA_ID+"="+areaId+"|"
                                +Constants.FIELD_CAMERA_IDS+"="+tmpInfos.toString().substring(1)+"|"
                                +Constants.FIELD_CAMERA_COUNT+"="+cameraCount+"|"
                                +Constants.FIELD_CAR_COUNT+"="+count;
                        return new Tuple2<String, String>(monitorId, infos);
                    }
                });

        /**
         * 从monitor_camera_info标准表中查询出来每一个卡口对应的camera的数量
         */
        String sqlText = "SELECT * FROM monitor_camera_info";
        JavaRDD<Row> standardRDD = spark.sql(sqlText).javaRDD();
        /**
         * 使用mapToPair算子将standardRDD变成KV格式的RDD
         * monitorId2CameraId   :
         * (K:monitor_id  v:camera_id)
         */
        JavaPairRDD<String, String> monitorId2CameraId = standardRDD
                .mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<String, String>(row.getString(0), row.getString(1));
            }
        });

        /**
         * 对每一个卡扣下面的信息进行统计，统计出来camera_count（这个卡扣下一共有多少个摄像头）,camera_ids(这个卡扣下，所有的摄像头编号拼接成的字符串)
         * 返回：
         * 	("monitorId","cameraIds=xxx|cameraCount=xxx")
         * 例如：
         * 	("0008","cameraIds=02322,01213,03442|cameraCount=3")
         * 如何来统计？
         * 	1、按照monitor_id分组
         * 	2、使用mapToPair遍历，遍历的过程可以统计
         */
        JavaPairRDD<String, String> standardMonitor2CameraInfos = monitorId2CameraId.groupByKey()
                .mapToPair(new PairFunction<Tuple2<String,Iterable<String>>, String, String>() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                String monitorId = tuple._1;
                Iterator<String> cameraIterator = tuple._2.iterator();
                int count = 0;
                StringBuilder cameraIds = new StringBuilder();
                while(cameraIterator.hasNext()){
                    cameraIds.append(","+cameraIterator.next());
                    count++;
                }
                //cameraIds=00001,00002,00003,00004|cameraCount=4
                String cameraInfos = Constants.FIELD_CAMERA_IDS+"="+cameraIds.toString().substring(1)+"|"
                        +Constants.FIELD_CAMERA_COUNT+"="+count;
                return new Tuple2<String, String>(monitorId,cameraInfos);
            }
        });

        /**
         * 将两个RDD进行比较，join  leftOuterJoin
         * 为什么使用左外连接？ 左：标准表里面的信息  右：实际信息
         */
        JavaPairRDD<String, Tuple2<String, Optional<String>>> joinResultRDD =
                standardMonitor2CameraInfos.leftOuterJoin(monitorId2CameraCountRDD);
        /**
         * carCount2MonitorRDD 最终返回的K,V格式的数据
         * K：实际监测数据中某个卡扣对应的总车流量
         * V：实际监测数据中这个卡扣 monitorId
         */
        JavaPairRDD<Integer, String> carCount2MonitorRDD = joinResultRDD
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String,Tuple2<String,Optional<String>>>>, Integer, String>() {

            @Override
            public Iterator<Tuple2<Integer, String>> call(Iterator<Tuple2<String, Tuple2<String, Optional<String>>>> iterator) throws Exception {
                List<Tuple2<Integer, String>> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    //储藏返回值
                    Tuple2<String, Tuple2<String, Optional<String>>> tuple = iterator.next();
                    String monitorId = tuple._1;
                    String standardCameraInfos = tuple._2._1;
                    Optional<String> factCameraInfosOptional = tuple._2._2;
                    String factCameraInfos = "";

                    if(factCameraInfosOptional.isPresent()){
                        //这里面是实际检测数据中有标准卡扣信息
                        factCameraInfos = factCameraInfosOptional.get();
                    }else{
                        //这里面是实际检测数据中没有标准卡扣信息
                        String standardCameraIds =
                                StringUtils.getFieldFromConcatString(standardCameraInfos, "\\|", Constants.FIELD_CAMERA_IDS);
                        String[] split = standardCameraIds.split(",");
                        int abnoramlCameraCount = split.length;

                        StringBuilder abnormalCameraInfos = new StringBuilder();
                        for(String cameraId: split){
                            abnormalCameraInfos.append(","+cameraId);
                        }
                        //abnormalMonitorCount=1|abnormalCameraCount=3|abnormalMonitorCameraInfos="0002":07553,07554,07556
                        monitorAndCameraStateAccumulator.add(
                                Constants.FIELD_ABNORMAL_MONITOR_COUNT +"=1|"
                                        +Constants.FIELD_ABNORMAL_CAMERA_COUNT+"="+abnoramlCameraCount+"|"
                                        +Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS+"="+monitorId+":"+abnormalCameraInfos.toString().substring(1));
                        //跳出了本次while
                        continue;
                    }
                    /**
                     * 从实际数据拼接的字符串中获取摄像头数
                     */
                    int factCameraCount = Integer.parseInt(StringUtils.getFieldFromConcatString(factCameraInfos, "\\|", Constants.FIELD_CAMERA_COUNT));
                    /**
                     * 从标准数据拼接的字符串中获取摄像头数
                     */
                    int standardCameraCount = Integer.parseInt(StringUtils.getFieldFromConcatString(standardCameraInfos, "\\|", Constants.FIELD_CAMERA_COUNT));
                    if(factCameraCount == standardCameraCount){
                        /*
                         * 	1、正常卡口数量
                         * 	2、异常卡口数量
                         * 	3、正常通道（此通道的摄像头运行正常）数，通道就是摄像头
                         * 	4、异常卡口数量中哪些摄像头异常，需要保存摄像头的编号
                         */
                        monitorAndCameraStateAccumulator.add(Constants.FIELD_NORMAL_MONITOR_COUNT+"=1|"+Constants.FIELD_NORMAL_CAMERA_COUNT+"="+factCameraCount);
                    }else{
                        /**
                         * 从实际数据拼接的字符串中获取摄像编号集合
                         */
                        String factCameraIds = StringUtils.getFieldFromConcatString(factCameraInfos, "\\|", Constants.FIELD_CAMERA_IDS);

                        /**
                         * 从标准数据拼接的字符串中获取摄像头编号集合
                         */
                        String standardCameraIds = StringUtils.getFieldFromConcatString(standardCameraInfos, "\\|", Constants.FIELD_CAMERA_IDS);

                        List<String> factCameraIdList = Arrays.asList(factCameraIds.split(","));
                        List<String> standardCameraIdList = Arrays.asList(standardCameraIds.split(","));
                        StringBuilder abnormalCameraInfos = new StringBuilder();
                        int abnormalCameraCount = 0;//不正常摄像头数
                        int normalCameraCount = 0;//正常摄像头数
                        for (String cameraId : standardCameraIdList) {
                            if(!factCameraIdList.contains(cameraId)){
                                abnormalCameraCount++;
                                abnormalCameraInfos.append(","+cameraId);
                            }
                        }
                        normalCameraCount = standardCameraIdList.size()-abnormalCameraCount;
                        //往累加器中更新状态
                        monitorAndCameraStateAccumulator.add(
                                Constants.FIELD_NORMAL_CAMERA_COUNT+"="+normalCameraCount+"|"
                                        +Constants.FIELD_ABNORMAL_MONITOR_COUNT+"=1|"
                                        +Constants.FIELD_ABNORMAL_CAMERA_COUNT+"="+abnormalCameraCount+"|"
                                        +Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS+"="+monitorId + ":" + abnormalCameraInfos.toString().substring(1));
                    }
                    //从实际数据拼接到字符串中获取车流量
                    int carCount = Integer.parseInt(StringUtils.getFieldFromConcatString(factCameraInfos, "\\|", Constants.FIELD_CAR_COUNT));
                    list.add(new Tuple2<Integer, String>(carCount,monitorId));
                }
                //最后返回的list是实际监测到的数据中，list[(卡扣对应车流量总数,对应的卡扣号),... ...]
                return  list.iterator();
            }
        });

        carCount2MonitorRDD = carCount2MonitorRDD.cache();
        /**
         * action 类算子触发以上操作
         *
         */
        List<Tuple2<Integer, String>> list = carCount2MonitorRDD.sortByKey(false).take(3);
        for (Tuple2<Integer, String> tuple2 : list) {
            System.out.println(tuple2._2+":"+ tuple2._1);
        }
        /**
         * monitorId2MonitorIdRDD ---- K:monitor_id V:monitor_id
         * 获取topN卡口的详细信息
         * monitorId2MonitorIdRDD.join(monitorId2RowRDD)
         */
        List<Tuple2<String, String>> monitorId2CarCounts = new ArrayList<>();
        for(Tuple2<Integer,String> t : list){
            monitorId2CarCounts.add(new Tuple2<String, String>(t._2, t._2));
        }
        JavaPairRDD<String, String> topNMonitor2CarFlow = sc.parallelizePairs(monitorId2CarCounts);

        getTopNDetailsByJoin(topNMonitor2CarFlow,monitor2DetailRDD);

        getTopNDetailsByBroadcast(sc, topNMonitor2CarFlow,monitor2DetailRDD);
        /**
         * 输出显示运算结果
         */
        showResult(monitorAndCameraStateAccumulator);
    }

    /**
     * 获取topN 卡口的车流量具体信息，存入数据库表 topn_monitor_detail_info 中
     * @param topNMonitor2CarFlow ---- (monitorId,monitorId)
     * @param monitor2DetailRDD ---- (monitorId,Row)
     */
    private static void getTopNDetailsByBroadcast( JavaSparkContext javaSparkContext,JavaPairRDD<String, String> topNMonitor2CarFlow, JavaPairRDD<String, Row> monitor2DetailRDD) {
        //将topNMonitor2CarFlow（只有5条数据）转成非K,V格式的数据，便于广播出去
        JavaRDD<String> topNMonitorCarFlow = topNMonitor2CarFlow.map(new Function<Tuple2<String, String>, String>() {

            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                return v1._1;
            }
        });

        List<String> topNMonitorIds = topNMonitorCarFlow.collect();
        final Broadcast<List<String>> broadcast_topNMonitorIds = javaSparkContext.broadcast(topNMonitorIds);

        JavaPairRDD<String, Row> filterTopNMonitor2CarFlow = monitor2DetailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {

            @Override
            public Boolean call(Tuple2<String, Row> monitorTuple) throws Exception {
                return broadcast_topNMonitorIds.value().contains(monitorTuple._1);
            }
        });

        /*filterTopNMonitor2CarFlow.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Row>>>() {

            @Override
            public void call(Iterator<Tuple2<String, Row>> tuple2Iterator) throws Exception {
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, Row> tuple = tuple2Iterator.next();
                    Row row = tuple._2;
                    System.out.println(row.toString());
                }
            }
        });*/
        System.out.println("getTopNDetailsByBroadcast");
        System.out.println(filterTopNMonitor2CarFlow.count());
    }


    /**
     * 获取topN 卡口的车流量具体信息，存入数据库表 topn_monitor_detail_info 中
     * @param topNMonitor2CarFlow ---- (monitorId,monitorId)
     * @param monitor2DetailRDD ---- (monitorId,Row)
     */
    private static void getTopNDetailsByJoin( JavaPairRDD<String, String> topNMonitor2CarFlow,
            JavaPairRDD<String, Row> monitor2DetailRDD) {
        /**
         * 获取车流量排名前N的卡口的详细信息   可以看一下是在什么时间段内卡口流量暴增的。
         */
        JavaPairRDD<String, Tuple2<String, Row>> javaPairRDD = topNMonitor2CarFlow.join(monitor2DetailRDD);


        JavaPairRDD<String, Row> topNDetail = javaPairRDD.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> t) throws Exception {
                return new Tuple2<String, Row>(t._1, t._2._2);
            }
        });

        /*topNDetail.foreachPartition((VoidFunction<Iterator<Tuple2<String, Row>>>) tuple2Iterator -> {
            while (tuple2Iterator.hasNext()) {
                Tuple2<String, Row> tuple = tuple2Iterator.next();
                Row row = tuple._2;
                System.out.println(row.toString());
            }
        });*/
        System.out.println(topNDetail.count());
    }

    /**
     * 输出显示运算结果
     * @param monitorAndCameraStateAccumulator
     */
    public static void showResult(Accumulator<String> monitorAndCameraStateAccumulator){
        monitorAndCameraStateAccumulator.value();
        /**
         * 累加器中值能在Executor段读取吗？
         * 		不能
         * 这里的读取时在Driver中进行的
         */
        String accumulatorVal = monitorAndCameraStateAccumulator.value();
        String normalMonitorCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_NORMAL_MONITOR_COUNT);
        String normalCameraCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_NORMAL_CAMERA_COUNT);
        String abnormalMonitorCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_ABNORMAL_MONITOR_COUNT);
        String abnormalCameraCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_ABNORMAL_CAMERA_COUNT);
        String abnormalMonitorCameraInfos = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS);

        System.out.println("-----------result---------------------");
        System.out.println("normalMonitorCount:"+normalMonitorCount);
        System.out.println("normalCameraCount:"+normalCameraCount);
        System.out.println("abnormalMonitorCount:"+abnormalMonitorCount);
        System.out.println("abnormalCameraCount:"+abnormalCameraCount);
        System.out.println("abnormalMonitorCameraInfos:"+abnormalMonitorCameraInfos);
        System.out.println("-----------result---------------------");
    }

}
