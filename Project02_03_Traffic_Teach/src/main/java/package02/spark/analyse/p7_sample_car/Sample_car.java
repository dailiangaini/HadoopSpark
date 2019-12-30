package package02.spark.analyse.p7_sample_car;

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
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import package02.spark.constant.Constants;
import package02.spark.domain.RandomExtractCar;
import package02.spark.domain.RandomExtractMonitorDetail;
import package02.spark.util.DateUtils;
import scala.Tuple2;

import java.util.*;

/**
 *
 */
public class Sample_car {
    public static void main(String[] args) {
        String startDate = "2019-12-01";
        String endDate = "2019-12-31";
        int extractNum = 100;
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
         * key:时间段   value：car
         * dateHourCar2DetailRDD ---- ("dateHour"="2018-01-01_08","car"="京X91427")
         */
         JavaPairRDD<String, String> dateHourCar2DetailRDD = cameraRDD.mapToPair(new PairFunction<Row, String,String>() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                String actionTime = row.getString(4);
                String dateHour = DateUtils.getDateHour(actionTime);//2018-04-01_08
                String car = row.getString(3);
                /**
                 * 为什么要使用组合Key？
                 *   	因为在某一个时间段内，这一两车很有可能经过多个卡扣
                 */
                String key = Constants.FIELD_DATE_HOUR + "=" + dateHour;
                String value = Constants.FIELD_CAR + "=" + car;
                return new Tuple2<String, String>(key, value);
            }
         });

        /**
         * 相同的时间段内出现的车辆我们去重
         * key:时间段   value：car
         * dateHour2DetailRDD ---- ("dateHour"="2017-10-10_08","car"="京X91427")
         */
        JavaPairRDD<String, String> dateHour2DetailRDD = dateHourCar2DetailRDD.distinct();

        /**
         * String：dateHour
         * Object:去重后的这个小时段的总的车流量
         */
        Map<String, Long> countByKey = dateHour2DetailRDD.countByKey();

        /**
         * 将<dateHour,car_count>这种格式改成格式如下： <date,<Hour,count>>
         */
        Map<String, Map<String,Long>> dateHourCountMap = new HashMap<>();

        for (Map.Entry<String, Long> entry : countByKey.entrySet()) {
            String dateHour = entry.getKey();//2018-01-01_08
            String[] dateHourSplit = dateHour.split("_");
            String date = dateHourSplit[0];
            String hour = dateHourSplit[1];
            //本日期时段对应的车辆数
            Long count = entry.getValue();

            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if(hourCountMap == null){
                hourCountMap = new HashMap<String,Long>();
                dateHourCountMap.put(date, hourCountMap);
            }

            hourCountMap.put(hour, count);
        }

        /**
         * 一共抽取100辆车，平均每天应该抽取多少辆车呢？
         * extractNumPerDay = 100 ， dateHourCountMap.size()为有多少不同的天数日期，就是多长
         */
        int extractNumPerDay = extractNum / dateHourCountMap.size();
        /**
         * 记录每天每小时抽取索引的集合
         * dateHourExtractMap ---- Map<"日期"，Map<"小时段"，List<Integer>(抽取数据索引)>>
         */
        Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<>();

        Random random = new Random();
        //dateHourCountMap<String,Map<String,Long>>
        for (Map.Entry<String, Map<String, Long>> entry : dateHourCountMap.entrySet()) {
            String date = entry.getKey();
            /**
             * hourCountMap  key:hour  value:carCount
             * 当前日期下，每小时对应的车辆数
             */
            Map<String, Long> hourCountMap = entry.getValue();

            //计算出这一天总的车流量
            long dateCarCount = 0L;
            Collection<Long> values = hourCountMap.values();
            for (long tmpHourCount : values) {
                dateCarCount += tmpHourCount;
            }

            /**
             * 小时段对应的应该抽取车辆的索引集合
             * hourExtractMap ---- Map<小时，List<>>
             */
            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if(hourExtractMap == null){
                hourExtractMap = new HashMap<String,List<Integer>>();
                dateHourExtractMap.put(date, hourExtractMap);
            }

            /**
             * 遍历的是每个小时对应的车流量总数信息
             * hourCountMap  key:hour  value:carCount
             */
            for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                //当前小时段
                String hour = hourCountEntry.getKey();
                //当前小时段对应的真实的车辆数
                long hourCarCount = hourCountEntry.getValue();

                //计算出这个小时的车流量占总车流量的百分比,然后计算出在这个时间段内应该抽取出来的车辆信息的数量
                int hourExtractNum = (int)(((double)hourCarCount / (double)dateCarCount) * extractNumPerDay);

                /**
                 * 如果在这个时间段内抽取的车辆信息数量比这个时间段内的车流量还要多的话，只需要将count的值赋值给hourExtractNum就可以
                 *
                 */
                if(hourExtractNum >= hourCarCount){
                    hourExtractNum = (int)hourCarCount;
                }

                //获取当前小时 存储随机数的List集合
                List<Integer> extractIndexs = hourExtractMap.get(hour);
                if(extractIndexs == null){
                    extractIndexs = new ArrayList<Integer>();
                    hourExtractMap.put(hour, extractIndexs);
                }

                /**
                 * 生成抽取的car的index，  实际上就是生成一系列的随机数   随机数的范围就是0-count(这个时间段内的车流量) 将这些随机数放入一个list集合中
                 * 那么这里这个随机数的最大值没有超过实际上这个时间点对应的中的车流量总数，这里的list长度也就是要抽取数据个数的大小。
                 * 假设在一天中，7~8点这个时间段总车流量为100，假设我们之前刚刚算出应该在7~8点抽出的车辆数为20
                 * 那么 我们怎么样随机抽取呢？
                 * 1.循环20次
                 * 2.每次循环搞一个0~100的随机数，放入一个list<Integer>中，那么这个list中的每一个元素就是我们这里说的car的index
                 * 3.最后得到一个长度为20的car的indexList<Integer>集合，一会取值，取20个，那么取哪个值呢，就取这里List中的下标对应的car
                 *
                 */
                for(int i = 0 ; i < hourExtractNum ; i++){
                    /**
                     *  50
                     *
                     */
                    int index = random.nextInt((int)hourCarCount);
                    while(extractIndexs.contains(index)){
                        index = random.nextInt((int)hourCarCount);
                    }
                    extractIndexs.add(index);
                }
            }

        }


        final Broadcast<Map<String, Map<String, List<Integer>>>> dateHourExtractBroadcast = sc.broadcast(dateHourExtractMap);

        /**
         * 在dateHour2DetailRDD中进行随机抽取车辆信息，
         * 首先第一步：按照date_hour进行分组，然后对组内的信息按照 dateHourExtractBroadcast参数抽取相应的车辆信息
         * 抽取出来的结果直接放入到MySQL数据库中。
         *
         * extractCarRDD ----抽取出来的所有车辆
         */
        JavaPairRDD<String, String> extractCarRDD = dateHour2DetailRDD.groupByKey().flatMapToPair(
                new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String,String>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                //将要返回的当前日期当前小时段下抽取出来的车辆集合
                List<Tuple2<String,String>> list = new ArrayList<>();
                //按index下标抽取的这个时间段对应的车辆集合
                List<RandomExtractCar> carRandomExtracts = new ArrayList<>();

                //2018-04-01_08
                String dateHour = tuple._1;
                Iterator<String> iterator = tuple._2.iterator();

                String date = dateHour.split("_")[0];
                String hour = dateHour.split("_")[1];

                Map<String, Map<String, List<Integer>>> dateHourExtractMap =
                        dateHourExtractBroadcast.value();

                List<Integer> indexList = dateHourExtractMap.get(date).get(hour);

                int index = 0;
                while(iterator.hasNext()){
                    String car = iterator.next().split("=")[1];
                    if(indexList.contains(index)){
                        list.add(new Tuple2<>(car, car));
                    }
                    index++;
                }
                return list.iterator();
            }
        });

        /**
         * key-value <car,row>
         * car2DetailRDD ---- ("京X91427",Row)
         *
         */
        JavaPairRDD<String, Row> car2DetailRDD = cameraRDD.mapToPair(new PairFunction<Row, String , Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                String car = row.getString(3);
                return new Tuple2<String, Row>(car, row);
            }
        });

        /**
         * extractCarRDD  K:car V:car
         * 抽取到的所有的car，这里去和开始得到的符合日期内的车辆详细信息car2DetailRDD ，得到抽取到的car的详细信息
         *
         */
        JavaPairRDD<String, Row> randomExtractCar2DetailRDD = extractCarRDD.distinct().join(car2DetailRDD).mapPartitionsToPair(
                new PairFlatMapFunction<Iterator<Tuple2<String,Tuple2<String,Row>>>, String,Row>() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;
            @Override
            public Iterator<Tuple2<String, Row>> call(Iterator<Tuple2<String, Tuple2<String, Row>>> iterator) throws Exception {
                List<RandomExtractMonitorDetail> randomExtractMonitorDetails = new ArrayList<>();
                List<Tuple2<String, Row>> list = new ArrayList<>();
                while(iterator.hasNext()){
                    Tuple2<String, Tuple2<String, Row>> tuple = iterator.next();
                    Row row = tuple._2._2;
                    String car = tuple._1;
                    list.add(new Tuple2<String, Row>(car, row));
                }
                return list.iterator();
            }
        });

        randomExtractCar2DetailRDD.foreach(new VoidFunction<Tuple2<String, Row>>() {
            @Override
            public void call(Tuple2<String, Row> tuple) throws Exception {
                String car = tuple._1;
                Row row = tuple._2;
                System.out.println("car:"+car+",row="+row.toString());
            }
        });

        System.out.println(randomExtractCar2DetailRDD.count());


    }
}
