package package02.spark.analyse.p10_dynamically_change_broadcast_variables;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/12/30 16:32
 */
public class Dynamic_change_broadcast_variables {
    /**
     * 读取文件，生成list，并广播
     * 若想改变广播变量的值，则只需要修改readFile的文件内容
     * @param args
     */
    public static void main(String[] args) {
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

        String path = "";
        List<String> blackCars = readFile(path);
        final Broadcast<List<String>> broadcast = sc.broadcast(blackCars);
    }

    /**
     * 读取文件
     * @param path
     * @return
     */
    private static List<String> readFile(String path) {
        List<String> list = new ArrayList<>();
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
            String line = br.readLine();
            while(line != null ){
                list.add(line);
                line = br.readLine();
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return list;
    }
}
