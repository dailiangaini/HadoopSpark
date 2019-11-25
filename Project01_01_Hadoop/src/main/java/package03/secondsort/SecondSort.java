package package03.secondsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/23 08:29
 */
public class SecondSort {


    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "/Users/dailiang/Documents/Software/hadoop-2.10.0");
        String inputFile = "/Users/dailiang/Documents/second";
        String outputDir = "/Users/dailiang/Documents/secondOut";

        //1.设置Configuration
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.framework.name", "local");
        configuration.set("fs.defaultFS","hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(configuration);
        //2.设置Job
        Job job = Job.getInstance(configuration, "SecondSort");
        job.setJarByClass(SecondSort.class);


        //3.设置input output
        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));
        if (fs.exists(new Path(outputDir))) {
            fs.delete(new Path(outputDir), true);
        }

        //4.设置map
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(Text.class);

        //5.设置reduce
        job.setReducerClass(MyReduce.class);
        job.setNumReduceTasks(1);

        // 6.设置 partitioner sort GroupingComparator  Combiner  CombinerKeyGroupingComparator
        job.setSortComparatorClass(MySortComparator.class);

        job.setPartitionerClass(MyPartitioner.class);

        job.setGroupingComparatorClass(MyGroupingComparator.class);

        //job.setCombinerClass(TqReducer.class);
        //job.setCombinerKeyGroupingComparatorClass(TqGroupingComparator.class);

        //job.setJar("/Users/dailiang/Documents/Code/StudyBigData/HadoopSpark/out/artifacts/HadoopSpark_jar/HadoopSpark.jar");
        //7.提交任务
        job.waitForCompletion(true);

    }
}
