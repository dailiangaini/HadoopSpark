package package02.tq;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import package02.tq.comparator.TqGroupingComparator;
import package02.tq.comparator.TqSortComparator;
import package02.tq.comparator.TqSortComparator2;
import package02.tq.entity.TQ;
import package02.tq.map.TqMapper;
import package02.tq.partitioner.TqPartitioner;
import package02.tq.reduce.TqReducer;
import package02.tq.reduce.TqReducer2;

import java.io.File;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/22 13:58
 */
public class TqMR2 {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "/Users/dailiang/Documents/Software/hadoop-2.10.0");
        String inputFile = "/Users/dailiang/Documents/tq";
        String outputDir = "/Users/dailiang/Documents/output3";


        //1.设置Configuration
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.framework.name", "local");
        //configuration.set("mapreduce.framework.name", "yarn");
        configuration.set("fs.defaultFS","hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(configuration);


        //2.设置Job
        Job job = Job.getInstance(configuration, "tq2");
        job.setJarByClass(TqMR2.class);


        //3.设置input output
        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));
        if (fs.exists(new Path(outputDir))) {
            fs.delete(new Path(outputDir), true);
        }

        //4.设置map
        job.setMapperClass(TqMapper.class);
        job.setMapOutputKeyClass(TQ.class);
        job.setMapOutputValueClass(Text.class);

        //5.设置reduce
        job.setReducerClass(TqReducer2.class);
        job.setNumReduceTasks(1);

        // 6.设置 partitioner sort GroupingComparator  Combiner  CombinerKeyGroupingComparator
        job.setSortComparatorClass(TqSortComparator2.class);

        //job.setPartitionerClass(TqPartitioner.class);

        //job.setGroupingComparatorClass(TqGroupingComparator.class);

        //job.setCombinerClass(TqReducer.class);
        //job.setCombinerKeyGroupingComparatorClass(TqGroupingComparator.class);

        //job.setJar("/Users/dailiang/Documents/Code/StudyBigData/HadoopSpark/out/artifacts/HadoopSpark_jar/HadoopSpark.jar");
        //7.提交任务
        job.waitForCompletion(true);

    }
}
