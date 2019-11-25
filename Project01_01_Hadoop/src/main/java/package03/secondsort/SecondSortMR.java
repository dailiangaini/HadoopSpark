package package03.secondsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/23 08:29
 */
public class SecondSortMR extends Configured implements Tool {
    Configuration configuration;
    @Override
    public void setConf(Configuration conf) {
        System.setProperty("hadoop.home.dir", "/Users/dailiang/Documents/Software/hadoop-2.10.0");
        configuration = new Configuration();
        configuration.set("mapreduce.framework.name", "local");
        configuration.set("fs.defaultFS","hdfs://localhost:9000");
        super.setConf(configuration);
    }

    @Override
    public Configuration getConf() {
        return super.getConf();
    }

    @Override
    public int run(String[] strings) throws Exception {
        System.setProperty("hadoop.home.dir", "/Users/dailiang/Documents/Software/hadoop-2.10.0");
        String inputFile = "/Users/dailiang/Documents/second";
        String outputDir = "/Users/dailiang/Documents/secondOut";

        Configuration configuration = getConf(); //获得配置文件对象
        Job job=Job.getInstance(configuration,"SecondSortMR");
        job.setJarByClass(SecondSortMR.class);

        FileSystem fs = FileSystem.get(configuration);
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
        //job.setNumReduceTasks(1);

        // 6.设置 partitioner sort GroupingComparator  Combiner  CombinerKeyGroupingComparator

        job.setGroupingComparatorClass(MyGroupingComparator.class);

        job.setSortComparatorClass(MySortComparator.class);

        //job.setPartitionerClass(MyPartitioner.class);

        //job.setCombinerClass(TqReducer.class);
        //job.setCombinerKeyGroupingComparatorClass(TqGroupingComparator.class);

        //job.setJar("/Users/dailiang/Documents/Code/StudyBigData/HadoopSpark/out/artifacts/HadoopSpark_jar/HadoopSpark.jar");
        //7.提交任务
        job.waitForCompletion(true);

        return job.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) {
        try {
            int returnCode =  ToolRunner.run(new SecondSortMR(),args);
            System.exit(returnCode);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
