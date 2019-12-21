package package02.hadoop.etl.test.mr_test;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import package02.hadoop.common.GlobalConstants;
import package02.hadoop.util.TimeUtil;

import java.io.IOException;


/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/12/18 17:58
 */
public class AnalyserLogDataRunner implements Tool {
    private static final Logger logger = Logger.getLogger(AnalyserLogDataRunner.class);
    private Configuration conf = null;
    private String dataInputPath = "/tmp/data/my_access.log";
    private String dataOutputPath = "/tmp/data/my_access_out.log";

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new AnalyserLogDataRunner(), args);
        } catch (Exception e) {
            logger.error("执行日志解析job异常", e);
            e.printStackTrace();
        }
    }


    @Override
    public void setConf(Configuration conf) {
//        this.conf = HBaseConfiguration.create(conf);
//        conf.set("hbase.zookeeper.quorum", "localhost");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
        this.conf = conf;
        // 设置本地运行
          conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        // 本地文件系统
        // conf.set("fs.defaultFS", "file:///");
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println("run begin");
        Configuration configuration = getConf();
        System.out.println("processArgs");
        this.processArgs(configuration, args);
        System.out.println("Job.getInstance");
        Job job = Job.getInstance(conf, "analyser_logdata22");
        // 设置本地提交job，集群运行，需要代码
        // File jarFile = EJob.createTempJar("target/classes");
        // ((JobConf) job.getConfiguration()).setJar(jarFile.toString());
        // 设置本地提交job，集群运行，需要代码结束

        job.setJarByClass(AnalyserLogDataRunner.class);

        job.setMapperClass(AnalyserLogDataMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置reducer配置
        // 1. 集群上运行，打成jar运行(要求addDependencyJars参数为true，默认就是true)
        // TableMapReduceUtil.initTableReducerJob(EventLogConstants.HBASE_NAME_EVENT_LOGS, null, job);
        // 2. 本地运行，要求参数addDependencyJars为false

        /**
         * 表名称：event_logs
         * 创建表：create 'event_logs', 'info'
         */
        System.out.println("initTableReducerJob");
        job.setReducerClass(AnalyserLogDataReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //TableMapReduceUtil.initTableReducerJob(EventLogConstants.HBASE_NAME_EVENT_LOGS, null, job, null, null, null, null, false);
        //job.setNumReduceTasks(0);

        // 设置输入路径
        this.setJobInput(job);
        System.out.println("waitForCompletion");
        return job.waitForCompletion(true) ? 0 : -1;
    }

    /**
     * 处理参数
     * @param conf
     * @param args
     */
    private void processArgs(Configuration conf, String[] args){
        String date = null;
        for (int i = 0; i <args.length ; i++) {
            if("-d".equals(args[i])){
                if(i+1 < args.length){
                    date = args[++i];
                }
            }
        }
        // 要求date格式为：yyyy-MM-dd
        if(StringUtils.isBlank(date) || !TimeUtil.isValidateRunningDate(date)){
            // date 是一个无效时间数据
            // 默认是昨天
            date = TimeUtil.getYesterday();
        }
        conf.set(GlobalConstants.RUNNING_DATE_PARAMES, date);
    }

    /**
     * 设置job的输入路径
     * @param job
     */
    private void setJobInput(Job job){
        Configuration conf = job.getConfiguration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);
            Path inputPath = new Path(dataInputPath);
            if(fs.exists(inputPath)){
                FileInputFormat.addInputPath(job, inputPath);
            }
            Path outPath = new Path(dataOutputPath);
            if(fs.exists(outPath)){
                fs.deleteOnExit(outPath);
            }
            FileOutputFormat.setOutputPath(job, outPath);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(null != fs){
                try {
                    fs.close();
                }catch (IOException e){
                }
            }
        }
    }
}
