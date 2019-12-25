package package02.hadoop.transformer.mr.activeuser;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import package02.hadoop.common.EventLogConstants;
import package02.hadoop.common.GlobalConstants;
import package02.hadoop.transformer.model.dim.StatsUserDimension;
import package02.hadoop.transformer.model.value.map.TimeOutputValue;
import package02.hadoop.transformer.model.value.reduce.MapWritableValue;
import package02.hadoop.transformer.mr.TransformerOutputFormat;
import package02.hadoop.transformer.mr.newuser.NewInstallUserMapper;
import package02.hadoop.transformer.mr.newuser.NewInstallUserReducer;
import package02.hadoop.transformer.mr.newuser.NewInstallUserRunner;
import package02.hadoop.util.TimeUtil;

import java.util.List;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/12/25 20:02
 */
public class ActiveUserRunner implements Tool {

    private static final Logger logger = Logger.getLogger(ActiveUserRunner.class);
    private Configuration conf = null;
    @Override
    public void setConf(Configuration conf) {
        // 添加自定义的配置文件
        conf.addResource("transformer-env.xml");
        conf.addResource("query-mapping.xml");
        conf.addResource("output-collector.xml");

        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        this.conf = HBaseConfiguration.create(conf);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
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

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        this.processArgs(conf,args);

        Job job = Job.getInstance(conf,"active_user");
        job.setJarByClass(ActiveUserRunner.class);

        TableMapReduceUtil.initTableMapperJob(
                initScans(job),
                ActiveUserMapper.class,
                StatsUserDimension.class,
                TimeOutputValue.class,
                job,
                false);

        job.setReducerClass(ActiveUserReducer.class);

        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);

        job.setOutputFormatClass(TransformerOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : -1;
    }
    protected List<Scan> initScans(Job job) {
        Scan scan = new Scan();
        String date = job.getConfiguration().get(GlobalConstants.RUNNING_DATE_PARAMES);
        long time = TimeUtil.parseString2Long(date);
        long endTime = time + GlobalConstants.DAY_OF_MILLISECONDS;
        String startRow = String.valueOf(time);
        String stopRow = String.valueOf(endTime);

        // 获取指定某天的数据
        scan.withStartRow(startRow.getBytes());
        scan.withStopRow(stopRow.getBytes());

        // 获取时间值为pv的数据
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        // 过滤数据，只分析page view event事件
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(EventLogConstants.EVENT_LOGS_FAMILY_NAME.getBytes(),
                Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(EventLogConstants.EventEnum.PAGEVIEW.alias));
        //filterList.addFilter(singleColumnValueFilter);

        // 定义mapper中需要获取的列名
        String[] columns = new String[] {
                // 事件名称
                EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME,
                // 用户id
                EventLogConstants.LOG_COLUMN_NAME_UUID,
                // 服务器时间
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,
                // 平台名称
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM,
                // 浏览器名称
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME,
                // 浏览器版本号
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION };
        filterList.addFilter(getColumnFilter(columns));

        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(EventLogConstants.HBASE_NAME_EVENT_LOGS));
        scan.setFilter(filterList);
        // 优化设置cache
        //scan.setBatch(500);
        // 启动cache blocks
        //scan.setCacheBlocks(true);
        // 设置每次返回的行数，默认值100，设置较大的值可以提高速度(减少rpc操作)，但是较大的值可能会导致内存异常。
        //scan.setCaching(1000);
        return Lists.newArrayList(scan);
    }
    protected Filter getColumnFilter(String[] columns) {
        int length = columns.length;
        byte[][] filter = new byte[length][];
        for (int i = 0; i < length; i++) {
            filter[i] = Bytes.toBytes(columns[i]);
        }
        return new MultipleColumnPrefixFilter(filter);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new ActiveUserRunner(), args);
    }

}
