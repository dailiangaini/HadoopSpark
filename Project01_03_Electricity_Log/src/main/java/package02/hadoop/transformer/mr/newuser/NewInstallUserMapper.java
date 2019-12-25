package package02.hadoop.transformer.mr.newuser;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import package02.hadoop.common.DateEnum;
import package02.hadoop.common.EventLogConstants;
import package02.hadoop.common.KpiType;
import package02.hadoop.etl.mr.AnalyserLogDataRunner;
import package02.hadoop.transformer.model.dim.StatsCommonDimension;
import package02.hadoop.transformer.model.dim.StatsUserDimension;
import package02.hadoop.transformer.model.dim.base.BrowserDimension;
import package02.hadoop.transformer.model.dim.base.DateDimension;
import package02.hadoop.transformer.model.dim.base.KpiDimension;
import package02.hadoop.transformer.model.dim.base.PlatformDimension;
import package02.hadoop.transformer.model.value.map.TimeOutputValue;

import java.io.IOException;
import java.util.List;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/12/24 14:31
 */
public class NewInstallUserMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(NewInstallUserMapper.class);
    /**
     * map 端输出的value对象
     */
    TimeOutputValue timeOutputValue = new TimeOutputValue();
    /**
     * map 输出的key的对象
     */
    StatsUserDimension statsUserDimension = new StatsUserDimension();
    /**
     * 列族
     */
    byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);

    /**
     * 定义模块纬度
     */
    KpiDimension newInstallUser = new KpiDimension(KpiType.NEW_INSTALL_USER.name);
    KpiDimension newInstallUserOfBrowser = new KpiDimension(KpiType.BROWSER_NEW_INSTALL_USER.name);

    /**
     * 输入记录数
     */
    protected int inputRecords = 0;
    /**
     * 过滤的记录数, 要求输入的记录没有进行任何输出
     */
    protected int filterRecords = 0;
    /**
     * 输出的记录条数
     */
    protected int outputRecords = 0;


    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        System.out.println("map Begin~~~");
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("map:").append("{key:").append(key.toString()).append("}")
                .append("{value:").append(value.toString()).append("}");
        logger.error(stringBuffer.toString());

        /**
         * 获取数据，时间， 浏览器信息，uuid，平台
         */
        String time = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
        String browserName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));
        String platform = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));
        String uuId = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID)));

        // 构建时间维度
        long timeofLong = Long.parseLong(time);
        DateDimension dateDimension = DateDimension.buildDate(timeofLong, DateEnum.DAY);

        // 构建浏览器维度
        List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName, browserVersion);

        // 构建平台为敌
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);

        // 给输出对象添加值
        timeOutputValue.setId(uuId);
        timeOutputValue.setTime(timeofLong);

        BrowserDimension defaultBrowser = new BrowserDimension("", "");

        // 构建维度组合
        // 构建公共维度组合对象
        StatsCommonDimension statsCommonDimension = statsUserDimension.getStatsCommon();
        statsCommonDimension.setDate(dateDimension);

        for (PlatformDimension platformDimension : platformDimensions) {
            /**
             * 默认的平台
             */
            // 1. 设置为一个默认值
            statsUserDimension.setBrowser(defaultBrowser);
            statsCommonDimension.setKpi(newInstallUser);
            statsCommonDimension.setPlatform(platformDimension);
            context.write(statsUserDimension, timeOutputValue);
            this.outputRecords++;

            /**
             * 遍历设置所有的平台信息
             */
            for (BrowserDimension browserDimension : browserDimensions) {
                statsCommonDimension.setKpi(newInstallUserOfBrowser);
                statsUserDimension.setBrowser(browserDimension);
                context.write(statsUserDimension, timeOutputValue);
                this.outputRecords++;
            }
        }


    }
}
