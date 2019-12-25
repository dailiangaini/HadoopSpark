package package02.hadoop.transformer.mr.activeuser;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import package02.hadoop.common.DateEnum;
import package02.hadoop.common.EventLogConstants;
import package02.hadoop.common.KpiType;
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
 * @Date: 2019/12/25 20:02
 */
public class ActiveUserMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(ActiveUserMapper.class);

    StatsUserDimension statsUserDimension = new StatsUserDimension();
    TimeOutputValue timeOutputValue = new TimeOutputValue();
    byte[] family = EventLogConstants.EVENT_LOGS_FAMILY_NAME.getBytes();

    /**
     * kpi信息
     */
    KpiDimension activeUserKpi = new KpiDimension(KpiType.ACTIVE_USER.name);
    KpiDimension activeUserOfBrowserKpi = new KpiDimension(KpiType.BROWSER_ACTIVE_USER.name);

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
        String uuId = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID)));
        String time = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
        String browserName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));
        String platform = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));

        // 构建时间维度
        long timeOfLong = Long.parseLong(time);
        DateDimension dateDimension = DateDimension.buildDate(timeOfLong, DateEnum.DAY);

        // 构建浏览器维度
        List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName, browserVersion);

        // 构建平台为敌
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);

        // 给输出对象添加值
        timeOutputValue.setId(uuId);
        timeOutputValue.setTime(timeOfLong);

        BrowserDimension defaultBrowser = new BrowserDimension("", "");

        // 构建维度组合
        // 构建公共维度组合对象
        StatsCommonDimension statsCommonDimension = statsUserDimension.getStatsCommon();
        statsCommonDimension.setDate(dateDimension);

        //平台维度和浏览器维度
        // 用户基本信息模块
        for (PlatformDimension platformDimension : platformDimensions) {
            statsCommonDimension.setPlatform(platformDimension);
            statsCommonDimension.setKpi(activeUserKpi);
            statsUserDimension.setBrowser(defaultBrowser);
            context.write(statsUserDimension, timeOutputValue);
            /**
             * 遍历设置所有的平台信息
             */
            for (BrowserDimension browserDimension : browserDimensions) {
                statsCommonDimension.setKpi(activeUserOfBrowserKpi);
                statsUserDimension.setBrowser(browserDimension);
                context.write(statsUserDimension, timeOutputValue);
            }
        }


    }
}
