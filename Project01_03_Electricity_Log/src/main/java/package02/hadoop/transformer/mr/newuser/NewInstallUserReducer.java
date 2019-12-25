package package02.hadoop.transformer.mr.newuser;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import package02.hadoop.common.KpiType;
import package02.hadoop.transformer.model.dim.StatsUserDimension;
import package02.hadoop.transformer.model.value.map.TimeOutputValue;
import package02.hadoop.transformer.model.value.reduce.MapWritableValue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/12/24 14:32
 */
public class NewInstallUserReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {
    private static final Logger logger = Logger.getLogger(NewInstallUserReducer.class);
    private MapWritableValue outputValue = new MapWritableValue();
    private Set<String> unique = new HashSet<String>();

    /**
     * 结果是需要去重的
     */
    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        System.out.println("reduce Begin~~~");
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("map:").append("{key:").append(key.toString()).append("}")
                .append("{value:").append(values.toString()).append("}");
        logger.error(stringBuffer.toString());

        this.unique.clear();
        // 开始计算uuid的个数
        for (TimeOutputValue value : values) {
            this.unique.add(value.getId());
        }
        MapWritable map = new MapWritable();
        /**
         * 指定key为-1（方便后面获取，可以随便指定）
         */
        map.put(new IntWritable(-1), new IntWritable(this.unique.size()));
        outputValue.setValue(map);

        // 设置kpi名称
        String kpiName = key.getStatsCommon().getKpi().getKpiName();
        /**
         * 给reduce value指定模块名称，告诉数据库要将数据插入到哪张表中
         */
        if (KpiType.NEW_INSTALL_USER.name.equals(kpiName)) {
            // 计算stats_user表中的新增用户
            outputValue.setKpi(KpiType.NEW_INSTALL_USER);
        } else if (KpiType.BROWSER_NEW_INSTALL_USER.name.equals(kpiName)) {
            // 计算stats_device_browser表中的新增用户
            outputValue.setKpi(KpiType.BROWSER_NEW_INSTALL_USER);
        }
        context.write(key, outputValue);
    }
}
