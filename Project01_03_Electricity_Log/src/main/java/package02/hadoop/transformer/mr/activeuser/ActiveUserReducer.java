package package02.hadoop.transformer.mr.activeuser;

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
 * @Date: 2019/12/25 20:02
 */
public class ActiveUserReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {
    private static final Logger logger = Logger.getLogger(ActiveUserReducer.class);
    private MapWritableValue outputValue = new MapWritableValue();
    private Set<String> unique = new HashSet<String>();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        System.out.println("reduce Begin~~~");
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("map:").append("{key:").append(key.toString()).append("}")
                .append("{value:").append(values.toString()).append("}");
        logger.error(stringBuffer.toString());
        this.unique.clear();
        for (TimeOutputValue timeOutputValue : values) {
            this.unique.add(timeOutputValue.getId());
        }
        MapWritable map = new MapWritable();
        /**
         * 指定key为-1（方便后面获取，可以随便指定）
         */
        map.put(new IntWritable(-2), new IntWritable(this.unique.size()));
        outputValue.setValue(map);

        String kpiName = key.getStatsCommon().getKpi().getKpiName();
        if(KpiType.ACTIVE_USER.name.equals(kpiName)){
            outputValue.setKpi(KpiType.ACTIVE_USER);
        }else if(KpiType.BROWSER_ACTIVE_USER.name.equals(kpiName)){
            outputValue.setKpi(KpiType.BROWSER_ACTIVE_USER);
        }
        context.write(key, outputValue);
    }
}
