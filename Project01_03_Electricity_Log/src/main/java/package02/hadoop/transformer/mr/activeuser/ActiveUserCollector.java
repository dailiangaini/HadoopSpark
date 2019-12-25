package package02.hadoop.transformer.mr.activeuser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import package02.hadoop.common.GlobalConstants;
import package02.hadoop.transformer.model.dim.StatsUserDimension;
import package02.hadoop.transformer.model.dim.base.BaseDimension;
import package02.hadoop.transformer.model.value.BaseStatsValueWritable;
import package02.hadoop.transformer.model.value.reduce.MapWritableValue;
import package02.hadoop.transformer.mr.IOutputCollector;
import package02.hadoop.transformer.service.IDimensionConverter;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/12/25 19:49
 */
public class ActiveUserCollector implements IOutputCollector {
    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt,
                        IDimensionConverter converter) throws SQLException, IOException {
        // 进行强制后获取对应的值
        StatsUserDimension statsUser = (StatsUserDimension) key;
        IntWritable activeUserValue = (IntWritable) ((MapWritableValue) value).getValue().get(new IntWritable(-2));

        // 进行参数设置
        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getDate()));
        pstmt.setInt(++i, activeUserValue.get());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, activeUserValue.get());

        // 添加到batch中
        pstmt.addBatch();
    }
}
