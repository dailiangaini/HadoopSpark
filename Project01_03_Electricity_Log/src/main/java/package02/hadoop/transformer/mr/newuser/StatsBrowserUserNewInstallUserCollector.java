package package02.hadoop.transformer.mr.newuser;

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
 * @Date: 2019/12/25 00:37
 */
public class StatsBrowserUserNewInstallUserCollector implements IOutputCollector {
    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        MapWritableValue mapWritableValue = (MapWritableValue) value;
        IntWritable newInstallUsers = (IntWritable) mapWritableValue.getValue().get(new IntWritable(-1));

        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getDate()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getBrowser()));
        pstmt.setInt(++i, newInstallUsers.get());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, newInstallUsers.get());
        pstmt.addBatch();
    }
}
