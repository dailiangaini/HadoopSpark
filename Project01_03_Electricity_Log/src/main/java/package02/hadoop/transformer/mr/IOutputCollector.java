package package02.hadoop.transformer.mr;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import package02.hadoop.transformer.model.dim.base.BaseDimension;
import package02.hadoop.transformer.model.value.BaseStatsValueWritable;
import package02.hadoop.transformer.service.IDimensionConverter;


/**
 * 自定义的配合自定义output进行具体sql输出的类
 * 
 * @author root
 *
 */
public interface IOutputCollector {

    /**
     * 具体执行统计数据插入的方法
     * 
     * @param conf
     * @param key
     * @param value
     * @param pstmt
     * @param converter
     * @throws SQLException
     * @throws IOException
     */
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException;
}
