package package02.hadoop.transformer.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import package02.hadoop.transformer.model.dim.base.PlatformDimension;
import package02.hadoop.transformer.service.IDimensionConverter;
import package02.hadoop.transformer.service.impl.DimensionConverterImpl;

import java.io.IOException;

/**
 * 操作日期dimension 相关的udf
 * 
 * @author root
 *
 */
public class PlatformDimensionUDF extends UDF {
    private IDimensionConverter converter = new DimensionConverterImpl();

    /**
     * 根据给定的platform返回id
     * 
     * @param platform
     * @return
     */
    public IntWritable evaluate(Text platform) {
        PlatformDimension platformDimension = new PlatformDimension(platform.toString());
        try {
            int id = this.converter.getDimensionIdByValue(platformDimension);
            return new IntWritable(id);
        } catch (IOException e) {
            throw new RuntimeException("获取PlatformDimensionUDF-id异常",e);
        }
    }

}
