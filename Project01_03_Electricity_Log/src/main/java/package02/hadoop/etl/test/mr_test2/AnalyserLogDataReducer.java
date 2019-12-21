package package02.hadoop.etl.test.mr_test2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/12/20 19:03
 */
public class AnalyserLogDataReducer extends Reducer<NullWritable, Text, Text, Text> {
    private final Logger logger = Logger.getLogger(AnalyserLogDataReducer.class);
    private Text value = new Text();
    private Text outKey = new Text();
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()){
            outKey.set("123");
            value.set(iterator.next());
            logger.error("AnalyserLogDataReducer~~~"+ key+ "~~~~"+value.toString());
            context.write(outKey, value);
        }
    }
}
