package package02.hadoop.etl.test.mr_test;

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
public class AnalyserLogDataReducer extends Reducer<Text, Text, Text, Text> {
    private final Logger logger = Logger.getLogger(AnalyserLogDataReducer.class);
    private Text value = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()){
            value.set(iterator.next());
            logger.error("AnalyserLogDataReducer~~~"+ key+ "~~~~"+value.toString());
            context.write(key, value);
        }
    }
}
