package package03.secondsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/23 10:49
 */
public class MyReduce extends Reducer<IntPair, Text, Text, Text> {
    Text rkey = new Text();
    Text rval = new Text();
    @Override
    protected void reduce(IntPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder stringBuffer = new StringBuilder();
        for (Text value : values) {
            stringBuffer.append(value).append("-");
        }
        String result = stringBuffer.substring(0, stringBuffer.length() - 1);
        rkey.set(key.getFirst().get()+"");
        rval.set(result);
        context.write(rkey, rval);
    }
}
