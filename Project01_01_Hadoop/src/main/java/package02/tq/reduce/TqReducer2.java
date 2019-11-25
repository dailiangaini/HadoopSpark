package package02.tq.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import package02.tq.entity.TQ;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/22 14:18
 */
public class TqReducer2 extends Reducer<TQ, Text, Text, Text>{
    Text rkey = new Text();
    Text rval = new Text();

    @Override
    protected void reduce(TQ key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        System.out.println(key);
        StringBuilder stringBuffer = new StringBuilder();
        for (Text value : values) {
            stringBuffer.append(value).append("-");
        }
        String result = stringBuffer.substring(0, stringBuffer.length() - 1);
        rkey.set(key.toString());
        rval.set(result);
        context.write(rkey, rval);
    }
}
