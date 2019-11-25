package package03.secondsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/23 08:38
 */
public class MyMapper extends Mapper<LongWritable, Text, IntPair, Text> {

    private final IntPair intkey = new IntPair();
    private final Text textValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        int left = 0;
        int right = 0;
        if (tokenizer.hasMoreTokens()){
            left = Integer.parseInt(tokenizer.nextToken());
            if (tokenizer.hasMoreTokens()){
                right = Integer.parseInt(tokenizer.nextToken());
            }
            intkey.set(left, right);
            textValue.set(right + "");
            context.write(intkey, textValue);
        }
    }
}
