package package03.secondsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/23 10:46
 */
public class MyPartitioner extends Partitioner<IntPair, Text> {
    @Override
    public int getPartition(IntPair intPair, Text text, int num) {
        return intPair.getFirst().get() % num;
    }
}
