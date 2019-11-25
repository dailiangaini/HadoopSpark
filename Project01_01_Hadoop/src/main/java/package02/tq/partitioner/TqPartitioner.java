package package02.tq.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import package02.tq.entity.TQ;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/22 14:21
 */
public class TqPartitioner extends Partitioner<TQ, Text> {

    @Override
    public int getPartition(TQ tq, Text text, int numPartitions) {
        return tq.getYear() % numPartitions;
    }
}
