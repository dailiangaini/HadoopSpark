package package03.secondsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/23 10:48
 */
public class MyGroupingComparator extends WritableComparator {
    protected MyGroupingComparator(){
        super(IntPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IntPair ip1 = (IntPair) a;
        IntPair ip2 = (IntPair) b;
        return ip1.getFirst().compareTo(ip2.getFirst());
    }
}
