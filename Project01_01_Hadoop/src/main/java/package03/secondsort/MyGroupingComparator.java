package package03.secondsort;

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
    public int compare(Object a, Object b) {
        IntPair ip1 = (IntPair) a;
        IntPair ip2 = (IntPair) b;
        return Integer.compare(ip1.getFirst(), ip2.getFirst());
    }
}
