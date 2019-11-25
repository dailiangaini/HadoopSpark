package package03.secondsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/23 10:40
 */
public class MySortComparator extends WritableComparator {
    protected MySortComparator(){
        super(IntPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IntPair ip1 = (IntPair) a;
        IntPair ip2 = (IntPair) b;
        int result = ip1.getFirst().compareTo(ip2.getFirst());
        if(0 == result){
            result = ip2.getSecond().compareTo(ip1.getSecond());
        }
        return result;
    }
}
