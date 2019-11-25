package package03.secondsort;

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
    public int compare(Object a, Object b) {
        IntPair ip1 = (IntPair) a;
        IntPair ip2 = (IntPair) b;
        int result = Integer.compare(ip1.getFirst(), ip2.getFirst());
        if(0 == result){
            result = -Integer.compare(ip1.getSecond(), ip2.getSecond());
        }
        return result;
    }
}
