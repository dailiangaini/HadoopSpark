package package02.tq.comparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import package02.tq.entity.TQ;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/22 14:27
 */
public class TqGroupingComparator extends WritableComparator{
    public TqGroupingComparator() {
        super(TQ.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TQ t1 = (TQ)a;
        TQ t2 = (TQ)b;
        int result = Integer.compare(t1.getYear(), t2.getYear());
        if(0 == result){
            result = Integer.compare(t1.getMonth(), t2.getMonth());
        }
        return result;
    }

}
