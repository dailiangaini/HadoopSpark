package package05.secondsort;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 自定义分组策略
 * 将组合将中第一个值相同的分在一组
 * @author zengzhaozheng
 */
public class DefinedGroupSort extends WritableComparator{
    public DefinedGroupSort() {
        super(CombinationKey.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CombinationKey ck1 = (CombinationKey)a;
        CombinationKey ck2 = (CombinationKey)b;
        return ck1.getFirstKey().compareTo(ck2.getFirstKey());
    }
}
