package package04.secondsort;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/24 22:01
 */
public class DefinedComparator extends WritableComparator {
    private static final Logger logger = LoggerFactory.getLogger(DefinedComparator.class);
    public DefinedComparator() {
        super(CombinationKey.class,true);
    }
    @Override
    public int compare(WritableComparable combinationKeyOne,
                       WritableComparable CombinationKeyOther) {
        logger.info("---------enter DefinedComparator flag---------");

        CombinationKey c1 = (CombinationKey) combinationKeyOne;
        CombinationKey c2 = (CombinationKey) CombinationKeyOther;

        /**
         * 确保进行排序的数据在同一个区内，如果不在同一个区则按照组合键中第一个键排序
         * 另外，这个判断是可以调整最终输出的组合键第一个值的排序
         * 下面这种比较对第一个字段的排序是升序的，如果想降序这将c1和c2倒过来（假设1）
         */
        if(!c1.getFirstKey().equals(c2.getFirstKey())){
            logger.info("---------out DefinedComparator flag---------");
            return c1.getFirstKey().compareTo(c2.getFirstKey());
        }
        else{//按照组合键的第二个键的升序排序，将c1和c2倒过来则是按照数字的降序排序(假设2)
            logger.info("---------out DefinedComparator flag---------");
            return Integer.compare(c1.getSecondKey(), c2.getSecondKey());//0,负数,正数
        }
        /**
         * （1）按照上面的这种实现最终的二次排序结果为：
         * sort1    1,2
         * sort2    3,54,77
         * sort6    20,22,221
         * （2）如果实现假设1，则最终的二次排序结果为:
         * sort6    20,22,221
         * sort2    3,54,77
         * sort1    1,2
         * （3）如果实现假设2，则最终的二次排序结果为:
         * sort1    2,1
         * sort2    77,54,3
         * sort6    221,22,20
         */
    }
}
