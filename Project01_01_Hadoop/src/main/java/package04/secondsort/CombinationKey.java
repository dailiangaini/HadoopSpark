package package04.secondsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/24 21:58
 */
public class CombinationKey implements WritableComparable<CombinationKey> {

    private static final Logger logger = LoggerFactory.getLogger(CombinationKey.class);
    private String firstKey;
    private int secondKey;

    public String getFirstKey() {
        return firstKey;
    }

    public void setFirstKey(String firstKey) {
        this.firstKey = firstKey;
    }

    public int getSecondKey() {
        return secondKey;
    }

    public void setSecondKey(int secondKey) {
        this.secondKey = secondKey;
    }

    @Override
    public void readFields(DataInput dateInput) throws IOException {
        // TODO Auto-generated method stub
        this.firstKey = dateInput.readUTF();
        this.secondKey = dateInput.readInt();
    }
    @Override
    public void write(DataOutput outPut) throws IOException {
        outPut.writeUTF(this.firstKey);
        outPut.writeInt(this.secondKey);
    }
    /**
     * 自定义比较策略
     * 注意：该比较策略用于mapreduce的第一次默认排序，也就是发生在map阶段的sort小阶段，
     * 发生地点为环形缓冲区(可以通过io.sort.mb进行大小调整)
     */
    @Override
    public int compareTo(CombinationKey combinationKey) {
        logger.info("-------CombinationKey flag-------");
        int result = this.firstKey.compareTo(combinationKey.getFirstKey());
        if(0 == result){
            result = -Integer.compare(this.getSecondKey(), combinationKey.getSecondKey());
        }
        return result;
    }
}
