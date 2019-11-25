package package03.secondsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/23 08:33
 */
public class IntPair implements WritableComparable<IntPair> {
    private IntWritable first;
    private IntWritable second;

    public IntPair() {
    }

    public IntPair(IntWritable first, IntWritable second) {
        this.first = first;
        this.second = second;
    }

    public IntWritable getFirst() {
        return first;
    }

    public void setFirst(IntWritable first) {
        this.first = first;
    }

    public IntWritable getSecond() {
        return second;
    }

    public void setSecond(IntWritable second) {
        this.second = second;
    }

    public void set(IntWritable first, IntWritable second){
        this.first = first;
        this.second = second;
    }

    @Override
    public int compareTo(IntPair o) {
        int result = this.first.compareTo(o.first);
        if(0 == result){
            result = this.second.compareTo(o.second);
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.first.get());
        dataOutput.writeInt(this.second.get());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.first = new IntWritable(dataInput.readInt());
        this.second = new IntWritable(dataInput.readInt());
    }
}
