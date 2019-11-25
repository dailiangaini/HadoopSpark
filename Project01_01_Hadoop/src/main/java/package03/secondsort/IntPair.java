package package03.secondsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/23 08:33
 */
public class IntPair implements WritableComparable<IntPair> {
    private int first;
    private int second;

    public IntPair() {
    }

    public IntPair(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    public void set(int first, int second){
        this.first = first;
        this.second = second;
    }

    @Override
    public int compareTo(IntPair o) {
        int result = Integer.compare(this.first, o.first);
        /*if(0 == result){
            result = -Integer.compare(this.second, o.second);
        }*/
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.first);
        dataOutput.writeInt(this.second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.first = dataInput.readInt();
        this.second = dataInput.readInt();
    }

    @Override
    public int hashCode() {
        return first;
    }

    @Override
    public boolean equals(Object right) {
        if (right == null){
            return false;
        }

        if (this == right) {
            return true;
        }

        if (right instanceof IntPair){

            IntPair r = (IntPair) right;
            return r.first == first && r.second == second;
        }

        return false;
    }

    @Override
    public String toString() {
        return "IntPair{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }

   /* public static void main(String[] args) {
        List<IntPair> list = Arrays.asList(
                new IntPair(40, 20),
                new IntPair(40, 10),
                new IntPair(40, 30),
                new IntPair(30, 20),
                new IntPair(30, 10),
                new IntPair(30, 30),
                new IntPair(50, 30),
                new IntPair(50, 10),
                new IntPair(50, 20)
        );
        Collections.sort(list);
        for (IntPair intPair: list){
            System.out.println(intPair);
        }
    }*/
}
