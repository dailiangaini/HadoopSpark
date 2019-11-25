package package02.tq.entity;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/22 13:43
 */
public class TQ implements WritableComparable<TQ> {
    private int year;
    private int month;
    private int day;
    private int wd;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getWd() {
        return wd;
    }

    public void setWd(int wd) {
        this.wd = wd;
    }

    /**
     * 比较，安装日期排序
     * @param thatTq
     * @return
     */
    @Override
    public int compareTo(TQ thatTq) {
        int result = Integer.compare(this.year, thatTq.year);
        if(0 == result){
            result = Integer.compare(this.month, thatTq.month);
            if(0 == result){
                result = Integer.compare(this.day, thatTq.day);
            }
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(month);
        dataOutput.writeInt(day);
        dataOutput.writeInt(wd);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readInt();
        this.month = dataInput.readInt();
        this.day = dataInput.readInt();
        this.wd = dataInput.readInt();
    }

   /* @Override
    public int hashCode() {
        return this.getYear() + this.getMonth() + this.getDay() + this.getDay();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof TQ){
            return (this.year == ((TQ) obj).year && this.month == ((TQ) obj).month && this.day == ((TQ) obj).day && this.wd == ((TQ) obj).wd);
        }
        return false;
    }*/

    @Override
    public String toString() {
        return "TQ{" +
                "year=" + year +
                ", month=" + month +
                ", day=" + day +
                ", wd=" + wd +
                '}';
    }
}
