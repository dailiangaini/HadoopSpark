package package02.tq.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import package02.tq.entity.TQ;

import java.io.IOException;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/22 14:18
 */
public class TqReducer extends Reducer<TQ, Text, Text, Text>{
    Text rkey = new Text();
    Text rval = new Text();

    @Override
    protected void reduce(TQ key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        System.out.println(key);
        for (Text v : values) {
            System.out.println(v);
        }
        int flg=0;
        int day=0;

        for (Text v : values) {
            System.out.println(v);

            if(flg==0){
                day=key.getDay();

                rkey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
                rval.set(key.getWd()+"");
                context.write(rkey,rval );
                flg++;
                break;

            }
            if(flg!=0 && day != key.getDay()){
                rkey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
                rval.set(key.getWd()+"");
                context.write(rkey,rval );
                break;
            }
        }
    }
}
