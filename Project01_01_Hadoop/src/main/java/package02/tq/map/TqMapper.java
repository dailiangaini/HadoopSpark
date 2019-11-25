package package02.tq.map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import package02.tq.entity.TQ;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/22 14:08
 */
public class TqMapper extends Mapper<Object, Text, TQ, Text>  {
    TQ tq= new TQ();
    Text vwd = new Text();
    @Override
    protected void map(Object key, Text value, Context context) throws InterruptedException, IOException {
        try {
            String[] strs = StringUtils.split(value.toString(), '\t');
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date date = null;

            date = sdf.parse(strs[0]);

            Calendar cal = Calendar.getInstance();
            cal.setTime(date);

            tq.setYear(cal.get(Calendar.YEAR));
            tq.setMonth(cal.get(Calendar.MONTH)+1);
            tq.setDay(cal.get(Calendar.DAY_OF_MONTH));

            int wd = Integer.parseInt(strs[1].substring(0, strs[1].length()-1));
            tq.setWd(wd);
            vwd.set(wd + "");
            context.write(tq, vwd);
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}
