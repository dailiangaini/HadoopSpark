package package02.hadoop.etl.test.mr_test2;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import package02.hadoop.common.EventLogConstants;
import package02.hadoop.etl.util.LoggerUtil;

import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/12/18 19:24
 */
public class AnalyserLogDataMapper extends Mapper<Object, Text, NullWritable, Put> {

    private final Logger logger = Logger.getLogger(AnalyserLogDataMapper.class);
    /**
     * 主要用于标志，方便查看过滤数据
     */
    private int inputRecords, filterRecords, outputRecords;
    /**
     * event_logs表的列簇名称: info
     */
    private byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
    /**
     *
     */
    private CRC32 crc32 = new CRC32();

    private Text key = new Text();
    private Text value = new Text();

    /**
     * map 操作
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        this.inputRecords++;
        this.logger.error("Analyse data of:" + value);
        System.out.println("Analyse data of:" + value);

        try {
            // 解析日志
            Map<String, String> clientInfo = LoggerUtil.handleLog(value.toString());

            // 过滤解析失败的数据
            if(clientInfo.isEmpty()){
                this.filterRecords++;
                return;
            }
            // 获取事件名称(en)
            String eventAliasName = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME);
            EventLogConstants.EventEnum eventEnum = EventLogConstants.EventEnum.valueOfAlias(eventAliasName);

            switch (eventEnum){
                case LAUNCH:
                case PAGEVIEW:
                case CHARGEREQUEST:
                case CHARGEREFUND:
                case CHARGESUCCESS:
                case EVENT:{
                    /**
                     * 处理数据
                     */
                    this.handleDate(clientInfo, eventEnum, context);
                    break;
                }
                default:{
                    this.filterRecords++;
                    this.logger.error("该数据错误");
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
            this.filterRecords++;
            this.logger.error("处理数据异常，数据："+ value, e);
        }


    }
    /**
     * 具体处理数据的方法
     *
     * @param clientInfo
     * @param context
     * @param event
     * @throws InterruptedException
     * @throws IOException
     */
    private void handleDate(Map<String, String> clientInfo, EventLogConstants.EventEnum event, Context context) throws IOException, InterruptedException {
        String uuid = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_UUID);
        String memberId = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID);
        String serverTime = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME);
        if(StringUtils.isNotBlank(serverTime)){
            // 服务器时间不能为空
            clientInfo.remove(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT);
            /**
             * timestrap + (uuid+memberId+event).crc
             */
            String rowkey = this.generateRowKey(uuid, memberId, event.alias, serverTime);

            Put put = new Put(Bytes.toBytes(rowkey));
            for(Map.Entry<String, String> entry: clientInfo.entrySet()){
                if(StringUtils.isNotBlank(entry.getKey()) && StringUtils.isNotBlank(entry.getValue())){
                    put.addColumn(family, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
                }
            }
            key.set(rowkey);
            this.logger.error(rowkey + "::" + put.toJSON());
            value.set(put.toJSON());
            context.write(NullWritable.get(), put);
            this.outputRecords++;
        }else {
            this.filterRecords++;
        }
    }

    /**
     * 根据uuid memberid servertime创建rowkey
     *
     * @param uuid
     * @param memberId
     * @param eventAliasName
     * @param serverTime
     * @return
     */
    private String generateRowKey(String uuid, String memberId, String eventAliasName, String serverTime) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(serverTime).append("—");
        this.crc32.reset();
        if(StringUtils.isNotBlank(uuid)){
            this.crc32.update(uuid.getBytes());
        }
        if(StringUtils.isNotBlank(memberId)){
            this.crc32.update(memberId.getBytes());
        }
        this.crc32.update(eventAliasName.getBytes());
        stringBuilder.append(this.crc32.getValue() % 100000000L);
        return stringBuilder.toString();
    }
}
