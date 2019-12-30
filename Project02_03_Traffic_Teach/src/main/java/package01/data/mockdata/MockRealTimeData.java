package package01.data.mockdata;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import package02.spark.util.DateUtils;
import package02.spark.util.StringUtils;

import java.util.Properties;
import java.util.Random;


public class MockRealTimeData extends Thread {
	
	private static final Random random = new Random();
	private static final String[] locations = new String[]{"鲁","京","京","京","沪","京","京","深","京","京"};
	private Producer<String, String> producer;
	
	public MockRealTimeData() {
		producer = new KafkaProducer<String, String>(createProducerConfig());
	}
	
	private Properties createProducerConfig() {
		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers","localhost:9092");
		kafkaProps.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer") ;
		return kafkaProps;
	}
	
	@Override
	public void run() {
		while(true) {	
			String date = DateUtils.getTodayDate();
			String baseActionTime = date + " " + StringUtils.fulfuill(random.nextInt(24)+"");
			baseActionTime = date + " " + StringUtils.fulfuill((Integer.parseInt(baseActionTime.split(" ")[1])+1)+"");
			String actionTime = baseActionTime + ":" + StringUtils.fulfuill(random.nextInt(60)+"") + ":" + StringUtils.fulfuill(random.nextInt(60)+"");
    		String monitorId = StringUtils.fulfuill(4, random.nextInt(9)+"");
    		String car = locations[random.nextInt(10)] + (char)(65+random.nextInt(26))+StringUtils.fulfuill(5,random.nextInt(99999)+"");
    		String speed = random.nextInt(260)+"";
    		String roadId = random.nextInt(50)+1+"";
    		String cameraId = StringUtils.fulfuill(5, random.nextInt(9999)+"");
    		String areaId = StringUtils.fulfuill(2,random.nextInt(8)+"");
			producer.send(new ProducerRecord<String, String>("RoadRealTimeLog", date+"\t"+monitorId+"\t"+cameraId+"\t"+car + "\t" + actionTime + "\t" + speed + "\t" + roadId + "\t" + areaId));
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}  
		}
	}
	
	/**
	 * 启动Kafka Producer
	 * @param args
	 */
	public static void main(String[] args) {
		MockRealTimeData producer = new MockRealTimeData();
		producer.start();
	}
	
}
