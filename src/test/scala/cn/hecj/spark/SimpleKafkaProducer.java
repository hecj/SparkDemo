package cn.hecj.spark;

/**
 * @program: SparkDemo1->SimpleKafkaProducer
 * @desc:
 * @author: hecj
 * @create: 20200309
 **/

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * 生产者
 */
public class SimpleKafkaProducer {
	private static KafkaProducer<String, String> producer;
	private final static String TOPIC = "access_log2";
	public SimpleKafkaProducer(){
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 3);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// 幂等性
		props.put("enable.idempotence",true);
		//设置分区类,根据key进行数据分区
		producer = new KafkaProducer<String, String>(props);
	}
	public void produce(){
		for (int i = 0;i<1000;i++){
//			String key = String.valueOf(i);
			try {
				Thread.sleep(100l);
			} catch (Exception ex){
			
			}
			String key = "k"+i;
			String day = DateFormatUtils.format(new Date(),"yyyy-MM-dd HH:mm:ss");
			String ip = "20.34.11."+RandomUtils.nextInt(1,100);
			String api = "/api/log";
			String user = ""+RandomUtils.nextInt(1,20);
			
			StringBuffer message = new StringBuffer();
			message.append(day).append(",");
			message.append(ip).append(",");
			message.append(api).append(",");
			message.append(user);
			
			Future<RecordMetadata> result = producer.send(new ProducerRecord<String, String>(TOPIC,key,message.toString()));
//			System.out.println(message);
		}
		producer.close();
	}
	
	public static void main(String[] args) {
		new SimpleKafkaProducer().produce();
	}
}
