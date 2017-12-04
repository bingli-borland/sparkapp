package streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

public class KafkaStreamingWordCount {

	public static void main(String[] args) throws InterruptedException {
		// 接收数据的地址和端口
		String zkQuorum = "192.168.2.129:2181";
		// 话题所在的组
		String group = "1";
		// 话题名称以“，”分隔
		String topics = "sex";
		// 每个话题的分片数
		int numThreads = 1;
		SparkConf sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]");
		final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(1000));
		jssc.checkpoint("."); // 设置检查点
		// 存放话题跟分片的映射关系
		Map<String, Integer> topicmap = new HashMap<String, Integer>();
		String[] topicsArr = topics.split(",");
		int n = topicsArr.length;
		for (int i = 0; i < n; i++) {
			topicmap.put(topicsArr[i], numThreads);
		}
		// 从Kafka中获取数据转换成RDD
		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, group,
				topicmap);
		// 从话题中过滤所需数据
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String line) {
				return Arrays.asList(line.split(" ")).iterator();
			}
		});

		// 对其中的单词进行统计(word, 1) tuple格式
		JavaPairDStream<String, Integer> mapToPairDStream = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		/**
		 * 对滑动窗口进行reduceByKeyAndWindow操作 其中，窗口长度是1秒，滑动时间间隔是1秒
		 */
		JavaPairDStream<String, Integer> reduceByKeyAndWindowDStream = mapToPairDStream
				.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {

					public Integer call(Integer v1, Integer v2) throws Exception {
						// TODO Auto-generated method stub
						return v1 + v2;
					}
				}, Durations.seconds(1), Durations.seconds(1), 1);
		
		reduceByKeyAndWindowDStream.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>() {

			@Override
			public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
				Map<String, Integer> map = rdd.collectAsMap();
				if (map.size() > 0) {
					publishToKafka(map);
				}
			}
		});
		jssc.start();
		jssc.awaitTermination();

	}

	public static void publishToKafka(Map map) throws Exception {
		Map<String, Object> props = new HashMap<String, Object>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.2.129:9092");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		// 实例化一个Kafka生产者
		KafkaProducer producer = new KafkaProducer<>(props);
		// rdd.colect即将rdd中数据转化为数组，然后write函数将rdd内容转化为json格式
		// 写入kafka
		ObjectMapper mapper = new ObjectMapper();
		String str = mapper.writeValueAsString(map);
		// 封装成Kafka消息，topic为"result"
		ProducerRecord message = new ProducerRecord<String, String>("result", null, str);
		// 给Kafka发送消息
		producer.send(message);
	}
}