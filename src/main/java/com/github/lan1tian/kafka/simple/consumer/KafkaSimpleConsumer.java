package com.github.lan1tian.kafka.simple.consumer;
//
//import com.github.lan1tian.kafka.simple.config.KafkaConfig;
//import kafka.consumer.ConsumerConfig;
//import kafka.consumer.ConsumerIterator;
//import kafka.consumer.KafkaStream;
//import kafka.javaapi.consumer.ConsumerConnector;
//import kafka.message.MessageAndMetadata;
//import kafka.serializer.StringDecoder;
//import kafka.utils.VerifiableProperties;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//
//public class KafkaSimpleConsumer {
//
//    private final ConsumerConnector consumer;
//
//    private KafkaSimpleConsumer() {
//        Properties props = new Properties();
//        //zookeeper 配置
//        props.put("zookeeper.connect", "127.0.0.1:2181");
//
//        //group 代表一个消费组
//        props.put("group.id", KafkaConfig.GROUP);
//
//        //zk连接超时
//        props.put("zookeeper.session.timeout.ms", "4000");
//        props.put("zookeeper.sync.time.ms", "200");
////        props.put("auto.commit.interval.ms", "1000");
////        props.put("auto.offset.reset", "smallest");
//        //序列化类
//        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        //关闭自动提交
//        props.put("enable.auto.commit", false);
//
//
//        ConsumerConfig config = new ConsumerConfig(props);
//
//        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
//    }
//
//    void consume() {
//        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
//        topicCountMap.put(KafkaConfig.TOPIC, new Integer(1));
//
//        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
//        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
//
//        Map<String, List<KafkaStream<String, String>>> consumerMap =
//                consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
//        KafkaStream<String, String> stream = consumerMap.get(KafkaConfig.TOPIC).get(0);
//        ConsumerIterator<String, String> it = stream.iterator();
//        while (it.hasNext()) {
//            MessageAndMetadata<String, String> metadata = it.next();
//            System.out.println("offset:" + metadata.offset() + ",message:" + metadata.message());
////            consumer.commitOffsets();
//        }
//        System.out.println("finish batch >>>");
//    }
//
//    public static void main(String[] args) {
//        new KafkaSimpleConsumer().consume();
//    }
//
//}
