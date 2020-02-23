package com.github.lan1tian.kafka.group.consumer;

import com.github.lan1tian.kafka.simple.config.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class GroupConsumer {

    private KafkaConsumer<String, String> consumer;

    private final int id;

    public GroupConsumer(int id) {
        this.id = id;
        Properties props = new Properties();
        props.put("client.id", "client-" + id);
        //zookeeper 配置
        props.put("bootstrap.servers", KafkaConfig.SERVER);

        //group 代表一个消费组
        props.put("group.id", KafkaConfig.GROUP);
        //zk连接超时
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        //关闭自动提交
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("auto.commit.interval.ms", "1000");
//        props.put("auto.offset.reset", "smallest");
        //序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singleton(KafkaConfig.TOPIC));
    }

    public void consume()  {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    if (id==2){
                        try {
                            TimeUnit.MILLISECONDS.sleep(Long.MAX_VALUE);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    System.out.printf("id = %d , partition = %d , offset = %d, key = %s, value = %s%n", id, record.partition(), record.offset(), record.key(), record.value());
                }
//                consumer.commitSync();
            }
        } finally {
            consumer.close();
        }
    }




    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 8; i++) {
            final int id = i;
            new Thread() {
                @Override
                public void run() {
                    new GroupConsumer(id).consume();
                }
            }.start();
        }
        TimeUnit.SECONDS.sleep(Long.MAX_VALUE);
    }

}
