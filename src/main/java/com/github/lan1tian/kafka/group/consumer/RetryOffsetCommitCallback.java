package com.github.lan1tian.kafka.group.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RetryOffsetCommitCallback implements OffsetCommitCallback {
    private static Logger LOGGER = LoggerFactory.getLogger(RetryOffsetCommitCallback.class);


    private KafkaConsumer<String, String> consumer;

    public RetryOffsetCommitCallback(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {

        if (exception != null) {
            LOGGER.info(exception.getMessage(), exception);
            consumer.commitAsync(offsets, this);
        }
    }

}
