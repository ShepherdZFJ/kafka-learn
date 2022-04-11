package com.shepherd.kafka.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Map;

/**
 * @author fjzheng
 * @version 1.0
 * @date 2022/1/25 17:44
 */
public class CustomConsumerInterceptor implements ConsumerInterceptor {
    @Override
    public ConsumerRecords onConsume(ConsumerRecords consumerRecords) {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void onCommit(Map map) {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
