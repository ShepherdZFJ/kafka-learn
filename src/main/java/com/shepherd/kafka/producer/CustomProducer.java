package com.shepherd.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author fjzheng
 * @version 1.0
 * @date 2022/1/24 14:48
 */

public class CustomProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        // kafka 集群，broker-list
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.10.0.18:9092");
        props.put("acks", "all");
        // 重试次数
        props.put("retries", 1);
        // 批次大小
        props.put("batch.size", 16384);
        // 等待时间
        props.put("linger.ms", 1);
        // RecordAccumulator 缓冲区大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.shepherd.kafka.producer.MyPartitioner");
        // 创建消息生产者
        Producer<String, String> producer = new KafkaProducer<>(props);
        // 发送消息
        /**
         * {@link ProducerRecord的构建函数
         *     public ProducerRecord(String topic, Integer partition, K key, V value) {
         *         this(topic, partition, (Long)null, key, value, (Iterable)null);
         *     }
         *     public ProducerRecord(String topic, K key, V value) {
         *         this(topic, (Integer)null, (Long)null, key, value, (Iterable)null);
         *     }
         *     public ProducerRecord(String topic, V value) {
         *         this(topic, (Integer)null, (Long)null, (Object)null, value, (Iterable)null);
         *     }
         * 1、支持指定分区；
         * 2、传入key，然后hash，再对分区数取余
         * 3、随机选择分区，下次选择的分区和上次的为不同分区。
         * }
         */
        for (int i = 0; i < 10; i++) {
            // 指定分区
         //   producer.send(new ProducerRecord<>("topic1", 1, Integer.toString(i), "val" + Integer.toString(i)));
            // 通过传入key计算分区
            producer.send(new ProducerRecord<String, String>("topic1",
                    Integer.toString(i), "val"+Integer.toString(i)));
        }
            producer.close();
    }


    public void test(){

    }
}
