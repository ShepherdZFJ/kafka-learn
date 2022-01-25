package com.shepherd.kafka.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author fjzheng
 * @version 1.0
 * @date 2022/1/25 10:02
 *
 */

/**
 * kafka中每个topic被划分为多个分区，那么生产者将消息发送到topic时，具体追加到哪个分区呢？这就是所谓的分区策略，
 * Kafka 为我们提供了默认的分区策略，同时它也支持自定义分区策略。其路由机制为：
 *
 * ① 若发送消息时指定了分区（即自定义分区策略），则直接将消息append到指定分区；
 *
 * ② 若发送消息时未指定 patition，但指定了 key（kafka允许为每条消息设置一个key），则对key值进行hash计算，根据计算结果路由到指定分区，
 *    这种情况下可以保证同一个 Key 的所有消息都进入到相同的分区，即使用key保证消息消息顺序，同时进行分区保证高吞吐量；
 *
 * ③  patition 和 key 都未指定，则使用kafka默认的分区策略，轮询选出一个 patition；
 *
 * ※ 我们来自定义一个分区策略，将消息发送到我们指定的partition，首先新建一个分区器类实现Partitioner接口，
 *   重写方法，其中partition方法的返回值就表示将消息发送到几号分区；
 *   在配置文件application.properties中配置：
 *   spring.kafka.producer.properties.partitioner.class=com.shepherd.kafka.partition.CustomizePartitioner
 */
public class CustomizePartitioner implements Partitioner {
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
