package com.kakaobank.util;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * 카프카 팩토리
 * 카프카 Producer/Consumer등을 생성하기위한 클래스
 *
 * 사용자로부터 kafka prop를 제공받아 (업다면 기본 설정) 주어진 토픽/ID를 통해 레코드를 생산/소비
 *
 */
public class KafkaFactory {
    private static final Logger logger = LoggerFactory.getLogger("KafkaFactory");
    private final Properties kafkaProps;

    public KafkaFactory(Properties props) {
        logger.fine("init custom props kafka factory instance");
        kafkaProps = props;
    }

    /**
     * 기본 producer 생성
     *
     * @return producer
     */
    public Producer<String, String> createProducer() {
        logger.fine("create producer");
        return new KafkaProducer<>(kafkaProps);
    }

    /**
     * 기본 consumer 생성
     * 구독하는 토픽 없음
     *
     * @return consumer
     */
    public Consumer<String, String> createConsumer() {

        logger.fine("create non subscribe topic consumer");
        return new KafkaConsumer<>(kafkaProps);
    }

    /**
     * 주어진 토픽을 구독하는 consumer 생성
     * 기본 클라이언트ID 사용
     *
     * @param topic 구독할 토픽 이름
     * @return consumer
     */
    public Consumer<String, String> createConsumer(String topic) {
        Consumer<String, String> consumer = createConsumer();
        consumer.subscribe(Collections.singletonList(topic));

        logger.fine("create subscribe consumer, topic : " + topic);
        return consumer;
    }

    /**
     * 주어진 다수의 토픽을 구독하는 Consumer 객체 생성
     *
     * @param topics 구독할 토픽 이름 컬렉션
     * @return
     */
    public Consumer<String, String> createConsumer(Collection<String> topics) {
        Consumer<String, String> consumer = createConsumer();
        consumer.subscribe(topics);

        logger.fine("return default consumer, subscribe topics : " + topics);
        return consumer;
    }
}