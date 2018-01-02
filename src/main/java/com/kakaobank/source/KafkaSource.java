package com.kakaobank.source;

import com.kakaobank.source.record.SimplePairRecord;
import com.kakaobank.util.KafkaFactory;
import com.kakaobank.util.LoggerFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * 카프카를 저장소로 사용하는 저장 객체
 * KafkaFactory 객체를 생성해서 이를 통해 consumer와 producer를 제공 받음
 *
 * 객체가 생성된 이후에 토픽 변경 불가
 * 다른 토픽에 접근하기 위해선 새로운 객체 생성
 *
 * Kafka Prop가 제공 되지 않는 다면 기본 설정으로 제공
 */
public class KafkaSource implements Source<SimplePairRecord<String, String>> {
    private static final Logger logger = LoggerFactory.getLogger("KafkaSource");

    private final String topic;
    private final KafkaFactory kafkaFactory;
    private final Producer<String, String> producer;
    private final Consumer<String, String> consumer;

    public KafkaSource(String topic, Properties kafkaProps) {
        this.topic = topic;
        this.kafkaFactory = new KafkaFactory(kafkaProps);
        this.producer = kafkaFactory.createProducer();
        this.consumer = kafkaFactory.createConsumer(topic);
    }

    /**
     * kafka consumer를 통해 레코드를 동기적으로 소비
     * consumer 객체에는 한번에 하나의 스레드만 접근하도록 동기화 블럭
     *
     * @param timeSec
     * @return
     */
    @Override
    public ConcurrentLinkedQueue<SimplePairRecord<String, String>> consume(int timeSec) {
        ConcurrentLinkedQueue<SimplePairRecord<String, String>> resultSet = new ConcurrentLinkedQueue<>();
        ConsumerRecords<String, String> consumerRecords;

        long start = System.currentTimeMillis();
        long threshold = start + ((long) timeSec) * 1000L;

        while (true) {
            if(System.currentTimeMillis() >= threshold) {
                break;
            }
            synchronized (consumer) {
                consumerRecords = consumer.poll(timeSec * 1000);
                consumer.commitSync();
            }
            Iterator<ConsumerRecord<String, String>> consumerRecordIter = consumerRecords.iterator();

            while(consumerRecordIter.hasNext()) {
                final ConsumerRecord<String, String> consumerRecord = consumerRecordIter.next();
                resultSet.add(new SimplePairRecord<>(consumerRecord.key(), consumerRecord.value()));
                logger.fine("read record : "
                        + "\n\tkey : " + topic
                        + "\n\tkey : " + consumerRecord.key()
                        + "\n\tvalue : " + consumerRecord.value()
                        + "\n\tpartition : " + consumerRecord.partition() + ", offset : " + consumerRecord.offset());
            }
            logger.fine("kafka source consume " + timeSec + " sec");

        }

        logger.fine("kafka source consume record count : " + resultSet.size());
        return resultSet;
    }

    /**
     * kafka producer를 통해 레코드를 동기적으로 생산
     *
     * @param record
     * @return
     */
    @Override
    public boolean write(SimplePairRecord<String, String> record) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(this.topic, record.getKey(), record.getValue());

        try {
            RecordMetadata metadata = producer.send(producerRecord).get();

            logger.fine("write record : "
                    + "\n\ttopic : " + this.topic
                    + "\n\tkey : " + record.getKey()
                    + "\n\tvalue : " + record.getValue()
                    + "\n\tpartition : " + metadata.partition() + ", offset : " + metadata.offset());
        } catch (InterruptedException e) {
            logger.severe("write record fail : " + record.mkString());
            e.printStackTrace();
            return false;
        } catch (ExecutionException e) {
            logger.severe("write record fail : " + record.mkString());
            e.printStackTrace();
            return false;
        } finally {
            synchronized (producer) {
                producer.flush();
            }
        }
        return true;
    }

    /**
     * 다수의의 레코드의 경우 한건씩 반복하며 레코드 생성
     *
     * @param records
     * @return
     */
    @Override
    public boolean write(Iterator<SimplePairRecord<String, String>> records) {
        boolean result = true;
        while(records.hasNext()) {
            if(this.write(records.next()) == false)
                result = false;
        }
        return result;
    }

    /**
     * 토픽의 마지막 커밋 오프셋과 마지막 오프셋을 비교하여
     * 현재 ClientID로 구독하는 토픽의 남은 레코드를 구함
     *
     * @return
     */
    @Override
    public long size() {
        Set<TopicPartition> topicPartitions;
        Map<TopicPartition, Long> endOffsets;
        long sum = 0;

        synchronized (consumer) {
            consumer.commitSync();
            topicPartitions = consumer.assignment();
            endOffsets = consumer.endOffsets(topicPartitions);
            for(TopicPartition topicPartition : topicPartitions) {
                logger.fine("topic : " + topicPartition.topic()
                        + ", partition : " + topicPartition.partition()
                        + ", commited offset : " + consumer.committed(topicPartition).offset()
                        + ", end offset : " + endOffsets.get(topicPartition)
                        + ", remain records : " + (endOffsets.get(topicPartition) - consumer.committed(topicPartition).offset()));
                sum = sum + endOffsets.get(topicPartition) - consumer.committed(topicPartition).offset();
            }
        }

        return sum;
    }

    @Override
    public boolean isEmpty() {
        if(size() <= 0) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean isNotEmpty() {
        if(size() > 0) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() {
        consumer.close();
        producer.close();
    }

    public String getTopic() {
        return this.topic;
    }

    public Set<TopicPartition> getAssignment() {
        return this.consumer.assignment();
    }
}
