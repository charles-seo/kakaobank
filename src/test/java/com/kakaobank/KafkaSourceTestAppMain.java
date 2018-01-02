package com.kakaobank;

import com.kakaobank.source.KafkaSource;

import static com.kakaobank.TestHelper.*;

/**
 * Kafka Source 테스트
 */
public class KafkaSourceTestAppMain {
    private static final TestHelper testhelper = TestHelper.getInstance();

    public static void main(String[] args) {
        KafkaSource simpleSource = new KafkaSource(testKafkaSimpleTopic, testKafkaSimpleProps);
        KafkaSource fromSource = new KafkaSource(testKafkaFromTopic, testKafkaFromProps);
        KafkaSource toSource = new KafkaSource(testKafkaToTopic, testKafkaToProps);

        kafkaSourceTestTemplate(simpleSource);
        kafkaSourceTestTemplate(fromSource);
        kafkaSourceTestTemplate(toSource);
    }

}
