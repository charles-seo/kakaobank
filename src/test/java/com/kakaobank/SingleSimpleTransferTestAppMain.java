package com.kakaobank;

import com.kakaobank.etl.SingleSimpleTransfer;
import com.kakaobank.source.KafkaSource;
import com.kakaobank.source.SimpleSource;
import com.kakaobank.source.record.SimplePairRecord;

import java.util.LinkedList;

import static com.kakaobank.TestHelper.*;

/**
 * Single Simple Transfer Test
 */
public class SingleSimpleTransferTestAppMain {
    private static final TestHelper testhelper = TestHelper.getInstance();

    public static void main(String[] args) {
        SimpleSource<SimplePairRecord<String, String>> fromSimpleSource = new SimpleSource<>();
        SimpleSource<SimplePairRecord<String, String>> toSimpleSource = new SimpleSource<>();
        KafkaSource fromKafkaSource = new KafkaSource(testKafkaFromTopic, testKafkaFromProps);
        KafkaSource toKafkaSource = new KafkaSource(testKafkaToTopic, testKafkaToProps);

        SingleSimpleTransfer<SimplePairRecord<String, String>> singleSimpleTransfer = new SingleSimpleTransfer<>(fromSimpleSource, toSimpleSource);
        SingleSimpleTransfer<SimplePairRecord<String, String>> singleKafkaTransfer = new SingleSimpleTransfer<>(fromKafkaSource, toKafkaSource);

        int size = 0;
        // 샘플 데이터 생성
        LinkedList<SimplePairRecord<String, String>> pairSample = TestHelper.pairMake(testRecord);

        // simple source 테스트
        // 출발지에 데이터 저장
        fromSimpleSource.write(pairSample.iterator());

        size = fromSimpleSource.consume(interval).size();
        test("from simple source record size : " + size, size == testRecord);

        fromSimpleSource.write(pairSample.iterator());
        singleSimpleTransfer.trans(interval, timeout);
        size = toSimpleSource.consume(1).size();
        test("from simple source record trans to simple source : " + size, size == testRecord);

        fromSimpleSource.write(pairSample.iterator());
        fromSimpleSource.write(pairSample.iterator());
        fromSimpleSource.write(pairSample.iterator());
        singleSimpleTransfer.distinctTrans(interval, timeout);
        size = toSimpleSource.consume(1).size();
        test("from simple source record distinct trans to simple source : " + size, size == testRecord);


        // kafka source 테스트
        // 출발지에 데이터 저장
        fromKafkaSource.write(pairSample.iterator());
        size = fromKafkaSource.consume(1).size();
        test("from kafka source record size : " + size, size == testRecord);

        fromKafkaSource.write(pairSample.iterator());
        singleKafkaTransfer.trans(interval, timeout);

        size = toKafkaSource.consume(1).size();
        test("from kafka source record trans to kafka source : " + size, size == testRecord);

        fromKafkaSource.write(pairSample.iterator());
        fromKafkaSource.write(pairSample.iterator());
        fromKafkaSource.write(pairSample.iterator());
        singleKafkaTransfer.distinctTrans(interval, timeout);

        size = toKafkaSource.consume(interval).size();
        test("from kafka source record distinct trans to kafka source : " + size, size == testRecord);
    }
}
