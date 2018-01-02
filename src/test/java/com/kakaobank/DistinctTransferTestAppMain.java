package com.kakaobank;

import static com.kakaobank.TestHelper.*;

import com.kakaobank.etl.DistinctTransfer;
import com.kakaobank.etl.SimpleTransfer;
import com.kakaobank.source.KafkaSource;
import com.kakaobank.source.SimpleSource;
import com.kakaobank.source.record.SimplePairRecord;

import java.util.List;

/**
* 중복 제거 테스트
*/

public class DistinctTransferTestAppMain {
    private static final TestHelper testhelper = TestHelper.getInstance();

    public static void main(String[] args) {
        String testName = null;
        boolean result = true;
        long size = 0;
        int testRecordCount = 100;
        int duplicateTime = 3;

        List<SimplePairRecord<String, String>> pairSample = pairMake(testRecordCount);

        KafkaSource fromKafkaSource = new KafkaSource(testKafkaFromTopic, testKafkaFromProps);
        SimpleSource<SimplePairRecord<String, String>> fromSimpleSource = new SimpleSource<>();
        KafkaSource toKafkaSource = new KafkaSource(testKafkaToTopic, testKafkaToProps);
        SimpleSource<SimplePairRecord<String, String>> toSimpleSource = new SimpleSource<>();

        SimpleTransfer<SimplePairRecord<String, String>> loadTransfer = new SimpleTransfer<>(fromKafkaSource, fromSimpleSource);
        DistinctTransfer<SimplePairRecord<String, String>> distinctTransfer = new DistinctTransfer<>(fromSimpleSource, toSimpleSource);
        SimpleTransfer<SimplePairRecord<String, String>> saveTransfer = new SimpleTransfer<>(toSimpleSource, toKafkaSource);

        test("from kafka source consume record ", fromKafkaSource.consume(interval).size() == 0);
        test("from simple source consume record ", fromSimpleSource.consume(interval).size() == 0);
        test("to simple source consume record ", toSimpleSource.consume(interval).size() == 0);
        test("to kafka source consume record ", toKafkaSource.consume(interval).size() == 0);

        test("write from kafka smaple test record " + testRecordCount, fromKafkaSource.write(pairSample.iterator()));
        test("from kafka source consume record ", fromKafkaSource.consume(interval).size() == testRecordCount);

        fromKafkaSource.write(pairSample.iterator());
        fromKafkaSource.write(pairSample.iterator());
        fromKafkaSource.write(pairSample.iterator());

        loadTransfer.run(interval, timeout);
        distinctTransfer.run(interval, timeout);
        saveTransfer.run(interval, timeout);

        System.out.println("from kafka source consume record " + fromKafkaSource.consume(interval).size());
        System.out.println("from simple source consume record " + fromSimpleSource.consume(interval).size());
        System.out.println("to simple source consume record " + toSimpleSource.consume(interval).size());
        System.out.println("to kafka source consume record " + toKafkaSource.consume(interval).size());
        System.out.println();

    }
}
