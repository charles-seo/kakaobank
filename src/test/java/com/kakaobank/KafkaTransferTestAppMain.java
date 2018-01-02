package com.kakaobank;

import com.kakaobank.etl.DistinctTransfer;
import com.kakaobank.etl.SimpleTransfer;
import com.kakaobank.source.KafkaSource;
import com.kakaobank.source.record.SimplePairRecord;

import java.util.List;

import static com.kakaobank.TestHelper.*;

/**
 * KafkaTransfer 테스트
 */
public class KafkaTransferTestAppMain {
    private static final TestHelper testhelper = TestHelper.getInstance();

    public static void main(String[] args) {
        String testName = null;
        boolean result = true;
        long size = 0;
        int testRecordCount = 100;
        int duplicateTime = 3;

        List<SimplePairRecord<String, String>> pairSample = pairMake(testRecordCount);

        KafkaSource fromSource = new KafkaSource(testKafkaFromTopic, testKafkaFromProps);
        KafkaSource toSource = new KafkaSource(testKafkaToTopic, testKafkaToProps);

        // 출발지 도착지 클리어
        test(">> kafka test from source consume remain record : " + fromSource.consume(3).size());
        test(">> kafka test to source consume remain record : " + toSource.consume(3).size());

        // 출발지에 데이터 저장
        test(">> from kafka source produce, : " + testRecordCount, fromSource.write(pairSample.iterator()));

        // 도착지로 단순 이동 실행
        SimpleTransfer<SimplePairRecord<String, String>> simpleTransfer = new SimpleTransfer<>(fromSource, toSource);

        // 에러 체크
        result = simpleTransfer.run(3, 10);
        test(">> kafka transfer errCheck", result);

        // 출발지 도착지 클리어
        test(">> kafka test from source consume remain record : 0", fromSource.consume(3).size() == 0);
        test(">> kafka test to source consume remain record : " + testRecordCount, toSource.consume(3).size() == testRecordCount);

        // 출발지에 데이터 저장
        for (int i = 0; i < duplicateTime; i++) {
            test(">> from kafka source produce, : " + testRecordCount + ", D" + i, fromSource.write(pairSample.iterator()));
        }

        // 중복 제거 이동 실행
        DistinctTransfer<SimplePairRecord<String, String>> distinctTransfer = new DistinctTransfer<>(fromSource, toSource);
        result = distinctTransfer.run(3, 5);

        // 에러 체크
        test(">> kafka distinct transfer errCheck", result);

        // 출발지 도착지 클리어
        test(">> kafka test from source consume remain record : 0", fromSource.consume(3).size() == 0);
        test(">> kafka test to source consume remain record : ");
        System.out.println(toSource.consume(3).size());
        test(">> kafka test to source consume remain record : " + testRecordCount, toSource.consume(3).size() == testRecordCount);
    }
}