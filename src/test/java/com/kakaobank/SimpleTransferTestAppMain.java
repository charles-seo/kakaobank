package com.kakaobank;

import com.kakaobank.etl.DistinctTransfer;
import com.kakaobank.etl.SimpleTransfer;
import com.kakaobank.etl.SingleSimpleTransfer;
import com.kakaobank.source.KafkaSource;
import com.kakaobank.source.SimpleSource;
import com.kakaobank.source.record.SimplePairRecord;

import java.util.LinkedList;

import static com.kakaobank.TestHelper.*;

/**
 * Simple Transfer Test
 */
public class SimpleTransferTestAppMain {
    private static final TestHelper testhelper = TestHelper.getInstance();

    public static void main(String[] args) {
        String testName = null;
        boolean result = true;
        long beforeSize = 0;

        SimpleSource<SimplePairRecord<String, String>> fromSource = new SimpleSource<>();
        SimpleSource<SimplePairRecord<String, String>> toSource = new SimpleSource<>();

        //샘플 데이터 생성
        LinkedList<SimplePairRecord<String, String>> pairSample = pairMake(testRecord);

        //출발지에 데이터 저장
        fromSource.write(pairSample.iterator());

        //단순 이동 실행
        SimpleTransfer<SimplePairRecord<String, String>> simpleTransfer = new SimpleTransfer<>(fromSource, toSource);
        System.out.println("from source : " + fromSource.size() + ", to source : " + toSource.size());
        beforeSize = toSource.size();
        result = simpleTransfer.run(interval, timeout);
        System.out.println("from source : " + fromSource.size() + ", to source : " + toSource.size());
        System.out.println("to source, before size : " + beforeSize + ", after size : " + toSource.size());
        test("simple transfer test", beforeSize == (toSource.size() - testRecord));

        //출발지에 데에터 저장
        fromSource.write(pairSample.iterator());
        fromSource.write(pairSample.iterator());
        fromSource.write(pairSample.iterator());

        //중복 제거 이동 실행
        System.out.println("from source : " + fromSource.size() + ", to source : " + toSource.size());
        DistinctTransfer<SimplePairRecord<String, String>> distinctTransfer = new DistinctTransfer<>(fromSource, toSource);
        beforeSize = toSource.size();
        distinctTransfer.run(interval, timeout);
        System.out.println("from source : " + fromSource.size() + ", to source : " + toSource.size());
        System.out.println("to source, before size : " + beforeSize + ", after size : " + toSource.size());
        test("simple distinct transfer test", beforeSize == (toSource.size() - testRecord));
    }
}
