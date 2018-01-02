package com.kakaobank;

import com.kakaobank.source.SimpleSource;
import com.kakaobank.source.record.SimplePairRecord;

import java.util.LinkedList;

import static com.kakaobank.TestHelper.*;

/**
 * Simple Source 테스트
 */
public class SimpleSourceTestAppMain {
    private static final TestHelper testhelper = TestHelper.getInstance();

    public static void main(String[] args) {
        String testName = null;
        boolean result = true;

        SimpleSource<SimplePairRecord<String, String>> pairSource = new SimpleSource<>();

        LinkedList<SimplePairRecord<String, String>> pairSample = pairMake(testRecord);
        pairSource.write(pairSample.iterator());

        //메모리큐 쓰기
        test("simple source write / size", pairSource.size() == testRecord);

        //메모리큐 읽기
        test("simple source consume", pairSource.consume(1).size() == testRecord);

    }
}
