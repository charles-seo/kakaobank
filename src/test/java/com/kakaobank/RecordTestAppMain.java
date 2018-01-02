package com.kakaobank;

import com.kakaobank.source.record.SimplePairRecord;
import static com.kakaobank.TestHelper.*;

/**
 * SimplePairRecord 테스트
 */
public class RecordTestAppMain {
    private static final TestHelper testhelper = TestHelper.getInstance();

    private static final long testEventID1 = -7656596631385128008L;
    private static final String testEventTimestamp1 = "timeStamp_-7656596631385128008";
    private static final String testServiceCode1 = "serviceCode_-7656596631385128008";
    private static final String testEventContext1 = "EventContext_-7656596631385128008";

    private static final long testEventID2 = 7656596631385128008L;
    private static final String testEventTimestamp2 = "timeStamp_7656596631385128008";
    private static final String testServiceCode2 = "serviceCode_7656596631385128008";
    private static final String testEventContext2 = "EventContext_7656596631385128008";

    private static final String testKey1 = Long.toString(testEventID1);
    private static final String testValue1 = testEventID1 + "," + testEventTimestamp1 + "," + testServiceCode1 + "," + testEventContext1;
    private static final String testKey2 = Long.toString(testEventID2);
    private static final String testValue2 = testEventID2 + "," + testEventTimestamp2 + "," + testServiceCode2 + "," + testEventContext2;

    public static void main(String[] args) {
        String testName = null;
        boolean result = true;

        // K-V 레코드 생성
        SimplePairRecord<String, String> testPairRecord1 = new SimplePairRecord<>(testKey1, testValue1);
        SimplePairRecord<String, String> testPairRecord2 = new SimplePairRecord<>(testKey2, testValue2);
        SimplePairRecord<String, String> testPairRecord3 = new SimplePairRecord<>(testKey1, testValue1);

        if (testPairRecord1.getKey().equals(testKey1) == false) result = false;
        else if (testPairRecord1.getValue().equals(testValue1) == false) result = false;

        testhelper.test(">> create test", result);

        //동등비교
        if (testPairRecord1.equal(testPairRecord3) == false) result = false;
        else if (testPairRecord1.equal(testPairRecord2) == true) result = false;

        testhelper.test(">> equal test", result);
    }
}