package com.kakaobank;

import com.kakaobank.source.KafkaSource;
import com.kakaobank.source.record.SimplePairRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * 테스트 헬퍼 클래스
 */
public class TestHelper {
    private static TestHelper testHelper = new TestHelper();

    public static final int interval = 1;
    public static final int timeout = 2;
    public static final int testRecord = 100;

    public static final String testKafkaFromTopic = "kakaobank-test-from-topic";
    public static final String testKafkaToTopic = "kakaobank-test-to-topic";
    public static final String testKafkaSimpleTopic = "kakaobank-test-simple-topic";

    public static final Properties testKafkaFromProps = testHelper.readProp("conf/kafka_from.properties");
    public static final Properties testKafkaToProps = testHelper.readProp("conf/kafka_to.properties");
    public static final Properties testKafkaSimpleProps = testHelper.readProp("conf/kafka_simple.properties");


    public static TestHelper getInstance() {
        return testHelper;
    }

    private TestHelper() {
    }

    /**
     * 테스트 스트링 레코드 생성기
     * 0 ~ recordCount까지 연속된 레코드 생성
     *
     * @param recordCount 생성할 레코드 수
     * @return 레코드 컬렉션
     */
    public static LinkedList<String> stringMake(int recordCount) {
        int seq = 0;
        LinkedList<String> records = new LinkedList<>();

        while (seq < recordCount) {
            records.add("record_" + seq);
            seq++;
        }
        return records;
    }

    /**
     * 테스트 키밸류 레코드 생성기
     * 0 ~ recordCount까지 연속된 레코드 생성
     *
     * @param recordCount
     * @return
     */
    public static LinkedList<SimplePairRecord<String, String>> pairMake(int recordCount) {
        int seq = 0;
        LinkedList<SimplePairRecord<String, String>> records = new LinkedList<>();

        while (seq < recordCount) {
            records.add(new SimplePairRecord("key_" + seq, "value_" + seq));
            seq++;
        }
        return records;
    }

    /**
     * 테스트 로깅
     *
     * @param testName 테스트 네임
     */
    public static void test(String testName) {
        System.out.println(">> " + testName + "\n");
    }

    /**
     * 테스트 로깅
     *
     * @param testName 테스트 네임
     * @param result   테스트 결과
     */
    public static void test(String testName, boolean result) {
        if (result) {
            System.out.println(testName + " : success\n");
        } else
            System.out.println(testName + " : fail\n");
    }

    /**
     * Load Properties
     *
     * @param propFile
     * @return
     */
    public static Properties readProp(String propFile) {
        Properties prop = new Properties();
        InputStream defaultInput = null;
        InputStream addInput = null;

        try {
            defaultInput = new FileInputStream("conf/kafka_default.properties");
            addInput = new FileInputStream(propFile);
            prop.load(defaultInput);
            prop.load(addInput);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (defaultInput != null) {
                try {
                    defaultInput.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    addInput.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return prop;
    }

    public static void waitSec(int timeSec) {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 카프카 저장소 확인 템플릿
     * @param kafkaSource
     */
    public static void kafkaSourceTestTemplate(KafkaSource kafkaSource) {
        final long testEventID1 = -7656596631385128008L;
        final String testEventTimestamp1 = "timeStamp_-7656596631385128008";
        final String testServiceCode1 = "serviceCode_-7656596631385128008";
        final String testEventContext1 = "EventContext_-7656596631385128008";
        final String testKey1 = Long.toString(testEventID1);
        final String testValue1 = testEventID1 + "," + testEventTimestamp1 + "," + testServiceCode1 + "," + testEventContext1;

        String testName = null;
        boolean result = true;

        SimplePairRecord<String, String> testPairRecord1 = new SimplePairRecord<>(testKey1, testValue1);

        // 쓰기
        result = kafkaSource.write(testPairRecord1);
        test(">> " + kafkaSource.getTopic() + ", kafka source write. ", result);

        // 읽기
        ConcurrentLinkedQueue<SimplePairRecord<String, String>> list = kafkaSource.consume(1);
        try {
            if(list.size() <= 0) {
                result = false;
            } else {
                result = testPairRecord1.equal(list.poll());
            }
        } catch (Exception e) {
            result = false;
            System.out.println(">> " + kafkaSource.getTopic() + ", kafka consume exception, consume count : " + list.size());
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

        // 쓰기/읽기
        int count = 0;
        for (SimplePairRecord<String, String> simplePairRecord : pairMake(100)) {
            if(kafkaSource.write(simplePairRecord)) count++;
        }
        test(">> " + kafkaSource.getTopic() + ", kafka source write 100", count == 100);

        waitSec(1);
        test(">> " + kafkaSource.getTopic() + ", kafka source remain 100", kafkaSource.size() == 100);
        waitSec(1);

        test(">> " + kafkaSource.getTopic() + ", kafka source read 100", kafkaSource.consume(1).size() == 100);
    }
}
