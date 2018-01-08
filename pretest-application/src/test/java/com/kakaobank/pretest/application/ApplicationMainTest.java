package com.kakaobank.pretest.application;

import com.kakaobank.pretest.framework.source.KafkaSource;
import com.kakaobank.pretest.framework.util.KafkaFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

/**
 * 어플리케이션 메인 테스트 케이스
 *
 * 어플리케이션 실행 시간 동안
 * 하나의 스레드가 1초마다 100개의 레코드 생성하여 from topic에 저장
 * 이후 1초마다 생성되는 레코드의 값은 동일
 *
 * 테스트 시간 절약을 위해 각 pipe 단계별 시간은 30 60 120 180으로 설정
 * 소스 코드 AppMain에 하드 코딩
 *
 * 이후 프로덕션에서는 실행될 리소스에 따라
 *
 */
public class ApplicationMainTest {
    @Before
    public void setUp() throws Exception {
        final KafkaSource<String, String> fromKafkaSource = new KafkaSource<>("test-application-from-topic", new KafkaFactory<>(this.readProps("conf/test/test_from_kafka_client.properties")));
        final KafkaSource<String, String> toKafkaSource = new KafkaSource<>("test-application-to-topic", new KafkaFactory<>(this.readProps("conf/test/test_to_kafka_client.properties")));

        // 토픽에 레코드가 남아 있다면 모두 소진
        while(true) {
            List readRecords = fromKafkaSource.read(3);
            if(readRecords.size() <= 0) break;
        }
        while(true) {
            List readRecords = toKafkaSource.read(3);
            if(readRecords.size() <= 0) break;
        }
    }

    /**
     * 케이스 1
     *
     * pipe step은 10초 -> 30초 -> 60초 순으로 진행
     * 어플리케이션은 60초동안 실행
     *
     * from 소스에 동일한 100건의 레코드 셋을 1초 간격으로 세번 생성
     *
     * 최종 목적지에는 중복이 제거된 레코드셋 1건이 저장되어야 성공
     */
    @Test
    public void test1MinScenario() {
        final ExecutorService pool = Executors.newFixedThreadPool(2);
        final KafkaSource<String, String> fromKafkaSource = new KafkaSource<>("test-application-from-topic", new KafkaFactory<>(this.readProps("conf/test/test_from_kafka_client.properties")));
        final KafkaSource<String, String> toKafkaSource = new KafkaSource<>("test-application-to-topic", new KafkaFactory<>(this.readProps("conf/test/test_to_kafka_client.properties")));

        final List sampleList = TestHelper.getInstance().createSampleEventRecord(100);
        final String[] args = {"test"};


        pool.submit(() -> {
          for(int i = 0; i < 3; i++) {
              fromKafkaSource.asyncWrite(sampleList);
              try {
                  Thread.sleep(1000L);
              } catch (InterruptedException e) {
                  e.printStackTrace();
              }
          }
        });

        AppMain.main(args);
        assertEquals(toKafkaSource.read(3).size(), 100);
    }

    /**
     * 케이스 2
     *
     * pipe step은 10초 -> 30초 -> 60초 순으로 진행
     * 어플리케이션은 120초동안 실행
     *
     * from 소스에 동일한 100건의 레코드 셋을 1초 간격으로 세번 생성
     * 그리고 1분 후에 100건의 레코드셋을 1초 간격으로 세번 생성
     *
     * 최종 목적지에는 중복이 제거된 레코드셋 2건이 저장되어야 성공
     *
     */
    @Test
    public void test2MinScenario() {
        final ExecutorService pool = Executors.newFixedThreadPool(2);
        final KafkaSource<String, String> fromKafkaSource = new KafkaSource<>("test-application-from-topic", new KafkaFactory<>(this.readProps("conf/test/test_from_kafka_client.properties")));
        final KafkaSource<String, String> toKafkaSource = new KafkaSource<>("test-application-to-topic", new KafkaFactory<>(this.readProps("conf/test/test_to_kafka_client.properties")));

        final List sampleList = TestHelper.getInstance().createSampleEventRecord(100);
        final String[] args = {"test"};


        pool.submit(() -> {
            // 테스트 데이터 저장
            for(int i = 0; i < 3; i++) {
                fromKafkaSource.asyncWrite(sampleList);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            // 1분 대기
            try {
                Thread.sleep(60000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // 테스트 데이터 저장
            for(int i = 0; i < 3; i++) {
                fromKafkaSource.asyncWrite(sampleList);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        AppMain.main(args);
        assertEquals(toKafkaSource.read(3).size(), 200);

    }

    public Properties readProps(String propFile) {
        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream(propFile);
            prop.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return prop;
    }
}