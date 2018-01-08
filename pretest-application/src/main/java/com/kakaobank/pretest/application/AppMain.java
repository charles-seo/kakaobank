package com.kakaobank.pretest.application;

import com.kakaobank.pretest.framework.source.KafkaSource;
import com.kakaobank.pretest.framework.etl.KafkaTransfer;
import com.kakaobank.pretest.application.etl.KafkaTransferWorker;
import com.kakaobank.pretest.framework.util.KafkaFactory;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * 10분 동안 from topic 에서 to topic으로 중복을 제거하고 이동하는 ETL Application
 *
 * 처리 프로세스
 * 1분 제거 -> pipe -> 3분 제거 -> pipe -> 5분 제거 -> pipe 10분 제거
 *
 * 각 과정당 사용하는 topic
 * fromTopic -> 1분동안 중복제거 -> fromTopic-1min
 * fromTopic-1min -> 3분동안 중복제거 -> fromTopic-3min
 * fromTopic-3min -> 5분동안 중복 제거 -> fromTopic-5min
 * fromTopic-5min -> 10분동안 중복 제거 -> toTopic
 */
public class AppMain {
    /**
     *
     * @param args
     *  0 : from topic
     *  1 : to topic
     *  2 : application 제한 시간
     */
    private static final Logger logger = Logger.getLogger(KafkaSource.class.getName());
    public static void main(String[] args) {
        final Properties appProps;

        // 1번째 입력 변수로 실행 모드를 판단
        // test 모드일 경우 conf/test의 테스트 어플리케이션 설정을 읽어 들임
        // live 모드일 경우 conf의 어플리케이션 설정을 읽어 들임
        if(args.length != 1) {
            logger.error("need 1 argument. (\'test\' or \'live\') : " + args.length);
            appProps = null;
            System.exit(1);
        } else if(args[0].equals("live")) {
            appProps = readProps("conf/application.properties");
        } else if(args[0].equals("test")) {
            appProps = readProps("conf/test/test-application.properties");
        } else {
            logger.error("incorrect argument. (\'test\' or \'live\') : " + args[0]);
            appProps = null;
            System.exit(1);
        }

        logger.info("kakaobank pretest application start.");
        logger.warn("read app props : " + appProps);

        // 설정팔일로부터 kafka 설정을 읽어 들임
        final Properties fromKafkaProp = readProps(appProps.getProperty("fromKafkaPropsFile"));
        final Properties toKafkaProp = readProps(appProps.getProperty("toKafkaPropsFile"));
        logger.warn("read from kafkaProps : " + fromKafkaProp);
        logger.warn("read to kafkaProps : " + toKafkaProp);

        // 설정파일로 부터 토픽을 설정
        final String fromTopic = appProps.getProperty("fromTopic");
        final String toTopic = appProps.getProperty("toTopic");
        logger.warn("from topic : " + fromTopic);
        logger.warn("to topic : " + toTopic);

        // 어플리케이션 제한 시간을 설정
        final int appLimitTimeSec = Integer.valueOf(appProps.getProperty("applicationLimitTime"));
        final long appLimitTImeMillis = System.currentTimeMillis() + appLimitTimeSec * 1000;
        logger.warn("limit timeSec : " + appLimitTimeSec);

        // ETL 서브 잡들의 반속 실행 주기를 설정
        final int taskSubTimeInterval = Integer.valueOf(appProps.getProperty("taskSubTimeInterval"));
        logger.warn("etl task sub task time interval : " + taskSubTimeInterval);

        // TimeSec 베이스 파이프 라인을 설정
        // 설정파일의 파이프 라인을 스트링으로 읽어 드림
        final String[] pipeTimeSteps = appProps.getProperty("pipeTimeSecStep").split(",");
        logger.warn("pipe time step : " + pipeTimeSteps);
        logger.warn("pipe time step size : " + pipeTimeSteps.length);

        // 스트링을 초단위 정수로 변환하면서 스탭을 검증
        final ArrayList<Integer> pipeTimeSecSteps = new ArrayList<>(pipeTimeSteps.length);
        for (String pipeTimeStep : pipeTimeSteps) {
            pipeTimeSecSteps.add(Integer.valueOf(pipeTimeStep));
        }
        logger.warn("pipe time step : " + pipeTimeSecSteps);

        // 익스큐터 서비스 각 스탭을 독립적인 싱글스레드 풀로 진행하기 위한 Future리스트와 리소스 정리를 위한 Pool리스트
        // 그리고 kafka ETL 워커들의 각 설정을 미리 작성하여 리스트업 시킴

        final ArrayList<Future<Boolean>> futureList = new ArrayList<>(pipeTimeSteps.length + 1);
        final ArrayList<ExecutorService> poolList = new ArrayList<>(pipeTimeSteps.length + 1);
        final ArrayList<KafkaTransfer> transferPipeList = new ArrayList(pipeTimeSteps.length + 1);
        logger.warn("futureList size : " + futureList.size());
        logger.warn("poolList size : " + poolList.size());
        logger.warn("transferPipeList size : " + transferPipeList.size());


        final KafkaTransferWorker kafkaTranferWorker = new KafkaTransferWorker(fromKafkaProp, toKafkaProp);

        // 출발지와 각 스탭의 토픽들을 설정
        // 토픽의 포스트 픽스를 이용해 토픽들의 파이프라인을 구성
        String beforePipeToTopicPostFix;
        String pipeToTopicPostFix = "-" + Integer.valueOf(pipeTimeSteps[0]) + "Sec";

        // 첫 파이프라인 설정
        transferPipeList.add(kafkaTranferWorker.work(fromTopic, fromTopic + pipeToTopicPostFix));
        logger.warn("first pipe line " + "from topic " + fromTopic + " to topic " + fromTopic + pipeToTopicPostFix);

        for(int i = 1; i < pipeTimeSteps.length - 1; i++) {
            beforePipeToTopicPostFix = pipeToTopicPostFix;
            pipeToTopicPostFix = "-" + Integer.valueOf(pipeTimeSteps[i]) + "Sec";
            transferPipeList.add(kafkaTranferWorker.work(fromTopic + beforePipeToTopicPostFix, fromTopic + pipeToTopicPostFix));
            logger.warn("pipe line " + "from topic " + fromTopic + beforePipeToTopicPostFix + " to topic " + fromTopic + pipeToTopicPostFix);
        }

        // 마지막 파이프라인 설정
        transferPipeList.add(kafkaTranferWorker.work(fromTopic + pipeToTopicPostFix, toTopic));
        logger.warn("last pipe line " + "from topic " + fromTopic + pipeToTopicPostFix + " to topic " +toTopic);

        // ETL 워커들을 싱글 스레드로 실행시키고 스레드풀을 리스트 업
        for(int i= 0; i < pipeTimeSteps.length; i ++) {
            final ExecutorService pool = Executors.newSingleThreadExecutor();
            futureList.add(pool.submit(createTask(transferPipeList.get(i), appLimitTImeMillis, pipeTimeSecSteps.get(i), taskSubTimeInterval)));
            poolList.add(pool);
            logger.warn("add task. pipeTimeSecSteps : " + pipeTimeSecSteps.get(i));
            logger.warn("add task. futureList : " + futureList.size());
            logger.warn("add pool. poolList : " + poolList.size());
        }

        // 모든 스레드가 끝났는지 확인
        while(true) {
            boolean isDone = true;
            for(Future<Boolean> future : futureList) {
                if(future.isDone() == false) isDone = false;
            }
            if(isDone) break;
        }

        // 모든 스레드가 끝났다면 결과 값을 취합
        boolean result = true;
        for(Future<Boolean> future : futureList) {
            try {
                if (future.get() == false) {
                    result = false;
                }
            } catch (InterruptedException e) {
                logger.error("app main task stop by interrupt. ", e);
                e.printStackTrace();
            } catch (ExecutionException e) {
                logger.error("app main task stop by execution exception. ", e);
                e.printStackTrace();
            } finally {
                for (ExecutorService pool : poolList) {
                    if(pool.isShutdown() == false) {
                        pool.shutdown();
                    }
                }
            }
        }

        // 모든 스레드들의 결과 값을 보고 중간에 에러가 있었는지 확인
        if(result) {
            logger.info("application finished.");
        } else {
            logger.info("application complete with error");
        }

        //최종 결과를 mysql에 저장

    }

    /**
     * 병렬로 실행하기 위한 task 생성 메소드
     *
     * @param pipe etl프로세스가 설정된 KafkaTransfer 객체
     * @param appLimitTImeMillis task 총 실행 시간(밀리초)
     * @param taskLimitTimeSec task의 인터벌(Time window) 시간(초)
     * @param taskSubIntervalTimeSec task 안의 병렬 처리 스레드의 반복 인터벌 시간(초)(repeat interval)
     * @return
     */
    private static Callable<Boolean> createTask(final KafkaTransfer pipe, final long appLimitTImeMillis, final int taskLimitTimeSec, final int taskSubIntervalTimeSec) {
        if(appLimitTImeMillis > 0) {
            return () -> {
                boolean result = true;
                do {
                    if(pipe.distinctMove(taskLimitTimeSec, taskSubIntervalTimeSec) == false) {
                        result = false;
                    }
                } while(System.currentTimeMillis() < appLimitTImeMillis);
                return result;
            };
        } else {
            return () -> {
                while (true) {
                    if(pipe.distinctMove(taskLimitTimeSec, taskSubIntervalTimeSec) == false);
                }
            };
        }
    }

    /**
     * propterties 파일을 읽기 위한 메소드
     * @param propFile propterties 위치 및 파일명
     * @return prop 객체
     */
    private static Properties readProps(String propFile) {
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
