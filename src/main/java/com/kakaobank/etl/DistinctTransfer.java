package com.kakaobank.etl;

import com.kakaobank.source.Source;
import com.kakaobank.util.LoggerFactory;

import java.util.Collection;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * 출발지 저장소에서 데이터를 읽어 들여 중복을 제고하고 목적지 저장소에 저장
 *
 * 출발지 저장소의 레코드를 해쉬테이블을 이용하여 카운팅
 *
 * 최종적으로 hashmap의 키를 가져와 목적지에 저장
 *
 * @param <T>
 */
public class DistinctTransfer<T> {
    private static final Logger logger = LoggerFactory.getLogger("DistinctTransfer");

    private Source<T> fromSource;
    private Source<T> toSource;
    private ConcurrentHashMap<T, Long> distinctStore;
    private static final int maxThread = Runtime.getRuntime().availableProcessors() + 1;


    public DistinctTransfer(Source fromSource, Source toSource) {
        this.fromSource = fromSource;
        this.toSource = toSource;
        this.distinctStore = new ConcurrentHashMap<>();
    }

    /**
     * 실행 메소드
     *
     * @param intervalTimeSec 소비에 사용할 시간
     * @param maxTimeSec 전체 작업 제한 시간
     * @return
     */
    public boolean run(int intervalTimeSec, int maxTimeSec) {
        ExecutorService executorPool = Executors.newFixedThreadPool(maxThread);
        Collection<Callable<Boolean>> tasks = new LinkedList<>();

        boolean result = true;
        int processRecordCounter = 0;

        // 최대 스레드 수 만큼 작업 리스트 생성
        int i = 0;
        while (i < maxThread) {
            tasks.add(getTask(intervalTimeSec));
            i++;
        }

        try {
            // 작업 리스트를 스레드풀에 제한 시간과 함께 전달
            logger.fine("invoke job.");
            List<Future<Boolean>> futureList = executorPool.invokeAll(tasks, maxTimeSec, TimeUnit.SECONDS);

            for (Future<Boolean> booleanFuture : futureList) {
                logger.fine("blocking job until return.");
                if(booleanFuture.get() != true) result = false;
                logger.fine("future is done : " + booleanFuture.isDone());
            }
            logger.fine("process record count : " + processRecordCounter);

            // 작업이 끝나길 기다린 후 해쉬 테이블의 키를 가져와 목적지 저장소에 저장 및 해쉬 테이블 초기화
           synchronized (distinctStore) {
               Enumeration<T> distinctRecords = distinctStore.keys();

               while(distinctRecords.hasMoreElements()) {
                   toSource.write(distinctRecords.nextElement());
               }

               distinctStore.clear();
           }
        } catch (InterruptedException | ExecutionException e) {
            logger.severe(e.getMessage());
            e.printStackTrace();
            result = false;
        } finally {
            executorPool.shutdown();
        }
        return result;
    }

    /**
     * 목적지 저장소의 데이터를 읽어 중복 제거용 해쉬테이블에 저장하며 카운트를 계산
     *
     * @param intervalTimeSec
     * @return
     */
    private Callable<Boolean> getTask(int intervalTimeSec) {
        return () -> {
            for (T record : fromSource.consume(intervalTimeSec)) {
                distinctStore.put(record, 0L);
                //distinctStore.compute(record, (k, v) -> v == null ? 1 : v + 1);
            }
            return true;
        };
    }
}
