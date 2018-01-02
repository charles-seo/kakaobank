package com.kakaobank.etl;

import com.kakaobank.source.Source;
import com.kakaobank.util.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * 단순히 한쪽 저장소에서 다른쪽 저장소로 옮기는 전송 객체
 * 코어수를 최대 스레드풀로 설정하여 병렬로 처리
 *
 * 각 스레드가 데이터를 읽어 들이는 시간과 전체 스레드 수행시간 제한 값을 입력 받음
 *
 *
 * @param <T>
 */
public class SimpleTransfer<T> {
    private static final Logger logger = LoggerFactory.getLogger("SimpleTransfer");

    private Source<T> fromSource;
    private Source<T> toSource;
    private static final int maxThread = Runtime.getRuntime().availableProcessors() + 1;

    public SimpleTransfer(Source fromSource, Source toSource) {
        this.fromSource = fromSource;
        this.toSource = toSource;
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
        // 최대 스레드 수 만큼 작업을 생성하여 작업 리스트 생성
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
                logger.fine("blocking job before get return.");
                if(booleanFuture.get() == false) result = false;
                if(booleanFuture.isDone() == false) result = false;
                logger.fine("future is done : " + booleanFuture.isDone());
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
            result = false;
        } catch (ExecutionException e) {
            e.printStackTrace();
            result = false;
        } finally {
            executorPool.shutdown();
        }
        return result;
    }

    /**
     * 원소스에서 제한 시간만큼 읽어 들인다음 목적지 소스로 레코드를 저장
     *
     * @param intervalTimeSec
     * @return
     */
    private Callable<Boolean> getTask(int intervalTimeSec) {
        return () -> toSource.write(fromSource.consume(intervalTimeSec).iterator());
    }
}
