package com.kakaobank.source;

import com.kakaobank.util.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * 메모리큐를 사용하는 단순 데이터 저장 객체
 *
 * @param <T>
 */
public class SimpleSource<T> implements Source<T> {
    private static final Logger logger = LoggerFactory.getLogger("SimpleSource");
    final private BlockingQueue<T> queue = new LinkedBlockingQueue<>();

    /**
     * timeSec만큼 시간을 소비하며 큐안의 레코드를 꺼내옴
     *
     * @param timeSec
     * @return
     */
    @Override
    public BlockingQueue<T> consume(int timeSec) {
        final BlockingQueue<T> result = new LinkedBlockingQueue<>();

        long start = System.currentTimeMillis();
        long threshold = start + ((long) timeSec) * 1000L;

        logger.fine("simple source consume " + timeSec + " sec");
        while (true) {
            if(System.currentTimeMillis() >= threshold) {
                break;
            }
            else {
                try {
                    T record = queue.poll(timeSec, TimeUnit.SECONDS);
                    if(record != null) {
                        result.add(record);
                    }
                } catch (InterruptedException e) {
                    logger.severe("simple source queue opll exception");
                    logger.severe(e.getMessage());
                    e.printStackTrace();
                }
            }
        }

        logger.fine("simple source consume record count : " + result.size());
        return result;
    }

    /**
     * SimpeSource에만 있는 메소드로
     * 큐에서 레코드를 하나 꺼냄
     *
     * @return
     */
    public T read() {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            logger.severe("sime source queue take exception");
            logger.severe(e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 큐에 한개의 레코드를 저장
     *
     * @param record
     * @return
     */
    @Override
    public boolean write(T record) {
        return queue.add(record);
    }

    /**
     * 큐에 다수의 레코드를 저장
     *
     * @param records
     * @return
     */
    @Override
    public boolean write(Iterator<T> records) {
        boolean result = true;

        while(records.hasNext()) {
            if(queue.add(records.next())) result = false;
        }

        return result;
    }

    /**
     * 현재 큐에 저장된 레코드의 수를 구합
     *
     * @return
     */
    @Override
    public long size() { return queue.size(); }

    @Override
    public boolean isEmpty() {
        if(size() <= 0) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean isNotEmpty() {
        if(size() > 0) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() {}
}