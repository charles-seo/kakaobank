package com.kakaobank.etl;

import com.kakaobank.source.Source;
import com.kakaobank.util.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SingleSimpleTransfer<T> {
    private static final Logger logger = LoggerFactory.getLogger("SimpleTransfer");

    final private Source<T> fromSource;
    final private Source<T> toSource;

    public SingleSimpleTransfer(Source<T> fromSource, Source<T> toSource) {
        this.fromSource = fromSource;
        this.toSource = toSource;
    }

    public void trans(int IntervalTimeSec, int timeSec) {
        long start = System.currentTimeMillis();
        long threshold = start + ((long) timeSec) * 1000L;

        while (true) {
            if (System.currentTimeMillis() >= threshold) {
                break;
            }
            toSource.write(fromSource.consume(IntervalTimeSec).iterator());
        }
    }

    public void distinctTrans(int intervalTimeSec, int timeSec) {
        Set<T> buffer = new HashSet<>();

        long start = System.currentTimeMillis();
        long threshold = start + ((long) timeSec) * 1000L;

        while (true) {
            if (System.currentTimeMillis() >= threshold) {
                break;
            }
            Stream<T> records = StreamSupport.stream(fromSource.consume(intervalTimeSec).spliterator(), false);

            records.distinct().forEach((T record) -> {
                buffer.add(record);
            });
        }
        toSource.write(buffer.iterator());
    }
}