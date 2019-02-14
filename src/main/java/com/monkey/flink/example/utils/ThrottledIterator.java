package com.monkey.flink.example.utils;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Objects;

/**
 * ThrottledIterator
 *
 * @author yong.han
 * 2019/2/14
 */
public class ThrottledIterator<T> implements Iterator<T>, Serializable {


    private final Iterator<T> source;

    private final long sleepBatchSize;
    private final long sleepBatchTime;

    private long lastBatchCheckTime;
    private long num;

    public ThrottledIterator(Iterator<T> source, long elementsPerSecond) {
        this.source = Objects.requireNonNull(source);

        if (!(source instanceof Serializable)) {
            throw new IllegalArgumentException("source must be java.io.Serializable");
        }

        if (elementsPerSecond >= 100) {
            this.sleepBatchSize = elementsPerSecond / 20;
            this.sleepBatchTime = 50;
        } else if (elementsPerSecond >= 1) {
            this.sleepBatchSize = 1;
            this.sleepBatchTime = 1000 / elementsPerSecond;
        } else {
            throw new IllegalArgumentException("elements per seconds must be positive and not zero");
        }


    }



    @Override
    public boolean hasNext() {
        return source.hasNext();
    }

    @Override
    public T next() {
        if (lastBatchCheckTime > 0) {
            if (++num >= sleepBatchSize) {
                num = 0;

                final long now = System.currentTimeMillis();
                final long elapsed = now - lastBatchCheckTime;

                if (elapsed < sleepBatchTime) {
                    try {
                        Thread.sleep(sleepBatchTime - elapsed);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                }
                lastBatchCheckTime = now;
            }
        } else {
            lastBatchCheckTime = System.currentTimeMillis();
        }
        return source.next();
    }
}
