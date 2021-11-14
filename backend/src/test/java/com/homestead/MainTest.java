package com.homestead;

import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author HanBin_Yang
 * @since 2021/11/14 15:38
 */
public class MainTest {
    private final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1, 1, 100L, TimeUnit.SECONDS,new ArrayBlockingQueue<>(100));

    @Test
    public void theadTest() throws InterruptedException {

        Thread thread = new Thread(() -> {
            int a = 1;
            System.out.println("ok");
        });

        thread.setUncaughtExceptionHandler((t, e) -> {
            System.err.println("error: ");
            System.err.println("2my2 Thead: " + e.fillInStackTrace());
        });
        threadPoolExecutor.setThreadFactory(runnable -> thread);

        threadPoolExecutor.submit(thread);

        Thread.sleep(1000L);
    }
}
