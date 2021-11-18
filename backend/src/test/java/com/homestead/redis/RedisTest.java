package com.homestead.redis;

import com.homestead.BaseTest;
import com.homestead.utils.DistributedLockUtils;

import com.homestead.utils.result.LockResult;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author HanBin_Yang
 * @since 2021/11/11 21:52
 */
public class RedisTest extends BaseTest {
/*    @Autowired
    private RedissonClient redissonClient;*/


    @Test
    public void lockTest() throws InterruptedException {
        ArrayList<Long> list = new ArrayList<>(500);
        int cirCount = 500;
        CountDownLatch countDownLatch = new CountDownLatch(cirCount);
        for (int i = 0; i < cirCount; i++) {
            new Thread(() -> {
                LockResult<Void> lockResult = DistributedLockUtils.executeTryLock("yhb:redis:test", 500, TimeUnit.MILLISECONDS, () -> {
                    //System.out.println("hello");

                    countDownLatch.countDown();
                    list.add(countDownLatch.getCount());
                });

                if (lockResult.isFailed()) {
                    System.out.println("获取锁失败: " + Thread.currentThread().getName());
                }
            }).start();
        }

        countDownLatch.await();
        System.out.println("list size=" + list.size());
    }


    /*@Test
    public void redissonLockTest() throws InterruptedException {
        String lockKey = "redisson:test";
        ArrayList<Integer> list = new ArrayList<>();

        int cirCount = 2;
        CountDownLatch countDownLatch = new CountDownLatch(cirCount);
        for (int i = 0; i < cirCount; i++) {
            new Thread(() -> {
                RLock lock = redissonClient.getLock(lockKey);
                try {
                    boolean b = lock.tryLock(120, 300, TimeUnit.SECONDS);
                    if (b) {
                        System.out.println("获取锁成功: " + Thread.currentThread().getName());
                    } else {
                        System.err.println("获取锁失败: " + Thread.currentThread().getName());
                    }
                    list.add(1);
                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }
            }).start();
        }

        countDownLatch.await();
        System.out.println("list size=" + list.size());
    }*/
}
