package com.homestead.utils.lock;

import cn.hutool.core.thread.ThreadUtil;
import com.homestead.utils.SpringBeanFactory;
import com.homestead.utils.function.VoidSupplier;
import com.homestead.utils.result.LockResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.util.StopWatch;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 分布式锁工具类
 * @author HanBin_Yang
 * @since 2021/11/17 10:40
 */
public class DistributedLock2Utils {
    private static final Logger log = LoggerFactory.getLogger("com.homestead.utils.redis.distributed.lock");

    private static final RedisTemplate<Object, Object> redisTemplate = SpringBeanFactory.getBean("redisTemplate", RedisTemplate.class);

    private static final DefaultRedisScript<Long> lockScript = new DefaultRedisScript<>();
    private static final DefaultRedisScript<Object> unlockScript = new DefaultRedisScript<>();

    private static final UUID uuid = UUID.randomUUID();

    // 默认redis失效时间 （秒）
    private static final int DEFAULT_EXPIRE_SECONDS = 120;

    private static final Long UNLOCK_MESSAGE = 0L;

    private static final long SLEEP_TIME = 800;
    private static final long MIN_SLEEP_TIME = 30;
    private static final RedisMessageListenerContainer container = SpringBeanFactory.getBean("redisMessageListenerContainer", RedisMessageListenerContainer.class);
    static {
        lockScript.setScriptSource(new ResourceScriptSource(new PathMatchingResourcePatternResolver().getResource("classpath:script/lock.lua")));
        lockScript.setResultType(Long.class);
        unlockScript.setScriptSource(new ResourceScriptSource(new PathMatchingResourcePatternResolver().getResource("classpath:script/unlock.lua")));
        boolean running = container.isRunning();
        System.out.println("container running = " + running);
    }

    /**
     * 尝试获取锁，获取不到 LockResult.isFailed = true
     * @param lockKey redis key
     * @param supplier 业务代码
     * @param <V> 返回值类型
     * @return RedisLockResult
     */
    public static <V> LockResult<V> executeTryLock(String lockKey, Supplier<V> supplier) {
        return executeTryLock(lockKey, 0, DEFAULT_EXPIRE_SECONDS, TimeUnit.SECONDS, supplier);
    }
    public static LockResult<Void> executeTryLock(String lockKey, VoidSupplier supplier) {
        return executeTryLock(lockKey, 0, DEFAULT_EXPIRE_SECONDS, TimeUnit.SECONDS, supplier);
    }

    public static <V> LockResult<V> executeTryLock(String lockKey, long waitTime, TimeUnit timeUnit, Supplier<V> supplier) {
        return executeTryLock(lockKey, waitTime, timeUnit.convert(DEFAULT_EXPIRE_SECONDS, TimeUnit.SECONDS), timeUnit, supplier);
    }
    public static LockResult<Void> executeTryLock(String lockKey, long waitTime, TimeUnit timeUnit, VoidSupplier supplier) {
        return executeTryLock(lockKey, waitTime, timeUnit.convert(DEFAULT_EXPIRE_SECONDS, TimeUnit.SECONDS), timeUnit, supplier);
    }

    public static <V> V executeLock(String lockKey, Supplier<V> supplier) {
        // 最多锁住1小时
        long expireSeconds = 60 * 60;
        return executeLock(lockKey, expireSeconds, TimeUnit.SECONDS, supplier);
    }
    public static void executeLock(String lockKey, VoidSupplier supplier) {
        // 最多锁住1小时
        long expireSeconds = 60 * 60;
        executeLock(lockKey, expireSeconds, TimeUnit.SECONDS, supplier);
    }

    /**
     * @param lockKey    redis key
     * @param waitTime   等待时间 0不等待
     * @param expireTime redis过期时间
     * @param timeUnit   时间单位
     * @param supplier   业务代码提供者
     * @param <V>        返回类型
     * @return LockResult包装对象
     */
    public static <V> LockResult<V> executeTryLock(String lockKey, long waitTime, long expireTime, TimeUnit timeUnit, Supplier<V> supplier) {
        return doExecuteTryLock(lockKey, waitTime, expireTime, timeUnit, supplier);
    }
    public static LockResult<Void> executeTryLock(String lockKey, long waitTime, long expireTime, TimeUnit timeUnit, VoidSupplier supplier) {
        return doExecuteTryLock(lockKey, waitTime, expireTime, timeUnit, supplier);
    }

    /**
     * @param lockKey redis key
     * @param expireTime redis失效时间
     * @param timeUnit 失效时间单位
     * @param supplier java supplier
     * @param <V> 业务代码返回值类型
     * @return 业务代码返回类型
     */
    public static <V> V executeLock(String lockKey, long expireTime, TimeUnit timeUnit, Supplier<V> supplier) {
        return doExecuteLock(lockKey, expireTime, timeUnit, supplier);
    }
    public static void executeLock(String lockKey, long expireTime, TimeUnit timeUnit, VoidSupplier supplier) {
        doExecuteLock(lockKey, expireTime, timeUnit, supplier);
    }

    private static <V> LockResult<V> doExecuteTryLock(String lockKey, long waitTime, long expireTime, TimeUnit timeUnit, Object supplier) {
        boolean lockFlag;
        try {
            lockFlag = tryLock(lockKey, waitTime, expireTime, timeUnit);
        } catch (Exception e) {
            log.error("尝试获取redis锁异常：message={}, lockKey={}", e.getMessage(), lockKey, e);
            throw new RuntimeException("尝试获取redis锁异常, lockKey=" + lockKey, e);
        }
        if (!lockFlag) {
            return LockResult.failed();
        }
        StopWatch stopWatch = new StopWatch("doExecuteTryLock-lockKey");
        try {
            stopWatch.start(lockKey);
            if (supplier instanceof Supplier){
                @SuppressWarnings("unchecked")
                V result = ((Supplier<V>) supplier).get();
                return LockResult.success(result);
            }
            ((VoidSupplier) supplier).get();
        } finally {
            try {
                unlock(lockKey);
            } catch (Exception e) {
                stopWatch.stop();
                log.error(stopWatch.prettyPrint());
                log.error("doExecuteTryLock解锁失败: required expireTime={}ms", timeUnit.toMillis(expireTime), e);
            }
        }
        return LockResult.success(null);
    }

    private static <V> V doExecuteLock(String lockKey, long expireTime, TimeUnit timeUnit, Object supplier) {
        try {
            // redis过期剩余时间
            Long redisExpTime = null;
            long sleepTime = SLEEP_TIME;
            int retry = 10;
            // 自旋计数
            long circleCount = 0;
            while ((redisExpTime = tryLock(lockKey, expireTime, timeUnit, retry)) != null) {
                if (redisExpTime < sleepTime) {
                    sleepTime = redisExpTime;
                }
                ThreadUtil.sleep(sleepTime);
                circleCount++;
                if (sleepTime * circleCount > 1000 * 60 * 60) {
                    log.warn("doExecuteLock wait too long: circleCount={}, lockKey={}", circleCount, lockKey);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("获取redis锁异常, lockKey=" + lockKey, e);
        }
        StopWatch stopWatch = new StopWatch("doExecuteLock-lockKey");
        try {
            stopWatch.start(lockKey);
            if (supplier instanceof Supplier){
                @SuppressWarnings("unchecked")
                V result = ((Supplier<V>) supplier).get();
                return result;
            }
            ((VoidSupplier) supplier).get();
        } finally {
            try {
                unlock(lockKey);
            } catch (Exception e) {
                stopWatch.stop();
                log.error(stopWatch.prettyPrint());
                log.error("doExecuteLock解锁失败: required expireTime={}ms", timeUnit.toMillis(expireTime), e);
            }
        }
        return null;
    }


    public static boolean tryLock(String lockKey, long waitTime, long leaseTime, TimeUnit unit) {
        long time = unit.toMillis(waitTime);
        long current = System.currentTimeMillis();
        Long ttl = tryAcquire(lockKey, leaseTime, unit);
        if (ttl == null) {
            return true;
        }
        time -= System.currentTimeMillis() - current;
        if (time <= 0) {
            return false;
        }
        current = System.currentTimeMillis();
        Semaphore semaphore = new Semaphore(0);
        UnlockListener listener = new UnlockListener(semaphore);
        PatternTopic topic = new PatternTopic(getChannelName(lockKey));
        subscribe(listener, topic);
        try {
            // wait subscribe
            Thread.sleep(20);
        } catch (InterruptedException ignored) {
        }
        try {
            time -= System.currentTimeMillis() - current;
            if (time <= 0) {
                return false;
            }
            while (true) {
                long currentTime = System.currentTimeMillis();
                ttl = tryAcquire(lockKey, leaseTime, unit);
                if (ttl == null) {
                    return true;
                }
                time -= System.currentTimeMillis() - currentTime;
                if (time <= 0) {
                    return false;
                }
                currentTime = System.currentTimeMillis();
                if (ttl >= 0 && ttl < time) {
                    semaphore.tryAcquire(ttl, TimeUnit.MILLISECONDS);
                } else {
                    semaphore.tryAcquire(time, TimeUnit.MILLISECONDS);
                }
                time -= System.currentTimeMillis() - currentTime;
                if (time <= 0) {
                    return false;
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unsubscribe(listener, topic);
        }
    }

    private static synchronized void subscribe(MessageListener listener, Topic topic) {
        container.addMessageListener(listener, topic);
    }

    private static synchronized void unsubscribe(MessageListener listener, Topic topic) {
        container.removeMessageListener(listener, topic);
    }

    private static Long tryAcquire(String lockKey, long leaseTime, TimeUnit timeUnit) {
        return redisTemplate.execute(
                lockScript,
                Collections.singletonList(lockKey),
                timeUnit.toMillis(leaseTime),
                uuid + ":" + Thread.currentThread().getId());
    }

    private static Long tryLock(String lockKey, long releaseTime, TimeUnit timeUnit, int retry) {
        Long result = tryLock(lockKey, releaseTime, timeUnit);
        if (result == null) {
            return result;
        }

        for (int i = 0; i < retry; i++) {
            result = tryLock(lockKey, releaseTime, timeUnit);
            if (result == null) {
                return result;
            }
            ThreadUtil.sleep(MIN_SLEEP_TIME);
        }

        return result;
    }

    private static Long tryLock(String lockKey, long releaseTime, TimeUnit timeUnit) {
        return redisTemplate.execute(
                lockScript,
                Collections.singletonList(lockKey),
                timeUnit.toMillis(releaseTime),
                uuid + ":" + Thread.currentThread().getId());
    }

    private static void unlock(String lockKey) {
        redisTemplate.execute(
                unlockScript,
                Arrays.<Object>asList(lockKey, getChannelName(lockKey)), UNLOCK_MESSAGE, DEFAULT_EXPIRE_SECONDS,
                uuid + ":" + Thread.currentThread().getId());
    }

    private static String getChannelName(String lockKey) {
        return prefixName(lockKey);
    }

    private static String prefixName(String name) {
        String prefix = "redis_lock__channel";
        if (name.contains("{")) {
            return prefix + ":" + name;
        }
        return prefix + ":{" + name + "}";
    }
}
