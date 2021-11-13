package com.homestead.utils;

import cn.hutool.core.thread.ThreadUtil;
import com.homestead.utils.function.VoidSupplier;
import com.homestead.utils.result.LockResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.util.StopWatch;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 分布式锁工具类
 * @author HanBin_Yang
 * @since 2021/11/17 10:40
 */
public class DistributedLockUtils {
    private static final Logger log = LoggerFactory.getLogger("com.homestead.utils.redis.distributed.lock");

    private static final StringRedisTemplate redisTemplate = SpringBeanFactory.getBean(StringRedisTemplate.class);

    private static final DefaultRedisScript<Long> lockScript = new DefaultRedisScript<>();
    private static final DefaultRedisScript<Object> unlockScript = new DefaultRedisScript<>();

    private static final UUID uuid = UUID.randomUUID();

    // 默认redis失效时间 （秒）
    private static final int DEFAULT_EXPIRE_SECONDS = 120;

    private static final long SLEEP_TIME = 800;

    static {
        lockScript.setScriptSource(new ResourceScriptSource(new PathMatchingResourcePatternResolver().getResource("classpath:script/lock.lua")));
        lockScript.setResultType(Long.class);
        unlockScript.setScriptSource(new ResourceScriptSource(new PathMatchingResourcePatternResolver().getResource("classpath:script/unlock.lua")));
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
        // redis过期剩余时间
        Long redisExpTime = null;
        try {
            long sleepTime = SLEEP_TIME;
            long waitTimeMills = timeUnit.toMillis(waitTime);
            // 自旋计数
            long circleCount = 0;
            while ((redisExpTime = tryLock(lockKey, expireTime, timeUnit)) != null) {
                if (waitTime <= 0) {
                    break;
                }
                if (redisExpTime < sleepTime) {
                    sleepTime = redisExpTime;
                }
                ThreadUtil.sleep(sleepTime);
                waitTimeMills -= sleepTime;
                // 已超时
                if (waitTimeMills <= 0) {
                    redisExpTime = tryLock(lockKey, expireTime, timeUnit);
                    break;
                }
                circleCount++;
                if (sleepTime * circleCount > 1000 * 60 * 60) {
                    log.warn("doExecuteTryLock锁自旋等待：circleCount={}, lockKey={}", circleCount, lockKey);
                }
            }
        } catch (Exception e) {
            log.error("尝试获取redis锁异常：message={}, lockKey={}", e.getMessage(), lockKey, e);
            throw new RuntimeException("尝试获取redis锁异常, lockKey=" + lockKey, e);
        }
        if (redisExpTime != null) {
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
            while ((redisExpTime = tryLock(lockKey, expireTime, timeUnit)) != null) {
                if (redisExpTime < sleepTime) {
                    sleepTime = redisExpTime;
                }
                ThreadUtil.sleep(sleepTime);
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
                Collections.singletonList(lockKey),
                uuid + ":" + Thread.currentThread().getId());
    }
}
