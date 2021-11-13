package com.homestead.redis;

import com.homestead.BaseTest;
import com.homestead.utils.DistributedLockUtils;
import com.homestead.utils.result.LockResult;
import org.junit.Test;

/**
 * @author HanBin_Yang
 * @since 2021/11/11 21:52
 */
public class RedisTest extends BaseTest {

    @Test
    public void lockTest() {
        LockResult<Void> lockResult = DistributedLockUtils.executeTryLock("homestead:lock:test", () -> System.out.println("true = " + true));
        if (lockResult.isFailed()) {
            throw new RuntimeException("获取分布式锁失败");
        }
    }
}
