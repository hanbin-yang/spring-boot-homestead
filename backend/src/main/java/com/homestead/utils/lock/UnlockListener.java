package com.homestead.utils.lock;

import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;

import java.util.concurrent.Semaphore;

/**
 * @author HanBin_Yang
 * @since 2021/11/17 20:15
 */
public class UnlockListener implements MessageListener {
    private final Semaphore semaphore;

    public UnlockListener(Semaphore semaphore) {
        this.semaphore = semaphore;
    }

    @Override
    public void onMessage(Message message, byte[] bytes) {
        semaphore.release();
       // System.out.println("通知Mesage semaphore message= " + new String(bytes) + ", sem=" + semaphore);
    }


    public Semaphore getSemaphore() {
        return semaphore;
    }
}
