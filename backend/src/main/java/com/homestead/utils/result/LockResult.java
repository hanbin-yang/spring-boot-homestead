package com.homestead.utils.result;

/**
 * @author YangHanBin
 * @date 2021-03-16 11:09
 */
public class LockResult<V> {
    private boolean lockResult;

    private V obj;

    public LockResult() {
        this.lockResult = true;
    }

    public LockResult(boolean flag) {
        this.lockResult = flag;
    }

    public LockResult(V obj) {
        this.lockResult = true;
        this.obj = obj;
    }

    public LockResult(boolean lockResult, V obj) {
        this.lockResult = lockResult;
        this.obj = obj;
    }

    public static <V> LockResult<V> failed() {
        return new LockResult<>(false);
    }

    public static <V> LockResult<V> success(V obj) {
        return new LockResult<>(obj);
    }

    public boolean isFailed() {
        return !lockResult;
    }

    public V getObj() {
        return obj;
    }

    public void setObj(V obj) {
        this.obj = obj;
    }
}
