package com.hmdp.service;

public interface ILock {
    boolean tryLock(long timeoutsec);

    void unlock();
}
