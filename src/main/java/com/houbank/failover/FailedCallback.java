package com.houbank.failover;

/**
 */
public interface FailedCallback<E> {
    void onFailed(E evt, Throwable throwable);
}