package com.geosot.javademo.leetcode.thread;

import java.util.concurrent.*;

public class ThreadPool {

    public static void main(String[] args) {

        // 创建线程池对象
        // 核心线程配置数：若是计算密集型任务，则配置成CPU核数 + 1
        // 核心线程配置数：若是I/O密集型任务，则配置成CPU核数 * 2
        ExecutorService pool = new ThreadPoolExecutor(3,5,8, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(4), Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());

        pool.submit(()->{

        });
    }
}
