package com.geosot.javademo.leetcode.thread;

public class Quqian {
    public static void main(String[] args) {
        Account account = new Account("10010", 10000);

        Thread t1 = new MyThread("小红", account);
        Thread t2 = new MyThread("小明", account);
        t1.start();
        t2.start();
    }
}
