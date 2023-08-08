package com.geosot.javademo.leetcode.thread;

public class MyThread extends Thread{
    private Account account;
    public MyThread(String name,Account acc){
        super(name);
        this.account = acc;
    }

    @Override
    public void run() {
        account.qumoney(10000);
    }
}
