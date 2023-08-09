package com.geosot.javademo.leetcode.thread;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Account {
    private String name;
    private double money;

    //方法三：Lock对象
    private final Lock lock = new ReentrantLock();

    public Account() {
    }

    public Account(String name, double money) {
        this.name = name;
        this.money = money;
    }

    // 方法二：同步方法 在方法的前面加synchronized关键词，这里也是用this作为锁
    // 同步代码块的范围小一点，性能要更好一点，同步方法是在加载方法时就上锁了
    public /*synchronized*/ void qumoney(double money) {
        String name = Thread.currentThread().getName();
        // 为取钱时间添加了锁，当有一个线程取到这个锁后其他线程只能等待
        // 此处若用一个字符则表示不管多少对象都只有一个锁，但对象不止一个
        // 因此这里应写this，代表当前线程传进来的对象，这里指账户对象
        // 若是静态方法则建议使用字节码（类名.class）对象作为锁
        // 方法一：同步代码块
//        synchronized (this) {
        try {
            // 只允许一个线程往下执行
            lock.lock();
            if(this.money >= money){
                System.out.println(name+"去除了"+money+"块钱");
                this.money -= money;
                System.out.println(name+"取钱后账户余额还剩"+this.money+"块钱");
            }else {
                System.out.println(name+"取钱失败，账户余额不足");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock(); //解锁
        }
//        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getMoney() {
        return money;
    }

    public void setMoney(double money) {
        this.money = money;
    }
}
