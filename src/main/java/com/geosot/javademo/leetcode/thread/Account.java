package com.geosot.javademo.leetcode.thread;

public class Account {
    private String name;
    private double money;

    public Account() {
    }

    public Account(String name, double money) {
        this.name = name;
        this.money = money;
    }

    public void qumoney(double money) {
        String name = Thread.currentThread().getName();
        if(this.money >= money){
            System.out.println(name+"去除了"+money+"块钱");
            this.money -= money;
            System.out.println(name+"取钱后账户余额还剩"+this.money+"块钱");
        }else {
            System.out.println(name+"取钱失败，账户余额不足");
        }
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
