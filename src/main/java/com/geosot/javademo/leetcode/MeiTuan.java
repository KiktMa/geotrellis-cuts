package com.geosot.javademo.leetcode;

import java.util.Scanner;

public class MeiTuan {
    public static void main(String[] args) {
        serarchRoad();
    }
    public static void reserach(String[] args) {
        Scanner in = new Scanner(System.in);

        int num = in.nextInt();

        int[] nums = new int[num];
        for(int i = 0;i<num;i++){
            nums[i] = in.nextInt();
        }

        int x = in.nextInt();
        int y = in.nextInt();
        for(int i = 0;i<num;i++){
            if(nums[i]==x&&(i+1<num && i-1>0)){
                System.out.println("Yes");
            }
        }
        System.out.println("No");
    }

    public static void serarchRoad(){
        Scanner in = new Scanner(System.in);

        int staions = in.nextInt();
        int[] roadLength = new int[staions+1];
        for(int i = 1;i<=staions;i++){
            roadLength[0] = 0;
            roadLength[i] = in.nextInt();
        }

        int staion1 = in.nextInt();
        int staion2 = in.nextInt();

        int front = 0;
        int back = 0;
        for(int i = staion1+1;i<=staion2;i++){
            front += roadLength[i];
        }

        for(int i = staion1-1, j = staion2;i>=0 && j<roadLength.length;i--,j++){
            back = roadLength[i]+roadLength[j];
        }

        if(front>=back){
            System.out.println(front);
        }else{
            System.out.println(back);
        }
    }
}
