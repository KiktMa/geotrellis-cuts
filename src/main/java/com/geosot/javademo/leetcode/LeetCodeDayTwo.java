package com.geosot.javademo.leetcode;

import java.lang.Math;

public class LeetCodeDayTwo {

    public static void main(String[] args) {
        int[] nums = new int[]{2,3,1,2,4,3};
        int ta = 7;
        System.out.println(minSubArrayLen(ta, nums));
    }

    /**
     * 给定数组和一个目标数，返回大于等于目标数的最小数组长度
     * 中等题，感觉有点难。。。还要继续看这个题
     * @param target 给给定值
     * @param nums 给定数组
     * @return 返回场长度
     */
    public static int minSubArrayLen(int target, int[] nums){
        int res = Integer.MAX_VALUE;
        int l = 0; // 子数字中的循环使用
        int sum = 0; // 子数组和
        for(int r = 0;r<nums.length;r++){
            sum += nums[r];

            // 若和大于target则进入循环将子数字长度进行记录
            while(sum>=target){
                res = Math.min(res,r-l+1); //选择每次子数字和大于target的数组长度最短的一个数组
                sum -= nums[l++]; // 将l迁移至不满足子数组之和大于target的位置，跳出while后继续寻找和大于target的子数组
            }
        }
        // 若res与原始值相等则表示整个数组加起来都没有大于target，否则就返回最小的子数组长度
        return res==Integer.MIN_VALUE?0:res;
    }
}