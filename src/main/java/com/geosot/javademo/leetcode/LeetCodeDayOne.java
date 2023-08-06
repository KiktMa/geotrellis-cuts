package com.geosot.javademo.leetcode;

import java.util.Arrays;

public class LeetCodeDayOne {

    public static void main(String[] args) {
        int[] nums = new int[]{-4,-2,0,3};
//        int search = search(nums,9);
//        System.out.println(search);

        int[] squares = sortedSquares(nums);
        System.out.println(Arrays.toString(squares));
    }

    /**
     * 数组的二分查找
     * @param nums 给定数组
     * @param target 给定要查找的目标值
     * @return 返回成功或失败
     */
    public static int search(int[] nums, int target){
        int start = 0;
        int end = nums.length;
        while (start<end){
            int mid = (start+end)/2;
            if(nums[mid]==target){
                return mid;
            }else if(nums[mid]<target){
                start = mid+1;
            }else {
                end = mid-1;
            }
        }
        return -1;
    }

    /**
     * 给定升序数组，再给定一个值进行插入,二分法
     * @param nums 数组
     * @param target 插值
     * @return 返回插值位置
     */
    public int searchInsert(int[] nums, int target) {
        int start = 0;
        int end = nums.length-1;
        while (start<=end){
            int mid = (start+end)/2;
            if(nums[mid]==target){
                return mid;
            }else if(nums[mid]<target){
                start = mid+1;
            }else {
                end = mid-1;
            }
        }
        return nums[start]>=target?start:start+1;
    }

    /**
     * 删除数组中的某个元素
     * @param nums 给定数组
     * @param val 要删除的值
     * @return 返回数组长度
     */
    public int removeElement(int[] nums, int val) {
        int i = 0;
        int j = 0;
        if(nums.length==1) {
            return 0;
        }
        while(j<nums.length-1){
            if(nums[j]==val){
                j++;
            }else{
                nums[i] = nums[j];
                i++;
                j++;
            }
        }
        return i;
    }

    /**
     * 给数组每个数平方再排序后返回
     * @param nums 给定数组
     * @return 但会新数组
     */
    public static int[] sortedSquares(int[] nums) {
        int i = 0;
        int j = nums.length - 1;
        for (int g = 0; nums[g] < 0; g++) {
            nums[g] = Math.abs(nums[g]);
        }

        int pivot = nums[i];
        while (i < j) {
            while (i < j && nums[j] >= pivot) {
                --j;
            }
            nums[i] = nums[j];
            while (i < j && nums[i] <= pivot) {
                ++i;
            }
            nums[j] = nums[i];
        }
        nums[i] = pivot;

        for(int k = 0;k<=j;k++){
            nums[k] = nums[k]*nums[k];
        }

        return nums;
    }
}
