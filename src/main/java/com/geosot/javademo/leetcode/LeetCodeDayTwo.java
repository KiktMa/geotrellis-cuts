package com.geosot.javademo.leetcode;


import com.geosot.javademo.entity.ListNode;

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

    /**
     * 给你一个正整数 n ，生成一个包含 1 到 n*n 所有元素
     * 且元素按顺时针顺序螺旋排列的 n x n 正方形矩阵 matrix。
     * @param n 正整数 n
     * @return 返回螺旋数组
     */
    public int[][] generateMatrix(int n) {
        int left = 0, right = n-1, top = 0, bottom = n-1;
        int count = 1, target = n * n;
        int[][] res = new int[n][n];
        //for循环中变量定义成i或j的细节：按照通常的思维，i代表行，j代表列
        //这样，就可以很容易区分出来变化的量应该放在[][]的第一个还是第二个
        //对于变量的边界怎么定义：
        //从左向右填充：填充的列肯定在[left,right]区间
        //从上向下填充：填充的行肯定在[top,bottom]区间
        //从右向左填充：填充的列肯定在[right,left]区间
        //从下向上填充：填充的行肯定在[bootom,top]区间
        //通过上面的总结会发现边界的起始和结束与方向是对应的
        while(count <= target){
            //从左到右填充，相当于缩小上边界
            for(int j = left; j <= right; j++) res[top][j] = count++;
            //缩小上边界
            top++;
            //从上向下填充，相当于缩小右边界
            for(int i = top; i <=bottom; i++) res[i][right] = count++;
            //缩小右边界
            right--;
            //从右向左填充，相当于缩小下边界
            for(int j = right; j >= left; j--) res[bottom][j] = count++;
            //缩小下边界
            bottom--;
            //从下向上填充，相当于缩小左边界
            for(int i = bottom; i >= top; i--) res[i][left] = count++;
            //缩小左边界
            left++;
        }
        return res;
    }

    /**
     * 从链表中删除元素
     * @param head 头节点
     * @param val 要删除的元素
     * @return 返回处理后的链表
     */
    public ListNode removeElements(ListNode head, int val) {
        while (head != null && head.val == val) {
            head = head.next;
        }
        // 已经为null，提前退出
        if (head == null) {
            return head;
        }
        // 已确定当前head.val != val
        ListNode pre = head;
        ListNode cur = head.next;
        while (cur != null) {
            if (cur.val == val) {
                pre.next = cur.next;
            } else {
                pre = cur;
            }
            cur = cur.next;
        }
        return head;
    }
}