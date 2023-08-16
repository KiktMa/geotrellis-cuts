package com.geosot.javademo.leetcode;

class ListNode{
    int val;
    ListNode next;
    ListNode(int val){
        this.val = val;
    }
}
class LeetCodeDay3 {
    int size;
    ListNode head;

    LeetCodeDay3(){
        size = 0;
        head = new ListNode(0);
    }

    int getVal(int index){
        if(index < 0 || index>size){
            return -1;
        }
        ListNode pre = head;
        for (int i = 0; i <= index; i++) {
            pre = pre.next;
        }
        return pre.val;
    }

    void addHead(int val){
        addVal(0,val);
    }

    void addTail(int val){
        addVal(size, val);
    }

    void addVal(int i, int val) {
        if(i > size) return;
        if(i < 0) i = 0;

        ListNode pred = head;
        for (int j = 0; j <= size; j++) {
            pred = pred.next;
        }

        ListNode newNode = new ListNode(val);
        newNode.next = pred.next;
        pred.next = newNode;
    }

    void deleteNode(int index){
        if(index < 0 || index >= size) return;
        if(index == 0){
            head = head.next;
            return;
        }

        ListNode pred = head;
        for (int i = 0; i <= index; i++) {
            pred = pred.next;
        }
        pred.next = pred.next.next;
    }
}
