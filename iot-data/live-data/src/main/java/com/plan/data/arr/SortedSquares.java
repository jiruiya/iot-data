package com.plan.data.arr;

public class SortedSquares {
  public int[] sortedSquares(int[] nums) {
    int length = nums.length;
    int[] res = new int[length];
    int start = 0, end = length - 1;
    int index = length-1;
    while (start<=end){
      if(nums[start]*nums[start]>=nums[end]*nums[end]){
        res[index] = nums[start]*nums[start];
        start++;
      }else{
        res[index] = nums[end]*nums[end];
        end--;
      }
      index--;
    }
    return res;
  }
}
