package com.plan.data.arr;

public class MinSubArrayLen {
  public int minSubArrayLen(int target, int[] nums) {
    int length = nums.length;
    int start = 0,end=1;
    int res = Integer.MAX_VALUE;
    int sum = nums[0];
    while (start<length){
      if(sum<target && end<length-1){
        end++;
        sum+=nums[end];
      }else{
        res = Math.min(res, end-start+1);
        sum-=nums[start];
        start++;
      }
    }
    return res==Integer.MAX_VALUE?0:res;
  }
  public static void main(String[] args){
    int[] nums = {2,3,1,2,4,3};
    int i = new MinSubArrayLen().minSubArrayLen(7,nums);
    System.out.println(i);
  }
}
