package com.plan.data.arr;

public class MinSubArrayLen {
  public int minSubArrayLen(int target, int[] nums) {
    int length = nums.length;
    int res = Integer.MAX_VALUE;
    int sum = 0, start = 0;
    for(int i=0;i<length;i++){
      sum += nums[i];
      while (sum>=target){
        res = Math.min(res, i-start+1);
        sum-=nums[start];
        start++;
      }
    }
    return res==Integer.MAX_VALUE?0:res;
  }
  public static void main(String[] args){
    int[] nums = {1,4,4};
    int i = new MinSubArrayLen().minSubArrayLen(4,nums);
    System.out.println(i);
  }

  public int minSubArrayLen1(int target, int[] nums) {
    int length = nums.length;
    int start = 0,end=1;
    int res = Integer.MAX_VALUE;
    int sum = nums[0];
    while (start<length){
      if(sum<target && end<length){
        sum+=nums[end];
        end++;
      }else if(sum>=target){
        res = Math.min(res, end-start);
        sum-=nums[start];
        start++;
      } else{
        break;
      }
    }
    return res==Integer.MAX_VALUE?0:res;
  }
}
