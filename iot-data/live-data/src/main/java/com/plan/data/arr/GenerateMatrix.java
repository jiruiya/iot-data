package com.plan.data.arr;

import java.util.Arrays;

public class GenerateMatrix {
  public int[][] generateMatrix(int n) {
    int[][] res = new int[n][n];
    if (n % 2 == 1) {
      res[n / 2][n / 2] = n * n;
    }
    int i = 0, j = 0;
    int num = 1;
    int index = 0;
    while (index < n / 2) {
      while (j < n - index - 1) {
        res[i][j++] = num++;
      }
      while (i < n - index - 1) {
        res[i++][j] = num++;
      }
      while (j > index) {
        res[i][j--] = num++;
      }
      while (i > index) {
        res[i--][j] = num++;
      }
      index++;
      i=index;
      j=index;
    }
    return res;
  }
  public static void main(String[] args){
    int[][] ints = new GenerateMatrix().generateMatrix(5);
    for(int[] a : ints){
      System.out.println(Arrays.toString(a));
    }

  }
}
