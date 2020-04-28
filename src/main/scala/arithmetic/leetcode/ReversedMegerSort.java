package arithmetic.leetcode;

import java.util.Arrays;

/**
 * leetcode: https://leetcode-cn.com/problems/shu-zu-zhong-de-ni-xu-dui-lcof/
 * 数组中的逆序对
 * 题目：
 * 在数组中的两个数字，如果前面一个数字大于后面的数字，则这两个数字组成一个逆序对。
 * 输入一个数组，求出这个数组中的逆序对的总数。
 * 示例 1:
 * 输入: [7,5,6,4]
 * 输出: 5
 * @author: hecj
 * @create: 20200428
 **/
public class ReversedMegerSort {

	public static void main(String[] args){
		int[] nums = {7,5,6,4};
		int total = reversePairs(nums);
		System.out.println("total "+total);
	}
	
	public static int reversePairs(int[] nums){
		int[] temps = new int[nums.length];
		int total = 0;
		total = mergeSort(nums,0,nums.length-1,temps,total);
		return total;
	}
	
	/**
	 * 归并排序(求逆序对)
	 * https://www.cnblogs.com/chengxiao/p/6194356.html
	 */
	public static int mergeSort(int[] nums,int left,int right,int[] temps,int total){
		// 终止条件
		if(left>=right){
			return total;
		}
		// 从从中间截断,分别排序左右两边的元素
		int mid = (right+left)/2;
		// 左边
		int lTotal = mergeSort(nums,left,mid,temps,total);
		// 右边
		int rTotal= mergeSort(nums,mid+1,right,temps,total);
		// 然后合并
		return merge(nums,left,mid,right,temps,lTotal+rTotal);
	}
	/**
	 * 合并元素
	 * @param nums
	 * @param left
	 * @param mid
	 * @param right
	 * @param temps
	 */
	public static int merge(int[] nums,int left,int mid,int right,int[] temps,int total){
		// 左边开始下标
		int l = left;
		// 右边开始下标
		int r = mid+1;
		// temps临时数组下班
		int index = 0;
		// 左右合并
		while(l<=mid && r<=right){
			if(nums[l]<=nums[r]){
				// 左边小于等于右边,将nums中左边的数据放入temps临时数组
				temps[index++]= nums[l++];
			}else{
				// 左边大于右边,将nums中右边的数据放入temps临时数组
				temps[index++]= nums[r++];
				
				// 如果左边的数大于右边的数,则组成逆序对,将右边的数据移动临时数组的同时计算逆序对
				// 当前移动的数 逆序对的个数等于 左边未移动的元素数，也就是 (mid-l+1)(+1是因为左右闭合区间,所以要+1,不然会少计算一个数)
				total += (mid-l+1);
				
			}
		}
		// 经过上面的while循环，左右两边一定有一方元素完全复制到了temps数组中
		// 剩下的一方中的元素一定是最大的元素，只需依次复制到temps中即可
		if(l<=mid){
			// 左边未复制完
			while(l<=mid){
				temps[index++] = nums[l++];
			}
		}
		if(r<=right){
			// 右边未复制完
			while(r<=right){
				temps[index++] = nums[r++];
			}
		}
		// 将temps中的数据依次复制到nums中
		for(int i=0;left<=right;i++){
			nums[left++] = temps[i];
		}
		return total;
	}
}
