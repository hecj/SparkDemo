package arithmetic;

import java.util.Arrays;

/**
 * @program: SparkDemo1->MegerSort
 * @desc: 归并排序
 * @author: hecj
 * @create: 20200427
 **/
public class MegerSort {

	public static void main(String[] args){
		int[] nums = {8,4,5,7,1,3,6,2};
		int[] temps = new int[nums.length];
		mergeSort(nums,0,nums.length-1,temps);
		System.out.println(Arrays.toString(nums));
	}
	
	/**
	 * 归并排序
	 * https://www.cnblogs.com/chengxiao/p/6194356.html
	 */
	public static void mergeSort(int[] nums,int left,int right,int[] temps){
		// 终止条件
		if(left>=right){
			return;
		}
		// 从从中间截断,分别排序左右两边的元素
		int mid = (right+left)/2;
		// 左边
		mergeSort(nums,left,mid,temps);
		// 右边
		mergeSort(nums,mid+1,right,temps);
		// 然后合并
		merge(nums,left,mid,right,temps);
	}
	/**
	 * 合并元素
	 * @param nums
	 * @param left
	 * @param mid
	 * @param right
	 * @param temps
	 */
	public static void merge(int[] nums,int left,int mid,int right,int[] temps){
		// 左边开始下标
		int l = left;
		// 右边开始下标
		int r = mid+1;
		// temps临时数组下班
		int index = 0;
		// 左右合并
		while(l<=mid && r<=right){
			if(nums[l]<=nums[r]){
				// 如果左边小于等于右边,将nums中左边的数据放入temps临时数组
				temps[index++]= nums[l++];
			}else{
				// 如果右边大于右边,将nums中右边的数据放入temps临时数组
				temps[index++]= nums[r++];
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
		System.out.println(Arrays.toString(temps));
		// 将temps中的数据依次复制到nums中
		for(int i=0;left<=right;i++){
			nums[left++] = temps[i];
		}
	}
}
