package arithmetic;

/**
 * 快速排序 挖坑法
 * @program: SparkDemo1->SpeedSort
 * @desc:
 * @author: hecj
 * @create: 20200426
 *
 * 示例：5,1,4,7,9,3
   找最右侧3为基准参照,

  54321
  
  14325

  14325

  
 
 思路：
 另左边下标为left, 右边为right
 left的往右移动，right往做移动,当left<right才有效，如果left=right说明只有一个元素了，没必要排序了
 
 
 leedcode 地址： https://leetcode-cn.com/problems/sort-an-array/
 
 
 **/
public class SpeedSort {

	public static void main(String[] args){
		int[] nums = {5,1,29,35,4,7,9,3,1,10,2,23,4,5,19};
		quickSort(nums,0,nums.length-1);
		for (int num : nums) {
			System.out.print(num+" ");
		}
	}
	
	/**
	 * 快速排序(挖坑法实现)
	 * https://www.runoob.com/w3cnote/quick-sort.html
	 5,1,4, 3, 7,9,3   base 5
	 3 1 4 3 7 9 5
	 
	 */
	public static void quickSort(int[] nums, int low, int high){
		// 终止递归条件
		if(low >= high){
			return;
		}
		int left = low,right = high,baseNum = nums[left];
		while(left < right){
			// 从右往左找第一个小于baseNum的元素 填入到baseNum的坑上面
			for(;left<right;right--){
				if(nums[right] < baseNum){
					nums[left] = nums[right];
					// 填坑后，left要往右移动以为
					left++;
					break;
				}
			}
			// 从左往右找第一个大于temp的元素
			for(;left<right;left++){
				if(nums[left] >= baseNum){
					nums[right] = nums[left];
					// 填坑后，right要往左移动以为
					right--;
					break;
				}
			}
		}
		
		// 递归
		System.out.println("left:"+left);
		System.out.println("right:"+right);
		
		// 填坑一轮后,left和right一定是相同的,也就是left左边是<baseNun,右边是>=baseNum
		// 将baseNum填入left位置
		nums[left] = baseNum;
		// 从left切分,分别排序左边和右边的元素
		quickSort(nums,low,left-1);
		quickSort(nums,left+1,high);
	}
}
