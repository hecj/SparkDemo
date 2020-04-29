package arithmetic.leetcode;

import java.util.HashMap;
import java.util.Map;

/**
 * leetcode https://leetcode-cn.com/problems/add-two-numbers/
 * @desc:
 * @author: hecj
 * @create: 20200428
2. 两数相加
给出两个 非空 的链表用来表示两个非负的整数。其中，它们各自的位数是按照 逆序 的方式存储的，并且它们的每个节点只能存储 一位 数字。
如果，我们将这两个数相加起来，则会返回一个新的链表来表示它们的和。
您可以假设除了数字 0 之外，这两个数都不会以 0 开头。
示例：
输入：(2 -> 4 -> 3) + (5 -> 6 -> 4)
输出：7 -> 0 -> 8
原因：342 + 465 = 807

来源：力扣（LeetCode）
链接：https://leetcode-cn.com/problems/add-two-numbers
著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
 
 **/
public class Problem20200428v2 {
	/**
	 * Definition for singly-linked list.
	 * public class ListNode {
	 *     int val;
	 *     ListNode next;
	 *     ListNode(int x) { val = x; }
	 * }
	 */
	public static void main(String[] args){
		Problem20200428v2 demo = new Problem20200428v2();
		
	}
	
	public ListNode addTwoNumbers(ListNode l1, ListNode l2) {
		// l1的真实数据
		ListNode list = new ListNode(0);
		ListNode temp = list;
		int addNum = 0;
		while(l1 != null || l2 != null || addNum !=0){
			
			if(l1 == null && l2 == null){
				if(addNum>0){
					temp.next = new ListNode(addNum);
					break;
				}
			}
			
			int l1Num = 0,l2Num = 0;
			// 两数之和
			if(l1 != null){
				l1Num = l1.val;
				l1 = l1.next;
			}
			if(l2 != null){
				l2Num = l2.val;
				l2 = l2.next;
			}
			int num = l1Num + l2Num+addNum;
			addNum = 0;
			if(num < 10){
				temp.next = new ListNode(num);
				temp = temp.next;
			} else{
				// 大于10,进1
				int val = num%10;
				addNum = num/10;
				temp.next = new ListNode(val);
				temp = temp.next;
			}
		}
		return list.next;
	}
	
	public class ListNode {
	     int val;
	     ListNode next;
	     ListNode(int x) { val = x; }
	}
}
