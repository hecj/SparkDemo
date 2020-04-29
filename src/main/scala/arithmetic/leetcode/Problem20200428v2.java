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
		int total1 = 0;
		int step = 1;
		while(l1!=null){
			total1 = total1+l1.val*step;
			step *= 10;
			l1 = l1.next;
		}
		int total2 = 0;
		int step2 = 10;
		while(l2!=null){
			total2 = total2+l2.val*step2;
			step2 *= 10;
			l2 = l2.next;
		}
		int total3= total1+total2;
		System.out.println(total3);
		ListNode list = new ListNode(0);
		ListNode temp = list;
		if(total3<10){
			return new ListNode(total3);
		}
		while(total3>0){
			int val = total3%10;
			total3 = total3/10;
			temp.next = new ListNode(val);
			temp = temp.next;
		}
		return list.next;
	}
	
	public class ListNode {
	     int val;
	     ListNode next;
	     ListNode(int x) { val = x; }
	}
}
