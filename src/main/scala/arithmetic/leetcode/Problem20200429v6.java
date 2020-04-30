package arithmetic.leetcode;

/**
 * 206. 反转链表
 * @desc:
 * @author: hecj
 * @create: 20200429

反转一个单链表。

示例:

输入: 1->2->3->4->5->NULL
输出: 5->4->3->2->1->NULL
进阶:
你可以迭代或递归地反转链表。你能否用两种方法解决这道题？

来源：力扣（LeetCode）
链接：https://leetcode-cn.com/problems/reverse-linked-list
著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
 
 **/
public class Problem20200429v6 {
	
	public static void main(String[] args){
		Problem20200429v6 demo = new Problem20200429v6();
		demo.test();
	}
	
	public void test(){
		int[] nums = {1,2,3,4};
		ListNode head = new ListNode(0);
		ListNode temp = head;
		for(int num : nums){
			temp.next = new ListNode(num);
			temp = temp.next;
		}
		reverseList(head.next);
	}
	
	public ListNode reverseList(ListNode head) {
		ListNode list = null;
		ListNode curr = head;
		while(curr != null){
			
			System.out.println(curr.val);
			
			curr = curr.next;
		}
		return list;
	}
	
	public class ListNode {
	     int val;
	     ListNode next;
	     ListNode(int x) { val = x; }
	}
}
