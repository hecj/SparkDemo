package arithmetic.leetcode;

/**
 * 203. 移除链表元素
 * @desc:
 * @author: hecj
 * @create: 20200429

删除链表中等于给定值 val 的所有节点。

示例:

输入: 1->2->6->3->4->5->6, val = 6
输出: 1->2->3->4->5
 
 **/
public class Problem20200429v5 {
	
	public static void main(String[] args){
		Problem20200429v5 demo = new Problem20200429v5();
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
		removeElements(head.next,3);
	}
	
	public ListNode removeElements(ListNode head, int val) {
		ListNode list = new ListNode(0);
		list.next = head;
		ListNode curr = head;
		ListNode pre = list;
		while(curr!=null){
			if(curr.val == val){
				// 删除当前元素
				pre.next = curr.next;
			} else{
				pre = curr;
			}
			curr = curr.next;
		}
		return list.next;
	}
	
	public class ListNode {
	     int val;
	     ListNode next;
	     ListNode(int x) { val = x; }
	}
}
