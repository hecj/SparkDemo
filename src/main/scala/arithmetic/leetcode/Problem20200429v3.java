package arithmetic.leetcode;

/**
 * 19. 删除链表的倒数第N个节点
 * @desc:
 * @author: hecj
 * @create: 20200429

给定一个链表，删除链表的倒数第 n 个节点，并且返回链表的头结点。

示例：

给定一个链表: 1->2->3->4->5, 和 n = 2.

当删除了倒数第二个节点后，链表变为 1->2->3->5.
说明：

给定的 n 保证是有效的。

进阶：

你能尝试使用一趟扫描实现吗？

来源：力扣（LeetCode）
链接：https://leetcode-cn.com/problems/remove-nth-node-from-end-of-list
著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
 
 **/
public class Problem20200429v3 {
	
	public static void main(String[] args){
		Problem20200429v3 demo = new Problem20200429v3();
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
		removeNthFromEnd(head.next,1);
	}
	
	/**
	 * 1->2->3->4->5, 和 n = 2.
	 * 使用一趟扫描完成
	 * @param head
	 * @param n
	 * @return
	 */
	public ListNode removeNthFromEnd(ListNode head, int n) {
		ListNode list = new ListNode(0);
		list.next = head;
		ListNode first = list;
		ListNode second = list;
		for(int i=0;i<n+1;i++){
			first = first.next;
		}
		
		while(first!= null){
			first = first.next;
			second = second.next;
		}
		second.next = second.next.next;
		return list.next;
	}
	
	public class ListNode {
	     int val;
	     ListNode next;
	     ListNode(int x) { val = x; }
	}
}
