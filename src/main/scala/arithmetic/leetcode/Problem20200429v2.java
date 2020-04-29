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
public class Problem20200429v2 {
	
	public static void main(String[] args){
		Problem20200429v2 demo = new Problem20200429v2();
		demo.test();
	}
	
	public void test(){
		int[] nums = {1,2,3,4,5};
		ListNode head = new ListNode(0);
		ListNode temp = head;
		for(int num : nums){
			temp.next = new ListNode(num);
			temp = temp.next;
		}
		removeNthFromEnd(head.next,2);
	}
	
	public ListNode removeNthFromEnd(ListNode head, int n) {
		// 先计算链表长度
		int len = 0;
		ListNode temp = head;
		while(temp != null){
			temp = temp.next;
			len++;
		}
		
		// 根据n位置删除链表
		ListNode list = head;
		ListNode temp2 = null;
		int i=0;
		while(list != null){
			if(i++ == len-n){
				if(temp2 == null){
					// 说明删除的是第一个元素,直接返回
					return list.next;
				}
				// 删除当前节点
				temp2.next = list.next;
				break;
			}
			temp2 = list;
			list = list.next;
		}
		return head;
	}
	
	public class ListNode {
	     int val;
	     ListNode next;
	     ListNode(int x) { val = x; }
	}
}
