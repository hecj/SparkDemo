package arithmetic.leetcode;

/**
 * leetcode https://leetcode-cn.com/problems/merge-two-sorted-lists/
 * @desc:
 * @author: hecj
 * @create: 20200429

将两个升序链表合并为一个新的升序链表并返回。新链表是通过拼接给定的两个链表的所有节点组成的。 

示例：

输入：1->2->4, 1->3->4
输出：1->1->2->3->4->4

来源：力扣（LeetCode）
链接：https://leetcode-cn.com/problems/merge-two-sorted-lists
著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。

 
 **/
public class Problem20200429 {
	
	public static void main(String[] args){
		Problem20200429 demo = new Problem20200429();
		demo.mergeTwoLists(null,null);
	}
	
	/**
	 * 输入：1->2->4, 1->3->4
	 * 输出：1->1->2->3->4->4
	 * 思路：利用归并排序的思想，依次比较将小的数放到临时链表中。
	 */
	public ListNode mergeTwoLists(ListNode l1, ListNode l2) {
		ListNode list = new ListNode(0);
		ListNode temp = list;
		while(l1 !=null || l2 != null){
			// 结束条件是 l1 == null && l2 == null
			if(l1 != null && l2 != null){
				// 对比l1和l2的值
				if(l1.val>l2.val){
					// 将l2 迁移到temp
					temp.next = new ListNode(l2.val);
					temp = temp.next;
					l2 = l2.next;
					continue;
				} else {
					// 将l1 迁移到temp
					temp.next = new ListNode(l1.val);
					temp = temp.next;
					l1 = l1.next;
					continue;
				}
			}
			
			if(l1 != null){
				// 将l1 迁移到temp
				temp.next = new ListNode(l1.val);
				temp = temp.next;
				l1 = l1.next;
			}
			
			if(l2 != null){
				// 将l2 迁移到temp
				temp.next = new ListNode(l2.val);
				temp = temp.next;
				l2 = l2.next;
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
