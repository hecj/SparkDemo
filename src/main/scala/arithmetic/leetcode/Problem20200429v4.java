package arithmetic.leetcode;

/**
 * 83. 删除排序链表中的重复元素
 * @desc:
 * @author: hecj
 * @create: 20200429

给定一个排序链表，删除所有重复的元素，使得每个元素只出现一次。

示例 1:

输入: 1->1->2
输出: 1->2
示例 2:

输入: 1->1->2->3->3
输出: 1->2->3

来源：力扣（LeetCode）
链接：https://leetcode-cn.com/problems/remove-duplicates-from-sorted-list
著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
 
 **/
public class Problem20200429v4 {
	
	public static void main(String[] args){
		Problem20200429v4 demo = new Problem20200429v4();
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
		deleteDuplicates(head.next);
	}
	
	/**
	 输入: 1->1->2
	 输出: 1->2
	 */
	public ListNode deleteDuplicates(ListNode head) {
		if(head == null){
			return null;
		}
		ListNode pre = head;
		ListNode curr = head.next;
		int num = pre.val;
		while(curr != null){
			if(num == curr.val){
				// 删除当前节点
				pre.next = curr.next;
				curr = curr.next;
			} else{
				num = curr.val;
				pre = curr;
				curr = curr.next;
			}
		}
		
		return head;
	}
	
	public class ListNode {
	     int val;
	     ListNode next;
	     ListNode(int x) { val = x; }
	}
}
