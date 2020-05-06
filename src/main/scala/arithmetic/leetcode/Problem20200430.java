package arithmetic.leetcode;

import java.util.HashSet;
import java.util.Set;

/**
 * 160. 相交链表
 * @desc:
 * @author: hecj
 
编写一个程序，找到两个单链表相交的起始节点。
输入：intersectVal = 8, listA = [4,1,8,4,5], listB = [5,0,1,8,4,5], skipA = 2, skipB = 3
输出：Reference of the node with value = 8
输入解释：相交节点的值为 8 （注意，如果两个链表相交则不能为 0）。从各自的表头开始算起，链表 A 为 [4,1,8,4,5]，链表 B 为 [5,0,1,8,4,5]。在 A 中，相交节点前有 2 个节点；在 B 中，相交节点前有 3 个节点。

来源：力扣（LeetCode）
链接：https://leetcode-cn.com/problems/intersection-of-two-linked-lists
著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。

注意：

如果两个链表没有交点，返回 null.
在返回结果后，两个链表仍须保持原有的结构。
可假定整个链表结构中没有循环。
程序尽量满足 O(n) 时间复杂度，且仅用 O(1) 内存。

 **/
public class Problem20200430 {
	
	public static void main(String[] args){
		Problem20200430 demo = new Problem20200430();
	}
	
	/**
	 * 使用set方式
	 * A [4,1,8,4,5]   a c +b   b c +a
	 * B [5,0,1,8,4,5]
	 * @param headA
	 * @param headB
	 * @return
	 */
	public ListNode getIntersectionNode(ListNode headA, ListNode headB) {
		Set<ListNode> set = new HashSet<ListNode>();
		// 先将headA存入set中
		ListNode curr = headA;
		while(curr!=null){
			set.add(curr);
			curr = curr.next;
		}
		ListNode currB = headA;
		while(currB!=null){
			if(set.contains(currB)){
				return currB;
			}
			currB = currB.next;
		}
		return null;
	}
	
	public class ListNode {
	     int val;
	     ListNode next;
	     ListNode(int x) { val = x; }
	}
}
