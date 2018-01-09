package MyAPI.Obj;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.Set;

import MyAPI.General.General;

import com.google.common.collect.Sets;

public class PowerSet {

	Integer[] elements;
	BitSet bits;
	
	public PowerSet(Integer[] elements) {
		this.elements=elements;
		bits = new BitSet(elements.length);
	}
	
	public LinkedList<Integer> getNextPermutation(){//the initail bits is all zero, corresponding to empty Permutation, this one get ignored
		if(increment()){
			return getCombination();
		}else {
			return null;
		}
	}
	
	/**
	 * The method increment increases a number represented in a set of bits. The algorithm clears 1 bits from the rightmost bit until a 0 bit is found. 
	 * It then sets the rightmost 0 bit to 1. For example, in order to increase the number 5 with bits {1, 0, 1}, it clears 1 bits from the right side and sets the rightmost 0 bit to 1. 
	 * The bits become {1, 1, 0} for the number 6, which is the result of increasing 5 by 1.
	 */
	private boolean increment() {
	    int index = elements.length - 1;

	    while(index >= 0 && bits.get(index)) {
	        bits.clear(index);
	        --index;
	    }

	    if(index < 0)
	        return false;

	    bits.set(index);
	    return true;
	}

	private LinkedList<Integer> getCombination(){
		LinkedList<Integer> combination = new LinkedList<Integer>();
	    for(int i = 0; i < elements.length; ++i) {
	        if(bits.get(i))
	            combination.add(elements[i]);
	    }
	    return combination;
	}
	
	/**
	 * use Sets.powerSet from Guava, but this require the number of element should be <=30 
	 */
	public static <M extends Object> ArrayList<LinkedList<Set<M>>> getPermutation_forSmallElemNum(Set<M> aa){
		ArrayList<LinkedList<Set<M>>> res=General.ini_ArrayList_LinkedList(aa.size()+1);
		for (Set<M> oneSet : Sets.powerSet(aa)) {
			res.get(oneSet.size()).add(oneSet);
		}
		return res;
	}
	
	public static void main(String[] args){
		Integer[] ele=General.makeRange(new Integer[]{0,30,1});
		PowerSet makePowerSet=new PowerSet(ele);
		LinkedList<Integer> one=null; int ind=0;
		System.out.println(Math.pow(2, ele.length));
		while ((one=makePowerSet.getNextPermutation())!=null) {
			System.out.println(ind+": "+one);
			ind++;
		}
	}

}
