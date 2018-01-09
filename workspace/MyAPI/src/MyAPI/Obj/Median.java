package MyAPI.Obj;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;

import MyAPI.General.General;

public class Median <V extends Object> {
	
	Class<V> clazz_V;
	LinkedList<V> values;
			
	public Median(Class<V> clazz_V) {
		this.clazz_V=clazz_V;
		values=new LinkedList<>();
	}
	
	public void addOneSample(V oneV){
		values.add(oneV);
	}
	
	public V getPercentMedian(float percent, Comparator<? super V> compartor){//percent is: 0~1, compartor is null means default order
		V[] sorted=General.list_to_arr(values, clazz_V);
		Arrays.sort(sorted, compartor);
		return sorted[(int) (sorted.length*percent)];
	}

}
