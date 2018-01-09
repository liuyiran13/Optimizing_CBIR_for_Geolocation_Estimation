package MyAPI.Obj;

import java.util.NavigableSet;
import java.util.TreeSet;

import MyAPI.General.ComparableCls.slave_masterFloat_DES;

public class SortTop<V> {

	TreeSet<slave_masterFloat_DES<V>> treeSetSort;
	int top;
	boolean isDES, isASC;
	float thr_min;
	float thr_max;
	
	public SortTop(String model, int top) throws InterruptedException {
		treeSetSort=new TreeSet<slave_masterFloat_DES<V>>();
		if (model.equalsIgnoreCase("DES")) {
			isDES=true;
		}else if (model.equalsIgnoreCase("ASC")) {
			isASC=true;
		}else {
			throw new InterruptedException("model should be DES or ASC! here model:"+model);
		}
		this.top=top;
		ini();
	}
	
	public void ini(){
		treeSetSort.clear();
		thr_min=Float.MAX_VALUE;
		thr_max=-1;
	}
	
	public void addOneSample(V sample, float score){
		if (isDES) {
	        // if the array is not full yet:
	        if (treeSetSort.size() < top) {
	        	treeSetSort.add(new slave_masterFloat_DES<V>(sample,score));
	            if (score<thr_min) //update current thr in doc_scores_order
	            	thr_min = score;
	        } else if (score>thr_min) { // if it is "better" than the least one in the current doc_scores_order
	            // remove the last one ...
	        	treeSetSort.remove(treeSetSort.first());
	            // add the new one ...
	        	treeSetSort.add(new slave_masterFloat_DES<V>(sample,score));
	            // update new thr in doc_scores_order
	        	thr_min = treeSetSort.first().getMaster();
	        }
		}else if(isASC){
	        // if the array is not full yet:
	        if (treeSetSort.size() < top) {
	        	treeSetSort.add(new slave_masterFloat_DES<V>(sample,score));
	            if (score>thr_max) //update current thr in doc_scores_order
	            	thr_max = score;
	        } else if (score<thr_max) { // if it is "better" than the least one in the current doc_scores_order
	            // remove the last one ...
	        	treeSetSort.remove(treeSetSort.last());
	            // add the new one ...
	        	treeSetSort.add(new slave_masterFloat_DES<V>(sample,score));
	            // update new thr in doc_scores_order
	        	thr_max = treeSetSort.last().getMaster();
	        }
		}
	}
	
	public NavigableSet<slave_masterFloat_DES<V>> getTopSamples(){
		NavigableSet<slave_masterFloat_DES<V>> sortedSet=null;
		if (isDES) {
			sortedSet= treeSetSort.descendingSet();
		}else if(isASC){
			sortedSet= treeSetSort;
		}
		return sortedSet;
	}
	
	public slave_masterFloat_DES<V> get1st(){
		return treeSetSort.first();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
