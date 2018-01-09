package MyAPI.Obj;

import java.util.ArrayList;
import java.util.HashMap;
import MyAPI.General.General;

public class Pair_int {
	public int A;
	public int B;
	
	public Pair_int(int A, int B) {
		super();
		this.A = A;
		this.B = B;
	}
	
	public String toString() {
		return A+"-"+B;
	}
	
	public static HashMap<Integer,Integer> CountPointFreq(ArrayList<Pair_int> pairs) {
		HashMap<Integer,Integer> unique=new HashMap<Integer,Integer>();
		for (Pair_int one : pairs) {
			General.updateMap(unique, one.A, 1);
			General.updateMap(unique, one.B, 1);
		}
		return unique;
	}
}
