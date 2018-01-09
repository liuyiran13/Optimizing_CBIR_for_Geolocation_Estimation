package MyAPI.Obj;

import java.util.HashMap;
import java.util.HashSet;

import MyAPI.General.General;

public class SelSample {
	
	public static class isContain_resID{
		public boolean isContain;
		public int resID;
		
		public isContain_resID(boolean isContain, int resID){
			this.isContain=isContain;
			this.resID=resID;
		}
	}
	
	boolean isSelection;
	boolean isHashMap;
	HashMap<Integer, Integer> hashMap;
	HashSet<Integer> hashSet;
	
	public SelSample(HashMap<Integer, Integer> hashMap, HashSet<Integer> hashSet, Disp disp) {
		if (hashMap==null && hashSet==null) {
			isSelection=false;
		}else {
			isSelection=true;
			if (hashMap!=null) {
				isHashMap=true;
				this.hashMap=hashMap;
				General.Assert(hashSet==null, "hashMap and hashSet cannot both exist!");
			}else {
				isHashMap=false;
				this.hashSet=hashSet;
				General.Assert(hashMap==null, "hashMap and hashSet cannot both exist!");
			}
		}
		disp.disp("conf SelSample done! isSelection:"+isSelection+", isHashMap:"+isHashMap);
	}
	
	public isContain_resID isContainThisSamp(int sampID){
		if (isSelection) {
			if (isHashMap) {
				Integer val=hashMap.get(sampID);
				if (val==null) {
					return new isContain_resID(false, sampID);
				}else {
					return new isContain_resID(true, val);
				}
			}else {
				if (hashSet.contains(sampID)) {
					return new isContain_resID(true, sampID);
				}else {
					return new isContain_resID(false, sampID);
				}
			}
		}else {
			return new isContain_resID(true, sampID);
		}
	}

}
