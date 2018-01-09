package MyAPI.Obj;

import java.util.LinkedList;
import java.util.List;

import MyCustomedHaoop.ValueClass.Int_SURFfeat_ShortArr;
import MyCustomedHaoop.ValueClass.SURFfeat;

public class SelectID {

	public static interface IDForSelection{
		public int getIDForSelection();
	}
	
	public SelectID() {
		
	}
	
	public static <T extends IDForSelection> LinkedList<T> select_in_twoSorted_ASC(int[] targetIDs, List<T> input){//targetIDs and input should be sorted in ASC by ID!
		LinkedList<T> res=new LinkedList<>();
		int lastTargetID=targetIDs[targetIDs.length-1];
		int beginInd=0; //begin index in targetIDs
		for (T one : input) {
			int thisID=one.getIDForSelection();
			if (thisID>lastTargetID) {
				break;
			}
			for (int i = beginInd; i < targetIDs.length; i++) {//< then stop, and update beginInd to current position of bblong
				if (thisID<targetIDs[i]) {//stop loop in targetIDs, input should loop to next one
					beginInd=i;
					break;
				}else if (thisID==targetIDs[i]) {// == then stop, and add this element, and update beginInd to the next position of bblong
					res.add(one);
					beginInd=i+1;//next one
					break;
				}
			}
		}
		return res;
	}
	
	public static void main(String[] args){
		int[] targetIDs=new int[]{6};
		LinkedList<Int_SURFfeat_ShortArr> input=new LinkedList<>();
		for (int i = 0; i < 10; i++) {
			input.add(new Int_SURFfeat_ShortArr(i, new SURFfeat[]{}, null));
		}
		System.out.println(select_in_twoSorted_ASC(targetIDs, input));
	}

}
