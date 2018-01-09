package MyAPI.imagR;

import java.util.ArrayList;

import MyAPI.General.General;
import MyAPI.Obj.Disp;
import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr;
import MyCustomedHaoop.ValueClass.IntList_FloatList;

public class IndexTrans {

	int[] s_to_l;
	boolean isNeedTranslate;
	
	public IndexTrans(Disp disp, Conf_ImageR conf_ImageR) throws InterruptedException {//for Hadoop
		this(disp, Conf_ImageR.ev_s_to_l);
	}
	
	public IndexTrans(Disp disp, String s_to_l_Path) throws InterruptedException {
		s_to_l=(int[]) General.readObject(s_to_l_Path);
		isNeedTranslate=(s_to_l!=null);
		disp.disp(isNeedTranslate?", s_to_l is loaded":", s_to_l is not loaded");
	}
	
	public boolean isNeedTranslate(){
		return isNeedTranslate;
	}
	
	public int getID(int id_in_S){
		if (isNeedTranslate) {
			return s_to_l[id_in_S];
		}else {
			return id_in_S;
		}
	}
	
	public ArrayList<Integer> translateOneList(ArrayList<Integer> oneRank){
		ArrayList<Integer> res=new ArrayList<>();
		if (isNeedTranslate) {
			for (int i = 0; i < oneRank.size(); i++) {
				res.add(s_to_l[oneRank.get(i)]);
			}
		} else {
			res=oneRank;
		}
		return res;
	}
	
	public IntList_FloatList translateOneList(IntList_FloatList oneRank){
		ArrayList<Integer> rankDocs=translateOneList(oneRank.getIntegers());
		return new IntList_FloatList(rankDocs, oneRank.getFloats());
	}
	
	public ArrayList<DID_Score_ImageRegionMatch_ShortArr> translateOneDocMatchList(ArrayList<DID_Score_ImageRegionMatch_ShortArr> oneRank){
		if (isNeedTranslate) {
			for (int i = 0; i < oneRank.size(); i++) {
				oneRank.get(i).dID_score.docID=s_to_l[oneRank.get(i).dID_score.docID];
			}
		} 
		return oneRank;
	}

}
