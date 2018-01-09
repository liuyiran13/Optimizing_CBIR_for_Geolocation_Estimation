package MyAPI.Geo.groupDocs;

import java.util.LinkedList;


import MyAPI.imagR.GTruth;
import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr;

public class GroupEstResult <G extends GroupDocs<DID_Score_ImageRegionMatch_ShortArr>>{

	public int queryReduncy;
	public int topVisRankedGTruthNum;
	public GTruth firstMatch;	
	public LinkedList<G> res;
	
	public GroupEstResult(int queryReduncy, int topVisRankedGTruthNum, GTruth firstMatch, LinkedList<G> res) {
		this.queryReduncy=queryReduncy;
		this.topVisRankedGTruthNum=topVisRankedGTruthNum;
		this.firstMatch=firstMatch;
		this.res=res;
	}

}
