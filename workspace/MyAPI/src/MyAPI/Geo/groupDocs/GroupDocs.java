package MyAPI.Geo.groupDocs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import MyAPI.General.General;
import MyAPI.General.General_IR;
import MyAPI.Interface.DID;
import MyAPI.Obj.DID_FeatInds_Score;
import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr;
import MyCustomedHaoop.ValueClass.ImageRegionMatch;

public class GroupDocs <T extends DID>{

	public LinkedList<T> docs;
	
	public GroupDocs(List<T> docs) {
		this.docs=new LinkedList<>(docs);
	}
	
	public static GroupDocs<DID_Score_ImageRegionMatch_ShortArr> orgraniseMatchesForOneGroup(ArrayList<DID_FeatInds_Score> groupMatches, float groupScore) throws InterruptedException{
		HashMap<Integer,ArrayList<ImageRegionMatch>> docID_matches=new HashMap<Integer, ArrayList<ImageRegionMatch>>();
		for (DID_FeatInds_Score one : groupMatches) {
			General.updateMap(docID_matches, one.docID, one.getImageRegionMatch());
		}
		//orgainse by docID
		LinkedList<DID_Score_ImageRegionMatch_ShortArr> dID_score_matches=new LinkedList<DID_Score_ImageRegionMatch_ShortArr>();
		ArrayList<Float> docScores=new ArrayList<>(docID_matches.size()*2);
		for (Entry<Integer, ArrayList<ImageRegionMatch>> one : docID_matches.entrySet()) {
			float docScore=0;
			for (ImageRegionMatch oneMatch : one.getValue()) {
				docScore+=oneMatch.matchScore;
			}
			dID_score_matches.add(new DID_Score_ImageRegionMatch_ShortArr(one.getKey(),docScore,one.getValue()));
			docScores.add(docScore);
//			dID_score_matches.add(new DID_Score_ImageRegionMatch_ShortArr(one.getKey(),groupScore,one.getValue()));
		}
		//sort docs within this loc
		LinkedList<DID_Score_ImageRegionMatch_ShortArr> sortedDocs=new LinkedList<DID_Score_ImageRegionMatch_ShortArr>(); LinkedList<Float> sortedScores=new LinkedList<Float>();
		General_IR.rank_get_AllSortedDocScores_treeSet(dID_score_matches, docScores, sortedDocs, sortedScores, "DES");
		return new GroupDocs<DID_Score_ImageRegionMatch_ShortArr>(sortedDocs);
	}
	
	public static int getMatchNum(GroupDocs<DID_Score_ImageRegionMatch_ShortArr> oneGroup){
		int res=0;
		for (DID_Score_ImageRegionMatch_ShortArr oneDoc : oneGroup.docs) {
			res+=oneDoc.matches.ObjArr.length;
		}
		return res;
	}
	
	public static double[] findMinMaxMatchingScore(List<DID_Score_ImageRegionMatch_ShortArr> docs){
		double[] min_max=new double[]{Double.MAX_VALUE, Integer.MIN_VALUE};
		for (DID_Score_ImageRegionMatch_ShortArr oneDoc : docs) {
			for (ImageRegionMatch oneMatch : oneDoc.matches.ObjArr) {
				min_max[0]=Math.min(min_max[0], oneMatch.matchScore);
				min_max[1]=Math.max(min_max[1], oneMatch.matchScore);
			}
		}
		return min_max;
	}

}
