package MyAPI.Obj;

import java.util.LinkedList;

import MyCustomedHaoop.ValueClass.Int_MatchFeatArr;

/**
 * used for merge match results from different vws
 *
 */
public class Score_MatchFeats{
	public float docScore;
	public LinkedList<Int_MatchFeatArr> feats;

	public Score_MatchFeats() {
		this.docScore = 0;
		this.feats = new LinkedList<Int_MatchFeatArr>();
	}
	
	public void mergeOneVWMatchGroup(float score, Int_MatchFeatArr oneVWMatches) {
		this.docScore+= score;
		if (oneVWMatches.feats.getArr().length>0) {//in some case, e.g., ASMK, for one vw, doc has score but do not have HE matches
			this.feats.add(oneVWMatches);
		}
	}
	
	public static Score_MatchFeats[] makeNewArray(int cap){
		Score_MatchFeats[] merge=new Score_MatchFeats[cap];
		for (int i = 0; i < merge.length; i++) {
			merge[i]=new Score_MatchFeats();
		}
		return merge;
	}
}
