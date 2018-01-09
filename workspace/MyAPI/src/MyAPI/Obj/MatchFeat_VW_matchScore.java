package MyAPI.Obj;

import MyAPI.Interface.FeatInd_Score;
import MyCustomedHaoop.ValueClass.MatchFeat_VW;


/**
 * Indexes of two associated features.
 *
 */
public class MatchFeat_VW_matchScore implements FeatInd_Score{

	public MatchFeat_VW matchFeat_VW;
	public float matchScore;

	public MatchFeat_VW_matchScore( MatchFeat_VW matchFeat_VW, float matchScore) {
		this.matchFeat_VW = matchFeat_VW;
		this.matchScore = matchScore;
	}
	
	public String toString() {
		return "MatchFeat_VW:"+matchFeat_VW.toString()+", matchScore:"+matchScore;
	}

	@Override
	public int getFeatInd() {
		return matchFeat_VW.docFeat.getFeatInd();
	}

	@Override
	public float getScore() {
		return matchScore;
	}
}