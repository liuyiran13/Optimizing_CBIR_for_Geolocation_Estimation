package MyAPI.Obj;

/**
 * 
 * Store matching score of each match between two images
 *
 */
public class MatchingScore {
	
	public float[] mastScore;
	public float[] seconderyScore;
	public int activeMatchNum_inMastScore;
	
	public MatchingScore(float[] mastScore, float[] seconderyScore, int activeMatchNum_inMastScore) {
		this.mastScore=mastScore;
		this.seconderyScore=seconderyScore;
		this.activeMatchNum_inMastScore=activeMatchNum_inMastScore;
	}

}
