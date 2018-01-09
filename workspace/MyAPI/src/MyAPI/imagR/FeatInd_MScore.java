package MyAPI.imagR;

import MyAPI.Interface.FeatInd_Score;

public class FeatInd_MScore implements FeatInd_Score{

	public int featInd;
	public float score;
	
	public FeatInd_MScore(int featInd, float score) {
		this.featInd=featInd;
		this.score=score;
	}

	@Override
	public int getFeatInd() {
		return featInd;
	}

	@Override
	public float getScore() {
		return score;
	}

}
