package MyAPI.Obj;

import MyAPI.Interface.FeatInd_Score;
import MyCustomedHaoop.ValueClass.ImageRegionMatch;

public class DID_FeatInds_Score implements FeatInd_Score {
	public int docID;
	public int featInd_Q;//feat in query
	public int blockInd_Q;//feat in query
	public int featInd_D_loc;//feat in doc, its ind in this doc
	public int featInd_D_global;//feat in doc, its ind in all docs
	public float matchingScore;
	public float IDFsquare;
	
	public DID_FeatInds_Score(int docID, int featInd_query, int blockInd_Q, int featInd_loc, int featInd_global, float matchingScore, float IDFsquare) {
		super();
		this.docID = docID;
		this.featInd_Q = featInd_query;
		this.blockInd_Q = blockInd_Q;
		this.featInd_D_loc = featInd_loc;
		this.featInd_D_global = featInd_global;
		this.matchingScore = matchingScore;
		this.IDFsquare=IDFsquare;
	}
	
	public String toString() {
		return "docID:"+docID+", featInd_Q:"+featInd_Q+", blockInd_Q:"+blockInd_Q+", featInd_D_loc:"+featInd_D_loc+", featInd_D_global:"+featInd_D_global+", matchingScore:"+matchingScore+", IDFsquare:"+IDFsquare;
	}

	@Override
	public int getFeatInd() {//use featInd_global as the global comparable feat ID
		return featInd_D_global;
	}

	@Override
	public float getScore() {
		return matchingScore*IDFsquare;
	}
	
	public ImageRegionMatch getImageRegionMatch(){
		return new ImageRegionMatch(featInd_Q,featInd_D_loc,getScore());
	}
}
