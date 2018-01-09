package MyAPI.imagR;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;
import MyAPI.Geo.groupDocs.ShowLocMatches;
import MyAPI.Obj.Disp;
import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr;
import MyCustomedHaoop.ValueClass.ImageRegionMatch;
import MyCustomedHaoop.ValueClass.SURFpoint;

public class ShowMatches {
	
	public static class OnePairOfImages{
		public int imgID_l;
		public int imgID_r;
		public List<ImageRegionMatch> showMatches;
		public float[] rankingScore;
		public LinkedList<SURFpoint> selPoints_l;//not used yet
		public LinkedList<SURFpoint> selPoints_r;
		
		public OnePairOfImages(int imgID_l, int imgID_r, List<ImageRegionMatch> showMatches, LinkedList<SURFpoint> selPoints_l, LinkedList<SURFpoint> selPoints_r, float[] rankingScore){
			this.imgID_l=imgID_l;
			this.imgID_r=imgID_r;
			this.showMatches=showMatches;
			this.selPoints_l=selPoints_l;
			this.selPoints_r=selPoints_r;
			this.rankingScore=rankingScore;
		}
		
		public double[] findMinMaxMatchingScore(){
			double[] min_max=new double[]{Double.MAX_VALUE, Integer.MIN_VALUE};
			for (ImageRegionMatch oneMatch : showMatches) {
				min_max[0]=Math.min(min_max[0], oneMatch.matchScore);
				min_max[1]=Math.max(min_max[1], oneMatch.matchScore);
			}
			return min_max;
		}
	}
	
	//data
	LinkedList<OnePairOfImages> imgPairs;
	//para
	protected boolean isNormalMatchScore;
	protected ImageDataManager imageDataManager_Q;
	protected ImageDataManager imageDataManager_D;
	protected int RGBInd;
	protected int pointEnlargeFactor;
	
	public ShowMatches(boolean isNormalMatchScore, ImageDataManager imageDataManager_Q, ImageDataManager imageDataManager_D, int RGBInd, int pointEnlargeFactor) {
		imgPairs=new LinkedList<OnePairOfImages>();
		this.isNormalMatchScore=isNormalMatchScore;
		this.imageDataManager_Q=imageDataManager_Q;
		this.imageDataManager_D=imageDataManager_D;
		this.RGBInd=RGBInd; 
		this.pointEnlargeFactor=pointEnlargeFactor;
	}
	
	public ShowMatches(ShowLocMatches copy){
		this(copy.isNormalMatchScore,copy.imageDataManager_Q,copy.imageDataManager_D,copy.RGBInd,copy.pointEnlargeFactor);
	}
	
	public void addOneImgPair(OnePairOfImages onePairOfImages){
		imgPairs.add(onePairOfImages);
	}
	
	public void addOneQueryRes(int queryID, List<DID_Score_ImageRegionMatch_ShortArr> docs_scores_matches, int maxShow){
		imgPairs.clear();
		int maxToShow=Math.min(maxShow, docs_scores_matches.size()); int num=0;
		for (DID_Score_ImageRegionMatch_ShortArr one : docs_scores_matches) {
			if (num<maxToShow) {
				imgPairs.add(new OnePairOfImages(queryID, one.getDID(), one.getMatches(), null, null, new float[]{one.getScore(),0f}));
			}else {
				break;
			}
			num++;
		}
	}
	
	public void iniData(){
		imgPairs.clear();
	}
	
	public void disp() throws InterruptedException, IOException {
		double[] min_max_global=null;
		if(isNormalMatchScore){
			//find min and max for normalize match strength
			min_max_global=new double[]{Double.MAX_VALUE, Integer.MIN_VALUE};
			for (OnePairOfImages onePairImg : imgPairs) {
				for (ImageRegionMatch oneMatch : onePairImg.showMatches) {
					min_max_global[0]=Math.min(min_max_global[0], oneMatch.matchScore);
					min_max_global[1]=Math.max(min_max_global[1], oneMatch.matchScore);
				}
			}
		}
		//show matches
		Disp disp=new Disp(false,"",null); int pairInd=0;
		for (OnePairOfImages onePairImg : imgPairs) {
			int QID=onePairImg.imgID_l; int DID=onePairImg.imgID_r;
			double[] min_max_local=null;
			if(isNormalMatchScore){
				min_max_local=min_max_global;
			}else{
				min_max_local=onePairImg.findMinMaxMatchingScore();
			}
			String title=getTitle(pairInd, onePairImg);
			General_BoofCV.showMatchingPoint(imageDataManager_Q.getPhoFeat(QID, disp).interestPoints, imageDataManager_D.getPhoFeat(DID, disp).interestPoints, onePairImg.showMatches, 
					imageDataManager_Q.getImage(QID, disp), imageDataManager_D.getImage(DID, disp), title, RGBInd, min_max_local, pointEnlargeFactor);
			pairInd++;
		}
	}
	
	protected String getTitle(int pairInd, OnePairOfImages onePairImg){
		return onePairImg.showMatches.size()+" matching points, Q"+onePairImg.imgID_l+"_D"+onePairImg.imgID_r+", finalScore:"+General.floatArrToString(onePairImg.rankingScore, "_", "0.00");
	}

}
