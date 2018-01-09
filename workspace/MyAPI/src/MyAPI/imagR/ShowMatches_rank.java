package MyAPI.imagR;

import java.util.List;

import MyAPI.General.General;
import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr;

public class ShowMatches_rank extends ShowMatches{

	//para
	int maxShowNum;
	//data
	int iniSelRankDocNum;
	List<Integer> selectedRanks_ini;
	
	
	public ShowMatches_rank(int maxShowNum, boolean isNormalMatchScore,
			ImageDataManager imageDataManager_Q,
			ImageDataManager imageDataManager_D, int RGBInd,
			int pointEnlargeFactor) {
		super(isNormalMatchScore, imageDataManager_Q, imageDataManager_D, RGBInd,
				pointEnlargeFactor);
		this.maxShowNum=maxShowNum;
	}
	
	public void addOneQuerySelRanks(int queryID, DID_Score_ImageRegionMatch_ShortArr[] rank_matches, List<Integer> selectedRanks_ini) throws InterruptedException{
		if (selectedRanks_ini!=null) {
			iniSelRankDocNum=selectedRanks_ini.size();
			this.selectedRanks_ini=selectedRanks_ini;
			addOneQueryRes(queryID, General.selectArr(rank_matches, General.ListToIntArr(selectedRanks_ini), 0), maxShowNum);
		}
	}
	
	@Override
	protected String getTitle(int pairInd, OnePairOfImages onePairImg){
		return "Q"+onePairImg.imgID_l+"_iniDNum"+iniSelRankDocNum+"_R"+selectedRanks_ini.get(pairInd)+": D"+onePairImg.imgID_r+", "+onePairImg.showMatches.size()+" matching points, finalScore:"+General.floatArrToString(onePairImg.rankingScore, "_", "0.00");
	}

}
