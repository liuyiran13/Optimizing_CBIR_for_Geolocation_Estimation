package MyAPI.Geo.groupDocs;

import java.util.HashSet;

import MyAPI.imagR.ImageDataManager;
import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr;

public class ShowCartoMatches extends ShowGroupMatches<CartoDocs<DID_Score_ImageRegionMatch_ShortArr>> {
	//common data
	HashSet<Integer>[] cartoIDs_allQ;
	//data per query
	Integer[] Cartos_perGroup_thisQ;
	HashSet<Integer> cartoIDs_thisQ;
	
	public ShowCartoMatches(int maxNumPerLoc, boolean isNormalMatchScore, ImageDataManager imageDataManager_Q, ImageDataManager imageDataManager_D, int RGBInd, int pointEnlargeFactor, 
			HashSet<Integer>[] cartoIDs_allQ) {
		super(maxNumPerLoc, isNormalMatchScore, imageDataManager_Q, imageDataManager_D, RGBInd, pointEnlargeFactor);
		this.cartoIDs_allQ=cartoIDs_allQ;
	}
	
	@Override
	public void iniForOneQuery(int queryID){
		super.iniForOneQuery(queryID);
		Cartos_perGroup_thisQ=null;
		cartoIDs_thisQ=cartoIDs_allQ[queryID];
	}
	
	@Override
	public void organiseQueryAllMatches(){
		super.organiseQueryAllMatches();
		Cartos_perGroup_thisQ=CartoDocs.getCartoList(super.groups).toArray(new Integer[0]);
	}
	
	@Override
	protected String getGroupInfo(int groupInd){//subclass can override this method to add spicific group info, e.g., location, carto
		return super.getGroupInfo(groupInd)+", cartoID:"+Cartos_perGroup_thisQ[groupInd];
	}
	
	@Override
	protected String getQInfo(){//subclass can override this method to add spicific Q info, e.g., location, carto
		return "QCartoIDs: "+cartoIDs_thisQ;
	}
	

}
