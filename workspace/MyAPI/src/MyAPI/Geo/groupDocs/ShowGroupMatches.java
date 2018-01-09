package MyAPI.Geo.groupDocs;

import java.io.IOException;
import java.util.LinkedList;

import MyAPI.General.General;
import MyAPI.imagR.ImageDataManager;
import MyAPI.imagR.ShowMatches;
import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr;

public abstract class ShowGroupMatches <G extends GroupDocs<DID_Score_ImageRegionMatch_ShortArr>> extends ShowMatches{

	//data
	public int queryID;
	LinkedList<G> groups;
	Integer[] groupSizes;
	Integer[] groupInds;
	//para
	int maxNumPerGroup;
	
	public ShowGroupMatches(int maxNumPerGroup, boolean isNormalMatchScore, ImageDataManager imageDataManager_Q, ImageDataManager imageDataManager_D, int RGBInd, int pointEnlargeFactor) {
		super(isNormalMatchScore, imageDataManager_Q, imageDataManager_D, RGBInd, pointEnlargeFactor);
		this.maxNumPerGroup=maxNumPerGroup;
	}
	
	public void iniForOneQuery(int queryID){
		this.queryID=queryID; 
		groups=new LinkedList<G>();
	}
	
	public void addOneGroup(G oneGroup){
		groups.add(oneGroup);
	}
	
	public void organiseQueryAllMatches(){
		//first organise the locs to feed the ShowMatches
		LinkedList<DID_Score_ImageRegionMatch_ShortArr> allMatches=new LinkedList<>();
		LinkedList<Integer> groupInds=new LinkedList<Integer>();
		groupSizes=new Integer[groups.size()];
		int loc_i=0;
		for (G oneGroup : groups) {
			int doc_i=0;
			for (DID_Score_ImageRegionMatch_ShortArr oneDoc : oneGroup.docs) {
				if (doc_i<maxNumPerGroup) {
					allMatches.add(oneDoc);
					groupInds.add(loc_i);
				}else{
					break;
				}
				doc_i++;
			}
			groupSizes[loc_i]=oneGroup.docs.size();
			loc_i++;
		}
		this.groupInds=groupInds.toArray(new Integer[0]);
		addOneQueryRes(queryID, allMatches, Integer.MAX_VALUE);
	}
	
	@Override
	protected String getTitle(int pairInd, OnePairOfImages onePairImg){
		int groupInd=groupInds[pairInd];
		return onePairImg.showMatches.size()+" matPoints, Q"+onePairImg.imgID_l+"_D"+onePairImg.imgID_r+", score:"+General.floatArrToString(onePairImg.rankingScore, "_", "0.00")
				+getQInfo()+", "+getGroupInfo(groupInd);
	}
	
	protected String getGroupInfo(int groupInd){//subclass can override this method to add spicific group info, e.g., location, carto
		return "group-"+groupInd+"_docs"+groupSizes[groupInd];
	}
	
	protected abstract String getQInfo();//subclass can override this method to add spicific group info, e.g., location, carto
	
	@Override
	public void disp() throws InterruptedException, IOException{
		organiseQueryAllMatches();
		super.disp();
	}
		
}
