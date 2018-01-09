package MyAPI.Geo.groupDocs;

import MyAPI.imagR.ImageDataManager;
import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr;

public class ShowLocMatches extends ShowGroupMatches<LocDocs<DID_Score_ImageRegionMatch_ShortArr>> {
	//common data
	float[][] latlons;
	//data per query
	LatLon[] latlons_perLoc;
	LatLon Q_latlon;
	
	public ShowLocMatches(int maxNumPerLoc, boolean isNormalMatchScore, ImageDataManager imageDataManager_Q, ImageDataManager imageDataManager_D, int RGBInd, int pointEnlargeFactor, 
			float[][] latlons) {
		super(maxNumPerLoc, isNormalMatchScore, imageDataManager_Q, imageDataManager_D, RGBInd, pointEnlargeFactor);
		this.latlons=latlons;
	}
	
	@Override
	public void iniForOneQuery(int queryID){
		super.iniForOneQuery(queryID);
		this.Q_latlon=new LatLon(latlons, queryID); 
	}
	
	@Override
	public void organiseQueryAllMatches(){
		super.organiseQueryAllMatches();
		latlons_perLoc=LocDocs.getLocationList(super.groups).toArray(new LatLon[0]);
	}
	
	@Override
	protected String getGroupInfo(int groupInd){//subclass can override this method to add spicific group info, e.g., location, carto
		return super.getGroupInfo(groupInd)+", latlon:"+latlons_perLoc[groupInd]+", "+Q_latlon.getDistStrInKm(latlons_perLoc[groupInd], "0.0");
	}
	
	@Override
	protected String getQInfo(){//subclass can override this method to add spicific Q info, e.g., location, carto
		return "QLoc: "+Q_latlon;
	}

}
