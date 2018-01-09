package MyAPI.Geo.groupDocs;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import MyAPI.General.General;
import MyAPI.General.General_geoRank;
import MyAPI.Interface.DID;
import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr;

/**
 * 
 * @author xinchaoli
 *
 * @param <T> T extends DID, e.g., DID_Score, DID_Score_ImageRegionMatch_ShortArr
 * @param <K> K extends I_LatLon
 */
public class LocListProc <T extends DID> extends GroupListProc<T, LocDocs<T>> {
	
	//user info
	private UserIDs userIDs;
	private boolean isNoSameUser;
	private boolean is1U1P;
	//make location list
	private float[][] latlons;
	private float isOneLocScale;
	//calculate groundTruth
	private float G_ForGTSize;
	
	public LocListProc(UserIDs userIDs, float[][] latlons, String taskLabel,
			float G_ForGTSize, int V_ForGTSize) {
		super(taskLabel, V_ForGTSize);//"_reRGroup@300@1000@0.01@true@false"
		//user info
		this.userIDs=userIDs;
		//make location list
		this.latlons=latlons;
		getLocListParams(taskLabel);
		//calculate groundTruth
		this.G_ForGTSize=G_ForGTSize;
		//check
		if (isNoSameUser || is1U1P) {
			General.Assert(userIDs!=null, "this task is for no-SameUser or 1U1P, userIDs should be no-null!");
		}
	}
	
	public void getLocListParams(String taskLabel){//"_reRGroup@300@1000@0.01@true@false"
		//get params for making location list
		String[] info = taskLabel.split("@");
		isOneLocScale=Float.valueOf(info[3]);
		//get para for user
		this.isNoSameUser=Boolean.valueOf(info[4]);
		this.is1U1P=Boolean.valueOf(info[5]);
	}
	
	public static String setLocListParams(int reRankScale,int topVisScale,float isOneLocScale, boolean isNoSameUser, boolean is1U1P){
		String label=setGroupListParams(reRankScale, topVisScale)+"@"+isOneLocScale+"@"+isNoSameUser+"@"+is1U1P;
		return label;
	}
	
	public void preFilterRankByUser(int queryName, ArrayList<DID_Score_ImageRegionMatch_ShortArr> docs_scores_matches){
		//check same user and delete query itself
		if (isNoSameUser) {//need delete same user, so query itself also deleted!
			General_geoRank.removeSameUser_forTopDocsInfos(docs_scores_matches, queryName, userIDs.userIDs_0, userIDs.userIDs_1);
		}else {//no need to delete same user, but only query itself should be deleted!
			General_geoRank.removeQueryItself_forTopDocsInfos(docs_scores_matches, queryName);
		}
	}
	
	public LatLon getOneDocLoc(int docID){
		return new LatLon(latlons, docID);
	}
	
	@Override
	boolean isCorrectGTruth(int queryID, int docID) {
		return General_geoRank.isOneLocation_GreatCircle(latlons[0][queryID],latlons[1][queryID],latlons[0][docID],latlons[1][docID],G_ForGTSize);
	}

	@Override
	boolean isCorrectGroup(int queryID, LocDocs<T> oneGroup) {
		return General_geoRank.isOneLocation_GreatCircle(oneGroup.latlon.lat,oneGroup.latlon.lon,latlons[0][queryID],latlons[1][queryID],G_ForGTSize);
	}

	@Override
	boolean isSameGroup(int docID, LocDocs<T> oneGroup) {
		return General_geoRank.isOneLocation_approximate(oneGroup.latlon.lat,oneGroup.latlon.lon,latlons[0][docID],latlons[1][docID],isOneLocScale);
	}

	@Override
	LocDocs<T> newOneElementGroup(List<T> temp) {
		return new LocDocs<>(temp, new LatLon(latlons,temp.get(0).getDID()));
	}

	@Override
	public int getQueryReduncy(int queryID) {//geo-reduncy
		return General_geoRank.findGeoNeighbors(queryID, G_ForGTSize, latlons).size();//geo-neighbor num
	}

	@Override
	boolean checkUser(int group_ind, int docID, ArrayList<HashSet<Long>> topLocationUsersList_0, ArrayList<HashSet<Integer>> topLocationUsersList_1) {
		return ((!is1U1P) || topLocationUsersList_0.get(group_ind).add(userIDs.userIDs_0[docID])==true || topLocationUsersList_1.get(group_ind).add(userIDs.userIDs_1[docID])==true);
	}

	@Override
	void upDateGroup(LocDocs<T> oneGroup) throws InterruptedException {//update this group's location
		oneGroup.latlon=new LatLon(General_geoRank.findCenterLoc(oneGroup.docs, latlons));
	}

	@Override
	void addUserForNewGroup(int docID,
			ArrayList<HashSet<Long>> topLocationUsersList_0,
			ArrayList<HashSet<Integer>> topLocationUsersList_1)
			throws InterruptedException {
		//add user
		if (is1U1P) {
			HashSet<Long> users_0=new HashSet<Long>(); HashSet<Integer> users_1=new HashSet<Integer>();
			users_0.add(userIDs.userIDs_0[docID]); users_1.add(userIDs.userIDs_1[docID]);
			topLocationUsersList_0.add(users_0); topLocationUsersList_1.add(users_1);
		}
	}

}
