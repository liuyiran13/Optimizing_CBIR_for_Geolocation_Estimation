package MyAPI.Geo.groupDocs;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import MyAPI.Interface.DID;
import MyAPI.imagR.GTruth;

/**
 * 
 * @author xinchaoli
 *
 * @param <T> T extends DID, e.g., DID_Score, DID_Score_ImageRegionMatch_ShortArr
 * @param <K> K extends I_CartoID
 */
public abstract class GroupListProc <T extends DID, G extends GroupDocs<T>> {
	
	//make group list
	protected boolean isNoGroup;
	protected int reRankScale;
	protected int topVisScale;
	//calculate groundTruth
	protected int V_ForGTSize;
	
	public GroupListProc(String taskLabel, int V_ForGTSize) {
		getGroupListParams(taskLabel);
		//calculate groundTruth
		this.V_ForGTSize=V_ForGTSize;
	}
	
	private void getGroupListParams(String taskLabel){
		//get params for making location list
		reRankScale=Integer.valueOf(taskLabel.split("@")[1]);
		topVisScale=Integer.valueOf(taskLabel.split("@")[2]);
		isNoGroup=(reRankScale<0)?true:false; //each doc is a group
	}
	
	public int getReRankScale(){
		return reRankScale;
	}
	
	public static String setGroupListParams(int reRankScale,int topVisScale){
		String label="_reRGroup@"+reRankScale+"@"+topVisScale;
		return label;
	}
	
	public LinkedList<GTruth> get_topRanked_GTruth(int queryName, List<T> visNeighbors){
		LinkedList<GTruth> gTruth=new LinkedList<GTruth>();
		for (int i = 0; i < Math.min(visNeighbors.size(),V_ForGTSize); i++) {
			int visNeig=visNeighbors.get(i).getDID();
			if(isCorrectGTruth(queryName, visNeig)){
				gTruth.add(new GTruth(i,visNeig));
			}
		}
		if (gTruth.size()==0) {//no ground truth for this query, mark this use {-1,-1}
			gTruth.add(GTruth.NoGTruthMarker);
		}
		return gTruth;
	}
	
	//find rank of the first true math
	public GTruth getFirstTrueMatch(int queryName, LinkedList<G> res){
		int rank=0; int trueMatchPho=0;
		for (G oneGroup : res) {
			if (isCorrectGroup(queryName, oneGroup)) {
				trueMatchPho=oneGroup.docs.get(0).getDID();
				break;
			}
			rank++;
		}
		if (rank<res.size()) {//true match exist
			return new GTruth(rank,trueMatchPho);
		}else{
			return GTruth.NoGTruthMarker;
		}			
	}
	
	//judge each Carto is good or not
	public LinkedList<Boolean> judgeEachGroup(int queryName, List<G> groups){
		LinkedList<Boolean> res=new LinkedList<>();
		for (G oneGroup : groups) {
			if(isCorrectGroup(queryName,oneGroup)){
				res.add(true);
			}else{
				res.add(false);
			}
		}
		return res;
	}
	
	public ArrayList<G> get_topGroupDocList(List<T> topDocs) throws InterruptedException{
		ArrayList<G> res=new ArrayList<G>(); ArrayList<HashSet<Long>> topLocationUsersList_0=new ArrayList<HashSet<Long>>(); ArrayList<HashSet<Integer>> topLocationUsersList_1=new ArrayList<HashSet<Integer>>();		
		if (isNoGroup) {//each doc is a group
			for(int di=0; di<Math.min(topDocs.size(),topVisScale); di++){
				//add doc
				ArrayList<T> temp=new ArrayList<T>(); 
				temp.add(topDocs.get(di));
				G oneGroup=newOneElementGroup(temp);
				//add this oneGroup
				res.add(oneGroup);
			}
		}else{
			for(int di=0; di<Math.min(topDocs.size(),topVisScale); di++){
				T oneDoc=topDocs.get(di);
				int thisDID=oneDoc.getDID();
				boolean docGroupExist=false;
				for(int i=0; i<res.size(); i++){
					G oneGroup=res.get(i);
					if(isSameGroup(thisDID, oneGroup)){//this doc's group already exist
						docGroupExist=true;
						//add this doc to this loc
						if (checkUser(i, thisDID, topLocationUsersList_0, topLocationUsersList_1)) {//check user requirment, e.g., 1 user only contribute 1 doc in one location
							oneGroup.docs.add(oneDoc);
							upDateGroup(oneGroup);
						}
					}
				}
				if(docGroupExist==false && res.size()<reRankScale){
					//add doc
					ArrayList<T> temp=new ArrayList<T>(); 
					temp.add(oneDoc);
					G oneGroup=newOneElementGroup(temp);
					//add this oneGroup
					res.add(oneGroup);
					//add user
					addUserForNewGroup(thisDID, topLocationUsersList_0, topLocationUsersList_1);
				}
			}
		}
		return res;
	}

	abstract boolean isCorrectGTruth(int queryID, int docID);//subclass should implement this method.
		
	abstract boolean isCorrectGroup(int queryID, G oneGroup);//subclass should implement this method.
	
	abstract boolean isSameGroup(int docID, G oneGroup);//subclass should implement this method.
	
	abstract boolean checkUser(int group_ind, int docID, ArrayList<HashSet<Long>> topLocationUsersList_0, ArrayList<HashSet<Integer>> topLocationUsersList_1);
	
	abstract void upDateGroup(G oneGroup) throws InterruptedException;
		
	abstract G newOneElementGroup(List<T> temp);//subclass should implement this method. one 1 doc element in temp, the new group
	
	abstract void addUserForNewGroup(int docID, ArrayList<HashSet<Long>> topLocationUsersList_0, ArrayList<HashSet<Integer>> topLocationUsersList_1) throws InterruptedException;

	public abstract int getQueryReduncy(int queryID);//subclass should implement this method.
}
