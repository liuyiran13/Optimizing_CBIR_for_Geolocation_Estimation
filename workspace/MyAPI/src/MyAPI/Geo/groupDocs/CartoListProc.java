package MyAPI.Geo.groupDocs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import MyAPI.Interface.DID;

/**
 * 
 * @author xinchaoli
 *
 * @param <T> T extends DID, e.g., DID_Score, DID_Score_ImageRegionMatch_ShortArr
 * @param <K> K extends I_CartoID
 */
public class CartoListProc <T extends DID> extends GroupListProc<T, CartoDocs<T>> {
	
	//make cartography list
	private HashSet<Integer>[] cartoIDs_Q;
	private int[] cartoIDs_db;
	private HashMap<Integer, HashSet<Integer>> groundTruth;
	
	public CartoListProc(HashSet<Integer>[] cartoIDs_Q, int[] cartoIDs_db, HashMap<Integer, HashSet<Integer>> groundTruth, String taskLabel, int V_ForGTSize) {
		super(taskLabel, V_ForGTSize);
		this.cartoIDs_Q=cartoIDs_Q;
		this.cartoIDs_db=cartoIDs_db;
		this.groundTruth=groundTruth;
	}
	
	public int getOneDocCarto(int docID){
		return cartoIDs_db[docID];
	}
	
	@Override
	boolean isCorrectGTruth(int queryID, int docID) {
		return cartoIDs_Q[queryID].contains(cartoIDs_db[docID]);
	}

	@Override
	boolean isCorrectGroup(int queryID, CartoDocs<T> oneGroup) {
		return cartoIDs_Q[queryID].contains(oneGroup.cartoID);
	}

	@Override
	boolean isSameGroup(int docID, CartoDocs<T> oneGroup) {
		return cartoIDs_db[docID]==oneGroup.cartoID;
	}

	@Override
	CartoDocs<T> newOneElementGroup(List<T> temp) {
		return new CartoDocs<>(temp, cartoIDs_db[temp.get(0).getDID()]);
	}

	@Override
	public int getQueryReduncy(int queryID) {//db photo num that belongs to the query's carto
		return groundTruth.get(queryID).size();
	}

	@Override
	boolean checkUser(int group_ind, int docID, ArrayList<HashSet<Long>> topLocationUsersList_0, ArrayList<HashSet<Integer>> topLocationUsersList_1) {
		return true;//no user info considered for CartoListProc
	}

	@Override
	void upDateGroup(CartoDocs<T> oneGroup) {
		//do nothing
	}

	@Override
	void addUserForNewGroup(int docID,
			ArrayList<HashSet<Long>> topLocationUsersList_0,
			ArrayList<HashSet<Integer>> topLocationUsersList_1)
			throws InterruptedException {
		//do nothing
	}

}
