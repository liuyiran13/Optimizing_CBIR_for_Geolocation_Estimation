package MyAPI.imagR;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Writable;

import MyAPI.General.General_geoRank;
import MyAPI.Geo.groupDocs.LocDocs;
import MyAPI.Interface.DID;
import MyCustomedHaoop.ValueClass.DID_Score;

public class GTruth implements Writable{
	
	public static final GTruth NoGTruthMarker=new GTruth(-1,-1); //no ground truth for this query, mark this use {-1,-1}
	public int rank;
	public int photoID;
	
	public GTruth(int rank, int photoID) {
		this.rank=rank;
		this.photoID=photoID;
	}
	
	public GTruth() {
		//do nothing, just for hadoop read/write 
	}
	
	public int[] getIntArr(){
		return new int[]{rank,photoID};
	}
	
	public boolean equals(GTruth that){
		if (rank==that.rank && photoID==that.photoID) {
			return true;
		}else {
			return false;
		}
	}
	
	@Override
	public String toString(){
		return rank+"_"+photoID;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		rank=in.readInt();
		photoID=in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(rank);
		out.writeInt(photoID);
	}
	
	public static LinkedList<int[]> getIntArrFormat(List<GTruth> target){
		LinkedList<int[]> res=new LinkedList<>();
		for (GTruth one : target) {
			res.add(one.getIntArr());
		}
		return res;
	}
	
	public static LinkedList<Integer> getRanks(List<GTruth> target){
		if (isExistGTruth(target)) {
			LinkedList<Integer> res=new LinkedList<>();
			for (GTruth one : target) {
				res.add(one.rank);
			}
			return res;
		}else{
			return null;
		}
	}
	
	public static int getGTSize(List<GTruth> target){
		int grounTSize=target.size(); 
		if (grounTSize==1 && target.get(0).equals(NoGTruthMarker)) {//if query do not have ground truth, then in GVR, it mark this with {-1,-1}
			grounTSize=0;
		}
		return grounTSize;
	}
	
	public static boolean isExistGTruth(List<GTruth> target){
		return getGTSize(target)>0;
	}
	
	public static GTruth get_firstTrueMatch(int queryName,ArrayList<Integer> topDocs, float isSameLocScale, float[][] latlons) {	
		int trueLocRank=General_geoRank.get_trueLocRank(queryName, topDocs, topDocs.size(), isSameLocScale, latlons);
		if(trueLocRank!=-1) //trueLocRank==-1 means true match do not exist in the doc list.
			return new GTruth(trueLocRank, topDocs.get(trueLocRank));
		else {
			return GTruth.NoGTruthMarker;
		}
	}
	
	public static <T extends DID> GTruth get_firstTrueMatch(int queryName, ArrayList<ArrayList<T>> topDocs, ArrayList<float[]> topLocations, float isSameLocScale, float[][] latlons) {	
		int trueLocRank=General_geoRank.get_trueLocRank(topLocations, queryName, isSameLocScale, latlons, topLocations.size());
		if(trueLocRank!=-1) //trueLocRank==-1 means true match do not exist in the doc list.
			return new GTruth(trueLocRank, topDocs.get(trueLocRank).get(0).getDID());
		else {
			return GTruth.NoGTruthMarker;
		}
	}
	
	public static <T extends DID> GTruth get_firstTrueMatch(int queryName, LinkedList<LocDocs<DID_Score>> res, float isSameLocScale, float[][] latlons) {	
		int trueLocRank=General_geoRank.get_trueLocRankG(res, queryName, isSameLocScale, latlons,res.size());
		if(trueLocRank!=-1) //trueLocRank==-1 means true match do not exist in the doc list.
			return new GTruth(trueLocRank, res.get(trueLocRank).docs.get(0).getDID());
		else {
			return GTruth.NoGTruthMarker;
		}
	}
}
