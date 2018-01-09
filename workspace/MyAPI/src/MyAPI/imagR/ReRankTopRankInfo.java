package MyAPI.imagR;

import MyAPI.General.General;
import MyAPI.General.General_IR;

public class ReRankTopRankInfo {

	int[] reRanks;
	int[] topRanks;
	
	public ReRankTopRankInfo(String reRank, String topRank) {
		reRanks=General.StrArrToIntArr(reRank.split(","));
		topRanks=General.StrArrToIntArr(topRank.split(","));
		General.Assert((topRanks.length==reRanks.length) || (topRanks.length==1), "err! topRanks.lenght should ==1 or reRank.length");
	}
	
	public int getTopRank(int ind){
		if (topRanks.length==1) {
			return topRanks[0];
		}else{
			return topRanks[ind];
		}
	}
	
	public int getTopRank_IniRank(){
		return General.getMax_ind_val(topRanks)[1];
	}
	
	public int getReRank(int ind){
		return reRanks[ind];
	}
	
	public int getMaxIniRankLength(){
		return Math.max(General.getMax_ind_val(topRanks)[1], General.getMax_ind_val(reRanks)[1]);
	}
		
	public String getTopRankLabel(int ind){
		return "_Top"+General_IR.makeNumberLabel(getTopRank(ind),"0");
	}
	
	public String getTopRankLabel_IniRank(){
		return "_Top"+General_IR.makeNumberLabel(getTopRank_IniRank(),"0");
	}
	
	public String getReRankLabel(int ind){
		return "_ReR"+General_IR.makeNumberLabel(getReRank(ind),"0");
	}
	
	public int getReRankNum(){
		return reRanks.length;
	}
	
	public int getTopRankNum(){
		return topRanks.length;
	}
	
	public String getReRanksInStr(String delimiter){
		return General.IntArrToString(reRanks, delimiter);
	}
	
	public String getTopRanksInStr(String delimiter){
		return General.IntArrToString(topRanks, delimiter);
	}

}
