package MyAPI.Geo;

import MyAPI.General.General;
import MyAPI.Obj.MakeHist_GVR;

public abstract class GVM_Evaluator {
	
	public int workModel;
	//**** common
	//para
	protected int num_evalRadius;
	public int num_topGroups;
	private int[] accumLevel;
	//data
	private int[][] totlocHist_reranks;
	private int queryNums;
	//**** for workModel_2: DivideQ
	//data
	private MakeHist_GVR makeHist_forGTSize;
	private MakeHist_GVR makeHist_forTopGTSize;
	private int maxGTSize, maxTopGTSize;
	private int maxGTSize_QueryName, maxTopGTSize_QueryName;
	
	abstract void set_num_evalRadius();
	
	void initialise(String evalFlag){
		String[] infos=evalFlag.split("@");
		//workModel
		if (infos[0].equalsIgnoreCase("noDivideQ")) { //evalFlag: noDivideQ@1,3,5,10
			workModel=1;//evaluate the whole query set without differentiating them
		}else if (infos[0].equalsIgnoreCase("DivideQ")) { //evalFlag: DivideQ@1,3,5,10@0,1,5,10,20,40,100@0,1,5,10,20,40,100
			workModel=2;//evaluate the whole query set + differentiating them by GTSize, GeoRedun
		}
		//accumLevel
		accumLevel=General.StrArrToIntArr(infos[1].split(","));
		
		//******* set num_evalRadius  **************
		set_num_evalRadius();

		this.num_topGroups=accumLevel[accumLevel.length-1];

		//**** set totlocHist_rerank ************//
		totlocHist_reranks=new int[num_evalRadius][num_topGroups+1]; //fist one is "not in top"
		
		//set queryNums
		queryNums=0;
				
		if (workModel==2) {
			//******* setup makeHist_forGTSize **************
			makeHist_forGTSize=new MakeHist_GVR(infos[2], num_evalRadius, num_topGroups);
			makeHist_forTopGTSize=new MakeHist_GVR(infos[3], num_evalRadius, num_topGroups);
			//******* set maxGeoRed, maxGTSize
			maxGTSize=0; maxTopGTSize=0;
		}
	}
	
	public void clearData(){
		totlocHist_reranks=new int[num_evalRadius][num_topGroups+1]; //fist one is "not in top"
		queryNums=0;
		if (workModel==2) {
			makeHist_forGTSize.iniData();
			makeHist_forTopGTSize.iniData();
			maxGTSize=0; maxTopGTSize=0;
		}
	}
	
	public void addOneQueryRank(int queryName, int[] trueGroupRanks, int grounTSize, int topGrounTSize){
		if (workModel==1) {
			addOneQueryRank_1(trueGroupRanks);
		}else {
			addOneQueryRank_2(queryName, trueGroupRanks, grounTSize, topGrounTSize);
		}
	}
	
	private void addOneQueryRank_1(int[] trueGroupRanks){
		//******* analysis this query's result by different evaluation radius *********************
		for (int i = 0; i < num_evalRadius; i++) {
			//set totlocHist
			totlocHist_reranks[i][trueGroupRanks[i]]++;
		}
		queryNums++;
	}
	
	
	private void addOneQueryRank_2(int queryName, int[] trueGroupRanks, int grounTSize, int topGrounTSize){
		//******* analysis this query's result by different evaluation radius *********************
		for (int i = 0; i < num_evalRadius; i++) {
			//get True-Location rank
			int trueLocRank=trueGroupRanks[i];
			//set groTSize_locHist_Group
			makeHist_forGTSize.addOneSample(grounTSize, i, trueLocRank);
			makeHist_forTopGTSize.addOneSample(topGrounTSize, i, trueLocRank);
			//set totlocHist
			totlocHist_reranks[i][trueLocRank]++;
		}
		//update maxGTSize
		if (maxGTSize<grounTSize) {
			maxGTSize=grounTSize;
			maxGTSize_QueryName=queryName;
		}
		if (maxTopGTSize<topGrounTSize) {
			maxTopGTSize=topGrounTSize;
			maxTopGTSize_QueryName=queryName;
		}
		queryNums++;
	}
	
	abstract String getEvalRadiusTitle(int evalRadInd);
	
	abstract String getQueryTrueGroupInfo(int queryID);
	
	public String getEvalationRes(int actQNum){
		int queryNumEva=(actQNum==-1)?queryNums:actQNum;
		//outPut as String
		StringBuffer outInfo=new StringBuffer();
		outInfo.append("queryNumEva:"+queryNumEva+", queryNumsExistRanks:"+queryNums+", accumLevel:"+"\t"+General.IntArrToString(accumLevel, "\t")+"\n");
		for (int i = 0; i < num_evalRadius; i++) {
			outInfo.append(getEvalRadiusTitle(i)+"\n");
			if (workModel==2) {
				// ** compute accumulated TrueLocHist for grouped grounTSize ***//
				outInfo.append(makeHist_forGTSize.makeRes(i, accumLevel, "******** Group grounTSize:  accumulated-TrueLocHist: \n",queryNumEva));
				// ** compute accumulated TrueLocHist for grouped topGrounTSize ***//
				outInfo.append(makeHist_forTopGTSize.makeRes(i, accumLevel, "******** Group topGrounTSize:  accumulated-TrueLocHist: \n",queryNumEva));
			}
			// ** compute accumulated TrueLocHist for totlocHist_ori, totlocHist_rerank ***//
			int[] totlocHistAccu_rerank=General.makeAccum(accumLevel, totlocHist_reranks[i]); //no "not in top" querys, so when compute percent, should ./ totalQueryNum
			outInfo.append("******** total "+queryNumEva+" querys:  accumulated-TrueLocHist: \n");
			outInfo.append(General.floatArrToString(General.normliseArr(totlocHistAccu_rerank, queryNumEva), "\t", "0.0000")+"\n\n");
		}
		if (workModel==2) {
			outInfo.append("******** maxGTSize:"+maxGTSize+", maxGTSize_QueryName:"+maxGTSize_QueryName+", its Info: "+getQueryTrueGroupInfo(maxGTSize_QueryName));
			outInfo.append("******** maxTopGTSize:"+maxTopGTSize+", maxTopGTSize_QueryName:"+maxTopGTSize_QueryName+", its Info: "+getQueryTrueGroupInfo(maxTopGTSize_QueryName));
		}
		return outInfo.toString()+"\n\n";
	}

}
