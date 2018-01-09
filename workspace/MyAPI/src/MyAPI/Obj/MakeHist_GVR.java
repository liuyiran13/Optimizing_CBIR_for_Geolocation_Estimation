package MyAPI.Obj;

import java.util.ArrayList;
import java.util.TreeMap;

import MyAPI.General.General;

public class MakeHist_GVR {
	//para
	private int[] binsForGTSize;
	private int num_evalRadius;
	private int num_topLocations;
	//data
	private ArrayList<TreeMap<Integer,int[]>>  groTSize_locHist_rerank_Groups;
	
	public MakeHist_GVR(String binsForGTSizeString, int num_evalRadius, int num_topLocations) {
		super();
		this.binsForGTSize=General.StrArrToIntArr(binsForGTSizeString.split(","));// grounTSize bins, 0,1,5,10,20,40,100
		this.num_evalRadius=num_evalRadius;
		this.num_topLocations=num_topLocations;
		iniData();
	}
	
	public void iniData(){
		this.groTSize_locHist_rerank_Groups= new ArrayList<TreeMap<Integer,int[]>>(num_evalRadius);//groTSize, histogram of trueLoactin position, fist one is "not in top"
		for (int i = 0; i < num_evalRadius; i++) {
			groTSize_locHist_rerank_Groups.add(new TreeMap<Integer, int[]>());
		}
	}
	
	public void addOneSample(int grounTSize, int ind_evalRadius, int trueLocRank){
		//set groTSize_locHist_Group
		int binInd=General.getBinInd_linear(binsForGTSize,grounTSize);
		if(groTSize_locHist_rerank_Groups.get(ind_evalRadius).containsKey(binInd)){
			groTSize_locHist_rerank_Groups.get(ind_evalRadius).get(binInd)[trueLocRank]++;
		}else{
			int[] queryNum_locHist =new int[num_topLocations+1];//fist one is "not in top"
			queryNum_locHist[trueLocRank]=1;
			groTSize_locHist_rerank_Groups.get(ind_evalRadius).put(binInd, queryNum_locHist);
		}
	}
	
	public String makeRes(int ind_evalRadius, int[] accumLevel, String title, int actQNum) {
		// ** compute accumulated TrueLocHist for grouped grounTSize ***//
		StringBuffer outInfo=new StringBuffer();
		outInfo.append(title); // "******** Group grounTSize:  accumulated-TrueLocHist: \n"
		int QNumExistRes=0;
		for(int binIndex:groTSize_locHist_rerank_Groups.get(ind_evalRadius).keySet()){
			int[] trueLocHist_rerank=groTSize_locHist_rerank_Groups.get(ind_evalRadius).get(binIndex);
			int qureyNum=General.sum_IntArr(trueLocHist_rerank);
			int[] trueLocHistAccu_rerank=General.makeAccum(accumLevel, trueLocHist_rerank);
			if(binIndex==0){//groundTruth size==0
				outInfo.append(0+"\t"+qureyNum+"\t"+General.floatArrToString(General.normliseArr(trueLocHistAccu_rerank, qureyNum), "\t", "0.0000")+"\n");
			}else{
				if(binIndex==binsForGTSize.length){//groundTruth size > bins' last value
					outInfo.append(">"+binsForGTSize[binsForGTSize.length-1]+"\t"+qureyNum+"\t"+General.floatArrToString(General.normliseArr(trueLocHistAccu_rerank, qureyNum), "\t", "0.0000")+"\n");
				}else{
					outInfo.append((binsForGTSize[binIndex-1]+1)+"--"+binsForGTSize[binIndex]+"\t"+qureyNum+"\t"+General.floatArrToString(General.normliseArr(trueLocHistAccu_rerank, qureyNum), "\t", "0.0000")+"\n");
				}
			}
			QNumExistRes+=qureyNum;
		}
		outInfo.append("noResQ \t"+(actQNum-QNumExistRes)+"\n");
		return outInfo.toString();
	}
	
}
