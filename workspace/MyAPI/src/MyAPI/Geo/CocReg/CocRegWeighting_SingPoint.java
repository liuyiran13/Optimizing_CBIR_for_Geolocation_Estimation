package MyAPI.Geo.CocReg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.math3.util.ArithmeticUtils;

import MyAPI.General.General;
import MyAPI.Obj.DID_FeatInds_Score;
import MyAPI.Obj.Median;
import MyAPI.Obj.Statistic_MultiClass_1D_Distribution;
import MyAPI.Obj.Statistics;
import MyAPI.imagR.IDFTable;
import MyAPI.imagR.IndexWordPresence;
import MyCustomedHaoop.ValueClass.PhotoPointsLoc;

/**
 * this version of CocRegWeighting is efficient than CocRegWeighting_preCompute. because it do not needs to enumerate all possible combinations of qRegs, many of these combination does not happen in any doc at all, so this is not efficient!
 * instead, this version only compute the commons docs when needed, (one cocReg in one doc, so it must happen at least in one doc, this avoid to compute common docs for no-exist-cocReg)
 *
 */
public class CocRegWeighting_SingPoint {//this QRegion weight is normalised to 0~1

//	public static class ComDocs_Weight{
//		ArrayList<Integer> commonDocs;
//		float weight;
//		
//		public ComDocs_Weight(ArrayList<Integer> commonDocs, float weight){
//			this.commonDocs=commonDocs;
//			this.weight=weight;
//		}
//		
//	}
//	
//	int qID;
//	int totDocNum;
//	IndexWordPresence indexQRegPresence;
//	IDFTable idfTable;
//	int levelNum;
//	ArrayList<HashMap<CocRegs, ComDocs_Weight>> cocReg_weights;
//	ArrayList<HashMap<CocRegs, Integer>> cocReg_freq;
//	float qRegFreqThr_perc;
//	int qRegFreqThr;
//	int[] oneFreqCocRegNums;//region group num that only happen once
//	
//	ConnectReg connectRegs;
//	
//	int docInd;
//	
//	public CocRegWeighting_SingPoint(int qID, PhotoPointsLoc queryInfo, int QRegionNum_approx, int totDocNum, IDFTable idfTable, 
//							int levelNum, float qRegFreqThr_perc) {
//		this.qID=qID;
//		this.totDocNum=totDocNum;
//		indexQRegPresence=new IndexWordPresence(QRegionNum_approx,totDocNum);
//		if (idfTable.get_totDocNum()!=totDocNum) {//this query's docNum is not equal with the pre-defined one, so needs to calculate for itself
//			this.idfTable=new IDFTable(totDocNum, true, true, totDocNum);
//		}else {
//			this.idfTable=idfTable;
//		}
//		this.levelNum=levelNum;
//		cocReg_weights=General.ini_ArrayList_HashMap(levelNum+1);//level ind is the cocReg dimision, so from 1
//		cocReg_freq=General.ini_ArrayList_HashMap(levelNum+1);
//		this.qRegFreqThr_perc=qRegFreqThr_perc;//0~1
//		oneFreqCocRegNums=new int[levelNum+1];
//		
//		connectRegs=new ConnectReg(queryInfo.feats.getArr(), 5);
//	}
//	
//	public void addOneLoc(int locID, int QRegionID){//locID and QRegionID are all start from 0
//		indexQRegPresence.addOneDocWord(locID, QRegionID);
//	}
//	
//	private void calculateQRegFreqThr(){
//		if (qRegFreqThr==0) {//qRegFreqThr is not calculated yet, qRegFreqThr's minmum is 1
//			Median<Integer> median=new Median<Integer>(Integer.class);
//			for (ArrayList<Integer> oneWord : indexQRegPresence.invertedIndex) {
//				if (oneWord.size()>0) {
//					median.addOneSample(oneWord.size());
//				}
//			}
//			qRegFreqThr=median.getPercentMedian(qRegFreqThr_perc, null);
//		}
//	}
//	
//	public void calculateCocRegWeight_connected(ArrayList<DID_FeatInds_Score> locMatches){//regID in locMatches should always in the ascending order!
//		float idf_thr=0.3f;
//		List<Set<DID_FeatInds_Score>> connectRegSet = connectRegs.getConnectComponents(locMatches); 
//		for (Set<DID_FeatInds_Score> oneSet : connectRegSet) {
//			//find minIDF
//			float minIDF=Integer.MAX_VALUE;
//			for (DID_FeatInds_Score oneMatch : oneSet) {
//				ComDocs_Weight w1=getOneCocRegsWeight(new CocRegs(new Integer[]{oneMatch.featInd_Q}),null);
//				oneMatch.score*=w1.weight;
//				if (minIDF>w1.weight) {
//					minIDF=w1.weight;
//				}
//			}
//			//spread minIDF
////			minIDF=minIDF>idf_thr?minIDF:0f;
////			for (DID_FeatInds_Score oneMatch : oneSet) {
////				oneMatch.score*=minIDF;
////			}
//			if(minIDF<idf_thr){
//				for (DID_FeatInds_Score oneMatch : oneSet) {
//					oneMatch.score=0;
//				}
//			}
//		}
////		//test: show each set with one color
////		float v=0;
////		for (Set<DID_FeatInds_Score> oneSet : connectRegSet) {
////			for (DID_FeatInds_Score oneMatch : oneSet) {
////				oneMatch.score=v;
////			}
////			v+=0.1f;
////		}
//	}
//	
//	
//	public void calculateCocRegWeight(ArrayList<DID_FeatInds_Score> locMatches){//regID in locMatches should always in the ascending order!
//		float[][] qCocRegWeights=new float[levelNum][locMatches.size()];
//		for (int i = 0; i < qCocRegWeights.length; i++) {//default is 1
//			qCocRegWeights[i]=General.makeAllOnes_floatArr(locMatches.size(), 1f);
//		}
//		//1Point IDF
//		for (int i = 0; i < locMatches.size(); i++) {
//			ComDocs_Weight w1=getOneCocRegsWeight(new CocRegs(new Integer[]{locMatches.get(i).blockInd_Q}),null);
//			qCocRegWeights[0][i]=w1.weight;
//		}
//		//multi-Points IDF
//		if (levelNum>=2) {
//			calculateQRegFreqThr();
//			//weighting
//			if (levelNum==2) {//2Points IDF
//				for (int i = 0; i < locMatches.size(); i++) {
//					ArrayList<Integer> commonDocs1=indexQRegPresence.getOneWordPresence(locMatches.get(i).blockInd_Q);
//					if (commonDocs1.size()>qRegFreqThr) {
//						for (int j = i+1; j < locMatches.size(); j++) {
//							ArrayList<Integer> commonDocs2=indexQRegPresence.getOneWordPresence(locMatches.get(j).blockInd_Q);
//							if (commonDocs2.size()>qRegFreqThr) {
//								float[] currentlevWei=qCocRegWeights[1];
//								ComDocs_Weight w2=getOneCocRegsWeight(new CocRegs(new Integer[]{locMatches.get(i).blockInd_Q,locMatches.get(j).blockInd_Q}), commonDocs1);
//								currentlevWei[i]=Math.min(currentlevWei[i], w2.weight); currentlevWei[j]=Math.min(currentlevWei[j], w2.weight);
//							}
//						}
//					}
//				}
////				qCocRegWeights[1]=General.normliseArr(qCocRegWeights[1], locMatches.size()-1);
//			}else if (levelNum==3) {//3Points IDF
//				for (int i = 0; i < locMatches.size(); i++) {
//					ComDocs_Weight w1=getOneCocRegsWeight(new CocRegs(new Integer[]{locMatches.get(i).blockInd_Q}),null);
//					if (w1.commonDocs.size()>qRegFreqThr) {
//						for (int j = i+1; j < locMatches.size(); j++) {
//							float[] currentlevWei=qCocRegWeights[1];
//							ComDocs_Weight w2=getOneCocRegsWeight(new CocRegs(new Integer[]{locMatches.get(i).blockInd_Q,locMatches.get(j).blockInd_Q}), w1.commonDocs);
//							currentlevWei[i]=Math.min(currentlevWei[i], w2.weight); currentlevWei[j]=Math.min(currentlevWei[j], w2.weight);
//							if (w2.commonDocs.size()>qRegFreqThr) {
//								for (int k = j+1; k < locMatches.size(); k++) {
//									currentlevWei=qCocRegWeights[2];
//									ComDocs_Weight w3=getOneCocRegsWeight(new CocRegs(new Integer[]{locMatches.get(i).blockInd_Q,locMatches.get(j).blockInd_Q,locMatches.get(k).blockInd_Q}), w2.commonDocs);
//									currentlevWei[i]=Math.min(currentlevWei[i], w3.weight); currentlevWei[j]=Math.min(currentlevWei[j], w3.weight); currentlevWei[k]=Math.min(currentlevWei[k], w3.weight);
//								}
//							}
//						}
//					}
//				}
//			}
//		}
//		//combine
//		String[] levelNames=new String[qCocRegWeights.length];
//		for (int i = 0; i < levelNames.length; i++) {
//			levelNames[i]=(i+1)+"Point";
//		}
//		Statistic_MultiClass_1D_Distribution weightStat=new Statistic_MultiClass_1D_Distribution(levelNames, new float[]{0,1,0.1f}, "0.0");
//		float[] thrs_perLev=new float[]{0f,0f};
//		for (int i = 0; i < qCocRegWeights.length; i++) {
//			for (int j = 0; j < qCocRegWeights[i].length; j++) {
//				if (qCocRegWeights[i][j]>thrs_perLev[i]) {
//					locMatches.get(j).score*=qCocRegWeights[i][j];
//				}else{
//					locMatches.get(j).score=0;
//				}
//				weightStat.addOneSample(i, (float) qCocRegWeights[i][j]);
//			}
//		}
////		for (int j = 0; j < qCocRegWeights[0].length; j++) {
////			res[j]*=qCocRegWeights[0][j]*qCocRegWeights[0][j];
////		}
////		if ((qID==4 || qID==22 || qID==25|| qID==57|| qID==67|| qID==75|| qID==137) && (docInd++<4 || locMatches.get(0).docID==187948|| locMatches.get(0).docID==5121)) {//show weights for one doc
////			weightStat.dispAsChart("weight hist for Q"+qID+"_D"+locMatches.get(0).docID, "weight", "portion");
////			General_Chart.drawLineChart("weight for points in Q"+qID+"_D"+locMatches.get(0).docID, "point index", "weight", levelNames, qCocRegWeights, null);
////		}
//	}
//	
//	private ComDocs_Weight getOneCocRegsWeight(CocRegs cocReg, ArrayList<Integer> previousLevelComDocs){
//		ComDocs_Weight res=cocReg_weights.get(cocReg.getRegDim()).get(cocReg);
//		if (res==null) {//this CocReg does not exist in cocReg_weight yet, calculate it!
//			if (previousLevelComDocs!=null) {
//				return caculateCocRegWeight(cocReg, previousLevelComDocs);//last element in cocReg is the new added reg
//			}else {
//				return caculateCocRegWeight(cocReg);
//			}
//		}else {
//			return res;
//		}
//	}
//	
//	private ComDocs_Weight caculateCocRegWeight(CocRegs cocReg){
//		ArrayList<ArrayList<Integer>> QRegDocs=new ArrayList<>(); boolean isOneFreqCocReg=false;
//		ArrayList<Integer> commonDocs=null;
//		for (int i = 0; i < cocReg.regIDs.length; i++) {
//			ArrayList<Integer> res=indexQRegPresence.getOneWordPresence(cocReg.regIDs[i]);
//			QRegDocs.add(res);
//			if (res.size()==1) {//this CocReg must only happen once
//				isOneFreqCocReg=true;
//				commonDocs=res;
//				break;
//			}
//		}
//		if (isOneFreqCocReg) {//this CocReg must only happen once
//			oneFreqCocRegNums[cocReg.getRegDim()]++;
//			return new ComDocs_Weight(commonDocs,idfTable.getOne(1));
//		}else {
//			commonDocs=General.findCommonElement_multipleSorted_ASC_loopShotArr(QRegDocs);
//			ComDocs_Weight w=new ComDocs_Weight(commonDocs,idfTable.getOne(commonDocs.size()));
//			//commonLocs.size() cannot be zero, must at least one
//			if (commonDocs.size()==1) {//this CocReg only happen once
//				oneFreqCocRegNums[cocReg.getRegDim()]++;
//			}else {//only CocReg that happens more than once can be put into the cocReg_weights, so that other doc can use this calculated weight.
//				cocReg_weights.get(cocReg.getRegDim()).put(cocReg, w);
//				cocReg_freq.get(cocReg.getRegDim()).put(cocReg, commonDocs.size());
//			}
//			return w;
//		}
//	}
//	
//	private ComDocs_Weight caculateCocRegWeight(CocRegs cocReg, ArrayList<Integer> previousLevelComDocs){//last element in cocReg is the new added reg
//		ArrayList<Integer> lastRegDocs=indexQRegPresence.getOneWordPresence(cocReg.getLastReg());
//		ArrayList<Integer> commonLocs=lastRegDocs.size()<previousLevelComDocs.size()? General.findCommonElement_twoSorted_ASC_loopShotArr(lastRegDocs, previousLevelComDocs)
//				:General.findCommonElement_twoSorted_ASC_loopShotArr( previousLevelComDocs, lastRegDocs);
//		ComDocs_Weight w=new ComDocs_Weight(commonLocs,idfTable.getOne(commonLocs.size()));
//		//commonLocs.size() cannot be zero, must at least one
//		if (commonLocs.size()==1) {//this CocReg only happen once
//			oneFreqCocRegNums[cocReg.getRegDim()]++;
//		}else {//only CocReg that happens more than once can be put into the cocReg_weights, so that other doc can use this calculated weight.
//			cocReg_weights.get(cocReg.getRegDim()).put(cocReg, w);
//			cocReg_freq.get(cocReg.getRegDim()).put(cocReg, commonLocs.size());
//		}
//		return w;
//	}
//	
//	private int getFreqQRegNum(){
//		int frequentQRegNum=0;
//		for (int regionID = 0; regionID < indexQRegPresence.getWordNum(); regionID++) {
//			if (indexQRegPresence.getOneWordPresence(regionID).size()>0) {//QRegion that happens at least in one location
//				frequentQRegNum++;
//			}
//		}
//		return frequentQRegNum;
//	}
//	
//	public String getStatistic(int topNum) throws InterruptedException{
//		General.Assert(cocReg_weights!=null, "err! cocReg_weights==null, please only call getIDFStatistic after all doc's cocReg_weights is calculated!");
//		//get frequentQRegNum
//		int frequentQRegNum=getFreqQRegNum();
//		//get statistics
//		StringBuffer res=new StringBuffer();
//		res.append("tot-doc num:"+idfTable.get_totDocNum()+", frequentQRegNum: "+frequentQRegNum+", statistics for each of tot-"+levelNum+" levels:\n");
//		String[] levelNames=new String[levelNum];
//		for (int i = 0; i < levelNames.length; i++) {
//			levelNames[i]=(i+1)+"Point";
//		}
//		Statistic_MultiClass_1D_Distribution freqStat=new Statistic_MultiClass_1D_Distribution(levelNames, new float[]{0,totDocNum,10f}, "0.0");
//		for (int lev = 1; lev < cocReg_weights.size(); lev++) {
//			int totPossibleCocRegs=(int) ArithmeticUtils.binomialCoefficient(frequentQRegNum, lev);
//			Statistics<CocRegs> stat_IDF=new Statistics<CocRegs>(topNum);
//			for (Entry<CocRegs, ComDocs_Weight> one : cocReg_weights.get(lev).entrySet()) {
//				stat_IDF.addSample(one.getValue().weight, one.getKey());
//			}
//			Statistics<CocRegs> stat_Freq=new Statistics<CocRegs>(topNum);
//			for (Entry<CocRegs, Integer> one : cocReg_freq.get(lev).entrySet()) {
//				stat_Freq.addSample(one.getValue(), one.getKey());
//				freqStat.addOneSample(lev-1, one.getValue());
//			}
//			res.append( "level-"+lev+", it should have Cn"+lev+"="+totPossibleCocRegs+" cocurrent regions, nullFreqCocRegNum: "+(totPossibleCocRegs-oneFreqCocRegNums[lev]-cocReg_weights.get(lev).size())+", oneFreqCocRegNum: "+oneFreqCocRegNums[lev]+"\n"
//							+ "moreThanOneFreqCocRegNum: IDF "+stat_IDF.getFullStatistics("0.0", false)+"\n"
//							+"Freq: "+stat_Freq.getFullStatistics("0.0", false)+"\n");
//		}
//		freqStat.dispAsChart("reg freq hist for Q"+qID, "reg's freq", "portion");
//		return res.toString();
//	}
}
