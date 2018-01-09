package MyAPI.Geo.CocReg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.math3.util.ArithmeticUtils;

import MyAPI.General.General;
import MyAPI.General.General_IR;
import MyAPI.General.Magic.ConnectComponent;
import MyAPI.Geo.groupDocs.GroupDocs;
import MyAPI.Obj.DID_FeatInds_Score;
import MyAPI.Obj.Statistic_MultiClass_1D_Distribution;
import MyAPI.Obj.Statistics;
import MyAPI.imagR.IDFTable;
import MyAPI.imagR.IndexWordPresence;
import MyAPI.imagR.ShowImgBlocks.BlockLink;
import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr;

/**
 * this version of CocRegWeighting is efficient than CocRegWeighting_preCompute. because it do not needs to enumerate all possible combinations of qRegs, many of these combination does not happen in any doc at all, so this is not efficient!
 * instead, this version only compute the commons docs when needed, (one cocReg in one doc, so it must happen at least in one doc, this avoid to compute common docs for no-exist-cocReg)
 *
 */
public class CocRegWeighting {
	
	int qID;
	int totDocNum;
	IndexWordPresence indexQRegPresence;
	IDFTable idfTable;
	//data
	int[] docIndex_groupID;
	HashMap<CocRegs, Integer> cocReg_freq;
	
	public CocRegWeighting(int qID, int QRegionNum_max, int totDocNum) {
		this.qID=qID;
		this.totDocNum=totDocNum;
		indexQRegPresence=new IndexWordPresence(QRegionNum_max,totDocNum);
		idfTable=new IDFTable(totDocNum, true, true, totDocNum);
		cocReg_freq=new HashMap<>();
	}
	
	public <G extends GroupDocs<DID_Score_ImageRegionMatch_ShortArr>> void setDocGroupIDs (ArrayList<G> groupDocMatches){
		//used for cocInDocLevel
		int totDocNum=0;
		for (G oneGroup: groupDocMatches) {
			totDocNum+=oneGroup.docs.size();
		}
		docIndex_groupID=new int[totDocNum];
		int groupID=0; int docIndex=0;
		for (G oneGroup: groupDocMatches) {
			for (DID_Score_ImageRegionMatch_ShortArr oneDoc : oneGroup.docs) {
				docIndex_groupID[docIndex]=groupID;
				docIndex++;
			}
			groupID++;
		}
	}
	
	public void addOneLoc(int locID, int QRegionID){//locID and QRegionID are all start from 0
		indexQRegPresence.addOneDocWord(locID, QRegionID);
	}
		
	public float calculateCocRegWeight_old(ArrayList<DID_FeatInds_Score> locMatches, LinkedList<BlockLink> links) throws InterruptedException{//regID in locMatches should always in the ascending order!
		if (locMatches.size()>1) {
			//sort locMatches by regID
			ArrayList<Integer> regIDs=new ArrayList<>();
			for (DID_FeatInds_Score one : locMatches) {
				regIDs.add(one.blockInd_Q);
			}
			ArrayList<DID_FeatInds_Score> locMatches_sorted=new ArrayList<>(); ArrayList<Integer> regIDs_sorted=new ArrayList<>();
			General_IR.rank_get_AllSortedDocIDs_treeSet(locMatches, regIDs, locMatches_sorted, regIDs_sorted, "ASC");
			//2Points IDF
//			HashSet<Integer> freqentMatches=new HashSet<>(); 
			float score=0; int linkNum=0;
			for (int i = 0; i < locMatches_sorted.size(); i++) {
				for (int j = i+1; j < locMatches_sorted.size(); j++) {
					CocRegs cogReg=new CocRegs(new Integer[]{locMatches_sorted.get(i).blockInd_Q, locMatches_sorted.get(j).blockInd_Q});
					Integer freq=cocReg_freq.get(cogReg);
					if (freq==null) {//this cogReg does not exist yet
						ArrayList<Integer> commonDocs1=indexQRegPresence.getOneWordPresence(locMatches_sorted.get(i).blockInd_Q);
						ArrayList<Integer> commonDocs2=indexQRegPresence.getOneWordPresence(locMatches_sorted.get(j).blockInd_Q);
						ArrayList<Integer> commonDocs = General.findCommonElement_twoSorted_ASC_loopShotArr(commonDocs1, commonDocs2);
						//used for cocInDocLevel
//						HashSet<Integer> uniLocs=new HashSet<>(); 
//						for (Integer oneDoc : commonDocs) {
//							uniLocs.add(docIndex_groupID[oneDoc]);
//						}
//						cocReg_freq.put(cogReg, uniLocs.size());
//						freq=uniLocs.size();
						//used for cocInGroupLevel
						cocReg_freq.put(cogReg, commonDocs.size());
						freq=commonDocs.size();
					}
//					if (freq>=3) {
//						freqentMatches.add(i);
//						freqentMatches.add(j);
//					}
					linkNum++;
					score+=idfTable.getOne(freq);
					//show links
					if (links!=null) {
						links.add(new BlockLink(cogReg.regIDs[0], cogReg.regIDs[1], freq));
					}
				}
			}
//			int i=0;
//			for (DID_FeatInds_Score one : locMatches_sorted) {
//				if (freqentMatches.contains(i)) {
//					one.score*=0;
//				}else {
//					one.score*=idfTable.getOne(1);
//				}
//				i++;
//			}
			return score/linkNum;
		}else {
			return 1f;
		}
	}
	
	public float calculateCocRegWeight(ArrayList<DID_FeatInds_Score> locMatches, LinkedList<BlockLink> links) throws InterruptedException{//regID in locMatches should always in the ascending order!
		//sort locMatches by regID
		TreeSet<Integer> regIDs_ASC=new TreeSet<>();//ASC order by default
		for (DID_FeatInds_Score one : locMatches) {
			regIDs_ASC.add(one.blockInd_Q);
		}
		ArrayList<Integer> regIDs_ASC_list=new ArrayList<>(regIDs_ASC);
		if (regIDs_ASC_list.size()>1) {
			//2Points IDF
//			HashSet<Integer> freqentMatches=new HashSet<>(); 
			float score=0; int linkNum=0;
			for (int i = 0; i < regIDs_ASC_list.size(); i++) {
				for (int j = i+1; j < regIDs_ASC_list.size(); j++) {
					CocRegs cogReg=new CocRegs(new Integer[]{regIDs_ASC_list.get(i), regIDs_ASC_list.get(j)});
					Integer freq=cocReg_freq.get(cogReg);
					if (freq==null) {//this cogReg does not exist yet
						ArrayList<Integer> commonDocs1=indexQRegPresence.getOneWordPresence(cogReg.regIDs[0]);
						ArrayList<Integer> commonDocs2=indexQRegPresence.getOneWordPresence(cogReg.regIDs[1]);
						ArrayList<Integer> commonDocs = General.findCommonElement_twoSorted_ASC_loopShotArr(commonDocs1, commonDocs2);
						//used for cocInDocLevel
//						HashSet<Integer> uniLocs=new HashSet<>(); 
//						for (Integer oneDoc : commonDocs) {
//							uniLocs.add(docIndex_groupID[oneDoc]);
//						}
//						cocReg_freq.put(cogReg, uniLocs.size());
//						freq=uniLocs.size();
						//used for cocInGroupLevel
						cocReg_freq.put(cogReg, commonDocs.size());
						freq=commonDocs.size();
					}
//					if (freq>=3) {
//						freqentMatches.add(i);
//						freqentMatches.add(j);
//					}
					linkNum++;
					score+=idfTable.getOne(freq);
					//show links
					if (links!=null) {
						links.add(new BlockLink(cogReg.regIDs[0], cogReg.regIDs[1], freq));
					}
				}
			}
//			int i=0;
//			for (DID_FeatInds_Score one : locMatches_sorted) {
//				if (freqentMatches.contains(i)) {
//					one.score*=0;
//				}else {
//					one.score*=idfTable.getOne(1);
//				}
//				i++;
//			}
			return score/linkNum;
		}else {
			return 1f;
		}
	}
	
	public float calculateCocRegWeight_connect(ArrayList<DID_FeatInds_Score> locMatches, LinkedList<BlockLink> links) throws InterruptedException{//regID in locMatches should always in the ascending order!
		//sort locMatches by regID
		TreeSet<Integer> regIDs_ASC=new TreeSet<>();//ASC order by default
		for (DID_FeatInds_Score one : locMatches) {
			regIDs_ASC.add(one.blockInd_Q);
		}
		ArrayList<Integer> regIDs_ASC_list=new ArrayList<>(regIDs_ASC);
		if (regIDs_ASC_list.size()>1) {
			int linkWeightThr=5;
			ConnectComponent<Integer> connectComp=new ConnectComponent<>();
			//add vertex
			for (Integer one:regIDs_ASC_list) {
				connectComp.addOneVertex(one);
			}
			//add link
			for (int i = 0; i < regIDs_ASC_list.size(); i++) {
				for (int j = i+1; j < regIDs_ASC_list.size(); j++) {
					CocRegs cogReg=new CocRegs(new Integer[]{regIDs_ASC_list.get(i), regIDs_ASC_list.get(j)});
					Integer freq=cocReg_freq.get(cogReg);
					if (freq==null) {//this cogReg does not exist yet
						ArrayList<Integer> commonDocs1=indexQRegPresence.getOneWordPresence(cogReg.regIDs[0]);
						ArrayList<Integer> commonDocs2=indexQRegPresence.getOneWordPresence(cogReg.regIDs[1]);
						ArrayList<Integer> commonDocs = General.findCommonElement_twoSorted_ASC_loopShotArr(commonDocs1, commonDocs2);
						//used for cocInDocLevel
//						HashSet<Integer> uniLocs=new HashSet<>(); 
//						for (Integer oneDoc : commonDocs) {
//							uniLocs.add(docIndex_groupID[oneDoc]);
//						}
//						cocReg_freq.put(cogReg, uniLocs.size());
//						freq=uniLocs.size();
						//used for cocInGroupLevel
						cocReg_freq.put(cogReg, commonDocs.size());
						freq=commonDocs.size();
					}
					if (freq>=linkWeightThr) {
						connectComp.addOneEdge(cogReg.regIDs[0], cogReg.regIDs[1]);
					}
					//show links
					if (links!=null) {
						links.add(new BlockLink(cogReg.regIDs[0], cogReg.regIDs[1], freq));
					}
				}
			}
//			float score=0f;
//			for (Set<Integer> oneComp:connectComp.getConnectComps()) {
//				if (oneComp.size()==1) {//only 1 point, use its IDF 
//					score+=idfTable.getOne(indexQRegPresence.getOneWordPresence(oneComp.toArray(new Integer[0])[0]).size());
//				}else {
//					TreeSet<Integer> oneComp_regIDs_ASC=new TreeSet<>(oneComp);//ASC order by default
//					ArrayList<Integer> oneComp_regIDs_ASC_list=new ArrayList<>(oneComp_regIDs_ASC);
////					float thisScore=0f; int thisLinkNum=0;
//					float thisScore=Integer.MAX_VALUE;
//					for (int i = 0; i < oneComp_regIDs_ASC_list.size(); i++) {
//						for (int j = i+1; j < oneComp_regIDs_ASC_list.size(); j++) {
//							CocRegs cogReg=new CocRegs(new Integer[]{oneComp_regIDs_ASC_list.get(i), oneComp_regIDs_ASC_list.get(j)});
//							Integer freq=cocReg_freq.get(cogReg);
////							thisScore+=idfTable.getOne(freq);
////							thisLinkNum++;
//							thisScore=Math.min(thisScore, idfTable.getOne(freq));
//						}
//					}
////					score+=(thisScore/thisLinkNum);
//					score+=thisScore;
//				}
//			}
//			return score;
			return connectComp.getConnectComps().size();
		}else {
			return 1f;
		}
	}
	
	private int getFreqQRegNum(){
		int frequentQRegNum=0;
		for (int regionID = 0; regionID < indexQRegPresence.getWordNum(); regionID++) {
			if (indexQRegPresence.getOneWordPresence(regionID).size()>0) {//QRegion that happens at least in one photo
				frequentQRegNum++;
			}
		}
		return frequentQRegNum;
	}
	
	public LinkedList<BlockLink> getStatistic(int topNum) throws InterruptedException{
		if (cocReg_freq.size()>0) {
			//get frequentQRegNum
			int frequentQRegNum=getFreqQRegNum();
			//get statistics
			int regionLev=2;
			String[] levelNames=new String[]{regionLev+"Point"};
			int maxFreq=General.getMinMax_entry(cocReg_freq).get(1).getFirst().getValue();
			Statistic_MultiClass_1D_Distribution freqStat=new Statistic_MultiClass_1D_Distribution(levelNames, new float[]{0,maxFreq,maxFreq/10f}, "0.0");
			int totPossibleCocRegs=(int) ArithmeticUtils.binomialCoefficient(frequentQRegNum, 2);
			Statistics<CocRegs> stat_Freq=new Statistics<CocRegs>(topNum); Statistics<CocRegs> stat_IDF=new Statistics<CocRegs>(topNum);
			LinkedList<BlockLink> links=new LinkedList<>();
			for (Entry<CocRegs, Integer> one : cocReg_freq.entrySet()) {
				stat_Freq.addSample(one.getValue(), one.getKey());
				stat_IDF.addSample(idfTable.getOne(one.getValue()), one.getKey());
				freqStat.addOneSample(0, one.getValue());
				//add to links
				links.add(new BlockLink(one.getKey().regIDs[0], one.getKey().regIDs[1], one.getValue()));
			}
			System.out.println("tot-doc num:"+idfTable.get_totDocNum()+", frequentQRegNum: "+frequentQRegNum+", statistics for "+regionLev+"Point-region idf:\n"
							+regionLev+"Point-region, it should have Cn"+regionLev+"="+totPossibleCocRegs+" cocurrent regions, nullFreqCocRegNum: "+(totPossibleCocRegs-cocReg_freq.size())+"\n"
							+"Freq: "+stat_Freq.getFullStatistics("0.0", false)+"\n"
							+ "IDF: "+stat_IDF.getFullStatistics("0.0", false)+"\n");
			freqStat.dispAsChart("reg freq hist for Q"+qID, "reg's freq", "portion");
			return links;
		}else {
			return null;
		}
		
	}
	
}
