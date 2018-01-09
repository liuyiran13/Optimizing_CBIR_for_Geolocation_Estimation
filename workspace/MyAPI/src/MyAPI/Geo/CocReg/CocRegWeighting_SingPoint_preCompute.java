package MyAPI.Geo.CocReg;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.math3.util.ArithmeticUtils;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;
import MyAPI.Obj.Statistics;
import MyAPI.imagR.IndexWordPresence;

/**
 * //this version of CocRegWeighting needs to enumerate all possible combinations of qRegs, many of these combination does not happen in any doc at all, so this is not a efficient one!
 *
 */
public class CocRegWeighting_SingPoint_preCompute {

	int QRegFreqThr;//its minmum should be 1 
	IndexWordPresence indexQRegPresence;
	HashMap<CocRegs, Float> cocReg_weights;
	HashMap<CocRegs, Integer> cocReg_freq;
	LinkedList<Integer> QFrequentRegions;
	HashSet<Integer> QWeightedRegions;
	int totDocNum;
	float maxCocRegWeight;
	int oneFreqRegGroupNum;//region group num that only happen once
	int nullFreqRegGroupNum;//region group num that only happen once
	
	public CocRegWeighting_SingPoint_preCompute(int QRegFreqThr, int QRegionNum, int totDocNum) {
		this.QRegFreqThr=QRegFreqThr;
		indexQRegPresence=new IndexWordPresence(QRegionNum,totDocNum);
		this.totDocNum=totDocNum;
		maxCocRegWeight=General_BoofCV.make_idf_log10(totDocNum, 1);//if one CocReg does not exist in cocReg_weight, it means both region's freq below QRegFreqThr, so it has high chance only happen once;
		maxCocRegWeight*=maxCocRegWeight; 
	}
	
	public void addOneLoc(int locID, int QRegionID){//locID and QRegionID are all start from 0
		indexQRegPresence.addOneDocWord(locID, QRegionID);
	}
	
	public Float getOneCocRegsWeight(CocRegs oneCoc){
		caculateQRegionWeight();
		Float res=cocReg_weights.get(oneCoc);
		if (res==null) {//this CocReg does not exist in cocReg_weight
			return 1f;
		}else {
			return res;
		}
	}
	
	public Float getNoExistCocRegWeight(){
		return 1f;
	}
	
	private void caculateQRegionWeight(){
		if (cocReg_weights==null) {
			cocReg_weights=new HashMap<>();
			cocReg_freq=new HashMap<>();
			nullFreqRegGroupNum=0;
			oneFreqRegGroupNum=0;
			//get QFrequentRegions
			QFrequentRegions=new LinkedList<>();
			for (int regionID = 0; regionID < indexQRegPresence.getWordNum(); regionID++) {
				if (indexQRegPresence.getOneWordPresence(regionID).size()>=QRegFreqThr) {//QRegion that happens at least QRegFreqThr locations
					QFrequentRegions.add(regionID);
				}
			}
			//find common docs that contains 2 coocurence regions within these frequent QRegions
			if (QFrequentRegions.size()>1) {
				for (int i = 0; i < QFrequentRegions.size(); i++) {
					Integer[] oneQRegDocs_i=indexQRegPresence.getOneWordPresence(QFrequentRegions.get(i)).toArray(new Integer[0]);
					for (int j = i+1; j < QFrequentRegions.size(); j++) {
						Integer[] oneQRegDocs_j = indexQRegPresence.getOneWordPresence(QFrequentRegions.get(j)).toArray(new Integer[0]);
						List<Integer> commonLocs=oneQRegDocs_i.length<oneQRegDocs_j.length? General.findCommonElement_twoSorted_ASC_loopShotArr(oneQRegDocs_i, oneQRegDocs_j)
									:General.findCommonElement_twoSorted_ASC_loopShotArr(oneQRegDocs_j, oneQRegDocs_i);
						if (commonLocs==null || commonLocs.size()==0) {
							nullFreqRegGroupNum++;
						}else if (commonLocs.size()==1) {
							oneFreqRegGroupNum++;
						}
						//Calculate the weight for locs contains Qregions in this Permutation  
						if (commonLocs!=null && commonLocs.size()>1) {
							float idf=General_BoofCV.make_idf_log10(totDocNum, commonLocs.size());
							cocReg_weights.put(new CocRegs(new Integer[]{QFrequentRegions.get(i), QFrequentRegions.get(j)}), idf*idf/maxCocRegWeight);
							cocReg_freq.put(new CocRegs(new Integer[]{QFrequentRegions.get(i), QFrequentRegions.get(j)}), commonLocs.size());
						}
					}
				}
			}
		}
	}
	
	public String getIDFStatistic(int topNum) throws InterruptedException{
		caculateQRegionWeight();
		Statistics<CocRegs> stat=new Statistics<CocRegs>(topNum);
		for (Entry<CocRegs, Float> one : cocReg_weights.entrySet()) {
			stat.addSample(one.getValue(), one.getKey());
		}
		return "tot-doc num:"+totDocNum+", QFrequentRegions: "+QFrequentRegions.size()
				+", should have Cn2="+(QFrequentRegions.size()>1?ArithmeticUtils.binomialCoefficient(QFrequentRegions.size(), 2):0)
				+" cocurrent regions, unique QWeightedRegions:"+getQWeightedRegions().size()+", nullFreqRegGroupNum: "+nullFreqRegGroupNum+", oneFreqRegGroupNum: "+oneFreqRegGroupNum+", "+stat.getFullStatistics("0.0", false);
	}
	
	public String getItermFreqStatistic(int topNum) throws InterruptedException{
		caculateQRegionWeight();
		Statistics<CocRegs> stat=new Statistics<CocRegs>(topNum);
		for (Entry<CocRegs, Integer> one : cocReg_freq.entrySet()) {
			stat.addSample(one.getValue(), one.getKey());
		}
		return "tot-doc num:"+totDocNum+", QFrequentRegions: "+QFrequentRegions.size()
				+", should have Cn2="+(QFrequentRegions.size()>1?ArithmeticUtils.binomialCoefficient(QFrequentRegions.size(), 2):0)
				+" cocurrent regions, unique QWeightedRegions:"+getQWeightedRegions().size()+", nullFreqRegGroupNum: "+nullFreqRegGroupNum+", oneFreqRegGroupNum: "+oneFreqRegGroupNum+", "+stat.getFullStatistics("0", false);
	}
	
	public HashSet<Integer> getQWeightedRegions(){
		if (QWeightedRegions==null) {//never calculated before
			caculateQRegionWeight();
			QWeightedRegions=new HashSet<>();
			for (Entry<CocRegs, Float> oneW : cocReg_weights.entrySet()) {
				Integer[] regIDs=oneW.getKey().regIDs;
				for (Integer oneReg : regIDs) {
					QWeightedRegions.add(oneReg);
				}
			}
		}
		return QWeightedRegions;
	}

}
