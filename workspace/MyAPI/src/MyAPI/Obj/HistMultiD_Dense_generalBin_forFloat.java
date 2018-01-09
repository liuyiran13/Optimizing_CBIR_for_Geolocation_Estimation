package MyAPI.Obj;

import java.util.ArrayList;
import java.util.LinkedList;
import MyAPI.General.General;

public class HistMultiD_Dense_generalBin_forFloat <M extends Object> {//multi-dimensional hist, as it is Sparse, use hashMap instead of muti-dim array
	/*
	 * the difference: between HistMultiD_Dense_forFloat and HistMultiD_Sparse_forFloat is how to save hist
	 * as HashMap or as array
	 * the same: both use one-dim histInd to index multi-dim bins
	 * _generalBin: means it can handle for bin with different interval, there can be no funtion to generate these bins.
	 */
	
	protected boolean oneVoteForTwoBin;
	protected float[][] bins;
	protected float[][] bins_middle; //for addOneSample_assignTwoBins
	protected int[] eachDimSize;
	protected int totBinNum;
	protected int[] hist; //as here, hist is dense, so use array instead of hashmap
	protected int[] hist_masterBin_forOneVoteForTwoBin; //for oneVoteForTwoBin, it may generate two bin that is max, in this case, we use materBinVote to decide which bin to choose
	protected int sampleNum;
	protected boolean saveSample;
	protected ArrayList<ArrayList<M>> samplesInBins;
	protected int[] MaxBin_ind_val;
	
	public HistMultiD_Dense_generalBin_forFloat(boolean saveSample, boolean oneVoteForTwoBin){
		this.saveSample=saveSample;
		this.oneVoteForTwoBin=oneVoteForTwoBin;
	}
	
	public void makeEqualBins(float[][] begin_end_step, String floatValueFormat, StringBuffer binInfo){
		//float value has some rounding problem, if 0~1, then, 0,0.1,0.2,0.3,0.400000006,0.5000007.....
		//this function assume, (end-begin) can be fully divided by step
		/*  setup[]={begin, end, step}*/
		bins=new float[begin_end_step.length][]; bins_middle=new float[begin_end_step.length][]; eachDimSize=new int[begin_end_step.length];
		for (int i = 0; i < begin_end_step.length; i++) {
			bins[i]=General.makeRange(begin_end_step[i]);//attention: the last value in initialBins should > all sample's maximum value! 
			bins_middle[i]=General.makeMiddleBins(bins[i]);
			eachDimSize[i]=bins[i].length+1; //hist num is bin.length+1
		}
		totBinNum=General.Multiplicative_IntArr(eachDimSize);
		iniHist();
		//binInfo
		if (binInfo!=null) {
			binInfo.append("\n in tot-"+eachDimSize.length+" dim, eachDimSize:"+General.IntArrToString(eachDimSize, ","));
			for (int i = 0; i < bins.length; i++) {
				if (bins[i].length<100) {
					binInfo.append(  "\n"+"Bins:       "+General.floatArrToString(bins[i], "_", floatValueFormat)
									+"\n"+"Bins_middle:"+General.floatArrToString(bins_middle[i], "_", floatValueFormat));
				}else {
					binInfo.append("\n no bin info for this bin-"+i+", because floatArrToString is slow when array is large, its length is larger than 100 which is "+bins[i].length);
				}
			}
		}
	}
	
	public void iniHist() {
		hist=new int[totBinNum]; 
		if (oneVoteForTwoBin) {
			this.hist_masterBin_forOneVoteForTwoBin=new int[totBinNum]; 
		}
		if (saveSample) {
			this.samplesInBins=General.ini_ArrayList_ArrayList(hist.length,10);
		}
		sampleNum=0;
		MaxBin_ind_val=null;
	}
	
	public void addOneSample(float sampleV[], M sample) {
		if (oneVoteForTwoBin) {
			addOneSample_assignTwoBins(sampleV,sample);
		}else {
			addOneSample_assignOneBin(sampleV,sample);
		}
	}
	
	protected void addOneSample_assignOneBin(float[] sampleV, M sample) {
		int[] binInds=new int[bins.length];
		for (int i = 0; i < bins.length; i++) {
			binInds[i]=General.getBinInd_linear(bins[i],sampleV[i]);
		}
		int binIndInOneDim=General.getOneDimInd_forMutiDimArr(binInds, eachDimSize);
		hist[binIndInOneDim]++;
		if (saveSample) {
			samplesInBins.get(binIndInOneDim).add(sample);
		}
		sampleNum++;
	}
	
	protected void addOneSample_assignTwoBins(float[] sampleV, M sample) {
		int[][] binInds_eachDim=new int[bins.length][2]; int[] masterBinInds=new int[bins.length];
		for (int i = 0; i < bins.length; i++) {
			binInds_eachDim[i]=General.getBinInd_linear_assignTwoBins(bins[i],bins_middle[i],sampleV[i]);
			masterBinInds[i]=binInds_eachDim[i][0];//save master binInd
		}
		int[][] binInds_allCombine=General.getAllCombinations(binInds_eachDim);
		for (int[] oneComb : binInds_allCombine) {
			int binIndInOneDim=General.getOneDimInd_forMutiDimArr(oneComb, eachDimSize);
			hist[binIndInOneDim]++;
			if (saveSample) {
				samplesInBins.get(binIndInOneDim).add(sample);
			}
		}
		//make masterBin voting: only vote for one master bin, used for oneVoteForTwoBin, it may generate two bin that is max, in this case, we use materBinVote to decide which bin to choose
		int binIndInOneDim=General.getOneDimInd_forMutiDimArr(masterBinInds, eachDimSize);
		hist_masterBin_forOneVoteForTwoBin[binIndInOneDim]++;
		//
		sampleNum++;
	}
	
	public float[] getBinsForChart(){
		//as for multi dim, use ind-inOnedim for chart, 
		float[] binsToShow=new float[totBinNum];
		for (int i = 0; i < binsToShow.length; i++) {
			binsToShow[i] = i;
		}
		return binsToShow;
	}
	
	public int[] getMaxBin_ind_val() {
		if (MaxBin_ind_val==null) {
			LinkedList<int[]> maxEntrys= General.getMax_ind_vals(hist);
			int[] finalSel=null;
			if (oneVoteForTwoBin && maxEntrys.size()>1) {
				int max_masterValue=Integer.MIN_VALUE; 
				for (int[] one : maxEntrys) {
					int masterValue=hist_masterBin_forOneVoteForTwoBin[one[0]];
					if (max_masterValue<masterValue) {
						max_masterValue=masterValue;
						finalSel=one;
					}
				}
				if (finalSel ==null ) {//bins in maxEntry are all not master bins, are voted by the nearby bin
					finalSel=maxEntrys.getFirst();
				}
			}else {
				finalSel=maxEntrys.getFirst();
			}
			MaxBin_ind_val=finalSel;
		}
		return MaxBin_ind_val;
	}
	
	public String getOneBinRang_inString(int binInd) {
		int[] binInds_eachDim=General.getMutiDimArr_FromOneDimInd(binInd, eachDimSize);
		StringBuffer binRang=new StringBuffer();
		for (int i = 0; i < binInds_eachDim.length; i++) {
			int binInd_eachDim=binInds_eachDim[i];
			binRang.append("\t Dim-"+i+": "+
					(binInd_eachDim==0 ? " < "+bins[i][binInd_eachDim] : binInd_eachDim==bins[i].length ? " > "+bins[i][binInd_eachDim-1] : bins[i][binInd_eachDim-1]+" <--<= "+bins[i][binInd_eachDim]));
		}
		return binRang.toString();
	}
	
	public float[][] getOneBinRang(int binInd) {
		int[] binInds_eachDim=General.getMutiDimArr_FromOneDimInd(binInd, eachDimSize);
		float[][] bins_eachDim=new float[binInds_eachDim.length][];
		for (int i = 0; i < bins_eachDim.length; i++) {
			bins_eachDim[i]=getOneBinRang_eachDim(binInds_eachDim[i], bins[i]);
		}
		return bins_eachDim;
	}
	
	public float[] getOneBinRang_eachDim(int binInd_eachDim, float[] thisDimBin) {
		if (binInd_eachDim==0) {
			return new float[]{Integer.MIN_VALUE, thisDimBin[0]};
		}else if (binInd_eachDim==thisDimBin.length) {
			return new float[]{thisDimBin[thisDimBin.length-1],Float.MAX_VALUE};
		}else {
			return new float[]{thisDimBin[binInd_eachDim-1],thisDimBin[binInd_eachDim]};
		}
	}
	
	public float[] getNormlisedHist(){
		int totVoteNum=sampleNum*(oneVoteForTwoBin?2*eachDimSize.length:1);
		return General.normliseArr(hist, totVoteNum);
	}
	
	public ArrayList<M> getMaxBinDeSamples() {
		return getOneBinDeSamples(getMaxBin_ind_val()[0]);
	}
	
	public ArrayList<M> getOneBinDeSamples(int binInd) {
		return samplesInBins.get(binInd);
	}
	
	public String makeRes(String floatValueFormat, boolean show_samplesInBins) {
		if (show_samplesInBins) {
			return "sampleNum:"+sampleNum+", tot-"+eachDimSize.length+" dim, eachDimSize:"+General.IntArrToString(eachDimSize, "_")+", hist on this bin:"+hist+
					"\n"+"samplesInBins: "+samplesInBins;
		}else {
			return "sampleNum:"+sampleNum+", tot-"+eachDimSize.length+" dim, eachDimSize:"+General.IntArrToString(eachDimSize, "_")+", hist on this bin:"+hist;
		}
	}
}
