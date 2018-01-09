package MyAPI.Obj;

import MyAPI.General.General;

public class HistMultiD_Dense_equalSizeBin_forFloat <M extends Object> extends HistMultiD_Dense_generalBin_forFloat<M> {//multi-dimensional hist, as it is Sparse, use hashMap instead of muti-dim array
	/*
	 * the difference: between HistMultiD_Dense_forFloat and HistMultiD_Sparse_forFloat is how to save hist
	 * as HashMap or as array
	 * the same: both use one-dim histInd to index multi-dim bins
	 * _equalSizeBin: means it can only handle for bin with same interval!
	 */
	
	protected float[][] eachDimBegEndStp;
	
	public HistMultiD_Dense_equalSizeBin_forFloat(boolean saveSample, boolean oneVoteForTwoBin){
		super(saveSample, oneVoteForTwoBin);
	}
	
	@Override
	public void makeEqualBins(float[][] begin_end_step, String floatValueFormat, StringBuffer binInfo){
		//float value has some rounding problem, if 0~1, then, 0,0.1,0.2,0.3,0.400000006,0.5000007.....
		//this function assume, (end-begin) can be fully divided by step
		/*  setup[]={begin, end, step}*/
		bins=new float[begin_end_step.length][]; bins_middle=new float[begin_end_step.length][]; eachDimSize=new int[begin_end_step.length]; eachDimBegEndStp=new float[begin_end_step.length][];
		for (int i = 0; i < begin_end_step.length; i++) {
			bins[i]=General.makeRange(begin_end_step[i]);//attention: the last value in initialBins should > all sample's maximum value! 
			bins_middle[i]=General.makeMiddleBins(bins[i]);
			eachDimSize[i]=bins[i].length+1; //hist num is bin.length+1
			eachDimBegEndStp[i]=new float[]{bins[i][0],bins[i][bins[i].length-1],begin_end_step[i][2]};
		}
		totBinNum=General.Multiplicative_IntArr(eachDimSize);
		iniHist();
		//binInfo
		if (binInfo!=null) {
			binInfo.append("\n in tot-"+eachDimSize.length+" dim, eachDimSize:"+General.IntArrToString(eachDimSize, ","));
			for (int i = 0; i < bins.length; i++) {
				if (bins[i].length<100) {
					binInfo.append(  "\n"+"BegEndStp:  "+General.floatArrToString(eachDimBegEndStp[i], "_", floatValueFormat)
									+"\n"+"Bins:       "+General.floatArrToString(bins[i], "_", floatValueFormat)
									+"\n"+"Bins_middle:"+General.floatArrToString(bins_middle[i], "_", floatValueFormat));
				}else {
					binInfo.append("\n no bin info for this bin-"+i+", because floatArrToString is slow when array is large, its length is larger than 100 which is "+bins[i].length);
				}
			}
		}
	}
	
	@Override
	protected void addOneSample_assignOneBin(float[] sampleV, M sample) {
		int[] binInds=new int[bins.length];
		for (int i = 0; i < bins.length; i++) {
			binInds[i]=General.getBinInd_equalBin(eachDimBegEndStp[i], bins[i].length, sampleV[i]);
		}
		int binIndInOneDim=General.getOneDimInd_forMutiDimArr(binInds, eachDimSize);
		hist[binIndInOneDim]++;
		if (saveSample) {
			samplesInBins.get(binIndInOneDim).add(sample);
		}
		sampleNum++;
	}
	
	@Override
	protected void addOneSample_assignTwoBins(float[] sampleV, M sample) {
		int[][] binInds_eachDim=new int[bins.length][2]; int[] masterBinInds=new int[bins.length];
		for (int i = 0; i < bins.length; i++) {
			binInds_eachDim[i]=General.getBinInd_equalBin_assignTwoBins(eachDimBegEndStp[i], bins[i].length, sampleV[i], bins_middle[i]);
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
}
