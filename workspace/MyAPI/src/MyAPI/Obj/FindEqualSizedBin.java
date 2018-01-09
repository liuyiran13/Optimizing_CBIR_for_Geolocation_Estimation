package MyAPI.Obj;

import java.util.ArrayList;

import MyAPI.General.General;

public class FindEqualSizedBin {
	private int[] initialBins;
	private int[] hist;
	public int[] target_Bins;
	private boolean needTest;
	private ArrayList<Integer> samples;
	
	public FindEqualSizedBin(int[] initialBins, boolean needTest){
		this.initialBins=initialBins;//attention: the last value in initialBins should > all sample's maximum value! other wise line-38 may get out of array exception
		this.hist=new int[initialBins.length+1];
		this.needTest=needTest;
		if (needTest) {
			this.samples=new ArrayList<Integer>();
		}
	}
	
	public void addOneSample(int sample) {
		if (needTest) {
			samples.add(sample);
		}
		//neighbor num statistics
		int binInd=General.getBinInd_linear(initialBins,sample);
		hist[binInd]++;
	}
	
	public String makeRes(int targetBinNum, int totSampNum) {
		int targeBinSize=totSampNum/targetBinNum+1;
		int currentAccu=0; int currentBinAccu=targeBinSize; int currentBinInd=0; target_Bins=new int[targetBinNum-1];
		for (int i = 0; i < hist.length; i++) {
			currentAccu+=hist[i];
			if (currentAccu>currentBinAccu) {
				target_Bins[currentBinInd]=initialBins[i];
				currentBinAccu+=targeBinSize;
				currentBinInd++;
			}
		}
		//test
		if (needTest) {
			int[] hist_test=new int[target_Bins.length+1];
			for (int one:samples) {
				//neighbor num statistics
				int binInd=General.getBinInd_linear(target_Bins,one);
				hist_test[binInd]++;
			}
			return "testInfo: targetBinNum:"+targetBinNum+", targeBinSize:"+targeBinSize
					+", target_Bins"+General.IntArrToString(target_Bins, "_")+", hist on this bin:"+General.IntArrToString(hist_test, "_");
		}else {
			return null;
		}
	}
}
