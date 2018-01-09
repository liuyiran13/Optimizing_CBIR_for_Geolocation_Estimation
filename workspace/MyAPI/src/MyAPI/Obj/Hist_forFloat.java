package MyAPI.Obj;

import java.util.ArrayList;

import MyAPI.General.General;

public class Hist_forFloat <M extends Object> {
	private float[] bins;
	private float[] bins_middle; //for addOneSample_assignTwoBins
	private int[] hist;
	private int sampleNum;
	private boolean saveSample;
	private ArrayList<ArrayList<M>> samplesInBins;
	
	public Hist_forFloat(boolean saveSample){
		this.saveSample=saveSample;
	}
	
	public String makeEqualBins(float[] binInfo, String floatValueFormat){//binInfo: start, end, step
		return this.makeEqualBins(binInfo[0], binInfo[1], binInfo[2], floatValueFormat);
	}
	
	public String makeEqualBins(float start, float end, float step, String floatValueFormat){
		//float value has some rounding problem, if 0~1, then, 0,0.1,0.2,0.3,0.400000006,0.5000007.....
		//this function assume, (end-begin) can be fully divided by step
		/*  setup[]={begin, end, step}*/
		this.bins=General.makeRange(new float[]{start,end,step});//attention: the last value in initialBins should > all sample's maximum value! 
		this.bins_middle=General.makeMiddleBins(bins);
		iniHist();
		return "\n"	+"Bins:       "+General.floatArrToString(bins, "_", floatValueFormat)+"\n"
					+"Bins_middle:"+General.floatArrToString(bins_middle, "_", floatValueFormat);
	}
	
	public void iniHist() {
		this.hist=new int[bins.length+1];
		if (saveSample) {
			this.samplesInBins=General.ini_ArrayList_ArrayList(hist.length, 10);
		}
		sampleNum=0;
	}
	
	public void addOneSample(float sampleV, M sample) {
		int binInd=General.getBinInd_linear(bins,sampleV);
		hist[binInd]++;
		if (saveSample) {
			samplesInBins.get(binInd).add(sample);
		}
		sampleNum++;
	}
	
	public void addOneSample_assignTwoBins(float sampleV, M sample) {
		int[] binInds=General.getBinInd_linear_assignTwoBins(bins,bins_middle,sampleV);
		for (int binInd : binInds) {
			hist[binInd]++;
			if (saveSample) {
				samplesInBins.get(binInd).add(sample);
			}
		}
		sampleNum++;
	}
	
	public float[] getBinsForChart(float bigNum){
		//as hist has length=bins.length+1, so if show hist, then bins size should also be bins.length+1, 
		//just add a big number to the last
		float[] binsToShow=new float[bins.length+1];
		for (int i = 0; i < bins.length; i++) {
			binsToShow[i] = bins[i];
		}
		binsToShow[bins.length]=bigNum;
		return binsToShow;
	}
	
	public int[] getMaxBin_ind_val() {
		return General.getMax_ind_val(hist);
	}
	
	public String getOneBinRang(int binInd) {
		return binInd==0 ? " < "+bins[binInd] : binInd==bins.length ? " > "+bins[binInd-1] : bins[binInd-1]+" <--<= "+bins[binInd];
	}
	
	public float[] getNormlisedHist(){
		return General.normliseArr(hist, -1);
	}
	
	public ArrayList<M> getMaxBinDeSamples() {
		return getOneBinDeSamples(General.getMax_ind_val(hist)[0]);
	}
	
	public ArrayList<M> getOneBinDeSamples(int binInd) {
		return samplesInBins.get(binInd);
	}
	
	public String makeRes(String floatValueFormat, boolean show_samplesInBins) {
		if (show_samplesInBins) {
			return "sampleNum:"+sampleNum+", Bins: "+General.floatArrToString(bins, "_", floatValueFormat)+", hist on this bin:"+General.IntArrToString(hist, "_")+
					"\n"+samplesInBins;
		}else {
			return "sampleNum:"+sampleNum+", Bins: "+General.floatArrToString(bins, "_", floatValueFormat)+", hist on this bin:"+General.IntArrToString(hist, "_");
		}
	}
}
