package MyAPI.Obj;

import java.util.ArrayList;

import MyAPI.General.General_Chart;

public class Statistic_MultiClass_1D_Distribution {

	ArrayList<Hist_forFloat<Float>> hists;
	float lastBinValue;
	String[] classNames;
	
	public Statistic_MultiClass_1D_Distribution(String[] classNames, float[] binInfo, String floatValueFormat) {//binInfo: start, end, step
		this.classNames=classNames;
		lastBinValue=binInfo[1];
		//ini hists
		hists=new ArrayList<>(classNames.length);
		for (int i=0; i<classNames.length; i++) {
			hists.add(new Hist_forFloat<Float>(false));
		}
		for (Hist_forFloat<Float> one : hists) {//all his use same binInfo
			one.makeEqualBins(binInfo, floatValueFormat);
		}
	}
	
	public void addOneSample(int classInd, float sample){
		hists.get(classInd).addOneSample(sample, sample);
	}
	
	public void ini(){
		for (Hist_forFloat<Float> one : hists) {//all his use same binInfo
			one.iniHist();
		}
	}
	
	public void dispAsChart(String title, String xLabel, String yLabel){
		float[][] chartData=new float[classNames.length][];
		float[] x=hists.get(0).getBinsForChart(lastBinValue*1.1f);
		for (int i = 0; i < hists.size(); i++) {
			chartData[i]=hists.get(i).getNormlisedHist();
		}
		General_Chart.drawLineChart(title, xLabel, yLabel, classNames, General_Chart.make_SameXValues_LineData(chartData, x));
	}
}
