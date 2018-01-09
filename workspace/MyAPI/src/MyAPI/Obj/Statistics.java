package MyAPI.Obj;

import java.text.DecimalFormat;
import java.util.NavigableSet;

import MyAPI.General.ComparableCls.slave_masterFloat_DES;

public class Statistics<T> {
	private SortTop<T> sortTop_DES;
	private SortTop<T> sortTop_ASC;
	private double total;
	private int sampleNum;
	
	public Statistics(int top) throws InterruptedException {
		iniObj(top);
	}
	
	private void iniObj(int top) throws InterruptedException {
		sortTop_DES=new SortTop<>("DES", top);
		sortTop_ASC=new SortTop<>("ASC", top);
		total=0;
		sampleNum=0;
	}
	
	public void addSample(float sample, T marker){
		total+=sample;
		sampleNum++;
		sortTop_DES.addOneSample(marker, sample);
		sortTop_ASC.addOneSample(marker, sample);
	}
	
	public float getMinValue() {
		return sortTop_ASC.get1st().getMaster();
	}
	
	public NavigableSet<slave_masterFloat_DES<T>> getMinValues() {
		return sortTop_ASC.getTopSamples();
	}
	
	public float getMaxValue() {
		return sortTop_DES.get1st().getMaster();
	}
	
	public NavigableSet<slave_masterFloat_DES<T>> getMaxValues() {
		return sortTop_DES.getTopSamples();
	}
	
	public double getTotalValue() {
		return total;
	}
	
	public int getSampleNum() {
		return sampleNum;
	}
	
	public double getAverage() {
		return total/sampleNum;
	}
	
	public String getFullStatistics(String pattern, boolean oneResOneLine) {
		DecimalFormat form=new DecimalFormat(pattern);
		if (oneResOneLine) {
			StringBuffer res=new StringBuffer();
			res.append("in total "+sampleNum+" samples, total:"+form.format(total)+", average:"+form.format(total/sampleNum)+"\n");
			//min
			res.append("mins: "+"\n");
			for (slave_masterFloat_DES<T> one:sortTop_ASC.getTopSamples()) {
				res.append(one+"\n");
			}
			//max
			res.append("max: "+"\n");
			for (slave_masterFloat_DES<T> one:sortTop_DES.getTopSamples()) {
				res.append(one+"\n");
			}
			return res.toString();
		}else {
			return "in total "+sampleNum+" samples, mins:"+sortTop_ASC.getTopSamples()+", maxs:"+sortTop_DES.getTopSamples()+", total:"+form.format(total)+", average:"+form.format(total/sampleNum);
		}
	}
	
	public static void main(String[] args) throws InterruptedException{
		//test
		Statistics<String> stat=new Statistics<String>(2);
		for (int i = 0; i < 100; i++) {
			stat.addSample(i, "Sam"+i);
		}
		System.out.println(stat.getFullStatistics("0",true));
	}
	
}
