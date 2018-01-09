package MyAPI.Obj;

import java.util.LinkedList;

public class Normlise {

	LinkedList<Double> samples;
	
	public Normlise() {
		samples=new LinkedList<>();
	}

	public void addOneSample(double a){
		samples.add(a);
	}
	
	public LinkedList<Double> normliseByMax() throws InterruptedException{
		//find abs max
		Statistics<String> stat=new Statistics<String>(1);
		for (Double one : samples) {
			stat.addSample((float)Math.abs(one), "");
		}
		//get res
		LinkedList<Double> res=new LinkedList<>();
		for (Double one : samples) {
			res.add(one/stat.getMaxValue());
		}
		return res;
	}
}
