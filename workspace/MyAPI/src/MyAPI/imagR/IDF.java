package MyAPI.imagR;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import MyAPI.General.General;
import MyAPI.Obj.Statistics;

public class IDF {
	public HashMap<Integer, Integer> iterm_docFreq;
	IDFTable idfTable;
	
	public IDF() {//when initialise, do not know totDocNum, so no IDFTable
		iterm_docFreq=new HashMap<Integer, Integer>();
	}
	
	public IDF(IDFTable idfTable) {//when initialise, do know totDocNum, so IDFTable
		this();
		this.idfTable=idfTable;
	}
	
	public void updateOneIterm(int iterm){
		General.updateMap(iterm_docFreq, iterm, 1);
	}
	
	public int get_Freq(int iterm){
		return iterm_docFreq.get(iterm);
	}
	
	public void makeIDFTable(int totDocNum, boolean isSquare, boolean isNormolise, int freqThr){
		General.Assert(idfTable==null, "err! idfTable alreay exist!");
		idfTable=new IDFTable(totDocNum, isSquare, isNormolise, freqThr);
	}
	
	public float getIDF(int iterm){
		return idfTable.getOne(iterm_docFreq.get(iterm));
	}
	
	public HashMap<Integer, Float> getAllIDFs(){
		HashMap<Integer, Float> iterm_idfs=new HashMap<>();
		for (Entry<Integer, Integer> one : iterm_docFreq.entrySet()) {
			iterm_idfs.put(one.getKey(), idfTable.getOne(one.getValue()));
		}
		return iterm_idfs;
	}
	
	public Set<Entry<Integer, Integer>> getAllFreqs(){
		return iterm_docFreq.entrySet();
	}
	
//	public FeatIDFs getFeatIDFs(){
//		calculateIDF();
//		LinkedList<Integer> keylist = new LinkedList<Integer>();
//		LinkedList<Float> valuelist=new LinkedList<Float>();
//		General.Map_to_List(iterm_idf, keylist, valuelist);
//		return new FeatIDFs(keylist, valuelist);
//	}
	
	public String getIDFStatistic(int topNum) throws InterruptedException{
		Statistics<Integer> stat=new Statistics<Integer>(topNum);
		for (Entry<Integer, Integer> one : iterm_docFreq.entrySet()) {
			stat.addSample(idfTable.getOne(one.getValue()), one.getKey());
		}
		return "IDFStatistic: "+stat.getFullStatistics("0.0", false);
	}

	public String getItermFreqStatistic(int topNum) throws InterruptedException{
		Statistics<Integer> stat=new Statistics<Integer>(topNum);
		for (Entry<Integer, Integer> one : iterm_docFreq.entrySet()) {
			stat.addSample(one.getValue(), one.getKey());
		}
		return "FreqStatistic: "+stat.getFullStatistics("0", false);
	}
	
}
