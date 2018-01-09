package MyAPI.imagR;

import java.util.HashMap;
import java.util.Map.Entry;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;

public class IDF_withStrength {//Strength shoud be 0~1
	HashMap<Integer, Float> iterm_docTotStrength;
	int docNum;
	HashMap<Integer, Float> idf;
	
	public IDF_withStrength(int docNum) {
		iterm_docTotStrength=new HashMap<Integer, Float>();
		this.docNum=docNum;
		idf=null;
	}
	
	public void updateOneIterm(int iterm, float score){//score shoud be 0~1
		General.updateMap(iterm_docTotStrength, iterm, score);
	}
	
	public void calculateIDF(){//
		idf=new HashMap<>();
		for (Entry<Integer, Float> one : iterm_docTotStrength.entrySet()) {
			idf.put(one.getKey(), General_BoofCV.make_idf_logE(docNum, one.getValue())) ;
		}
	}
	
	public float getIDF(int iterm){
		if (idf==null) {
			calculateIDF();
		}
		return iterm_docTotStrength.get(iterm);
	}

}
