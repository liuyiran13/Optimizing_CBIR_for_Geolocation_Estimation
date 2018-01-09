package MyAPI.imagR;

import MyAPI.General.General_BoofCV;

public class IDFTable {

	int totDocNum;
	float[] idfTable;
	boolean isSquare; 
	boolean isNormolise;
	
	public IDFTable(int totDocNum, boolean isSquare, boolean isNormolise, int freqThr) {
		this.totDocNum=totDocNum;
		idfTable=new float[totDocNum+1];
		freqThr=freqThr<0?Integer.MAX_VALUE:freqThr;
		for (int i = 1; i < totDocNum; i++) {
			if (i<=freqThr) {
				idfTable[i]=General_BoofCV.make_idf_log10(totDocNum, i);
			}
		}
		if (totDocNum==1) {//some case only 1 doc
			idfTable[1]=1f;
		}else{
			if (isSquare) {
				for (int i = 1; i < totDocNum; i++) {
					idfTable[i]*=idfTable[i];
				}
			}
			if (isNormolise) {
				float maxIDF=idfTable[1];
				for (int i = 1; i < totDocNum; i++) {
					idfTable[i]/=maxIDF;
				}
			}
		}
	}
	
	public float getOne(int docFreq){
		return idfTable[docFreq];
	}
	
	public int get_totDocNum(){
		return totDocNum;
	}

}
