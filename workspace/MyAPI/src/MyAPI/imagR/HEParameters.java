package MyAPI.imagR;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;

public class HEParameters {
	public int HMDistThr;
	private double HMWeight_deta;
	public float[] hammingW;
	
	public HEParameters(int HMDistThr, double HMWeight_deta) {
		ini(HMDistThr, HMWeight_deta);
	}
	
	public HEParameters(String rerankHEPara) {//rerankHEPara: reRHE@18@20
		String[] temp=rerankHEPara.split("@");
		ini(Integer.valueOf(temp[1]), Double.valueOf(temp[2]));
	}
	
	private void ini(int HMDistThr, double HMWeight_deta){
		this.HMDistThr=HMDistThr; //if HMDistThr==64, then it is BOF
		this.HMWeight_deta=HMWeight_deta;
		hammingW= General_BoofCV.make_HMWeigthts(HMWeight_deta, HMDistThr); //HMWeight_deta<=0 means no-hammingWeighting
	}
	
	@Override
	public String toString(){
		return "HMDistThr:"+HMDistThr+", HMWeight_deta:"+HMWeight_deta+", hammingW:"+General.floatArrToString(hammingW, "_", "0.00");
	}

}
