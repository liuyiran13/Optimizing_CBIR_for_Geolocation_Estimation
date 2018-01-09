package MyAPI.imagR;

public class MutiAssVW {
	
	public boolean isMultiAss;
	public int vws_NN;
	public double alph_NNDist;
	public double deta_vwSoftWeight;
	
	public MutiAssVW(boolean isMultiAss, int vws_NN, double alph_NNDist, double deta_vwSoftWeight) {
		ini(isMultiAss, vws_NN, alph_NNDist, deta_vwSoftWeight);
	}
	
	public MutiAssVW(String flag) {//isMultiAss@vws_NN@alph_NNDist@deta_vwSoftWeight
		String[] info=flag.split("@");
		if (Boolean.valueOf(info[0])) {
			ini(Boolean.valueOf(info[0]), Integer.valueOf(info[1]), Double.valueOf(info[2]), Double.valueOf(info[3]));
		}else{
			ini(Boolean.valueOf(info[0]), 1, 0, 0);
		}
	}
	
	private void ini(boolean isMultiAss, int vws_NN, double alph_NNDist, double deta_vwSoftWeight){
		this.isMultiAss=isMultiAss;
		this.vws_NN=vws_NN;//10
		this.alph_NNDist=alph_NNDist;//1.2
		this.deta_vwSoftWeight=deta_vwSoftWeight;
	}

}
