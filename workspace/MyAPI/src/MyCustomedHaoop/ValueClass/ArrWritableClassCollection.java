package MyCustomedHaoop.ValueClass;

import java.util.List;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;

public class ArrWritableClassCollection{
	
	public static class MatchFeat_Arr extends AbstractArrWritable<MatchFeat> {
	    public MatchFeat_Arr() {
	        super(MatchFeat.class);
	    }
	    
	    public MatchFeat_Arr(MatchFeat[] objArr) {
	        super(objArr, MatchFeat.class);
	    }
	    
	    public MatchFeat_Arr(List<MatchFeat> objArr) {
	        super(objArr, MatchFeat.class);
	    }
	}
	
	public static class SURFPointOnlyLoc_Arr extends AbstractArrWritable<SURFpoint_onlyLoc> {
	    public SURFPointOnlyLoc_Arr() {
	        super(SURFpoint_onlyLoc.class);
	    }
	    
	    public SURFPointOnlyLoc_Arr(SURFpoint_onlyLoc[] objArr) {
	        super(objArr, SURFpoint_onlyLoc.class);
	    }
	    
	    public SURFPointOnlyLoc_Arr(List<SURFpoint_onlyLoc> objArr) {
	        super(objArr, SURFpoint_onlyLoc.class);
	    }
	}
	
	public static class TVector_Arr extends AbstractArrWritable<TVector> {
		
		public TVector_Arr() {
	        super(TVector.class);
	    }
		
		public TVector_Arr(int vwNum) {
	        this();
	        iniTVectors(vwNum);
	    }
	    
	    public TVector_Arr(TVector[] objArr) {
	        super(objArr, TVector.class);
	    }
	    
	    public TVector_Arr(List<TVector> objArr) {
	        super(objArr, TVector.class);
	    }
	    
	    public void iniTVectors(int vwNum){
			ObjArr=new TVector[vwNum];
			for (int i = 0; i < ObjArr.length; i++) {
				ObjArr[i]=new TVector();
			}
		}
	    
	    public void addOneDoc_featList(int vw, int photoName, List<SURFfeat> featArr, byte[] aggSig){
	    	ObjArr[vw].addOneDoc(photoName, featArr, aggSig);
	    }
	    
	    public void addOneDoc_featArr(int vw, int photoName, SURFfeat[] featArr, byte[] aggSig){
	    	ObjArr[vw].addOneDoc(photoName, featArr, aggSig);
	    }
	    
	    public void sortByDocID(boolean disp) throws InterruptedException{
			for (int i = 0; i < ObjArr.length; i++) {
				General.dispInfo_ifNeed(disp, "", "sortByDocID for TVector:"+i);
				ObjArr[i].sortByDocID(disp);
				disp=false;//only show once
			}
		}
	    
	    public float[] makeIDFsquare(int totDocNum){
			//get idf_squre
	    	float[] idf_squre=new float[ObjArr.length];
			for (int vw_i = 0; vw_i < ObjArr.length; vw_i++) {
				if (ObjArr[vw_i].docNum()>0) {//this vw exist docs
					idf_squre[vw_i]=(float)Math.pow(General_BoofCV.make_idf_logE(totDocNum, ObjArr[vw_i].docNum()), 2); 
				}
			}
			return idf_squre;
		}
	    
	    public TVector getOneTVector(int vw){
	    	return ObjArr[vw];
	    }
	}
	
	public static class HESig_ShortArr_Arr extends AbstractArrWritable<HESig_ShortArr_AggSig> {
	    public HESig_ShortArr_Arr() {
	        super(HESig_ShortArr_AggSig.class);
	    }
	    
	    public HESig_ShortArr_Arr(HESig_ShortArr_AggSig[] objArr) {
	        super(objArr, HESig_ShortArr_AggSig.class);
	    }
	    
	    public HESig_ShortArr_Arr(List<HESig_ShortArr_AggSig> objArr) {
	        super(objArr, HESig_ShortArr_AggSig.class);
	    }
	}
}

