package MyAPI.imagR;

import java.util.ArrayList;
import java.util.HashMap;

import MyAPI.General.General_BoofCV;
import MyAPI.Obj.Disp;
import MyCustomedHaoop.ValueClass.DocInfo;
import MyCustomedHaoop.ValueClass.PhotoAllFeats;
import MyCustomedHaoop.ValueClass.PhotoPointsLoc;
import MyCustomedHaoop.ValueClass.SURFfeat_ShortArr_AggSig;
import MyCustomedHaoop.ValueClass.SURFpoint;
import MyCustomedHaoop.ValueClass.SURFpoint_ShortArr;

public class PhotoAllFeats_orgVW{
	public int ID;
	public ArrayList<SURFpoint> interestPoints;
	public HashMap<Integer,SURFfeat_ShortArr_AggSig> VW_Sigs; 
	public int height;
	public int width;
	
	public PhotoAllFeats_orgVW(int ID, ArrayList<SURFpoint> interestPoints, HashMap<Integer,SURFfeat_ShortArr_AggSig> VW_Sigs, int height, int width){
		this.ID=ID;
		this.interestPoints=interestPoints;
		this.VW_Sigs=VW_Sigs;
		this.height=height;
		this.width=width;
	}
	
	public PhotoAllFeats_orgVW(int ID, PhotoAllFeats feats, Disp disp){
		this.ID=ID;
		//interestPoints
		this.interestPoints=feats.group_InterestPoints();
		//VW_Sigs
		this.VW_Sigs=feats.group_VW_SURFfeatAggSig();
		//height, width
		this.height=feats.height;
		this.width=feats.width;
		//disp
		disp.disp("photID: "+ID+", "+feats.toString());
	}
	
	public SURFpoint[] getIntersetPoint(){
		return interestPoints.toArray(new SURFpoint[0]);
	}
	
	public PhotoPointsLoc getPhotoPointsLoc(){
		return new PhotoPointsLoc(width, height, new SURFpoint_ShortArr(interestPoints).getSURFPointOnlyLoc());
	}
	
	public DocInfo getDocInfo(float[] idf_square){
		return new DocInfo((short)interestPoints.size(), computeBagOfVWVectorNorm(), computeIDFBagOfVWVectorNorm(idf_square), computeIDF1VW1FeatVectorNorm(idf_square), (short)width, (short)height);
	}
	
	public float computeBagOfVWVectorNorm(){
		return General_BoofCV.computeBagOfVWVectorNorm(VW_Sigs);
	}
	
	public float computeIDFBagOfVWVectorNorm(float[] idf_square){
		return General_BoofCV.computeIDFBagOfVWVectorNorm(VW_Sigs, idf_square);
	}
	
	public float computeIDF1VW1FeatVectorNorm(float[] idf_square){
		return General_BoofCV.computeIDF1VW1FeatVectorNorm(VW_Sigs, idf_square);
	}
	
	public int getMaxDim(){
		return Math.max(height, width);
	}
	
	public int getFeatNum(){
		return interestPoints.size();
	}
}