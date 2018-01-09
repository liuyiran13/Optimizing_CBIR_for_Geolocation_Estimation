package MyAPI.General;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import MyAPI.Interface.FeatInd_Score;
import MyAPI.Obj.HistMultiD_Sparse_equalSizeBin_forFloat;
import MyAPI.Obj.MatchFeat_VW_matchScore;
import MyAPI.Obj.Pair_int;
import MyCustomedHaoop.ValueClass.DocAllMatchFeats;
import MyCustomedHaoop.ValueClass.Int_MatchFeatArr;
import MyCustomedHaoop.ValueClass.MatchFeat;
import MyCustomedHaoop.ValueClass.MatchFeat_VW;
import MyCustomedHaoop.ValueClass.SURFpoint;

public class PGMSource {

	@SuppressWarnings("rawtypes")
	public static void main(String[] args){
		//config hist
		HistMultiD_Sparse_equalSizeBin_forFloat hist_PariAngle=new HistMultiD_Sparse_equalSizeBin_forFloat<Pair_int>(true,true);
		float rotationStep=0.52f;//0.52 is 30.C: 0.26,0.52,0.78
		float scaleStep=0.2f;//0.1,0.2,0.3
		float[][] begin_end_step=new float[][]{{(float) -Math.PI,(float) Math.PI,rotationStep},{-1,1,scaleStep}};
		hist_PariAngle.makeEqualBins(begin_end_step, "0.00", null);
		//run
//		scoreDoc_byPGM(...);
	}
	
	public static float[] loadIDFs(String filePath) throws IOException{
		BufferedReader reader= new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"));
		float[] res= General.StrArrToFloatArr(reader.readLine().split(","));
		reader.close();
		return res;
	}
	
	public static float scoreDoc_byPGM(DocAllMatchFeats docMatches, SURFpoint[] thisQueryFeats, int queryID, boolean isUPRightFeat,
			float[] idf_squre, boolean isOnlyUseHMDistFor1Vs1, HistMultiD_Sparse_equalSizeBin_forFloat<Integer> hist, boolean disp) throws InterruptedException, IOException {
		/**
		 * based on the initial rank(doc_scores_order), for each doc, do 1vs1 and Angel check! return final score
		 */
		int thisDocID=docMatches.DocID; int iniMatchNum=docMatches.getMatchNum();
		//select 1vs1 matches
		ArrayList<MatchFeat_VW> matches_1vs1=isOnlyUseHMDistFor1Vs1?select1V1Match_basedOnDist(docMatches,queryID,disp):select1V1Match_basedOnScore(docMatches, idf_squre, queryID, disp);
		//do hist
		ArrayList<MatchFeat_VW> matches_1vs1HV=selectHistMatch(matches_1vs1, thisQueryFeats, hist, true, disp);
		//do PG
		float finalScore=weightMatchByHistAndAngle(matches_1vs1HV, thisQueryFeats, isUPRightFeat, hist, disp);
		//********* debug disp info **********//
		if (disp==true){ 
			System.out.println("\t iniMatchNum:"+iniMatchNum+", do hist&angle check, matches_1vs1HV:"+matches_1vs1HV.size());
			System.out.println("\t for this doc-"+thisDocID+", finalScore:"+finalScore);
			disp=false; //only disp once!
		}
		return finalScore;
	}
	
	
	public static ArrayList<MatchFeat_VW> selectHistMatch(ArrayList<MatchFeat_VW> matches, SURFpoint[] interestPoints_Q, HistMultiD_Sparse_equalSizeBin_forFloat<Integer> hist, boolean isReturn, boolean disp){
		//calculate transfer from query to doc, select matches that are in the major transfer bin
		hist.iniHist(); //initialise hist
		int total = matches.size();
		//make pair angle hist
		for(int i=0; i < total; i++) {
			SURFpoint docPoint=matches.get(i).docFeat.getSURFpoint();
			SURFpoint queryPoint=interestPoints_Q[matches.get(i).QFeatInd];
			//rotation
			float rotation=makeRotationTo_NPtoPP(docPoint.angle-queryPoint.angle);
			//scale
			float scale=(float)Math.log10(docPoint.scale/queryPoint.scale);
			//add to hist
			hist.addOneSample(new float[]{rotation,scale}, i);//use point index i in goodMatches as sample 
		}
		if (disp) {
			int goodBinInd=hist.getMaxBin_ind_val()[0];
			System.out.println("\t Info in selectHistMatch: "+hist.makeRes("0.00", false, false));
			System.out.println("\t in tot-"+total+" goodMatches, hist selected "+hist.getOneBinDeSamples(goodBinInd).size()+" goldMatches, "
			+"in bin: "+hist.getOneBinRang_inString(goodBinInd));
		}
		if (isReturn) {
			ArrayList<Integer> goldMatches_ind=hist.getMaxBinDeSamples(); //only use maxBin's point as good candidate
			ArrayList<MatchFeat_VW> goldMatches=new ArrayList<MatchFeat_VW>(goldMatches_ind.size()*2);
			for (int ind : goldMatches_ind) {
				goldMatches.add(matches.get(ind));
			}
			return goldMatches;
		}else {
			return null;
		}
	}
	
	public static float makeRotationTo_NPtoPP(float angleDif) {
		/**angleDif here is in the form of -2pi~2pi
		 * make oritaion differenct from [-2pi~2pi] to (-pi~pi], 
		 * (pi~2pi] is corresponding to (-pi~0]
		 * [-2pi~-pi] is corresponding to [0~pi]
		 */
		if (angleDif>Math.PI) {
			return (float) (angleDif-2*Math.PI);
		}else if (angleDif<=-Math.PI) {
			return (float) (angleDif+2*Math.PI);
		}else {
			return angleDif;
		}		
	}
	
	public static ArrayList<MatchFeat_VW> select1V1Match_basedOnDist(DocAllMatchFeats docMatches, int queryID, boolean disp) {
		//rank by 1vs1
		int thisDocID=docMatches.DocID;
		HashMap<Short,ArrayList<MatchFeat_VW>>  matcheCandidates = new HashMap<Short,ArrayList<MatchFeat_VW>>(); 
		int vwNum=0; int totHMmatchNum=0;
		for(Int_MatchFeatArr oneVW_matches:docMatches.feats.feats){// loop over all vw_MatchFeats
			int vw=oneVW_matches.Integer;
			MatchFeat[] matchFeats=oneVW_matches.feats.getArr();
			for (MatchFeat oneMatchFeat:matchFeats) {
				short queryFeatInd=oneMatchFeat.QFeatInd;
    			if (matcheCandidates.containsKey(queryFeatInd)) {
    				matcheCandidates.get(queryFeatInd).add(new MatchFeat_VW(oneMatchFeat,vw));
				}else {
					ArrayList<MatchFeat_VW> newList=new ArrayList<MatchFeat_VW>();
					newList.add(new MatchFeat_VW(oneMatchFeat,vw));
					matcheCandidates.put(queryFeatInd, newList);
				}
			}
			vwNum++;
			totHMmatchNum+=matchFeats.length;
		}
		//select 1vs1 matches
		ArrayList<MatchFeat_VW> selectedMatches=select1V1Match_basedOnDist(matcheCandidates);
		//for debug
		if (disp==true) {
			System.out.println("\t show one example for 1VS1 check of one doc, queryID:"+queryID+", thisDocID:"+thisDocID+", their matches:");
			System.out.println("\t total matched vws number: "+vwNum+", tot match Num among all vws: "+totHMmatchNum);
			System.out.println("\t 1vs1 matches: "+selectedMatches.size());
		}
		return selectedMatches;
	}
	
	public static ArrayList<MatchFeat_VW> select1V1Match_basedOnDist(HashMap<Short, ArrayList<MatchFeat_VW>> matchCandidates) {
		ArrayList<MatchFeat_VW> goodMatches=new ArrayList<MatchFeat_VW>(); int qi_num=matchCandidates.size();
    	if (qi_num!=0) {
    		if (qi_num==1) {//only 1 in qis
    			short qi =matchCandidates.keySet().toArray(new Short[0])[0];
    			MatchFeat_VW bestDj=findBestDj(qi, matchCandidates); //dj,minHammingDist,vwInd
    			goodMatches.add(bestDj);
			}else {
	    		//rank qi based on link num, 
	    		for (int i = 0; i < qi_num; i++) {
	    			Entry<Short, ArrayList<MatchFeat_VW>> current=null; int minlinkNum=Integer.MAX_VALUE;
	    			for (Entry<Short, ArrayList<MatchFeat_VW>> oneQi : matchCandidates.entrySet()) {
						if (minlinkNum>oneQi.getValue().size()) {
							minlinkNum=oneQi.getValue().size();
							current=oneQi;
						}
					}
	    			if (minlinkNum==1) {
	    				int minHammingDist=Integer.MAX_VALUE;
	        			for (Entry<Short, ArrayList<MatchFeat_VW>> oneQi : matchCandidates.entrySet()) {
	    					if (minlinkNum==oneQi.getValue().size()) {//only 1 link
	    						short thisHammingDist=oneQi.getValue().get(0).HMDist;
	    						if (thisHammingDist<minHammingDist) {
	    							minHammingDist=thisHammingDist;
	    							current=oneQi;
	    						}
	    					}
	    				}
					}
	    			short qi=current.getKey();
	    			if (!current.getValue().isEmpty()) {//this qi has matches!
	    				MatchFeat_VW bestDj=findBestDj(qi, matchCandidates); //dj,minHammingDist,vwInd
	        			goodMatches.add(bestDj);
	    				//remove qi
	    				matchCandidates.remove(qi);
	    				//remove dj
	    				for (Entry<Short, ArrayList<MatchFeat_VW>> oneQi : matchCandidates.entrySet()) {
	    					int toRemoveDjInd=-1;
	    					for(int k=0; k<oneQi.getValue().size();k++){
	    	    				if (oneQi.getValue().get(k).docFeat.getFeatInd() == bestDj.docFeat.getFeatInd() ) {
	    	    					toRemoveDjInd=k;
	    	    					break;
	    						}
	    	    			}
	    					if (toRemoveDjInd>-1) {//this oneQi contains dj
	    						oneQi.getValue().remove(toRemoveDjInd);
							}
						}
					}else{//attention: if empty, shoud not break! the rest may still have match exist!
						matchCandidates.remove(qi);
					}
				}
			}
		}
    	return goodMatches;
	}
	
	public static ArrayList<MatchFeat_VW> select1V1Match_basedOnScore(DocAllMatchFeats docMatches, float[] idf, int queryID, boolean disp) {
		int thisDocID=docMatches.DocID;
		//make QFeatInd links
		HashMap<Short, ArrayList<MatchFeat_VW_matchScore>> matchCandidates=new HashMap<Short, ArrayList<MatchFeat_VW_matchScore>>();
		int vwNum=0; int totHMmatchNum=0;
		for(Int_MatchFeatArr oneVW_matches:docMatches.feats.feats){// loop over all vw_MatchFeats
			int vw=oneVW_matches.Integer;
			MatchFeat[] matchFeats=oneVW_matches.feats.getArr();
			for (MatchFeat oneMatchFeat:matchFeats) {
				short queryFeatInd=oneMatchFeat.QFeatInd;
				float matchScore=idf[vw];
    			if (matchCandidates.containsKey(queryFeatInd)) {
    				matchCandidates.get(queryFeatInd).add(new MatchFeat_VW_matchScore(new MatchFeat_VW(oneMatchFeat,vw),matchScore));
				}else {
					ArrayList<MatchFeat_VW_matchScore> newList=new ArrayList<MatchFeat_VW_matchScore>();
					newList.add(new MatchFeat_VW_matchScore(new MatchFeat_VW(oneMatchFeat,vw),matchScore));
					matchCandidates.put(queryFeatInd, newList);
				}
			}
			vwNum++;
			totHMmatchNum+=matchFeats.length;
		}
		//find 1vs1 matches
		ArrayList<MatchFeat_VW_matchScore> goodMatches_temp =select1V1Match_basedOnScore(matchCandidates);
		//return
		ArrayList<MatchFeat_VW> goodMatches=new ArrayList<MatchFeat_VW>(goodMatches_temp.size());
		for (MatchFeat_VW_matchScore one : goodMatches_temp) {
			goodMatches.add(one.matchFeat_VW);
		}
		//for debug
		if (disp==true) {
			System.out.println("\t show one example for 1VS1 check of one doc, queryID:"+queryID+", thisDocID:"+thisDocID+", their matches:");
			System.out.println("\t total matched vws number: "+vwNum+", tot match Num: "+totHMmatchNum);
			System.out.println("\t 1vs1 matches: "+goodMatches.size());
		}
    	return goodMatches;
	}
	
	public static <T extends FeatInd_Score> ArrayList<T> select1V1Match_basedOnScore(HashMap<Short, ArrayList<T>> matchCandidates) {
		//find 1vs1 matches
		ArrayList<T> goodMatches=new ArrayList<T>(); int qi_num=matchCandidates.size();
    	if (qi_num!=0) {
    		if (qi_num==1) {//only 1 in qis
    			short qi =matchCandidates.keySet().toArray(new Short[0])[0];
    			T bestDj=findBestDj_maxScore(matchCandidates.get(qi)); //dj,maxScore
    			goodMatches.add(bestDj);
			}else {
	    		//rank qi based on link num, 
	    		for (int i = 0; i < qi_num; i++) {
	    			Entry<Short, ArrayList<T>> current=null; int minlinkNum=Integer.MAX_VALUE;
	    			for (Entry<Short, ArrayList<T>> oneQi : matchCandidates.entrySet()) {
						if (minlinkNum>oneQi.getValue().size()) {
							minlinkNum=oneQi.getValue().size();
							current=oneQi;
						}
					}
	    			if (minlinkNum==1) {
	    				float maxScore=Float.MIN_VALUE; 
	        			for (Entry<Short, ArrayList<T>> oneQi : matchCandidates.entrySet()) {
	    					if (minlinkNum==oneQi.getValue().size()) {//only 1 link
	    						float thisMaxScore=oneQi.getValue().get(0).getScore();
	    						if (maxScore<thisMaxScore) {
	    							maxScore=thisMaxScore;
	    							current=oneQi;
	    						}
	    					}
	    				}
					}
	    			short qi=current.getKey();
	    			if (!current.getValue().isEmpty()) {//this qi has matches!
	    				T bestDj=findBestDj_maxScore(matchCandidates.get(qi)); //dj,minHammingDist,vwInd
	        			goodMatches.add(bestDj);
	    				//remove qi
	    				matchCandidates.remove(qi);
	    				//remove dj
	    				for (Entry<Short, ArrayList<T>> oneQi : matchCandidates.entrySet()) {
	    					int toRemoveDjInd=-1;
	    					for(int k=0; k<oneQi.getValue().size();k++){
	    	    				if (oneQi.getValue().get(k).getFeatInd() == bestDj.getFeatInd() ) {
	    	    					toRemoveDjInd=k;
	    	    					break;
	    						}
	    	    			}
	    					if (toRemoveDjInd>-1) {//this oneQi contains dj
	    						oneQi.getValue().remove(toRemoveDjInd);
							}
						}
					}else{//attention: if empty, shoud not break! the rest may still have match exist!
						matchCandidates.remove(qi);
					}
				}
			}
		}
    	return goodMatches;
	}
	
	public static MatchFeat_VW findBestDj(short qi, HashMap<Short, ArrayList<MatchFeat_VW>>  matchCandidates){
		short minHammingDist=Short.MAX_VALUE; 
		MatchFeat_VW bestOne=null;
		for(MatchFeat_VW one_dj_HMDist : matchCandidates.get(qi)){
			if (one_dj_HMDist.HMDist < minHammingDist) {
				minHammingDist=one_dj_HMDist.HMDist; 
				bestOne=one_dj_HMDist;
			}
		}
		return bestOne;
	}
	
	public static <T extends FeatInd_Score> T findBestDj_maxScore(ArrayList<T>  matchCandidates){
		float maxScore=-1; 
		T bestOne=null;
		for(T one_match : matchCandidates){
			if (one_match.getScore() > maxScore) {
				maxScore=one_match.getScore(); 
				bestOne=one_match;
			}
		}
		return bestOne;
	}
	
	public static float weightMatchByHistAndAngle(ArrayList<MatchFeat_VW> goldMatches, SURFpoint[] interestPoints_Q, boolean isUPRightFeat,
			HistMultiD_Sparse_equalSizeBin_forFloat<Integer> hist, boolean disp) throws InterruptedException{
		//refine these goldMatches by pari-wise angle or scale
		int goodBinInd=hist.getMaxBin_ind_val()[0];
		float[] res_angle=new float[goldMatches.size()]; 
		float[][] goodBins_rot_sca=hist.getOneBinRang(goodBinInd);
		if (isUPRightFeat) {//when feat is upRight, then rotation is turned off, so the rotation bin found by HV is useless!
			float step_half=hist.getOneDim_BegEndStp(0)[2]/2;
			goodBins_rot_sca[0]=new float[]{-step_half,step_half};//-step/2 ~ step/2
		}
		int total_goldMatches = goldMatches.size(); float res=0f;
		if (total_goldMatches>2) {
			//make pair angle hist
			for(int i=0; i < total_goldMatches; i++) {
				for(int j=i+1; j < total_goldMatches; j++) {
					//i,j is a pair in goodMatches, and is two point in docImage, so they generate a vector, vector0 in doc, vector1 in query!
					float[] vector0=new float[]{goldMatches.get(i).docFeat.getSURFpoint().x-goldMatches.get(j).docFeat.getSURFpoint().x,
							goldMatches.get(i).docFeat.getSURFpoint().y-goldMatches.get(j).docFeat.getSURFpoint().y}; //doc vector
					float[] vector1=new float[]{interestPoints_Q[goldMatches.get(i).QFeatInd].x-interestPoints_Q[goldMatches.get(j).QFeatInd].x,
							interestPoints_Q[goldMatches.get(i).QFeatInd].y-interestPoints_Q[goldMatches.get(j).QFeatInd].y};//query vector
					//get vectorLength
					double vectorLength_0=Math.sqrt(vectorInnerMut(vector0,vector0));
					double vectorLength_1=Math.sqrt(vectorInnerMut(vector1,vector1));
					//get angle
					float angle=vectorRotationAngle(vector1, vector0, vectorLength_1, vectorLength_0);//from query vector_1 to doc vector_0, (-pi~pi]
					//get scale
					float scale=(float) Math.log10(vectorLength_0/vectorLength_1);
					//judge goodness
					if (isInRange(angle, goodBins_rot_sca[0]) && isInRange(scale, goodBins_rot_sca[1])) {
						res_angle[i]++; res_angle[j]++;
					}
				}
			}
			int finalSelNum=0;
			for (int i = 0; i < res_angle.length; i++) {
				if (res_angle[i]>0) {
					finalSelNum++;
				}
				res+=res_angle[i];
			}
			//show info
			if (disp) {
				
				System.out.println("\t Info in weightMatchByHistAndAngle: ");
				System.out.println("\t do pair-wise angle check, in tot-"+total_goldMatches*(total_goldMatches-1)/2+" pairs"
							+", final pari-wise angel selected pointNum:"+finalSelNum);
			}
		}else if (disp) {
			System.out.println("\t Info in weightMatchByHistAndAngle: ");
			System.out.println("\t no pair-wise angle check! as hist selected matches is less than 3");
		}
		return res;
	}
	
	public static float vectorInnerMut(float[] feat1, float[] feat2) {//two vector's inner product
		float res=0; 
		for (int j=0;j<feat1.length;j++){
			res+=feat1[j]*feat2[j];
		}
		return res;
	}
	
	public static float vectorRotationAngle(float[] feat1, float[] feat2, double vectorLength_1, double vectorLength_2){//two 2D-vector's rotation angle, feat2->feat1, in (-pi,pi] format
		//counterclockwise is positive rotation direction!!
		float crossProduct=feat1[0]*feat2[1]-feat1[1]*feat2[0];
		if (crossProduct>=0) {//feat2 is counterclockwise from feat1, 
			return (float) Math.acos(vectorAngleCos(feat1, feat2, vectorLength_1, vectorLength_2));
		}else {//feat2 is clockwise from feat1, when crossProduct==0, two vector is parallel, then angle ==0 or pi
			return (float) -Math.acos(vectorAngleCos(feat1, feat2, vectorLength_1, vectorLength_2));
		}
	}
	
	public static float vectorAngleCos(float[] feat1, float[] feat2, double vectorLength_1, double vectorLength_2) {//two vector's angle in cos format
		if (vectorLength_1>0 && vectorLength_2>0) {//already have vectorLength in previous, so save computing time
			return (float) (vectorInnerMut(feat1, feat2)/vectorLength_1/vectorLength_2);
		}else {
			return (float) (vectorInnerMut(feat1, feat2)/Math.sqrt(vectorInnerMut(feat1,feat1))/Math.sqrt(vectorInnerMut(feat2,feat2)));
		}
	}
	
	public static boolean isInRange(float v, float[] low_high) {
		if (v>=low_high[0] && v<=low_high[1]) {
			return true;
		}else {
			return false;
		}
	}
	
	public static float[] StrArrToFloatArr(String[] strArr) {
		float[] floatArr=new float[strArr.length];
		for(int i=0;i<strArr.length;i++){
			floatArr[i]=Float.valueOf(strArr[i]);
		}
		return floatArr;
	}

}
