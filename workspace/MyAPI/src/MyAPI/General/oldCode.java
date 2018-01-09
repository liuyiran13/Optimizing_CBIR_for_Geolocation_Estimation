package MyAPI.General;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import MyAPI.General.ComparableCls.slave_masterFloat_DES;
import MyAPI.General.Magic.HungarianAlgorithm;
import MyAPI.General.myComparator.ValueComparator_SetSize_ASC;
import MyCustomedHaoop.ValueClass.ImageRegionMatch;

public class oldCode {
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static float compare_HESigs_slowSortAllLinks(byte[][] Sigs_A, byte[][] Sigs_B, int HESigByteNum, int HMDistThr, float[] hammingW, ArrayList<ImageRegionMatch> matches, float idf) throws InterruptedException{
		//get match links and score (sorted by TreeSet)
    	HashMap<Integer, TreeSet<slave_masterFloat_DES<Integer>>> qi_treeSet_dj_score=new HashMap<Integer, TreeSet<slave_masterFloat_DES<Integer>>>(Sigs_A.length);
    	for(int qi=0;qi<Sigs_A.length;qi++){
    		for(int dj=0;dj<Sigs_B.length;dj++){
				int hammingDist=General.get_DiffBitNum(Sigs_A[qi], Sigs_B[dj], 0, HESigByteNum);// computing time: 15% of BigInteger!!
//				int hammingDist=(new BigInteger(querySigs.get(ei))).xor(new BigInteger(TSigs[i][dj])).bitCount();
				if(hammingDist<=HMDistThr){
					float HM_Score=hammingW[hammingDist];
					if (qi_treeSet_dj_score.containsKey(qi)) {
						qi_treeSet_dj_score.get(qi).add(new slave_masterFloat_DES<Integer>(dj, HM_Score));
					}else {
						TreeSet<slave_masterFloat_DES<Integer>> dj_score=new TreeSet<slave_masterFloat_DES<Integer>>(); //default ascending order
						dj_score.add(new slave_masterFloat_DES<Integer>(dj, HM_Score));
						qi_treeSet_dj_score.put(qi, dj_score);
					}
				}
			}
		}
    	float hmWeights=0;
    	if (qi_treeSet_dj_score.size()!=0) {
    		//rank qi based on link num, 
    		//***** sort doc_scores *********
    		TreeMap<Integer, TreeSet<slave_masterFloat_DES<Integer>>> qi_treeSet_dj_score_ASC=null;
    		ValueComparator_SetSize_ASC<Integer,slave_masterFloat_DES<Integer>> mvCompartor = new ValueComparator_SetSize_ASC(qi_treeSet_dj_score);  
    		qi_treeSet_dj_score_ASC = new TreeMap<Integer, TreeSet<slave_masterFloat_DES<Integer>>>(mvCompartor);
    		qi_treeSet_dj_score_ASC.putAll(qi_treeSet_dj_score); 
    		for (int i = 0; i < qi_treeSet_dj_score.size(); i++) {
    			Entry<Integer, TreeSet<slave_masterFloat_DES<Integer>>> current = qi_treeSet_dj_score_ASC.firstEntry();
    			int qi=current.getKey();
    			if (!current.getValue().isEmpty()) {//this qi has no dj matches anymore!
    				int dj=current.getValue().last().getSlave();
        			float hmScore=current.getValue().last().getMaster();
        			if (matches!=null) {
        				matches.add(new ImageRegionMatch(General.byteArr_to_short(Sigs_A[qi], HESigByteNum, 2), General.byteArr_to_short(Sigs_B[dj], HESigByteNum, 2), hmScore*idf));
        			}
        			hmWeights+=hmScore;
        			for (int oneQi : qi_treeSet_dj_score_ASC.keySet()) {
        				slave_masterFloat_DES<Integer> toRemove=null;
    					for ( slave_masterFloat_DES<Integer> oneDj_Score: qi_treeSet_dj_score_ASC.get(oneQi)) {
    						if (oneDj_Score.getSlave()==dj) {
    							toRemove=oneDj_Score;
    						}
    					}
        				qi_treeSet_dj_score_ASC.get(oneQi).remove(toRemove);
        			}
				}//attention: if empty, shoud not break! the rest may still have match exist!
    			qi_treeSet_dj_score_ASC.remove(qi);
			}
		}
		
		return hmWeights;
	}
	
	public static float compare_HESigs_oriMutVsMut(byte[][] Sigs_A, byte[][] Sigs_B, int HESigByteNum, int HMDistThr, float[] hammingW, ArrayList<ImageRegionMatch> matches, float idf) throws InterruptedException{
		/**
		 * oir-way to find match, problem for this: may exist multi vs multi
		 */
		//get match link and score
		float hmWeights=0;
    	for(int qi=0;qi<Sigs_A.length;qi++){
    		for(int dj=0;dj<Sigs_B.length;dj++){
				int hammingDist=General.get_DiffBitNum(Sigs_A[qi], Sigs_B[dj], 0, HESigByteNum);// computing time: 15% of BigInteger!!
//				int hammingDist=(new BigInteger(querySigs.get(ei))).xor(new BigInteger(TSigs[i][dj])).bitCount();
				if(hammingDist<=HMDistThr){
					hmWeights+=hammingW[hammingDist];
					//add match
					if (matches!=null) {
	    				matches.add(new ImageRegionMatch(General.byteArr_to_short(Sigs_A[qi], HESigByteNum, 2), General.byteArr_to_short(Sigs_B[dj], HESigByteNum, 2), hammingW[hammingDist]*idf));
	    			}
				}
			}
		}
		return hmWeights;
	}

	public static float compare_HESigs_simpleHashMap(byte[][] Sigs_A, byte[][] Sigs_B, int HESigByteNum, int HMDistThr, float[] hammingW, ArrayList<ImageRegionMatch> matches, float idf) throws InterruptedException{
		//get match link and score
		HashMap<Integer, HashMap<Integer,Float>> qi_hashMap_dj_score=new HashMap<Integer, HashMap<Integer,Float>>(Sigs_A.length);
    	for(int qi=0;qi<Sigs_A.length;qi++){
    		HashMap<Integer,Float> dj_score=new HashMap<Integer, Float>();
    		for(int dj=0;dj<Sigs_B.length;dj++){
				int hammingDist=General.get_DiffBitNum(Sigs_A[qi], Sigs_B[dj], 0, HESigByteNum);// computing time: 15% of BigInteger!!
//				int hammingDist=(new BigInteger(querySigs.get(ei))).xor(new BigInteger(TSigs[i][dj])).bitCount();
				if(hammingDist<=HMDistThr){
					float HM_Score=hammingW[hammingDist];
					dj_score.put(dj, HM_Score);
				}
			}
    		if (dj_score.size()!=0) {
    			qi_hashMap_dj_score.put(qi, dj_score);
			}
		}
    	float hmWeights=0; int qi_num=qi_hashMap_dj_score.size();
    	if (qi_num!=0) {
    		if (qi_num==1) {//only 1 in qis
    			int qi =qi_hashMap_dj_score.keySet().toArray(new Integer[0])[0];
    			int dj=-1; float hmScore=0;
    			for(Entry<Integer, Float> one_dj_score : qi_hashMap_dj_score.get(qi).entrySet()){
    				if (one_dj_score.getValue() > hmScore) {
    					dj=one_dj_score.getKey(); hmScore=one_dj_score.getValue();
					}
    			}
    			//add match
				if (matches!=null) {
    				matches.add(new ImageRegionMatch(General.byteArr_to_short(Sigs_A[qi], HESigByteNum, 2), General.byteArr_to_short(Sigs_B[dj], HESigByteNum, 2), hmScore*idf));
    			}
				//update hmWeights
    			hmWeights+=hmScore;
			}else {
	    		//rank qi based on link num, 
	    		for (int i = 0; i < qi_num; i++) {
	    			Entry<Integer, HashMap<Integer,Float>> current=null; int minlinkNum=Integer.MAX_VALUE;
	    			for (Entry<Integer, HashMap<Integer,Float>> oneQi : qi_hashMap_dj_score.entrySet()) {
						if (minlinkNum>oneQi.getValue().size()) {
							minlinkNum=oneQi.getValue().size();
							current=oneQi;
						}
					}
	    			if (minlinkNum==1) {
	    				float maxlinkScore=0;
	        			for (Entry<Integer, HashMap<Integer,Float>> oneQi : qi_hashMap_dj_score.entrySet()) {
	    					if (minlinkNum==oneQi.getValue().size()) {//only 1 link
	    						float thisLinkScore=oneQi.getValue().values().toArray(new Float[0])[0];
	    						if (maxlinkScore<thisLinkScore) {
	    							maxlinkScore=thisLinkScore;
	    							current=oneQi;
	    						}
	    					}
	    				}
					}
	    			
	    			int qi=current.getKey();
	    			if (!current.getValue().isEmpty()) {//this qi has dj matches!
	        			int dj=-1; float hmScore=0;
	        			for(Entry<Integer, Float> one_dj_score:current.getValue().entrySet()){
	        				if (one_dj_score.getValue() > hmScore) {
	        					dj=one_dj_score.getKey(); hmScore=one_dj_score.getValue();
							}
	        			}
	        			if (matches!=null) {
	        				matches.add(new ImageRegionMatch(General.byteArr_to_short(Sigs_A[qi], HESigByteNum, 2), General.byteArr_to_short(Sigs_B[dj], HESigByteNum, 2), hmScore*idf));
	        			}
	        			qi_hashMap_dj_score.remove(qi);
	        			hmWeights+=hmScore;
	    				for (int oneQi : qi_hashMap_dj_score.keySet()) {
	    					qi_hashMap_dj_score.get(oneQi).remove(dj);
	        			}
					}else{//attention: if empty, shoud not break! the rest may still have match exist!
						qi_hashMap_dj_score.remove(qi);
					}
				}
			}
		}
		return hmWeights;
	}
	
	public static float compare_HESigs_matrix_oldTemp(byte[][] Sigs_A, byte[][] Sigs_B, int HESigByteNum, int HMDistThr, float[] hammingW, ArrayList<ImageRegionMatch> matches, float idf) throws InterruptedException{
		//get match link and score
		float[][] qi_dj_score =new float[Sigs_A.length][Sigs_B.length]; ArrayList<Integer> qis=new ArrayList<Integer>(); ArrayList<Integer> qi_matchNum=new ArrayList<Integer>();
    	for(int qi=0;qi<Sigs_A.length;qi++){
    		int existMatchNum=0;
    		for(int dj=0;dj<Sigs_B.length;dj++){
				int hammingDist=General.get_DiffBitNum(Sigs_A[qi], Sigs_B[dj], 0, HESigByteNum);// computing time: 15% of BigInteger!!
//				int hammingDist=(new BigInteger(querySigs.get(ei))).xor(new BigInteger(TSigs[i][dj])).bitCount();
				if(hammingDist<=HMDistThr){
					float HM_Score=hammingW[hammingDist];
					qi_dj_score[qi][dj]=HM_Score;
					existMatchNum++;
				}
			}
    		if (existMatchNum>0) {
    			qis.add(qi);
    			qi_matchNum.add(existMatchNum);
			}
		}
    	float hmWeights=0;
    	if (qis.size()!=0) {
    		if (qis.size()==1) {//only 1 in qis
    			int qi=qis.get(0);
    			int dj=-1; float hmScore=0;
    			for(int dj_c=0; dj_c< qi_dj_score[qi].length; dj_c++){
    				if (qi_dj_score[qi][dj_c] > hmScore) {
    					dj=dj_c;
    					hmScore=qi_dj_score[qi][dj_c];
					}
    			}
    			//add match
				if (matches!=null) {
    				matches.add(new ImageRegionMatch(General.byteArr_to_short(Sigs_A[qi], HESigByteNum, 2), General.byteArr_to_short(Sigs_B[dj], HESigByteNum, 2), hmScore*idf));
    			}
				//update hmWeights
    			hmWeights+=hmScore;
			}else {
				//rank qi based on link num, 
	    		int existQi=qis.size(); int[] qi_matchNum_intArr=General.ListToIntArr(qi_matchNum);
	    		for (int i = 0; i < existQi; i++) {
	    			int minInd= General.getMin_ind_val(qi_matchNum_intArr)[0];
	    			if (qi_matchNum_intArr[minInd]!=0) {
	    				//find link: qi, dj, hmScore
	    				int qi=qis.get(minInd);
	    				if (qi_matchNum_intArr[minInd]==1) {//link num ==1
	        				float maxlinkScore=0;
	        				for (int j = 0; j < qis.size(); j++) {
	        					if (qi_matchNum_intArr[j]==1) {//only 1 link
	        						for (int j2 = 0; j2 < qi_dj_score[qis.get(j)].length; j2++) {
	        							if (maxlinkScore<qi_dj_score[qis.get(j)][j2]) {
	            							maxlinkScore=qi_dj_score[qis.get(j)][j2];
	            							qi=qis.get(j);
	            							break;
	            						}
	    							}    						
	        					}
	    					}
	        				minInd=qis.indexOf(qi);
	    				}
	    				int dj=-1; float hmScore=0;
	        			for(int dj_c=0; dj_c< qi_dj_score[qi].length; dj_c++){
	        				if (qi_dj_score[qi][dj_c] > hmScore) {
	        					dj=dj_c;
	        					hmScore=qi_dj_score[qi][dj_c];
	    					}
	        			}
	        			//add match
	    				if (matches!=null) {
	        				matches.add(new ImageRegionMatch(General.byteArr_to_short(Sigs_A[qi], HESigByteNum, 2), General.byteArr_to_short(Sigs_B[dj], HESigByteNum, 2), hmScore*idf));
	        			}
	    				//update hmWeights
	        			hmWeights+=hmScore;
	        			//remove dj from qi_dj_score, update qi_matchNum_intArr
	        			for (int qi_c = 0; qi_c < qi_dj_score.length; qi_c++) {
	        				if (qi_dj_score[qi_c][dj]!=0) {
	        					qi_dj_score[qi_c][dj]=0;
	        					qi_matchNum_intArr[qis.indexOf(qi_c)]-=1;
	    					}
	    				}
	        			//remove qi
	        			qi_matchNum_intArr[minInd]=Integer.MAX_VALUE;
					}else{//attention: if empty, shoud not break! the rest may still have match exist!
						//remove qi
	        			qi_matchNum_intArr[minInd]=Integer.MAX_VALUE;
					}
	    		}
			}
		}
		
		return hmWeights;
	}
	
	public static float compare_HESigs_optimAssign(byte[][] Sigs_A, byte[][] Sigs_B, int HESigByteNum, int HMDistThr, float[] hammingW, ArrayList<ImageRegionMatch> matches, float idf) throws InterruptedException{
		/**
		 * an optimal way to find good matches by solve the "assignment problem" using Hungarian algorithm in time O(n^3)
		 */
		//get matches 
		int toMarkNoLink=HESigByteNum*8*10;//maximum HM dist is the bit number of HESig
		int[][] qi_dj_dist =new int[Sigs_A.length][Sigs_B.length]; ArrayList<Integer> qis=new ArrayList<Integer>(); HashSet<Integer> djs_hashSet=new HashSet<Integer>();
    	for(int qi=0;qi<Sigs_A.length;qi++){
    		int existMatchNum=0;
    		for(int dj=0;dj<Sigs_B.length;dj++){
				int hammingDist=General.get_DiffBitNum(Sigs_A[qi], Sigs_B[dj], 0, HESigByteNum);// computing time: 15% of BigInteger!!
//				int hammingDist=(new BigInteger(querySigs.get(ei))).xor(new BigInteger(TSigs[i][dj])).bitCount();
				if(hammingDist<=HMDistThr){
					qi_dj_dist[qi][dj]=hammingDist;
					djs_hashSet.add(dj);
					existMatchNum++;
				}else {
					qi_dj_dist[qi][dj]=toMarkNoLink;//mark no-link, so cost should be max, named "unreal-link"
				}
			}
    		if (existMatchNum>0) {
    			qis.add(qi);
			}
		}
    	float hmWeights=0;
    	if (qis.size()!=0) {
    		if (qis.size()==1) {//only 1 in qis
    			int qi=qis.get(0);
    			int dj=-1; int minHMDist=toMarkNoLink;
    			for(int dj_c=0; dj_c< qi_dj_dist[qi].length; dj_c++){
    				if (qi_dj_dist[qi][dj_c] < minHMDist) {
    					minHMDist=qi_dj_dist[qi][dj_c];
    					dj=dj_c;
					}
    			}
    			//make hmWeights
    			hmWeights+=hammingW[qi_dj_dist[qi][dj]];
    			//add match
				if (matches!=null) {
    				matches.add(new ImageRegionMatch(General.byteArr_to_short(Sigs_A[qi], HESigByteNum, 2), General.byteArr_to_short(Sigs_B[dj], HESigByteNum, 2), hammingW[qi_dj_dist[qi][dj]]*idf));
				}
			}else if (djs_hashSet.size()==1) {//only 1 in djs
				int dj=djs_hashSet.toArray(new Integer[0])[0];
				int qi=-1; int minHMDist=toMarkNoLink;
    			for(int qi_c=0; qi_c< qi_dj_dist.length; qi_c++){
    				if (qi_dj_dist[qi_c][dj] < minHMDist) {
    					minHMDist=qi_dj_dist[qi_c][dj];
    					qi=qi_c;
					}
    			}
    			//make hmWeights
    			hmWeights+=hammingW[qi_dj_dist[qi][dj]];
    			//add match
				if (matches!=null) {
    				matches.add(new ImageRegionMatch(General.byteArr_to_short(Sigs_A[qi], HESigByteNum, 2), General.byteArr_to_short(Sigs_B[dj], HESigByteNum, 2), hammingW[qi_dj_dist[qi][dj]]*idf));
				}
			}else {
				//make cost matrix
		    	ArrayList<Integer> djs=new ArrayList<Integer>(djs_hashSet);
		    	double[][] costMatrix=new double[qis.size()][djs.size()];
		    	for (int i = 0; i < costMatrix.length; i++) {
					for (int j = 0; j < costMatrix[0].length; j++) {//use dist as cost
						int qi=qis.get(i);
						int dj=djs.get(j);
						costMatrix[i][j]=qi_dj_dist[qi][dj];
					}
				}
		    	//find assignment 
		    	HungarianAlgorithm assignProb_hug=new HungarianAlgorithm(costMatrix);
		    	int[] assig=assignProb_hug.execute();
		    	//make hmWeights
		    	for (int i = 0; i < assig.length; i++) {
					if (assig[i]!=-1) {//this qi do have assignment!
						int qi=qis.get(i);
						int dj=djs.get(assig[i]);
						if (qi_dj_dist[qi][dj]!=toMarkNoLink) {//qi have assignment, but this is a "unreal-link", because the HungarianAlgorithm must provide assignment for nodes
							hmWeights+=hammingW[qi_dj_dist[qi][dj]];
						}
					}
				}
		    	//add match
				if (matches!=null) {
					for (int i = 0; i < assig.length; i++) {
						if (assig[i]!=-1) {//this qi do have assignment!
							int qi=qis.get(i);
							int dj=djs.get(assig[i]);
							if (qi_dj_dist[qi][dj]!=toMarkNoLink) {//qi have assignment, but this is a "unreal-link", because the HungarianAlgorithm must provide assignment for nodes
			    				matches.add(new ImageRegionMatch(General.byteArr_to_short(Sigs_A[qi], HESigByteNum, 2), General.byteArr_to_short(Sigs_B[dj], HESigByteNum, 2), hammingW[qi_dj_dist[qi][dj]]*idf));
							}
						}
					}
    			}
			}
    	}
		return hmWeights;
	}
	
}
