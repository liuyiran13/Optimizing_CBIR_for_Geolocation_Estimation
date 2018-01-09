package MyAPI.General;
import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.imageio.ImageIO;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;

import MyAPI.General.ComparableCls.slave_masterFloat_ASC;
import MyAPI.General.ComparableCls.slave_masterFloat_DES;
import MyAPI.General.ComparableCls.slave_masterInteger_DES;
import MyAPI.General.myComparator.ValueComparator_Dou_DES;
import MyAPI.General.myComparator.ValueComparator_Float_ASC;
import MyAPI.General.myComparator.ValueComparator_Float_DES;
import MyAPI.General.myComparator.Comparator_FloatArr;
import MyAPI.Geo.groupDocs.UserIDs;
import MyAPI.Obj.Disp;
import MyAPI.imagR.GTruth;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
import MyCustomedHaoop.ValueClass.DID_Score;
import MyCustomedHaoop.ValueClass.DID_Score_Arr;
import MyCustomedHaoop.ValueClass.GeoExpansionData.fistMatch_GTruth_Docs_Locations;
import MyCustomedHaoop.ValueClass.IntList_FloatList;


public class General_IR {

	public static void transferDist_to_Sim(ArrayList<Float> docScores, float a) {
		for (int i = 0; i < docScores.size(); i++) {
			docScores.set(i, dist_to_sim(docScores.get(i),a));
		}
	}
	 
	public static String rankToString(int top, List<Integer> ranks, List<Float> scores){
		int actTop=Math.min(top, ranks.size());
		return "top-"+top+" ranks: "+ranks.subList(0, actTop)+", scores:"+scores.subList(0, actTop);
	}
	
	public static void transferDists_to_Sims(ArrayList<DID_Score> docScores, float a) {
		for (int i = 0; i < docScores.size(); i++) {
			docScores.get(i).score=dist_to_sim(docScores.get(i).score, a);
		}
	}
	
	public static String makeNumberLabel(int num, String decimalFormat) {
		DecimalFormat form=new DecimalFormat(decimalFormat);//decimalFormat: 0, 0.0
		if (num<1000) {
			return num+"";
		}else if (num<1000000) {
			int[] res=General.mod_withInt(num, 1000);
			if (res[1]==0) {
				return res[0]+"K";
			}else {
				return form.format((double)num/1000)+"K";
			}
		}else {
			int[] res=General.mod_withInt(num, 1000000);
			if (res[1]==0) {
				return res[0]+"M";
			}else {
				return ""+form.format((double)num/1000000)+"M";
			}
		}
	}
	
	public static float dist_to_sim(float dist,float a){
		return (float) Math.exp(-(dist/a));
	}
	
	public static float AP(HashSet<Integer> groundTruth, ArrayList<Integer> rankedlist) {
		float AP=0; 
		if(groundTruth.size()!=0){
			int retri_rel_num=0; //retrieved-relevent-doc num
			for(int i=0;i<rankedlist.size();i++){
				if(groundTruth.contains(rankedlist.get(i))){ //i-th doc is relevent
					retri_rel_num++;
					AP=AP+(float)retri_rel_num/(i+1); //retrieved-relevent-doc num divided by retrieved-doc num
				}
			}
			if (retri_rel_num!=0) {
				AP=AP/groundTruth.size();
			}
		}
		return AP;
	}
	
	public static float AP_smoothed(HashSet<Integer> groundTruth, ArrayList<Integer> rankedlist) {//evaluation metrix used in Oxford building, and hever, 
		float AP=0; 
		if(groundTruth.size()!=0){
			int retri_rel_num=0; //retrieved-relevent-doc num
			float old_precision=1;
			for(int i=0;i<rankedlist.size();i++){
				if(groundTruth.contains(rankedlist.get(i))){ //i-th doc is relevent
					retri_rel_num++;
					float precision=(float)retri_rel_num/(i+1);
					AP=AP+(precision+old_precision)/2; //retrieved-relevent-doc num divided by retrieved-doc num
				}
				old_precision=(float)retri_rel_num/(i+1);
			}
			if (retri_rel_num!=0) {
				AP=AP/groundTruth.size();
			}
		}
		return AP;
	}
	
	public static float AP(ArrayList<Integer> rankedSamples_TrueClassLabels, int targetClassID, int groundTruthNum) {
		float AP=0; 
		int retri_rel_num=0; //retrieved-relevent-doc num
		for(int i=0;i<rankedSamples_TrueClassLabels.size();i++){
			if(rankedSamples_TrueClassLabels.get(i)==targetClassID){ //i-th doc is relevent
				retri_rel_num++;
				AP=AP+(float)retri_rel_num/(i+1); //retrieved-relevent-doc num divided by retrieved-doc num
			}
		}
		AP=AP/groundTruthNum;
		return AP;
	}
		
	public static String addPhotoPath_MovePhoto(String phoSourceType, int photoIndex, String rankShowPath_photos, int saveInterval, 
			String imageBasePath, int total_photos, MapFile.Reader[] mapFiles) throws Exception{
		String imgFileName=null;
		if (phoSourceType.equalsIgnoreCase("PhotoFile_3M")) {
			imgFileName=photoIndex+"_"+total_photos+".jpg";
			if (!new File(rankShowPath_photos+imgFileName).exists()) {//copy photos to rankShowPath_photos folder 
				General.forTransfer(imageBasePath+(photoIndex/saveInterval*saveInterval+1)+"-"+(photoIndex/saveInterval+1)*saveInterval+"/"+imgFileName, rankShowPath_photos+imgFileName);
			}
		}else if (phoSourceType.equalsIgnoreCase("MapFile")) {//copy photos to rankShowPath_photos folder 
			imgFileName=photoIndex+".jpg";
			if (!new File(rankShowPath_photos+imgFileName).exists()) {
				BufferedImage img = General_Hadoop.readValueFromMFiles( photoIndex,  saveInterval, mapFiles, new BufferedImage_jpg(), new Disp(true, "", null)).getBufferedImage("photoIndex:"+photoIndex, Disp.getNotDisp());
				if (img.getAlphaRaster()!=null) {
					img=General_BoofCV.convertTo3BandColorBufferedImage(img);
				}
				ImageIO.write(img, "jpg", new File(rankShowPath_photos+imgFileName));
			}
		}else if (phoSourceType.equalsIgnoreCase("IndexIsName")) {//copy photos to rankShowPath_photos folder 
			imgFileName=photoIndex+".jpg";
			if (!new File(rankShowPath_photos+imgFileName).exists()) {
				General.forTransfer(imageBasePath+imgFileName, rankShowPath_photos+imgFileName);
			}
		}else {
			throw new InterruptedException("phoSourceType in addPhotoPath_MovePhoto should be PhotoFile_3M, MapFile or IndexIsName! here it is :"+phoSourceType);
		}
		return imgFileName;
	}
	
	public static float random_AP(ArrayList<Integer> Samples_TrueClassLabels, int targetClassID, int groundTruthNum) {
		int[] randOrder=General.randIndex(Samples_TrueClassLabels.size());//random order
		float AP=0; 
		int retri_rel_num=0; //retrieved-relevent-doc num
		for(int i=0;i<randOrder.length;i++){
			if(Samples_TrueClassLabels.get(randOrder[i])==targetClassID){ //i-th doc is relevent
				retri_rel_num++;
				AP=AP+(float)retri_rel_num/(i+1); //retrieved-relevent-doc num divided by retrieved-doc num
			}
		}
		AP=AP/groundTruthNum;
		return AP;
	}
	
	public static double MAP(int classNum, int[] testSamples_label, double[][] prob_estimates_ori, int[] modelLabels) {
		double Map=0;
	    for (int i = 0; i < classNum; i++) { //i: class index in modelLabels
	    	//make rank
	    	HashMap<Integer, Double> totDocScores =new HashMap<Integer, Double>();
	    	for (int j = 0; j < prob_estimates_ori.length; j++) {
	    		totDocScores.put(j, prob_estimates_ori[j][i]);
			}
	    	ValueComparator_Dou_DES mvCompartor = new ValueComparator_Dou_DES(totDocScores);
			TreeMap<Integer,Double> totDocScores_Des = new TreeMap<Integer,Double>(mvCompartor);
			totDocScores_Des.putAll(totDocScores);
			//make groudTruth
			HashSet<Integer> gT=new HashSet<Integer>();
			for (int j = 0; j < testSamples_label.length; j++) {
				if (testSamples_label[j]==modelLabels[i]) {
					gT.add(j);
				}
			}
			//make AP
			Map+=General_IR.AP(gT,new ArrayList<Integer>(totDocScores_Des.keySet()));
		}
	    Map/=classNum;
		return Map;
	}
	
	public static double random_MAP(int classNum, int[] testSamples_label, int[] modelLabels) {
		double Map=0;
	    for (int i = 0; i < classNum; i++) { //i: class index in modelLabels
	    	//make random rank
	    	int[] randIndex=General.randIndex(testSamples_label.length);
	    	ArrayList<Integer> randomRank=new ArrayList<Integer>(randIndex.length);
	    	for (int integer : randIndex) {
	    		randomRank.add(integer);
			}
			//make groudTruth
			HashSet<Integer> gT=new HashSet<Integer>();
			for (int j = 0; j < testSamples_label.length; j++) {
				if (testSamples_label[j]==modelLabels[i]) {
					gT.add(j);
				}
			}
			//make AP
			Map+=General_IR.AP(gT,randomRank);
		}
	    Map/=classNum;
		return Map;
	}
	
	public static float[][] PR_Curve(HashSet<Integer> groundTruth, ArrayList<Integer> rankedlist, ArrayList<Float> rankScores, float[] thresholds, boolean isRetrunPRCurve) {
		float[][] PR_curve=new float[thresholds.length][2]; 
		if(groundTruth.size()!=0){
			for (int thr_i = 0; thr_i < thresholds.length; thr_i++) {//each threshold gives a precision and a recall
				int totPositive=0; int truePositive=0;
				for (int i = 0; i < rankedlist.size(); i++) {
					if (rankScores.get(i)>thresholds[thr_i]) {
						totPositive++;
						if (groundTruth.contains(rankedlist.get(i))) {
							truePositive++;
						}
					}else {
						break;
					}
				}
				PR_curve[thr_i]=isRetrunPRCurve?
									new float[]{truePositive/totPositive,truePositive/groundTruth.size()}//precision, recall
									:new float[]{truePositive,totPositive};//truePositive, totPositive. this is to evaluate tot-pairwise PR: queries are all photos in the retrieval set, and use retrieval to get pairwise scores between all photos 
			}
		}
		return PR_curve;
	}
	
	public static float rankingScore_log(int rank) {// 1/log(rank+1), rank is from 1!!
		float score=(float) (1/Math.log(rank+1));
		return score;
	}
	
	public static void combineRanks_rankingScore_log(IntList_FloatList[] ranks, int Rank_thr, int topDocLength, ArrayList<Integer> topDocs, ArrayList<Float> topScores) throws InterruptedException {//
		HashMap<Integer, Float> doc_score=new HashMap<Integer, Float>(Rank_thr*ranks.length);
		for (int i = 0; i < ranks.length; i++) {
			int ranklen=Math.min(Rank_thr,ranks[i].getIntegers().size());
			for (int j = 0; j < ranklen; j++) {
				int doc=ranks[i].getIntegers().get(j);
				float score=rankingScore_log(j+1);
				if (doc_score.containsKey(doc)) {
					doc_score.put(doc, doc_score.get(doc)+score);
				}else {
					doc_score.put(doc, score);
				}
			}
		}
		rank_get_TopDocScores_treeMap(doc_score, topDocLength, topDocs, topScores, "DES");
		
	}
	
	public static void mergeSortedList_ASC(float[] result, DID_Score[] list) {//index in result is the DID!
		for (DID_Score one:list) {
			result[one.docID]+=one.score;
	    }
	}  
	
	public static void mergeSortedList_ASC(ArrayList<DID_Score> l1, ArrayList<DID_Score> l2) {//l1 and l2 is in ASC order, merge into l1, also in ASC
		for (int index1 = 0, index2 = 0; index2 < l2.size(); index1++) {
	    	if (index1 == l1.size() || l1.get(index1).docID > l2.get(index2).docID) {
	            l1.add(index1, l2.get(index2));
	            index2++;
			}else if (l1.get(index1).docID == l2.get(index2).docID) {
				l1.get(index1).score+=l2.get(index2).score;
	            index2++;
			}
	    }
	}  
	
	public static ArrayList<DID_Score> mergeSortedList_ASC(ArrayList<DID_Score> list1, ArrayList<DID_Score> list2, ArrayList<DID_Score> result) {//l1 and l2 is in ASC order, merge into l1, also in ASC
	    if (result==null) {
	    	result = new ArrayList<DID_Score>();
	    }else {
			result.clear();
		}
	    int index1=0; int index2=0;
		while (index1<list1.size() && index2<list2.size()){
			if (list1.get(index1).docID < list2.get(index2).docID){
				result.add(list1.get(index1));
				index1++;
			}else if (list1.get(index1).docID > list2.get(index2).docID) {
				result.add(list2.get(index2));
				index2++;
			}else {
				result.add(new DID_Score(list1.get(index1).docID, list1.get(index1).score+list2.get(index2).score));
				index1++; index2++;
			}
		}
		while (index1<list1.size()){//list1 owns more
			result.add(list1.get(index1));
			index1++;
		}
		while (index2<list2.size()) {//list2 owns more
			result.add(list2.get(index2));
			index2++;
		}
		return result;
	}
	
	public static <V extends Object> void rank_get_AllSortedDocScores_ArraySort(ArrayList<V> docs, ArrayList<Float> scores, ArrayList<V> sortedDocs, ArrayList<Float> sortedScores, String model) throws InterruptedException {
		General.Assert(docs.size()==scores.size(), "err in rank_get_AllSortedDocScores_ArraySort, docs and scores no equal length! "+docs.size()+"_"+scores.size());
		//compared with rank_get_TopDocScores_treeSet, this is for sort all docs and scores!
		sortedDocs.clear(); sortedScores.clear();
		Integer[] sortOrder = new Integer[docs.size()];		       
		for(int i=0; i<sortOrder.length; i++){
            sortOrder[i] = i;
        }	
		Comparator_FloatArr comp=new Comparator_FloatArr(scores,model);
		Arrays.sort(sortOrder, comp);
		for(int i=0; i<sortOrder.length; i++){
			sortedDocs.add(docs.get(sortOrder[i]));
			sortedScores.add(scores.get(sortOrder[i]));
		}
	}
	
	public static <V extends Object> void rank_get_AllSortedDocScores_treeSet(List<V> docs, ArrayList<Float> scores, List<V> sortedDocs, List<Float> sortedScores, String model) throws InterruptedException {
		General.Assert(docs.size()==scores.size(), "err in rank_get_AllSortedDocScores_treeSet, docs and scores no equal length! "+docs.size()+"_"+scores.size());
		//compared with rank_get_TopDocScores_treeSet, this is for sort all docs and scores!
		sortedDocs.clear(); sortedScores.clear();
		TreeSet<slave_masterFloat_DES<V>> doc_scores_order=new TreeSet<slave_masterFloat_DES<V>>(); //default ascending order
		int i = 0; 
		for (V oneDoc:docs) {
	        doc_scores_order.add(new slave_masterFloat_DES<V>(oneDoc,scores.get(i)));  
	        i++;
		}
		if (model.equalsIgnoreCase("DES")) {	        
			for (slave_masterFloat_DES<V> slaveInt_masterFloat : doc_scores_order.descendingSet()) {//in descending
				sortedDocs.add(slaveInt_masterFloat.getSlave());
				sortedScores.add(slaveInt_masterFloat.getMaster());
			}
		}else if (model.equalsIgnoreCase("ASC")) {
			for (slave_masterFloat_DES<V> slaveInt_masterFloat : doc_scores_order) {//in default ascending order
				sortedDocs.add(slaveInt_masterFloat.getSlave());
				sortedScores.add(slaveInt_masterFloat.getMaster());
			}
		}else {
			throw new InterruptedException("model should be DES or ASC! here model:"+model);
		}
	}
	
	public static <V extends Object> void rank_get_AllSortedDocScores_treeSet(V[] docs, final Float[] scores, V[] sortedDocs, Float[] sortedScores, String model) throws InterruptedException {
		General.Assert(docs.length==sortedDocs.length, "err in rank_get_AllSortedDocScores_treeSet, docs and scores no equal length! "+docs.length+"_"+sortedDocs.length);
		//compared with rank_get_TopDocScores_treeSet, this is for sort all docs and scores!
		TreeSet<slave_masterFloat_DES<V>> doc_scores_order=new TreeSet<slave_masterFloat_DES<V>>(); //default ascending order
		for (int i = 0; i < docs.length; i++) {
	        doc_scores_order.add(new slave_masterFloat_DES<V>(docs[i],scores[i]));    
		}
		if (model.equalsIgnoreCase("DES")) {	  
			int i=0;
			for (slave_masterFloat_DES<V> slaveInt_masterFloat : doc_scores_order.descendingSet()) {//in descending
				sortedDocs[i]=slaveInt_masterFloat.getSlave();
				sortedScores[i]=slaveInt_masterFloat.getMaster();
				i++;
			}
		}else if (model.equalsIgnoreCase("ASC")) {
			int i=0;
			for (slave_masterFloat_DES<V> slaveInt_masterFloat : doc_scores_order) {//in default ascending order
				sortedDocs[i]=slaveInt_masterFloat.getSlave();
				sortedScores[i]=slaveInt_masterFloat.getMaster();
				i++;
			}
		}else {
			throw new InterruptedException("model should be DES or ASC! here model:"+model);
		}
	}

	public static <V extends Object> void rank_get_AllSortedDocIDs_treeSet(ArrayList<V> docInfos, ArrayList<Integer> docIDs, ArrayList<V> sortedDocInfos, ArrayList<Integer> sortedDocIDs, String model) throws InterruptedException {
		General.Assert(docInfos.size()==docIDs.size(), "err in rank_get_AllSortedDocIDs_treeSet, docs and scores no equal length! "+docInfos.size()+"_"+docIDs.size());
		//compared with rank_get_TopDocScores_treeSet, this is for sort all docs and scores!
		sortedDocInfos.clear(); sortedDocIDs.clear();
		TreeSet<slave_masterInteger_DES<V>> docInfo_docIDs_order=new TreeSet<slave_masterInteger_DES<V>>(); //default ascending order
		for (int i = 0; i < docInfos.size(); i++) {
			docInfo_docIDs_order.add(new slave_masterInteger_DES<V>(docInfos.get(i),docIDs.get(i)));    
		}
		if (model.equalsIgnoreCase("DES")) {	        
			for (slave_masterInteger_DES<V> slave_masterInt : docInfo_docIDs_order.descendingSet()) {//in descending
				sortedDocInfos.add(slave_masterInt.getSlave());
				sortedDocIDs.add(slave_masterInt.getMaster());
			}
		}else if (model.equalsIgnoreCase("ASC")) {
			for (slave_masterInteger_DES<V> slave_masterInt : docInfo_docIDs_order) {//in default ascending order
				sortedDocInfos.add(slave_masterInt.getSlave());
				sortedDocIDs.add(slave_masterInt.getMaster());
			}
		}else {
			throw new InterruptedException("model should be DES or ASC! here model:"+model);
		}
	}
	
	public static <V extends Comparable<V>> LinkedList<V> rank_get_AllSortedDocIDs_treeSet(List<V> docInfos, String model) throws InterruptedException {
		TreeSet<V> docInfo_docIDs_order=new TreeSet<>(docInfos); //default ascending order
		if (model.equalsIgnoreCase("DES")) {	   
			return new LinkedList<>(docInfo_docIDs_order.descendingSet());
		}else if (model.equalsIgnoreCase("ASC")) {
			return new LinkedList<>(docInfo_docIDs_order);
		}else {
			throw new InterruptedException("model should be DES or ASC! here model:"+model);
		}
	}
	
	public static <V extends Object> void rank_get_TopDocScores_treeSet(ArrayList<V> docs, ArrayList<Float> scores, int top, ArrayList<V> topDocs,ArrayList<Float> topScores, String model) throws InterruptedException {
		General.Assert(docs.size()==scores.size(), "err in rank_get_TopDocScores_treeSet, docs and scores no equal length! "+docs.size()+"_"+scores.size());
		topDocs.clear(); topScores.clear();
		TreeSet<slave_masterFloat_DES<V>> doc_scores_order=new TreeSet<slave_masterFloat_DES<V>>();
		//**a fast way to get top-ranked docs
		int actNumTopDocs=Math.min(docs.size(),top); //some query does not have enough docs in rank list
		if (model.equalsIgnoreCase("DES")) {
			float thr_min=Float.MAX_VALUE;
			for (int i = 0; i < docs.size(); i++) {
				V doc=docs.get(i);
				float score=scores.get(i);
		        // if the array is not full yet:
		        if (doc_scores_order.size() < actNumTopDocs) {
		        	doc_scores_order.add(new slave_masterFloat_DES<V>(doc,score));
		            if (score<thr_min) //update current thr in doc_scores_order
		            	thr_min = score;
		        } else if (score>thr_min) { // if it is "better" than the least one in the current doc_scores_order
		            // remove the last one ...
		        	doc_scores_order.remove(doc_scores_order.first());
		            // add the new one ...
		        	doc_scores_order.add(new slave_masterFloat_DES<V>(doc,score));
		            // update new thr in doc_scores_order
		        	thr_min = doc_scores_order.first().getMaster();
		        }
			}
			for (slave_masterFloat_DES<V> slaveInt_masterFloat : doc_scores_order.descendingSet()) {//in descending, default ascending order
				topDocs.add(slaveInt_masterFloat.getSlave());
				topScores.add(slaveInt_masterFloat.getMaster());
			}
		}else if (model.equalsIgnoreCase("ASC")) {
			float thr_max=-1;
			for (int i = 0; i < docs.size(); i++) {
				V doc=docs.get(i);
				float score=scores.get(i);
		        // if the array is not full yet:
		        if (doc_scores_order.size() < actNumTopDocs) {
		        	doc_scores_order.add(new slave_masterFloat_DES<V>(doc,score));
		            if (score>thr_max) //update current thr in doc_scores_order
		            	thr_max = score;
		        } else if (score<thr_max) { // if it is "better" than the least one in the current doc_scores_order
		            // remove the last one ...
		        	doc_scores_order.remove(doc_scores_order.last());
		            // add the new one ...
		        	doc_scores_order.add(new slave_masterFloat_DES<V>(doc,score));
		            // update new thr in doc_scores_order
		        	thr_max = doc_scores_order.last().getMaster();
		        }
			}
			for (slave_masterFloat_DES<V> slaveInt_masterFloat : doc_scores_order) {//in default ascending order
				topDocs.add(slaveInt_masterFloat.getSlave());
				topScores.add(slaveInt_masterFloat.getMaster());
			}
		}else {
			throw new InterruptedException("model should be DES or ASC! here model:"+model);
		}
	}

	public static  <V extends Object, K extends Number> void rank_get_TopDocScores_treeSet(HashMap<V, K> doc_scores, int top, ArrayList<V> topDocs,ArrayList<Float> topScores, String model) throws InterruptedException {
		topDocs.clear(); topScores.clear();
		TreeSet<slave_masterFloat_DES<V>> doc_scores_order=new TreeSet<slave_masterFloat_DES<V>>(); //default ascending order
		//**a fast way to get top-ranked docs
		int actNumTopDocs=Math.min(doc_scores.size(),top); //some query does not have enough docs in rank list
		if (model.equalsIgnoreCase("DES")) {
			float thr_min=Float.MAX_VALUE;
			for (Entry<V, K> one : doc_scores.entrySet()) {
				V doc=one.getKey();
				float score=one.getValue().floatValue();
		        // if the array is not full yet:
		        if (doc_scores_order.size() < actNumTopDocs) {
		        	doc_scores_order.add(new slave_masterFloat_DES<V>(doc,score));
		            if (score<thr_min) //update current thr in doc_scores_order
		            	thr_min = score;
		        } else if (score>thr_min) { // if it is "better" than the least one in the current doc_scores_order
		            // remove the last one ...
		        	doc_scores_order.remove(doc_scores_order.first());
		            // add the new one ...
		        	doc_scores_order.add(new slave_masterFloat_DES<V>(doc,score));
		            // update new thr in doc_scores_order
		        	thr_min = doc_scores_order.first().getMaster();
		        }
			}
			for (slave_masterFloat_DES<V> slaveInt_masterFloat : doc_scores_order.descendingSet()) {//in descending
				topDocs.add(slaveInt_masterFloat.getSlave());
				topScores.add(slaveInt_masterFloat.getMaster());
			}
		}else if (model.equalsIgnoreCase("ASC")) {
			float thr_max=-1;
			for (Entry<V, K> one : doc_scores.entrySet()) {
				V doc=one.getKey();
				float score=one.getValue().floatValue();
		        // if the array is not full yet:
		        if (doc_scores_order.size() < actNumTopDocs) {
		        	doc_scores_order.add(new slave_masterFloat_DES<V>(doc,score));
		            if (score>thr_max) //update current thr in doc_scores_order
		            	thr_max = score;
		        } else if (score<thr_max) { // if it is "better" than the least one in the current doc_scores_order
		            // remove the last one ...
		        	doc_scores_order.remove(doc_scores_order.last());
		            // add the new one ...
		        	doc_scores_order.add(new slave_masterFloat_DES<V>(doc,score));
		            // update new thr in doc_scores_order
		        	thr_max = doc_scores_order.last().getMaster();
		        }
			}
			for (slave_masterFloat_DES<V> slaveInt_masterFloat : doc_scores_order) {//in default ascending order
				topDocs.add(slaveInt_masterFloat.getSlave());
				topScores.add(slaveInt_masterFloat.getMaster());
			}
		}else {
			throw new InterruptedException("model should be DES or ASC! here model:"+model);
		}
	}
	
	public static <K extends Object, V extends Number> void rank_get_TopDocScores_treeMap(HashMap<K, V> doc_scores, int top, ArrayList<K> topDocs, ArrayList<Float> topScores, String model) throws InterruptedException {
		//**a fast way to get top-ranked docs
		topDocs.clear(); topScores.clear();
		int actNumTopDocs=Math.min(doc_scores.size(),top); //some query does not have enough docs in rank list
		TreeMap<K,Float> doc_scores_order; 
		if (model.equalsIgnoreCase("DES")) {
			ValueComparator_Float_DES mvCompartor = new ValueComparator_Float_DES(doc_scores);
			doc_scores_order = new TreeMap<K,Float>(mvCompartor);
			float thr_min=Float.MAX_VALUE;
			for (Entry<K, V> one : doc_scores.entrySet()) {
				K doc=one.getKey();
				float score=one.getValue().floatValue();
		        // if the array is not full yet:
		        if (doc_scores_order.size() < actNumTopDocs) {
		        	doc_scores_order.put(doc, score);
		            if (score<thr_min) //update current thr in doc_scores_order
		            	thr_min = score;
		        } else if (score>thr_min) { // if it is "better" than the least one in the current doc_scores_order
		            // remove the last one ...
		        	doc_scores_order.pollLastEntry();
		            // add the new one ...
		        	doc_scores_order.put(doc, score);
		            // update new thr in doc_scores_order
		        	thr_min = doc_scores_order.lastEntry().getValue();
		        }
			}
		}else if (model.equalsIgnoreCase("ASC")) {
			ValueComparator_Float_ASC mvCompartor = new ValueComparator_Float_ASC(doc_scores);
			doc_scores_order = new TreeMap<K,Float>(mvCompartor);
			float thr_max=-1;
			for (Entry<K, V> one : doc_scores.entrySet()) {
				K doc=one.getKey();
				float score=one.getValue().floatValue();
		        // if the array is not full yet:
		        if (doc_scores_order.size() < actNumTopDocs) {
		        	doc_scores_order.put(doc, score);
		            if (score>thr_max) //update current thr in doc_scores_order
		            	thr_max = score;
		        } else if (score<thr_max){ // if it is "better" than the least one in the current doc_scores_order
		            // remove the last one ...
		        	doc_scores_order.pollLastEntry();
		            // add the new one ...
		        	doc_scores_order.put(doc, score);
		            // update new thr in doc_scores_order
		        	thr_max = doc_scores_order.lastEntry().getValue();
		        }
			}
		}else {
			throw new InterruptedException("model should be DES or ASC! here model:"+model);
		}

		topDocs.addAll(new ArrayList<K>(doc_scores_order.keySet()));
		for (K doc:topDocs) {
			topScores.add(doc_scores_order.get(doc));
		}		
	}
	
	public static void rank_get_TopDocScores_slow(HashMap<Integer,Float> doc_scores, int top, ArrayList<Integer> topDocs,ArrayList<Float> topScores, String model) throws InterruptedException {
		topDocs.clear(); topScores.clear();
		//***** sort doc_scores *********
		TreeMap<Integer,Float> doc_scores_order=null;
		if (model.equalsIgnoreCase("DES")) {
			ValueComparator_Float_DES mvCompartor = new ValueComparator_Float_DES(doc_scores);
			doc_scores_order = new TreeMap<Integer,Float>(mvCompartor);
		}else if (model.equalsIgnoreCase("ASC")) {
			ValueComparator_Float_ASC mvCompartor = new ValueComparator_Float_ASC(doc_scores);
			doc_scores_order = new TreeMap<Integer,Float>(mvCompartor);
		}else {
			throw new InterruptedException("model should be DES or ASC! here model:"+model);
		}
		//**a slow way to get top-ranked docs, rank all docs
		doc_scores_order.putAll(doc_scores);
		int actNumTopDocs=Math.min(doc_scores.size(),top); //some query does not have enough docs in rank list
		topDocs.addAll(new ArrayList<Integer>(doc_scores_order.keySet()).subList(0, actNumTopDocs));
		for (Integer doc:topDocs) {
			topScores.add(doc_scores_order.get(doc));
		}		
	}
	
	public static <V extends Object> void rank_get_TopDocScores_PriorityQueue(ArrayList<V> docs, ArrayList<Float> scores, int top, ArrayList<V> topDocs,ArrayList<Float> topScores, String model, boolean isSorted, boolean isClearTopDocs) throws InterruptedException {
		General.Assert(docs.size()==scores.size(), "err in rank_get_TopDocScores_PriorityQueue, docs and scores no equal length! "+docs.size()+"_"+scores.size());
		if (isClearTopDocs) {//sometime, need to concate two ranked list, then isClearTopDocs==false
			topDocs.clear(); topScores.clear();
		}
		if (docs.size()==0) {
			return;
		}else if (docs.size()==1) {
			topDocs.add(docs.get(0));
			topScores.add(scores.get(0));
		}else {
			//**a fast way to get top-unRanked docs
			int actNumTopDocs=Math.min(docs.size(),top); //some query does not have enough docs in rank list
			if (model.equalsIgnoreCase("DES")) {
				PriorityQueue<slave_masterFloat_DES<V>> doc_scores_queue=new PriorityQueue<slave_masterFloat_DES<V>>();
				float thr_min=Float.MAX_VALUE;
				for (int i = 0; i < docs.size(); i++) {
					V doc=docs.get(i);
					float score=scores.get(i);
			        // if the array is not full yet:
			        if (doc_scores_queue.size() < actNumTopDocs) {
			        	doc_scores_queue.add(new slave_masterFloat_DES<V>(doc,score));
			            if (score<thr_min) //update current thr in doc_scores_order
			            	thr_min = score;
			        } else if (score>thr_min) { // if it is "better" than the least one in the current doc_scores_order
			            // remove the last one ...
			        	doc_scores_queue.poll();
			            // add the new one ...
			        	doc_scores_queue.offer(new slave_masterFloat_DES<V>(doc,score));
			            // update new thr in doc_scores_order
			        	thr_min = doc_scores_queue.peek().getMaster();
			        }
				}
				if (isSorted) {//top obj is sorted
					ArrayList<slave_masterFloat_DES<V>> order= get_topRanked_from_PriorityQueue(doc_scores_queue, doc_scores_queue.size());
					for (int i = 0; i < order.size(); i++) {
						topDocs.add(order.get(i).getSlave());
						topScores.add(order.get(i).getMaster());
					}
				}else {//top obj is un-sorted
					for (slave_masterFloat_DES<V> oneElement : doc_scores_queue) {
						topDocs.add(oneElement.getSlave());
						topScores.add(oneElement.getMaster());
					}
				}
			}else if (model.equalsIgnoreCase("ASC")) {
				PriorityQueue<slave_masterFloat_ASC<V>> doc_scores_queue=new PriorityQueue<slave_masterFloat_ASC<V>>();
				float thr_max=-1;
				for (int i = 0; i < docs.size(); i++) {
					V doc=docs.get(i);
					float score=scores.get(i);
			        // if the array is not full yet:
			        if (doc_scores_queue.size() < actNumTopDocs) {
			        	doc_scores_queue.add(new slave_masterFloat_ASC<V>(doc,score));
			            if (score>thr_max) //update current thr in doc_scores_order
			            	thr_max = score;
			        } else if (score<thr_max) { // if it is "better" than the least one in the current doc_scores_order
			        	// remove the last one ...
			        	doc_scores_queue.poll();
			            // add the new one ...
			        	doc_scores_queue.offer(new slave_masterFloat_ASC<V>(doc,score));
			            // update new thr in doc_scores_order
			        	thr_max = doc_scores_queue.peek().getMaster();
			        }
				}
				if (isSorted) {//top obj is sorted
					ArrayList<slave_masterFloat_ASC<V>> order= get_topRanked_from_PriorityQueue(doc_scores_queue, doc_scores_queue.size());
					for (int i = 0; i < order.size(); i++) {
						topDocs.add(order.get(i).getSlave());
						topScores.add(order.get(i).getMaster());
					}
				}else {//top obj is un-sorted
					for (slave_masterFloat_ASC<V> oneElement : doc_scores_queue) {
						topDocs.add(oneElement.getSlave());
						topScores.add(oneElement.getMaster());
					}
				}
			}else {
				throw new InterruptedException("model should be DES or ASC! here model:"+model);
			}
		}
	}
	
	public static <V extends Object> ArrayList<V> get_topRanked_from_PriorityQueue(PriorityQueue<V> doc_scores_queue, int top){
		int actTop=Math.min(top, doc_scores_queue.size());
		ArrayList<V> reverseOrder=new ArrayList<V>(2*actTop);
		for (int i = 0; i < actTop; i++) {
			reverseOrder.add(doc_scores_queue.poll());
		}
		ArrayList<V> order=new ArrayList<V>(2*actTop);
		for (int i = reverseOrder.size()-1; i > -1; i--) {
			order.add(reverseOrder.get(i));
		}
		return order;
	}
	
	public static void showRanks_GoodVsWrong_geoRel(int[] rankInd_right, int[] rankInd_wrong, String[] rankLabels, String[] rankPaths, String showRankDir, int showTopLoc, 
			float[][] latlons, UserIDs userID, float isSameLoc, int showQueryNum, 
			int saveInterval, String phoSourceType, String imageBasePath, int total_photos, MapFile.Reader[] imgMapFiles, int minGTSize, int maxGTSize) throws Exception {
		
		//make taskLabel, select query
		StringBuffer taskLabel=new StringBuffer();
		taskLabel.append("_GTSizeRang-"+minGTSize+"-"+maxGTSize);
    	ArrayList<Integer> selQuerys=new ArrayList<Integer>(); int totalQueryNum=0; int totalQueryNum_0=0;
		if (rankInd_right!=null) {
			for (int  right : rankInd_right) {
				taskLabel.append("_R_"+rankLabels[right]);
			}
	       	totalQueryNum=selectQuerys_geoRel(General.selectArrStr(rankPaths, rankInd_right,0,0), true, selQuerys, latlons, isSameLoc, true, minGTSize, maxGTSize);
		}
		if (rankInd_wrong!=null) {
			for (int  wrong : rankInd_wrong) {
				taskLabel.append("_W_"+rankLabels[wrong]);
			}
	    	totalQueryNum_0=selectQuerys_geoRel(General.selectArrStr(rankPaths, rankInd_wrong,0,0), false, selQuerys, latlons, isSameLoc, rankInd_right==null?true:false, minGTSize, maxGTSize);
		}
		totalQueryNum=Math.max(totalQueryNum, totalQueryNum_0);//if rankInd_right or rankInd_wrong is null, queryNum returned from selectQuerys will be -1
    	System.out.println("total queryNum in rank file:"+totalQueryNum+", selected: "+selQuerys.size());
    	//set rankShowPath
    	String rankShowPath=showRankDir+"showTopLoc"+showTopLoc+taskLabel.toString()+"_inHTML/";
    	//act show query num
    	int selQueryNum=selQuerys.size();
    	int actShowQueryNum=Math.min(selQueryNum, showQueryNum);
    	//html title
    	String HtmlTitle="show ranks for: "+taskLabel+" \n"+General.StrArrToStr(rankPaths, "\n")
    			+"G_ForGTSize=1km, V_ForGTSize=10,000 \n"
    			+"qualified query num:"+selQueryNum+" in total "+totalQueryNum+" queries, here show "+actShowQueryNum+" queries";
    	System.out.println("HtmlTitle: "+HtmlTitle);
    	//show ranks
    	showGeoReleventRanks(selQuerys.subList(0, actShowQueryNum), rankShowPath, HtmlTitle, rankLabels, rankPaths, showTopLoc, 
    			latlons, userID, isSameLoc, saveInterval, phoSourceType, imageBasePath, total_photos, imgMapFiles);
	}
	
	public static void showRanks_GoodVsWrong_APCompare(String dataFlag, int rankInd_right, int rankInd_wrong, String[] rankLabels, String[] rankPaths, String showRankDir, int showTopDoc, int showMaxGT,
			HashMap<Integer, HashSet<Integer>> groundTruth, int[] s_to_l, HashMap<Integer, HashSet<Integer>> junks, DecimalFormat scoreFormat, float AP_thr, int showQueryNum, 
			int saveInterval, MapFile.Reader[] imgMapFiles_forRelPhotos) throws Exception {
		//select query
    	ArrayList<Integer> selQuerys=new ArrayList<Integer>();
    	float[] qNum_MAPs=selectQuerys_APCompare(dataFlag, rankInd_right>-1?rankPaths[rankInd_right]:null, rankInd_wrong>-1?rankPaths[rankInd_wrong]:null, selQuerys, groundTruth, s_to_l, junks, AP_thr);
    	System.out.println("total queryNum in rank file:"+qNum_MAPs[0]+", selected: "+selQuerys.size());
		//make taskLabel
		StringBuffer taskLabel=new StringBuffer();
		taskLabel.append("_AP_thr@"+AP_thr);
		if (rankInd_right>-1) {//when rankInd_right<0, then no compare, only sel query whose AP<-AP_thr, this AP_thr should be negative!
			taskLabel.append("_R_"+rankLabels[rankInd_right]+"@MAP"+new DecimalFormat("0.000").format(qNum_MAPs[1]));
		}
		if (rankInd_wrong>-1) {//when rankInd_wrong<0, then no compare, only sel query whose AP>AP_thr
			taskLabel.append("_W_"+rankLabels[rankInd_wrong]+"@MAP"+new DecimalFormat("0.000").format(qNum_MAPs[2]));
		}
    	//set rankShowPath
    	String rankShowPath=showRankDir+"showTopDoc"+showTopDoc+dataFlag+taskLabel.toString()+"_inHTML/";	
    	//act show query num
    	int selQueryNum=selQuerys.size();
    	int actShowQueryNum=Math.min(selQueryNum, showQueryNum);
    	//html title
    	String HtmlTitle="show ranks for: "+taskLabel+" \n"+General.StrArrToStr(rankPaths, "\n")
    			+"AP_thr="+AP_thr+", qualified query num:"+selQueryNum+" in total "+qNum_MAPs[0]+" queries, here show "+actShowQueryNum+" queries";
    	System.out.println("HtmlTitle: "+HtmlTitle);
    	//show ranks
    	showLabelReleventRanks(dataFlag, selQuerys.subList(0, actShowQueryNum), rankShowPath, HtmlTitle, rankLabels, rankPaths, showTopDoc, showMaxGT,
    			s_to_l, groundTruth, junks, scoreFormat, saveInterval, imgMapFiles_forRelPhotos);
	}

	public static void showGeoReleventRanks(List<Integer> selQuerys, String rankShowPath, String HtmlTitle, String[] rankLabels, String[] rankPaths, int showTopLoc, 
			float[][] latlons, UserIDs userID, float isSameLoc, int saveInterval, String phoSourceType, String imageBasePath, int total_photos, MapFile.Reader[] imgMapFiles) throws Exception {

    	String rankShowPath_photos=rankShowPath+"photos/";
    	General.makeORdelectFolder(rankShowPath);
    	General.makeORdelectFolder(rankShowPath_photos);
    	PrintWriter html = new PrintWriter(new OutputStreamWriter(new FileOutputStream(
    			rankShowPath+"index.html",false), "UTF-8"),true);
    	String rankShowPath_photosInHTML="./photos/";//rankShowPath in html, ./ is the folder for save the html file

    	DecimalFormat deciForm=new DecimalFormat("0");
    	
    	//make rank-readers
    	int selQueryNum=selQuerys.size();
    	MapFile.Reader[] rankFiles=General_Hadoop.openAllMapFiles(rankPaths);
    	
    	//show ranks
    	int htmlSentenceType;
		for (int queryInd = 0; queryInd < selQueryNum; queryInd++) {
			for (int rankFileInd = 0; rankFileInd < rankPaths.length; rankFileInd++) {
				int queryName=selQuerys.get(queryInd);
				fistMatch_GTruth_Docs_Locations Value_RankScores= new fistMatch_GTruth_Docs_Locations();//creat one Value_RankScores per rankFileInd, prevent one file does not contain key, so the Value_RankScores is still unchanged! == the one in previous rankFileInd 
				rankFiles[rankFileInd].get(new IntWritable(queryName), Value_RankScores);
				DID_Score_Arr[] topDocs = Value_RankScores.Docs.getArr();
				float[][] topLocs=Value_RankScores.topLocations.getArrArr();
				int[] firstMatch=Value_RankScores.fistMatch.getIntArr();
				int showTopLoc_act=Math.min(showTopLoc, topDocs.length);
				//set query's ground truth
				ArrayList<String> gTruth=new ArrayList<String>(); ArrayList<String> gTruth_Caption=new ArrayList<String>(); ArrayList<String> gTruth_Color=new ArrayList<String>();
				int gTruth_size=Value_RankScores.getGTSize(); int geoDens=Value_RankScores.geoDensity;
				if (gTruth_size==0) {//if query do not have ground truth, then in GVR, it mark this with {-1,-1}
					ArrayList<Integer> geoNeighbors= General_geoRank.findGeoNeighbors(queryName, isSameLoc, latlons);
					int[] randInd=General.randIndex(new Random(), geoNeighbors.size());
					for (int i = 0; i < Math.min(9, randInd.length); i++) {
						int phoID=geoNeighbors.get(randInd[i]);
						//set gTruth_photos
						gTruth.add(addPhotoPath_MovePhoto(phoSourceType, phoID, rankShowPath_photos, saveInterval, imageBasePath, total_photos, imgMapFiles));
						//set gTruth_Caption
						gTruth_Caption.add("geoDens"+geoDens+userID.getUserIDlabel(phoID)+"_"+phoID);
						//set gTruth_Color
						gTruth_Color.add("rgb("+General.floatArrToString_nolastDelimiter(General.mul(Color.red.getRGBColorComponents(null), 255), ",", "0")+")");
					}					
				}else {
					for(int i=0;i<Math.min(9,gTruth_size);i++){
						GTruth one_gTruth = Value_RankScores.gTruths[i];
						//set gTruth_photos
						gTruth.add(addPhotoPath_MovePhoto(phoSourceType, one_gTruth.photoID, rankShowPath_photos, saveInterval, imageBasePath, total_photos, imgMapFiles));
						//set gTruth_Caption
						gTruth_Caption.add(one_gTruth.rank+userID.getUserIDlabel(one_gTruth.photoID)+"_"+one_gTruth.photoID);
						//set gTruth_Color
						gTruth_Color.add("rgb("+General.floatArrToString_nolastDelimiter(General.mul(Color.green.getRGBColorComponents(null), 255), ",", "0")+")");
					}
				}
				//set query
				String queryFileName=addPhotoPath_MovePhoto(phoSourceType, queryName, rankShowPath_photos, saveInterval, imageBasePath, total_photos, imgMapFiles);
				String queryCaption=rankLabels[rankFileInd]+"_"+gTruth_size+"_"+geoDens+userID.getUserIDlabel(queryName);
				String queryColor="rgb("+General.floatArrToString_nolastDelimiter(General.mul(Color.black.getRGBColorComponents(null), 255), ",", "0")+")";
				//set ranked docs
				ArrayList<ArrayList<String>> groupPhotoPaths=new ArrayList<ArrayList<String>>(showTopLoc_act); ArrayList<ArrayList<String>> groupPhotoDiscrptions=new ArrayList<ArrayList<String>>(showTopLoc_act); ArrayList<ArrayList<String>> groupPhotoColors=new ArrayList<ArrayList<String>>(showTopLoc_act);
				ArrayList<String> groupCaption=new ArrayList<String>();
				for(int i=0;i<showTopLoc_act;i++){
					DID_Score[] thisLoc=topDocs[i].getArr();
					float[] loc=topLocs[i];
					String thisLoc_color=General_geoRank.isOneLocation_GreatCircle(latlons[0][queryName], latlons[1][queryName], loc[0], loc[1], isSameLoc)?
							"rgb("+General.floatArrToString_nolastDelimiter(General.mul(Color.green.getRGBColorComponents(null), 255), ",", "0")+")"
							:"rgb("+General.floatArrToString_nolastDelimiter(General.mul(Color.red.getRGBColorComponents(null), 255), ",", "0")+")";
					//add loc photos
					ArrayList<String> thisloc_Docs=new ArrayList<String>(); ArrayList<String> thisloc_disps=new ArrayList<String>(); ArrayList<String> thisloc_colors=new ArrayList<String>();
					for (DID_Score oneDoc : thisLoc) {
						thisloc_Docs.add(addPhotoPath_MovePhoto(phoSourceType, oneDoc.docID, rankShowPath_photos, saveInterval, imageBasePath, total_photos, imgMapFiles));
						thisloc_disps.add(i+userID.getUserIDlabel(oneDoc.docID)+"_"+oneDoc.docID+"_"+deciForm.format(oneDoc.score)+"_"+deciForm.format(General.calculateGeoDistance(queryName,oneDoc.docID,latlons,"GreatCircle"))+"km");
						thisloc_colors.add(thisLoc_color);
					}
					groupPhotoPaths.add(thisloc_Docs);
					groupPhotoDiscrptions.add(thisloc_disps);
					groupPhotoColors.add(thisloc_colors);
					groupCaption.add(General.floatArrToString(loc, ", ", "0.000")+deciForm.format(General.calculateGeoDistance(latlons[0][queryName], latlons[1][queryName], loc[0], loc[1], "GreatCircle"))+"km");
				}
				//set firstMatch
				if (firstMatch[0]>=showTopLoc_act) {
					ArrayList<String> firstMatch_Docs=new ArrayList<String>(); ArrayList<String> firstMatch_disps=new ArrayList<String>(); ArrayList<String> firstMatch_colors=new ArrayList<String>();
					int firstMatch_PhoID=firstMatch[1];
					firstMatch_Docs.add(addPhotoPath_MovePhoto(phoSourceType, firstMatch_PhoID, rankShowPath_photos, saveInterval, imageBasePath, total_photos, imgMapFiles));
					firstMatch_disps.add("1st match: "+firstMatch[0]);
					firstMatch_colors.add("rgb("+General.floatArrToString_nolastDelimiter(General.mul(Color.green.getRGBColorComponents(null), 255), ",", "0")+")");
					groupPhotoPaths.add(firstMatch_Docs);
					groupPhotoDiscrptions.add(firstMatch_disps);
					groupPhotoColors.add(firstMatch_colors);
					groupCaption.add(deciForm.format(General.calculateGeoDistance(queryName, firstMatch_PhoID, latlons, "GreatCircle"))+"km");
				}
				
				//set htmlSentenceType
				if ( queryInd==0 && rankFileInd==0 ) {
					htmlSentenceType=0;
				}else {
					htmlSentenceType=10;
				}
				if ( queryInd==selQueryNum-1 && rankFileInd==(rankPaths.length-1)) {
					htmlSentenceType=1;
				}
				General.showPhoto_inHTML( html, HtmlTitle, htmlSentenceType,  rankShowPath_photosInHTML,  queryFileName, queryCaption, queryColor,
						gTruth,gTruth_Caption, gTruth_Color, 3,
						null, null, null, groupPhotoPaths,groupPhotoDiscrptions,groupPhotoColors,groupCaption,3);
			}
		}
		
		//close mapFile
		General_Hadoop.closeAllMapFiles(rankFiles);
	}
	
	public static void showLabelReleventRanks(String dataFlag, List<Integer> selQuerys, String rankShowPath, String HtmlTitle, String[] rankLabels, String[] rankPaths, int showTopDoc, int showMaxGT,
			int[] s_to_l, HashMap<Integer, HashSet<Integer>> groundTrue, HashMap<Integer, HashSet<Integer>> junks, DecimalFormat scoreFormat, int saveInterval, MapFile.Reader[] imgMapFiles_forRelPhotos) throws Exception{
		boolean isOxford=dataFlag.contains("Oxford"); boolean isBarcelonaBuilding=dataFlag.contains("Barcelona");
		String rankShowPath_photos=rankShowPath+"photos/";
    	General.makeORdelectFolder(rankShowPath);
    	General.makeORdelectFolder(rankShowPath_photos);
    	PrintWriter html = new PrintWriter(new OutputStreamWriter(new FileOutputStream(
    			rankShowPath+"index.html",false), "UTF-8"),true);
    	String rankShowPath_photosInHTML="./photos/";//rankShowPath in html, ./ is the folder for save the html file
    	
    	//make rank-readers
    	int selQueryNum=selQuerys.size();
    	MapFile.Reader[] rankFiles=General_Hadoop.openAllMapFiles(rankPaths);
    	//make commons
    	ArrayList<String> gtPath_ForHTML=new ArrayList<String>();
		ArrayList<String> gtCaption_ForHTML=new ArrayList<String>();
		ArrayList<String> gtColor_ForHTML=new ArrayList<String>();
		ArrayList<String> phoPath_ForHTML=new ArrayList<String>();
		ArrayList<String> phoCaption_ForHTML=new ArrayList<String>();
		ArrayList<String> phoColor_ForHTML=new ArrayList<String>();
		
    	//show ranks
    	int htmlSentenceType;
		for (int queryInd = 0; queryInd < selQueryNum; queryInd++) {
			int queryName=selQuerys.get(queryInd);
			HashSet<Integer> thisQGroundTru=null;
			HashSet<Integer> thisQJunks=null;
			if (isOxford) {//for Oxford, groundTruth and junks are indexed by building ind
				int buildingInd=queryName/1000;
				thisQGroundTru=groundTrue.get(buildingInd);
				thisQJunks=junks.get(buildingInd);
			}else if (isBarcelonaBuilding) {
				int buildingInd=queryName/10000;
				thisQGroundTru=groundTrue.get(buildingInd);
			}else {
				thisQGroundTru=groundTrue.get(queryName);
			}
			int gTruth_size=thisQGroundTru.size(); 
			ArrayList<Integer> thisQGroundTru_list=new ArrayList<Integer>(thisQGroundTru);
			for (int rankFileInd = 0; rankFileInd < rankPaths.length; rankFileInd++) {
				//get ranked docs
				IntList_FloatList Value_RankScores= new IntList_FloatList();//creat one Value_RankScores per rankFileInd, prevent one file does not contain key, so the Value_RankScores is still unchanged! == the one in previous rankFileInd 
				rankFiles[rankFileInd].get(new IntWritable(queryName), Value_RankScores);
				ArrayList<Integer> topDocs_docIndexInS=new ArrayList<Integer>(Value_RankScores.getIntegers());
				ArrayList<Integer> topDocs_oriIDs=new ArrayList<Integer>(topDocs_docIndexInS.size());
				for (int i = 0; i < topDocs_docIndexInS.size(); i++) {
					topDocs_oriIDs.add(s_to_l[topDocs_docIndexInS.get(i)]);
				}
				ArrayList<Integer> topDocs_oriIDs_forAP=new ArrayList<Integer>(topDocs_oriIDs);
				if (isOxford) {//for Oxford, delete junk photos
					topDocs_oriIDs_forAP.removeAll(thisQJunks);
				}
				//set query
				String queryFileName=addPhotoPath_MovePhoto("MapFile", queryName, rankShowPath_photos, saveInterval, null, 0, imgMapFiles_forRelPhotos);
				String queryCaption=queryName+"_"+gTruth_size+"_"+rankLabels[rankFileInd]+"_AP"+new DecimalFormat("0.000").format(AP_smoothed(thisQGroundTru, topDocs_oriIDs_forAP));
				String queryColor="rgb("+General.floatArrToString_nolastDelimiter(General.mul(Color.black.getRGBColorComponents(null), 255), ",", "0")+")";
				//set query's ground truth
				gtPath_ForHTML.clear();
				gtCaption_ForHTML.clear();
				gtColor_ForHTML.clear();
				for(int i=0;i<Math.min(showMaxGT,thisQGroundTru_list.size());i++){
					int oneGroundTru=thisQGroundTru_list.get(i);
					int rank=topDocs_oriIDs.indexOf(oneGroundTru);
					int phoID=oneGroundTru;
					//set gTruth_photos
					gtPath_ForHTML.add(addPhotoPath_MovePhoto("MapFile", phoID, rankShowPath_photos, saveInterval, null, 0, imgMapFiles_forRelPhotos));
					//set gTruth_Caption
					gtCaption_ForHTML.add(rank+"_"+phoID);
					//set gTruth_Color
					gtColor_ForHTML.add("rgb("+General.floatArrToString_nolastDelimiter(General.mul(Color.green.getRGBColorComponents(null), 255), ",", "0")+")");
				}
				//set docs
				phoPath_ForHTML.clear();
				phoCaption_ForHTML.clear();
				phoColor_ForHTML.clear();
				int showTopLoc_act=Math.min(showTopDoc, topDocs_oriIDs.size());
				for (int i=0; i< showTopLoc_act;i++) {
					int doc=topDocs_oriIDs.get(i);
					int docIndex=topDocs_docIndexInS.get(i);
					//set photo
					phoPath_ForHTML.add(General_IR.addPhotoPath_MovePhoto("MapFile", doc, rankShowPath_photos, saveInterval, null, 0, imgMapFiles_forRelPhotos));
					//set photo_Caption
					phoCaption_ForHTML.add(i+"_"+doc+"_"+docIndex+"_"+scoreFormat.format(Value_RankScores.getFloats().get(i)));
					//set photo_Color
					if (thisQGroundTru.contains(doc)) {
						phoColor_ForHTML.add("rgb("+General.floatArrToString_nolastDelimiter(General.mul(Color.green.getRGBColorComponents(null), 255), ",", "0")+")");
					}else if(isOxford){
						if (thisQJunks.contains(doc)) {
							phoColor_ForHTML.add("rgb("+General.floatArrToString_nolastDelimiter(General.mul(Color.yellow.getRGBColorComponents(null), 255), ",", "0")+")");
						}else {
							phoColor_ForHTML.add("rgb("+General.floatArrToString_nolastDelimiter(General.mul(Color.red.getRGBColorComponents(null), 255), ",", "0")+")");
						}
					}else{
						phoColor_ForHTML.add("rgb("+General.floatArrToString_nolastDelimiter(General.mul(Color.red.getRGBColorComponents(null), 255), ",", "0")+")");
					}
				}
				//set htmlSentenceType
				if ( queryInd==0 && rankFileInd==0 ) {
					htmlSentenceType=0;
				}else {
					htmlSentenceType=10;
				}
				if ( queryInd==selQueryNum-1 && rankFileInd==(rankPaths.length-1)) {
					htmlSentenceType=1;
				}
				General.showPhoto_inHTML( html, HtmlTitle, htmlSentenceType,  rankShowPath_photosInHTML,  queryFileName, queryCaption, queryColor,
						gtPath_ForHTML,gtCaption_ForHTML, gtColor_ForHTML, 3,
						phoPath_ForHTML, phoCaption_ForHTML, phoColor_ForHTML, null,null,null,null,0);
			}
		}
		//close 
		html.close();
		General_Hadoop.closeAllMapFiles(rankFiles);
	}
	
	public static int selectQuerys_geoRel(String[] targetRankPaths, boolean RightWrong, ArrayList<Integer> selQuerys, 
			float[][] latlons, float isSameLoc, boolean fistAssign, int minGTSize, int maxGTSize) throws IOException{
		if (targetRankPaths!=null) {
			MapFile.Reader[] MapFileR_Rank=General_Hadoop.openAllMapFiles(targetRankPaths);
	    	IntWritable Key_queryName= new IntWritable();
	 		int queryNum=0;
	    	for (int i = 0; i < MapFileR_Rank.length; i++) {
	    		int queryNumThis=0;
	    		ArrayList<Integer> tempSelQ=new ArrayList<Integer>();
	    		//judge query Right&Wrong
    			fistMatch_GTruth_Docs_Locations Value_RankScores= new fistMatch_GTruth_Docs_Locations();
	    		while (MapFileR_Rank[i].next(Key_queryName, Value_RankScores)) {
	    			//for ranks should be good
	        		int queryName=Key_queryName.get();
	        		int grounTSize=Value_RankScores.getGTSize(); 
	        		boolean	gTSizeInRang= (grounTSize>=minGTSize && grounTSize<=maxGTSize);
	        		//get top Locations
					float[][] topLocations=Value_RankScores.topLocations.getArrArr();
					//get True-Location rank
					int trueLocRank=General_geoRank.get_trueLocRank(topLocations, queryName, 1, isSameLoc, latlons)+1;
	        		boolean isGood_0=(trueLocRank==1);
	        		if (gTSizeInRang && isGood_0==RightWrong)
	        			tempSelQ.add(queryName);
	        		queryNumThis++;
	    		}
	    		if (i==0) {
	    			queryNum=queryNumThis;
				}else {
					General.Assert_onlyWarning(queryNumThis==queryNum, "err in selectQuerys, no-equal query num in rank file! queryNumThis:"+queryNumThis+", Vs. queryNum:"+queryNum);
				}    		
	    		if (i==0 && fistAssign) {
	    			selQuerys.addAll(tempSelQ); 
				}else {
					selQuerys.retainAll(tempSelQ);
				}
	    		MapFileR_Rank[i].close();
			}
	    	return queryNum;
		}else {
			return -1;
		}
	}
	
	public static float[] selectQuerys_APCompare(String dataFlag, String targetRankPath_H, String targetRankPath_L, ArrayList<Integer> selQuerys, 
			HashMap<Integer, HashSet<Integer>> groundTruth, int[] s_to_l, HashMap<Integer, HashSet<Integer>> junks, float AP_thr) throws IOException, InterruptedException{
		boolean isOxford=dataFlag.contains("Oxford"); boolean isBarcelonaBuilding=dataFlag.contains("Barcelona");
		String[] rankPaths=null;
		if (targetRankPath_L==null && targetRankPath_H==null) {
			throw new InterruptedException("targetRankPath_L and targetRankPath_H are both null!");
		}else if (targetRankPath_L==null) {//if targetRankPath_L==null, then no compare, only sel query whose AP>AP_thr, this AP_thr should be positive!
			rankPaths=new String[]{targetRankPath_H};
		}else if (targetRankPath_H==null) {//if targetRankPath_H==null, then no compare, only sel query whose AP<AP_thr, -AP>-AP_thr, this -AP_thr should be negative!
			rankPaths=new String[]{targetRankPath_L};
		}else {
			rankPaths=new String[]{targetRankPath_H,targetRankPath_L};
		}
		MapFile.Reader[] MapFileR_Rank=General_Hadoop.openAllMapFiles(rankPaths);
	    IntWritable Key_queryName= new IntWritable();
	 	float MAP0=0; float MAP1=0;
	 	int queryNum=0;
	 	selQuerys.clear(); ArrayList<Integer> oriIDs;
	 	IntList_FloatList Value_RankScores= new IntList_FloatList();
		while (MapFileR_Rank[0].next(Key_queryName, Value_RankScores)) {
    		int queryName=Key_queryName.get();
    		HashSet<Integer> thisQ_groundTru=null;
    		//for rank should be good
			oriIDs=new ArrayList<Integer>(Value_RankScores.getIntegers().size());
			for (int di = 0; di < Value_RankScores.getIntegers().size(); di++) {
				oriIDs.add(s_to_l[Value_RankScores.getIntegers().get(di)]);
			}
			if (isOxford) {//for Oxford, delete junk photos
				int buildingInd=queryName/1000;
				thisQ_groundTru=groundTruth.get(buildingInd);
				oriIDs.removeAll(junks.get(buildingInd));//remove junks
			}else if (isBarcelonaBuilding) {
				int buildingInd=queryName/10000;
				thisQ_groundTru=groundTruth.get(buildingInd);
			}else {
				thisQ_groundTru=groundTruth.get(queryName);
			}
			float AP0=General_IR.AP_smoothed(thisQ_groundTru, oriIDs);
			MAP0+=AP0;
			//for rank should be bad
			float AP1=0;
			if (MapFileR_Rank.length==2) {
				MapFileR_Rank[1].get(Key_queryName, Value_RankScores);
				oriIDs=new ArrayList<Integer>(Value_RankScores.getIntegers().size());
				for (int di = 0; di < Value_RankScores.getIntegers().size(); di++) {
					oriIDs.add(s_to_l[Value_RankScores.getIntegers().get(di)]);
				}
				if (isOxford) {//for Oxford, delete junk photos
					int buildingInd=queryName/1000;
					thisQ_groundTru=groundTruth.get(buildingInd);
					oriIDs.removeAll(junks.get(buildingInd));//remove junks
				}
				AP1=General_IR.AP_smoothed(thisQ_groundTru, oriIDs);
				MAP1+=AP1;
			}
			//judge
			if ((AP0-AP1)*Math.signum(AP_thr)>AP_thr) {
				selQuerys.add(queryName);
			}
			queryNum++;
		}
		General_Hadoop.closeAllMapFiles(MapFileR_Rank);
		return new float[]{queryNum,MAP0/queryNum,MAP1/queryNum};
	}
	
	public static void main(String[] args) throws UnsupportedEncodingException, InterruptedException {//for debug!
		//binary --> big number --> Hex  (can save data space)
//		int bitNum=64; 
//		StringBuffer HESig=new StringBuffer(); 
//		for(int i=0;i<bitNum;i++){
//			if(i%5==0)
//				HESig.append("0");
//			else
//				HESig.append("1");
//		}
//		BigInteger integ=new BigInteger(HESig.toString(),2); //transfer binary to BigInteger
//		byte[] integ_byte=integ.toByteArray(); //str.getBytes();//integ_1.toByteArray();
//		System.out.println("integ_byte.length:"+integ_byte.length+", "+integ_byte[0]);//if integ_byte.length==9, then integ_byte[0] always 0000!
//		BigInteger integ_br=new BigInteger(integ_byte);
//		System.out.println(integ_br+": \t"+StrleftPad(integ_br.toString(2),0,64,"0"));
//		System.out.println("Character.MAX_RADIX:"+Character.MAX_RADIX);
//		int RADIX=36;
//		String hex=integ.toString(RADIX);//transfer BigInteger to hex value, can save data space
//		System.out.println(integ);
//		System.out.println(HESig);
//		System.out.println(hex);
//		BigInteger integ_recover=new BigInteger(hex,RADIX);// read from hex value
//		System.out.println("integ_recover:"+integ_recover);
//		//hamming distance for binary
//		BigInteger integ_1=new BigInteger("1011101",2);
//		BigInteger integ_2=new BigInteger("0100100",2);
//		int HM_dis=integ_1.xor(integ_2).bitCount();
//		System.out.println("HM_dis: "+HM_dis);
//		//hamming distance 
//		integ_1=new BigInteger("3c8tqzeqgbrwc",RADIX);
////		String str="3c8tqzeqgbrwc";
//		byte[] integ_1_byte=integ_1.toByteArray(); //str.getBytes();//integ_1.toByteArray();
//		System.out.println("integ_1_byte.length:"+integ_1_byte.length+", "+integ_1_byte[8]);
//		BigInteger integ_1_br=new BigInteger(integ_1_byte);
//		System.out.println(integ_1_br+": \t"+StrleftPad(integ_1_br.toString(2),0,65,"0"));
//
//		System.out.println(integ_1+": \t"+StrleftPad(integ_1.toString(2),0,65,"0"));
//		integ_2=new BigInteger("1d3bhd21q7rtr",RADIX);
//		System.out.println(integ_2+": \t"+StrleftPad(integ_2.toString(2),0,65,"0"));
//		HM_dis=integ_1.xor(integ_2).bitCount();
//		System.out.println("HM_dis: "+HM_dis);
//		//String to binary
//		String git="foo?/n";
//		byte[] bytes=git.getBytes("UTF-8");
//		for (byte b : bytes){
//			int b_int=b;
//			System.out.println(Integer.toBinaryString(b_int));
//		}
		
//		BitSet HESig1=new BitSet(bitNum); 
//		System.out.println("HESig1.size():"+HESig1.size());
//		for(int i=0;i<bitNum;i++){
//			HESig1.set(i);
//		}
//		System.out.println(HESig1.toString());
//		BitSet HESig2=new BitSet(bitNum); 
//		System.out.println("HESig2.size():"+HESig2.size());
//		for(int i=0;i<bitNum;i=i+25){
//			HESig2.set(i);
//		}
//		System.out.println("HESig2.length():"+HESig2.length());
//		System.out.println("HESig2.toString(): "+HESig2.toString());
//		byte[] kk=BitSettoByteArray(HESig2);
//		System.out.println("byte[] kk=BitSettoByteArray(HESig2); kk.length: "+kk.length);
//		BitSet jj=ByteArraytoBitSet(kk);
//		System.out.println("jj=ByteArraytoBitSet(kk), jj.size(): "+jj.size());
//		System.out.println("jj.length(): "+jj.length());
//		System.out.println("jj.toString(): "+jj.toString());
//		
//		
//		HESig1.xor(jj);
//		System.out.println("HESig1.xor(jj): "+HESig1.toString());
//		System.out.println("HESig1.cardinality(): "+HESig1.cardinality());
//		
//		int kq=-3565656;
//		byte[] kj = ByteBuffer.allocate(4).putInt(kq).array();// int to byte[]
//		System.out.println(ByteBuffer.wrap(kj).getInt());//byte[] to int
		
//		Float fl=new Float(3.123456789);
//		System.out.println(fl);
//		System.out.println(fl.doubleValue());
		
//		System.out.println(General.calculateGeoDistance(37.11, 100.1, 37.13, 100.1, "GreatCircle"));
//		double dou=37.08;
//		int kk=(int) (dou*10);
//		System.out.println("dou:"+dou+", kk:"+kk);
		
//		//*********  test rank top: rank_get_TopDocScores  ********
//    	long startTime; String model="DES"; //DES ASC
//    	int docNum=10000; //Integer.valueOf(args[0])
//    	int repNum=1; int top=10;
//    	Random random=new Random(); HashMap<DID_Score,Float> doc_scores=new HashMap<DID_Score, Float>(docNum);
//    	ArrayList<DID_Score> docs=new ArrayList<DID_Score>(docNum); ArrayList<Float> scores=new ArrayList<Float>(docNum);
//    	for (int i = 0; i < docNum; i++) {
//    		float score=random.nextFloat();
//    		docs.add(new DID_Score(i,score));
//    		scores.add(score);
//    		doc_scores.put(new DID_Score(i,score), score);
//		}
//    	ArrayList<DID_Score> topDocs=new ArrayList<DID_Score>(top); ArrayList<Float> topScores=new ArrayList<Float>(top);
//    	//test fast method 0
//    	startTime=System.currentTimeMillis();
//    	for (int i = 0; i < repNum; i++) {
//    		rank_get_TopDocScores_treeSet( docs, scores,  top, topDocs, topScores, model);
//		}
//    	System.out.println("rank_get_TopDocScores_treeSet( docs, scores,  top, topDocs, topScores, model), "+General.dispTime((System.currentTimeMillis()-startTime), "ms"));
//    	System.out.println(topDocs);
//    	System.out.println(topScores);
//    	//test fast method 0
//    	startTime=System.currentTimeMillis();
//    	for (int i = 0; i < repNum; i++) {
//    		rank_get_TopDocScores_PriorityQueue( docs, scores,  top, topDocs, topScores, model, true);
//		}
//    	System.out.println("rank_get_TopDocScores_PriorityQueue( docs, scores,  top, topDocs, topScores, model, true), "+General.dispTime((System.currentTimeMillis()-startTime), "ms"));
//    	System.out.println(topDocs);
//    	System.out.println(topScores);
//    	//test fast method 0
//    	startTime=System.currentTimeMillis();
//    	for (int i = 0; i < repNum; i++) {
//    		rank_get_TopDocScores_treeSet( doc_scores,  top, topDocs, topScores, model);
//		}
//    	System.out.println("rank_get_TopDocScores_treeSet( doc_scores,  top, topDocs, topScores, model), "+General.dispTime(System.currentTimeMillis()-startTime, "ms"));
//    	System.out.println(topDocs);
//    	System.out.println(topScores);
//    	//test fast method 1
//    	startTime=System.currentTimeMillis();
//    	for (int i = 0; i < repNum; i++) {
//    		rank_get_TopDocScores_treeMap( doc_scores,  top, topDocs, topScores,model);
//		}
//    	System.out.println("rank_get_TopDocScores_treeMap( doc_scores,  top, topDocs, topScores,model), "+General.dispTime(System.currentTimeMillis()-startTime, "ms"));
//    	System.out.println(topDocs);
//    	System.out.println(topScores);
//    	//test slow method
//    	startTime=System.currentTimeMillis();
//    	for (int i = 0; i < repNum; i++) {
//    		rank_get_TopDocScores_slow( doc_scores,  top, topDocs, topScores, model);
//    	}
//    	System.out.println(General.dispTime(System.currentTimeMillis()-startTime, "ms"));
//    	System.out.println(topDocs);
//    	System.out.println(topScores);
		
//    	//*********  test rank all: rank_get_AllSortedDocScores  ********
//    	long startTime;
//    	int docNum=100000; int rep=10;
//    	Random random=new Random(); HashMap<Integer,Float> doc_scores=new HashMap<Integer, Float>(docNum);
//    	ArrayList<Integer> docs=new ArrayList<Integer>(docNum); ArrayList<Float> scores=new ArrayList<Float>(docNum);
//    	for (int i = 0; i < docNum; i++) {
//    		float score=random.nextFloat();
//    		docs.add(i);
//    		scores.add(score);
//    		doc_scores.put(i, score);
//		}
//    	ArrayList<Integer> sortedDocs=new ArrayList<Integer>(docNum); ArrayList<Float> sortedScores=new ArrayList<Float>(docNum);
//    	//test method 0
//    	startTime=System.currentTimeMillis();
//    	for (int i = 0; i < rep; i++) {
//    		rank_get_AllSortedDocScores_ArraySort( docs, scores, sortedDocs, sortedScores, "DES");
//		}
//    	System.out.println("rank_get_AllSortedDocScores_ArraySort( docs, scores, sortedDocs, sortedScores, DES), "+General.dispTime((System.currentTimeMillis()-startTime), "ms"));
//    	System.out.println(sortedDocs.subList(0, Math.min(docNum, 10)));
//    	System.out.println(sortedScores.subList(0, Math.min(docNum, 10)));
//    	//test method 1
//    	startTime=System.currentTimeMillis();
//    	for (int i = 0; i < rep; i++) {
//    		rank_get_TopDocScores_treeSet( docs, scores, docs.size(), sortedDocs, sortedScores, "DES");
//		}
//    	System.out.println("rank_get_TopDocScores_treeSet( docs, scores, docs.size(), sortedDocs, sortedScores, DES), "+General.dispTime((System.currentTimeMillis()-startTime), "ms"));
//    	System.out.println(sortedDocs.subList(0, Math.min(docNum, 10)));
//    	System.out.println(sortedScores.subList(0, Math.min(docNum, 10)));
//    	//test method 2
//    	startTime=System.currentTimeMillis();
//    	for (int i = 0; i < rep; i++) {
//    		rank_get_AllSortedDocScores_treeSet( docs, scores, sortedDocs, sortedScores, "DES");
//		}
//    	System.out.println("rank_get_AllSortedDocScores_treeSet( docs, scores, sortedDocs, sortedScores, DES), "+General.dispTime((System.currentTimeMillis()-startTime), "ms"));
//    	System.out.println(sortedDocs.subList(0, Math.min(docNum, 10)));
//    	System.out.println(sortedScores.subList(0, Math.min(docNum, 10)));
//    	//test method 2_arr
//    	Integer[] docs_IntArr=docs.toArray(new Integer[0]);
//    	Float[] scores_FloatArr=scores.toArray(new Float[0]);
//    	Integer[] sortedDocs_IntArr=new Integer[docs_IntArr.length];
//    	Float[] sortedScores_FloatArr=new Float[docs_IntArr.length];
//    	startTime=System.currentTimeMillis();
//    	for (int i = 0; i < rep; i++) {
//    		rank_get_AllSortedDocScores_treeSet( docs_IntArr, scores_FloatArr, sortedDocs_IntArr, sortedScores_FloatArr, "DES");
//		}
//    	System.out.println("rank_get_AllSortedDocScores_treeSet( docs, scores, sortedDocs, sortedScores, DES), "+General.dispTime((System.currentTimeMillis()-startTime), "ms"));
//    	System.out.println(sortedDocs.subList(0, Math.min(docNum, 10)));
//    	System.out.println(sortedScores.subList(0, Math.min(docNum, 10)));
    	
//    	//*********  test rank all: rank_get_AllSortedDocIDs_treeSet  ********
//    	long startTime;
//    	int docNum=100000; int rep=10;
//    	Random random=new Random();
//    	ArrayList<Integer> docInfos=new ArrayList<Integer>(docNum); ArrayList<Integer> docIDs=new ArrayList<Integer>(docNum);
//    	for (int i = 0; i < docNum; i++) {
//    		int rand=random.nextInt(docNum);
//    		docInfos.add(i);
//    		docIDs.add(rand);
//		}
//    	ArrayList<Integer> docInfos_sorted=new ArrayList<Integer>(docNum); ArrayList<Integer> docIDs_sorted=new ArrayList<Integer>(docNum);
//    	//test
//    	startTime=System.currentTimeMillis();
//    	for (int i = 0; i < rep; i++) {
//    		rank_get_AllSortedDocIDs_treeSet( docInfos, docIDs, docInfos_sorted, docIDs_sorted, "ASC");
//		}
//    	System.out.println("rank_get_AllSortedDocIDs_treeSet( docInfos, docIDs, docInfos_sorted, docIDs_sorted, DES), "+General.dispTime((System.currentTimeMillis()-startTime), "ms"));
//    	System.out.println(docInfos_sorted.subList(0, Math.min(docNum, 10)));
//    	System.out.println(docIDs_sorted.subList(0, Math.min(docNum, 10)));
    	
    	//*********  test mergeSortedList_ASC  ********
    	Random random=new Random(); long startTime;
    	int docNum=10*1000*1000; int rep=1;
    	ArrayList<DID_Score> list1=new ArrayList<DID_Score>(2*docNum); ArrayList<DID_Score> list2=new ArrayList<DID_Score>(docNum);
    	for (int i = 0; i < docNum; i++) {
    		list1.add(new DID_Score(i, random.nextFloat()));
    		list2.add(new DID_Score(2*i, random.nextFloat()));
    	}
    	System.out.println("list1:"+list1.subList(0, Math.min(list1.size(), 10)));
    	System.out.println("list2:"+list2.subList(0, Math.min(list2.size(), 10)));
    	//method1-fast, but need DID form 0!
    	float[] mergResu=new float[2*docNum]; DID_Score[] list1_arr=list1.toArray(new DID_Score[0]); DID_Score[] list2_arr=list2.toArray(new DID_Score[0]);
    	startTime=System.currentTimeMillis(); 
    	for (int i = 0; i < rep; i++) {
    		mergeSortedList_ASC(mergResu, list1_arr);
    		mergeSortedList_ASC(mergResu, list2_arr);
    	}
    	System.out.println("mergeSortedList_ASC(mergResu, list) finished: "+General.dispTime((System.currentTimeMillis()-startTime), "ms"));
    	System.out.print("mergResu:");
    	int top=0;
    	while (top<10) {
			if (mergResu[top]>0) {
		    	System.out.print("docID:"+top+", score:"+mergResu[top]+", ");

			}
			top++;
		}
    	System.out.println();
    	//method2-slow
    	startTime=System.currentTimeMillis(); ArrayList<DID_Score> result=new ArrayList<DID_Score>(list1.size()+list2.size());
    	for (int i = 0; i < rep; i++) {
    		mergeSortedList_ASC(list1, list2, result);
    	}
    	System.out.println("mergeSortedList_ASC(list1, list2, result) finished: "+General.dispTime((System.currentTimeMillis()-startTime), "ms"));
    	System.out.println("resul:"+result.subList(0, Math.min(docNum, 10)));
    	//method3-slow
    	startTime=System.currentTimeMillis();
    	for (int i = 0; i < rep; i++) {
    		mergeSortedList_ASC(list1, list2);
    	}
    	System.out.println("mergeSortedList_ASC(list1, list2) finished: "+General.dispTime((System.currentTimeMillis()-startTime), "ms"));
    	System.out.println("list1:"+list1.subList(0, Math.min(docNum, 10)));
    	System.out.println("list2:"+list2.subList(0, Math.min(docNum, 10)));
    	
//		//*********  dist_to_sim  ********
//		System.out.println(dist_to_sim(10,50)+"");
//		System.out.println(dist_to_sim(20,50)+"");
    	
//    	//compare two Integer
//    	Integer a=new Integer(1);
//    	Integer b=new Integer(1);
//    	System.out.println(a==b.intValue());
	}

}
