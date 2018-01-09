package MyAPI.Geo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;
import MyAPI.General.General_IR;
import MyAPI.Geo.groupDocs.GroupDocs;
import MyAPI.Geo.groupDocs.GroupEstResult;
import MyAPI.Geo.groupDocs.GroupListProc;
import MyAPI.Geo.groupDocs.ShowGroupMatches;
import MyAPI.Obj.DID_FeatInds_Score;
import MyAPI.Obj.Statistic_MultiClass_1D_Distribution;
import MyAPI.imagR.GTruth;
import MyAPI.imagR.IDF;
import MyAPI.imagR.IDFTable;
import MyAPI.imagR.ImageBlock;
import MyAPI.imagR.ShowMatches_rank;
import MyAPI.imagR.ShowImgBlocks;
import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr;
import MyCustomedHaoop.ValueClass.ImageRegionMatch;
import MyCustomedHaoop.ValueClass.PhotoPointsLoc;
import MyCustomedHaoop.ValueClass.ComposeWritableClassCollection.PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr;

public abstract class GVM <G extends GroupDocs<DID_Score_ImageRegionMatch_ShortArr>, 
							P extends GroupListProc<DID_Score_ImageRegionMatch_ShortArr, G>, 
							S extends ShowGroupMatches<G>>{
	
	protected class Vari {
		
		boolean isVisNN;
		boolean isGVR;
		boolean isGVM;
		boolean isGVM_withIDF;
		//para for all
		float thr_matchScore;
		//paras for isGVM
		float[] matchingScoreSmooths;
		boolean isExpan_1vs1;
		boolean isExpan_bestDoc;
		boolean isExpan_sumDoc;
		boolean isExpan_1vs1LocAsVec;
		boolean isExpan_bestDocLocAsVec;
		boolean isExpan_sumDocLocAsVec;
		int freqThr;
		//***** temp data for process one query ***** 
		//1.commons
		ArrayList<Integer> group_inds; 
		ArrayList<Float> group_Scores;
		ArrayList<G> group_Matches_ori;
		//2.GVM
		ImageBlock queryImageBlock_groupFeat;
		ImageBlock queryImageBlock_idf;
		IDF idf_Indep;
		ArrayList<ArrayList<DID_FeatInds_Score>> group_Matches_GVM;
		

		public Vari(String schemelabel, int reRankScale) throws InterruptedException{
			//String label: GVM@smoothFactor@imBlockPortion@IDFBlockPortion@isExpan
			group_inds=new ArrayList<Integer>(); group_Scores=new ArrayList<Float>();
			//get rerank scheme
			String[] paras=schemelabel.split("@");
			if (paras[0].equalsIgnoreCase("VisNN")) {//VisNN@thr_matchScore
				this.isVisNN=true;
				this.thr_matchScore=Float.valueOf(paras[1]);//GVR@thr_matchScore
			}else if(paras[0].equalsIgnoreCase("GVR")){
				this.isGVR=true;
				this.thr_matchScore=Float.valueOf(paras[1]);
			}else if(paras[0].equalsIgnoreCase("GVM")){//GVM@thr_matchScore@smoothFactor@imBlockPortion@IDFBlockPortion@isExpan@freqThr
				this.isGVM=true;
				this.thr_matchScore=Float.valueOf(paras[1]);
				group_Matches_GVM=new ArrayList<>();
				//smoothFactor
				double matchingScoreSmooth=Double.valueOf(paras[2]);
				matchingScoreSmooths=new float[100];//if ini GVM matching score > 100, then all smoothed to 1
				for (int oriScore = 0; oriScore < matchingScoreSmooths.length; oriScore++) {
					matchingScoreSmooths[oriScore]=(float) (1-Math.exp(-Math.pow(oriScore/matchingScoreSmooth, 2)));
				}
				//imBlockPortion
				queryImageBlock_groupFeat=new ImageBlock(Integer.valueOf(paras[3]));//imBlockPortion==0 means no blocking
				//IDFBlockPortion
				int queryImageBlock_idf_potion=Integer.valueOf(paras[4]);
				if (queryImageBlock_idf_potion>=0) {
					isGVM_withIDF=true;
					queryImageBlock_idf=new ImageBlock(queryImageBlock_idf_potion);//imBlockPortion==0 means no blocking
				}else if (queryImageBlock_idf_potion==-1) {//this means no IDF
					isGVM_withIDF=false;
				}else {
					General.Assert(false, "queryImageBlock_idf_potion in GVM should >=-1, here:"+queryImageBlock_idf_potion);
				}
				//isExpan
				if (paras[5].equalsIgnoreCase("1vs1")) {
					isExpan_1vs1=true;
				}else if (paras[5].equalsIgnoreCase("bestDoc")) {
					isExpan_bestDoc=true;
				}else if (paras[5].equalsIgnoreCase("sumDoc")) {
					isExpan_sumDoc=true;
				}else if (paras[5].equalsIgnoreCase("1vs1LocAsVec")) {
					isExpan_1vs1LocAsVec=true;
				}else if (paras[5].equalsIgnoreCase("bestDocLocAsVec")) {
					isExpan_bestDocLocAsVec=true;
				}else if (paras[5].equalsIgnoreCase("sumDocLocAsVec")) {
					isExpan_sumDocLocAsVec=true;
				}
				freqThr=Integer.valueOf(paras[6]);
			}else {
				General.Assert(false, "label in GVM should contains VisNN, GVR, GVM here:"+schemelabel);
			}
		}
		
		public void scoreGroups(int queryID, PhotoPointsLoc queryInfo, ArrayList<G> groupDocMatches) throws InterruptedException, IOException{
			group_inds.clear(); group_Scores.clear(); group_Matches_ori=groupDocMatches;
			if (isVisNN) {
				scoreLocs_VisNN(groupDocMatches);
			}else if(isGVR){
				scoreLocs_GVR(groupDocMatches);
			}else if(isGVM){
				if (isGVM_withIDF) {
					queryImageBlock_idf.iniForOneImage(queryInfo);
				}
				queryImageBlock_groupFeat.iniForOneImage(queryInfo);
				group_Matches_GVM.clear(); 
				scoreLocs_GVM(queryID, queryInfo, groupDocMatches);
			}
		}
		
		private void scoreLocs_VisNN(ArrayList<G> groupDocMatches) {
			for (int loc_i = 0; loc_i < groupDocMatches.size(); loc_i++) {
				group_inds.add(loc_i);
				//use ori visual rank
				group_Scores.add((float) (groupDocMatches.size()-loc_i));
			}
		}
		
		private void scoreLocs_GVR(ArrayList<G> groupDocMatches) {
			for (int loc_i = 0; loc_i < groupDocMatches.size(); loc_i++) {
				group_inds.add(loc_i);
				//use ori matches within all photo's matches in this loc
				List<DID_Score_ImageRegionMatch_ShortArr> locMatches=groupDocMatches.get(loc_i).docs;
				//sum score
				float thisLocScore=0;
				for (DID_Score_ImageRegionMatch_ShortArr oneMatch : locMatches) {
					thisLocScore+=oneMatch.getScore();
				}
				group_Scores.add(thisLocScore);
			}
		}
		
		private void scoreLocs_GVM(int queryID, PhotoPointsLoc queryInfo, ArrayList<G> groupDocMatches) throws IOException, InterruptedException {
			//caculate qFeatInd_IDF
			if (isGVM_withIDF) {
				IDFTable idfTable=new IDFTable(groupDocMatches.size(), true, true, freqThr);
				idf_Indep=new IDF(idfTable); 
				for (G oneGroup: groupDocMatches) {
					//gather matched qFeatInds from this group
					HashSet<Short> qBlockInds_idf=new HashSet<>();
					for (DID_Score_ImageRegionMatch_ShortArr oneDoc : oneGroup.docs) {
						for (ImageRegionMatch oneMatch : oneDoc.matches.ObjArr) {
							short QFeatInd=(short) oneMatch.src; //cast to short, please check!
							//1.single ass, make keyID for queryImageBlock_idf
							short qBlockInd=(short) queryImageBlock_idf.getBlockID(QFeatInd);
							qBlockInds_idf.add(qBlockInd);
							//2.muti ass (not good), make keyID for queryImageBlock_idf
//							Integer[] qBlockInds=queryImageBlock_idf.getBlockID_MultAss(QFeatInd);
//							for (Integer qBlockInd : qBlockInds) {
//								qBlockInds_idf.add(qBlockInd.shortValue());
//							}
						}
					}
					//update qFeatInd IDF
					for (Short qBlockInd : qBlockInds_idf) {
						idf_Indep.updateOneIterm(qBlockInd);
					}
				}
			}
			//******* make final scores for each group ************
			int groupInd_inOriMatches=0;
			for (G oneGroup: groupDocMatches) {
				ArrayList<DID_FeatInds_Score> thisGroupFinalDocMatches=null; float thisGroupScore=0;
				if (isExpan_1vs1) {//0. Exp, make 1vs1 matches for query with all docs in this group, and sum match scores
					thisGroupFinalDocMatches = select1V1Match_forGroupPhoto(oneGroup.docs);
					thisGroupScore=calculateScoreFrom1vs1Matches(thisGroupFinalDocMatches,false);
				}else if (isExpan_1vs1LocAsVec) {
					thisGroupFinalDocMatches = select1V1Match_forGroupPhoto(oneGroup.docs);
					thisGroupScore=calculateScoreFrom1vs1Matches(thisGroupFinalDocMatches,true);
				}else {//1. noExp, use the best doc for each group
					float maxDocScore=Integer.MIN_VALUE; float sumScore=0f; ArrayList<DID_FeatInds_Score> allDocsMatches=new ArrayList<DID_FeatInds_Score>();
					boolean isNorm=isExpan_bestDocLocAsVec;
					for (DID_Score_ImageRegionMatch_ShortArr oneDoc : oneGroup.docs) {
						//find block level 1vs1 matches
						LinkedList<DID_Score_ImageRegionMatch_ShortArr> oneDocList=new LinkedList<>();
						oneDocList.add(oneDoc);
						ArrayList<DID_FeatInds_Score> thisDocMatches=select1V1Match_forGroupPhoto(oneDocList);//only 1 doc, DID are the sam
						//get final score for this doc
						float thisDocScore=calculateScoreFrom1vs1Matches(thisDocMatches,isNorm);
						//updata the best doc in this group
						if (thisDocScore>maxDocScore) {
							thisGroupFinalDocMatches=thisDocMatches;
							maxDocScore=thisDocScore;
						}
						//sum
						sumScore+=thisDocScore;
						allDocsMatches.addAll(thisDocMatches);
					}
					if (isExpan_bestDoc || isExpan_bestDocLocAsVec) {
						thisGroupScore=maxDocScore;
					}else if (isExpan_sumDoc) {
						thisGroupScore=sumScore;
					}else if (isExpan_sumDocLocAsVec) {
						thisGroupScore=calculateScoreFrom1vsMMatches_withNorm(allDocsMatches);
					}
				}
				//save
				group_inds.add(groupInd_inOriMatches); 
				group_Matches_GVM.add(thisGroupFinalDocMatches);//for isExpan_bestDoc and isExpan_sumDoc is the same: only best doc matches
				group_Scores.add(thisGroupScore);
				groupInd_inOriMatches++;
			}
		}
		
		private float smoothMatchingScore(float oriScore){//0~1
			int scoreInt=(int)oriScore;
			if (scoreInt>=matchingScoreSmooths.length) {
				return 1f;
			}else {
				return matchingScoreSmooths[(int)oriScore];
			}
		}
		
		private float calculateScoreFrom1vs1Matches(ArrayList<DID_FeatInds_Score> matches, boolean isNorm) throws InterruptedException{
			//1 visual element in query only has 1 matched visual element in the element cloud of each location
			float thisGroupScore=0;
			if (isNorm) {
				float norm=0f;
				for (int i = 0; i < matches.size(); i++) {
					DID_FeatInds_Score oneMatch=matches.get(i);
					float score=oneMatch.getScore();
					if (score<0.0001) {
						matches.remove(i);
						i--;
					}else {
						thisGroupScore+=score;
						norm+=oneMatch.matchingScore*oneMatch.matchingScore*oneMatch.IDFsquare;
					}
				}
				if (matches.size()>0) {
					thisGroupScore/=Math.sqrt(norm);
				}else {
					thisGroupScore=0;
				}
			}else {
				for (int i = 0; i < matches.size(); i++) {
					DID_FeatInds_Score oneMatch=matches.get(i);
					float score=oneMatch.getScore();
					if (score<0.0001) {
						matches.remove(i);
						i--;
					}else {
						thisGroupScore+=score;
					}
				}
			}
			return thisGroupScore;
		}
		
		private float calculateScoreFrom1vsMMatches_withNorm(ArrayList<DID_FeatInds_Score> matches) throws InterruptedException{
			//TF-IDF vector space model
			if (matches.size()>0) {
				HashMap<Integer, ArrayList<DID_FeatInds_Score>> qFeatInd_matches=new HashMap<Integer, ArrayList<DID_FeatInds_Score>>();
				for (DID_FeatInds_Score oneMatch : matches) {
					General.updateMap(qFeatInd_matches, oneMatch.blockInd_Q, oneMatch);
				}
				float resScore=0f; float norm=0f;
				for (Entry<Integer, ArrayList<DID_FeatInds_Score>> one_qFeatInd_matches : qFeatInd_matches.entrySet()) {
					float thisSq=0f;
					for (DID_FeatInds_Score did_FeatInds_Score : one_qFeatInd_matches.getValue()) {
						resScore+=did_FeatInds_Score.getScore();
						thisSq+=did_FeatInds_Score.matchingScore;
					}
					norm+=thisSq*thisSq*one_qFeatInd_matches.getValue().get(0).IDFsquare;
				}
				if (resScore>0.0001) {
					return (float) (resScore/Math.sqrt(norm));
				}else {
					return 0;
				}
			}else {
				return 0;
			}
		}
		
		private ArrayList<DID_FeatInds_Score> select1V1Match_forGroupPhoto(List<DID_Score_ImageRegionMatch_ShortArr> matches) throws InterruptedException, IOException {
			//make QBlockInd links, use TreeMap to make the ascending order of QBlockInd!
			TreeMap<Short, ArrayList<DID_FeatInds_Score>> matchCandidates=new TreeMap<Short, ArrayList<DID_FeatInds_Score>>();
			int global_docFeatInd=0;
			for (DID_Score_ImageRegionMatch_ShortArr one : matches) {
				int docID=one.getDID();
				for (ImageRegionMatch oneMatch : one.matches.ObjArr) {
					short QFeatInd=(short) oneMatch.src; //cast to short, please check!
					//make keyID for isGVM_featIndLevel or isGVM_blockIndLevel
					short keyID=(short) queryImageBlock_groupFeat.getBlockID(QFeatInd);
					float matchScore=oneMatch.matchScore;
					DID_FeatInds_Score oneLink=null;
					if (isGVM_withIDF) {//weight correspondences with IDF
						short qBlockInd_idf=(short) queryImageBlock_idf.getBlockID(QFeatInd);
						oneLink=new DID_FeatInds_Score(docID, QFeatInd, keyID, oneMatch.dst, global_docFeatInd, smoothMatchingScore(oneMatch.matchScore),idf_Indep.getIDF(qBlockInd_idf));
					}else {
						oneLink=new DID_FeatInds_Score(docID, QFeatInd, keyID, oneMatch.dst, global_docFeatInd, matchScore,1f);
					}
					General.updateMap(matchCandidates, keyID, oneLink);
					global_docFeatInd++;
				}
			}
			//find 1vs1 matches
			ArrayList<DID_FeatInds_Score> goodMatches=General_BoofCV.select1V1Match_for1vsM_basedOnScore(matchCandidates);
	    	return goodMatches;
		}
		
		public GroupDocs<DID_Score_ImageRegionMatch_ShortArr> getOneGroupMatches(int locInd) throws InterruptedException{
			if (isVisNN || isGVR) {//both isVisNN and isGVR do not change the mathes in one loc
				return group_Matches_ori.get(locInd);
			}else if(isGVM){
				return GroupDocs.orgraniseMatchesForOneGroup(group_Matches_GVM.get(locInd), group_Scores.get(locInd));
			}else{
				return null;
			}
		}
	}
	
	Vari vari;
	int num_topGroups;
	int num_topGroups_toShow;
	ShowMatches_rank showGTMatches;
	P groupListProc;
	S showGroupMatches;
	ShowImgBlocks showQBlocks;
	//temp data for one query to show
	int queryID;
	PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr iniVisRank;
	LinkedList<GTruth> gTruths;
	ArrayList<G> groupDocMatches_ini;
	ArrayList<Integer> topGroupInds;
	int fistMatchGroupInd;
	//temp data for all querys stat
	public Statistic_MultiClass_1D_Distribution matchNumDistri_1vs1;
	int[] topWrongLocNum;

	
	public GVM(String taskLabel, int num_topGroups, int num_topGroups_toShow, ShowMatches_rank showGTMatches, P groupListProc, S showGroupMatches, ShowImgBlocks showQBlocks) throws InterruptedException {//for data with GPS
		this.vari=new Vari(taskLabel, groupListProc.getReRankScale());
		this.num_topGroups=num_topGroups;
		this.num_topGroups_toShow=num_topGroups_toShow;
		this.showGTMatches=showGTMatches;
		this.groupListProc=groupListProc;
		this.showGroupMatches=showGroupMatches;
		this.showQBlocks=showQBlocks;
		if (vari.isExpan_1vs1) {
			topWrongLocNum=new int[]{1,2,5,10};
			matchNumDistri_1vs1= new Statistic_MultiClass_1D_Distribution((General.IntArrToString(topWrongLocNum, ",")+"maxMatNum").split(","), new float[]{0f,2f,0.1f}, "0.0");
		}
	}
	
	public void showRes() throws InterruptedException, IOException{
		add_GTMatches_ToShow(queryID, iniVisRank.obj_2.ObjArr, gTruths);
		//ini showLocMatches
		showGroupMatches.iniForOneQuery(queryID);
		//ini showQBlocks
		showQBlocks.clearPhotos();
		if (vari.isGVM_withIDF) {
			showQBlocks.addOnePhotoPoints(queryID, "Q"+queryID+"_region idf", vari.queryImageBlock_idf, vari.idf_Indep.getAllIDFs(), null, null);
		}
		int topNum=0;
		for (Integer groupInd : topGroupInds) {
			//add top ranked group matches
			if (topNum<num_topGroups_toShow) {
				//some doc's matches are all filtered out in calculateScoreFromMatches
				if (vari.getOneGroupMatches(groupInd).docs.size()>0) {
					addOneGroupMatchToShow(vari.getOneGroupMatches(groupInd), groupDocMatches_ini.get(groupInd));
				}
			}
			topNum++;
		}
		if (fistMatchGroupInd>=0) {
			addOneGroupMatchToShow(vari.getOneGroupMatches(fistMatchGroupInd), groupDocMatches_ini.get(fistMatchGroupInd));
		}
		//show
		if (showGTMatches!=null) {
			showGTMatches.disp();
		}
		showGroupMatches.disp();
		showQBlocks.disp();
	}
	
	public GroupEstResult<G> ProcessOneQuery(int queryID, PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr iniVisRank, boolean disp) throws InterruptedException, IOException{
		this.queryID=queryID; this.iniVisRank=iniVisRank;
		ArrayList<DID_Score_ImageRegionMatch_ShortArr> docs_scores_matches=new ArrayList<DID_Score_ImageRegionMatch_ShortArr>(Arrays.asList(iniVisRank.obj_2.ObjArr));
		preProcessVisRank(queryID, docs_scores_matches);
		int totRank_length=docs_scores_matches.size();
		if (totRank_length>0) {
			//calculate groundTruth
			gTruths= groupListProc.get_topRanked_GTruth(queryID, docs_scores_matches);
			if (GTruth.getGTSize(gTruths)>0) {
				//calculate cartoReduncy and saveInto gTruth
				int queryReduncy=groupListProc.getQueryReduncy(queryID);//relevent doc num
				//make locations
				groupDocMatches_ini=groupListProc.get_topGroupDocList(docs_scores_matches);
				//rerank locations
				LinkedList<G> res=rerank_Groups_byMatches(queryID, iniVisRank.obj_1, groupDocMatches_ini);
				//find rank of the first true math
				GTruth fistMatch = groupListProc.getFirstTrueMatch(queryID, res);
				if(disp==true){
					System.out.println("QueryName:"+queryID+", visual rank-length:"+totRank_length+", fistMatch:"+fistMatch);
					disp=false;
				}
				//ana_ElementLocDistribution
				ana_ElementLocDistribution(queryID);
				//return
				return new GroupEstResult<>(queryReduncy, GTruth.getGTSize(gTruths), fistMatch, res);
			}else {
				return null;
			}
		}else {
			if(disp==true){
				System.out.println("QueryName:"+queryID+", visual rank-length:"+totRank_length);
				disp=false;
			}
			return null;
		}
	}
	
	private void ana_ElementLocDistribution(int queryID){
		if (vari.isExpan_1vs1) {
			LinkedList<Boolean> isCorrs = groupListProc.judgeEachGroup(queryID, vari.group_Matches_ori);
			//get trueLoc_maxMatchNum
			int loc_ind=0; 
			int trueLoc_maxMatchNum=-1; int wrongLoc_maxMatchNum=-1;
			for (Boolean isCorr_thisLoc : isCorrs) {
				int thisLocMatchNum=vari.group_Matches_GVM.get(loc_ind).size();
				if (isCorr_thisLoc.booleanValue()==true) {
					trueLoc_maxMatchNum=Math.max(thisLocMatchNum, trueLoc_maxMatchNum);
				}else {
					wrongLoc_maxMatchNum=Math.max(thisLocMatchNum, wrongLoc_maxMatchNum);
				}
				loc_ind++;
			}
			if (trueLoc_maxMatchNum>2) {
				matchNumDistri_1vs1.addOneSample(topWrongLocNum.length, (float)wrongLoc_maxMatchNum/trueLoc_maxMatchNum);
				//get wrongLocToTrueLoc_MatchNumRate
				loc_ind=0; int topWrongLoc_ind=0;
				for (Boolean isCorr_thisLoc : isCorrs) {
//					if (isCorr_thisLoc.booleanValue()==false) {
//						elemLocDistri.addOneSample(0, (float)vari.group_Matches_GVM.get(loc_ind).size()/trueLoc_maxMatchNum);
//						break;
//					}
					if (isCorr_thisLoc.booleanValue()==false) {
						if (loc_ind>=topWrongLocNum[topWrongLocNum.length-1]) {
							break;
						}else{
							float thisLocMatchNum=(float)vari.group_Matches_GVM.get(loc_ind).size();
							for (int i = 0; i < topWrongLocNum.length; i++) {
								if (topWrongLoc_ind<topWrongLocNum[i]) {
									matchNumDistri_1vs1.addOneSample(i, thisLocMatchNum/trueLoc_maxMatchNum);
								}
							}
						}
						topWrongLoc_ind++;
					}
					loc_ind++;
				}
			}
		}
	}
	
	public void disp_elemLocDistri(String title){
		if (matchNumDistri_1vs1!=null) {
			matchNumDistri_1vs1.dispAsChart(title+"_1vs1Matches", "MatchNum_W_T_Rate", "percent in all wrongLoc_TrueLoc_pairs");
		}
	}
	
	protected abstract void preProcessVisRank(int queryID, ArrayList<DID_Score_ImageRegionMatch_ShortArr> docs_scores_matches);//

	protected void cutOffIniMatchingScore(ArrayList<DID_Score_ImageRegionMatch_ShortArr> docs_scores_matches){
		for (int i = 0; i < docs_scores_matches.size(); i++) {
			DID_Score_ImageRegionMatch_ShortArr oneDoc = docs_scores_matches.get(i);
			LinkedList<ImageRegionMatch> res=new LinkedList<>();
			for (ImageRegionMatch oneMatch : oneDoc.matches.ObjArr) {
				if (oneMatch.matchScore>=vari.thr_matchScore) {
					res.add(oneMatch);
				}
			}
			if (res.size()<oneDoc.matches.ObjArr.length) {
				if (res.size()>0) {
					oneDoc.matches.ObjArr=res.toArray(new ImageRegionMatch[0]);
				}else {
					docs_scores_matches.remove(i);
					i--;
				}
			}
		}
	}
	
	protected LinkedList<G> rerank_Groups_byMatches(int queryID, PhotoPointsLoc queryInfo, ArrayList<G> groupDocMatches_ini) throws InterruptedException, IOException{
		//score locations
		vari.scoreGroups(queryID, queryInfo, groupDocMatches_ini);
		//rank locations
		topGroupInds=new ArrayList<Integer>(); ArrayList<Float> location_Scores_top=new ArrayList<Float>();
		General_IR.rank_get_TopDocScores_PriorityQueue(vari.group_inds, vari.group_Scores, num_topGroups, topGroupInds, location_Scores_top, "DES", true, true);
		//output
		LinkedList<G> res=new LinkedList<G>(); 
		for (Integer groupInd : topGroupInds) {
			//some doc's matches are all filtered out in calculateScoreFromMatches
			if (vari.getOneGroupMatches(groupInd).docs.size()>0) {
				res.add(makeGroupMatch(vari.getOneGroupMatches(groupInd), groupDocMatches_ini.get(groupInd)));
			}
		}
		//add fist matched group
		int fistMatchRank=groupListProc.getFirstTrueMatch(queryID, res).rank;
		fistMatchGroupInd=fistMatchRank>=0?topGroupInds.get(fistMatchRank):-1;
		return res;//the matches here are original ones, not the final one, the final one is only saved in addOneGroupMatchToShow when needs to show!
	}
		
	protected void addOneGroupMatchToShow(GroupDocs<DID_Score_ImageRegionMatch_ShortArr> finalGroupMatch, G iniGroupMatch) throws InterruptedException{
		if (showGroupMatches!=null) {
			showGroupMatches.addOneGroup(makeGroupMatch(finalGroupMatch, iniGroupMatch));
		}
	}
	
	protected abstract G makeGroupMatch(GroupDocs<DID_Score_ImageRegionMatch_ShortArr> finalGroupMatch, G iniGroupMatch) throws InterruptedException;
	
	protected void add_GTMatches_ToShow(int queryID, DID_Score_ImageRegionMatch_ShortArr[] ObjArr, List<GTruth> gTruth) throws InterruptedException{
		if (showGTMatches!=null) {
			showGTMatches.iniData();//what ever this query has gtruth or not, always iniData
			showGTMatches.addOneQuerySelRanks(queryID, ObjArr, GTruth.getRanks(gTruth));
		}
	}

	public static void main(String[] args) throws InterruptedException, IOException{
		for (float oriScore = 0; oriScore < 20; oriScore++) {
			System.out.println(oriScore+": "+(float) (1-Math.exp(-Math.pow(oriScore/10, 2))));
		}
	}
}
