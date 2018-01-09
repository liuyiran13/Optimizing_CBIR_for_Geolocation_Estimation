package MyAPI.imagR;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;
import MyAPI.Obj.DID_FeatInds_Score;
import MyAPI.Obj.Disp;
import MyAPI.Obj.Score_MatchFeats;
import MyAPI.imagR.ImageDataManager;
import MyAPI.imagR.ImageR_Evaluation;
import MyAPI.imagR.MakeRank;
import MyAPI.imagR.PhotoAllFeats_orgVW;
import MyAPI.imagR.ScoreDoc;
import MyAPI.imagR.ShowMatches;
import MyCustomedHaoop.ValueClass.DID_Score;
import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr;
import MyCustomedHaoop.ValueClass.DocAllMatchFeats;
import MyCustomedHaoop.ValueClass.ImageRegionMatch;
import MyCustomedHaoop.ValueClass.IntList_FloatList;
import MyCustomedHaoop.ValueClass.Int_MatchFeatArr;
import MyCustomedHaoop.ValueClass.Int_MatchFeatArr_Arr;
import MyCustomedHaoop.ValueClass.SURFfeat_ShortArr_AggSig;
import MyCustomedHaoop.ValueClass.TVector;


/**
 * linux:
 * 
 * SanFran:		krenew -s -- java -Xms130g -Xmx150g -cp ImageR.jar:$CLASSPATH MyAPI.imagR.ImageR _InMemIndex _ImgDataS /tudelft.net/staff-bulk/ewi/insy/mmc/XinchaoLi/SanFran/ dataSet/SanFrancisco_querys_transIndex_S_to_S.hashMap /tudelft.net/staff-bulk/ewi/insy/mmc/XinchaoLi/PhotoDataBase/SanFrancisco_StreetView/SanFrancisco_inSInd_MFiles/,/tudelft.net/staff-bulk/ewi/insy/mmc/XinchaoLi/SanFran/feats/SIFTUPRightINRIA2_QDPCIVW20k_MA_SanFran_Q/ /tudelft.net/staff-bulk/ewi/insy/mmc/XinchaoLi/PhotoDataBase/SanFrancisco_StreetView/SanFrancisco_inSInd_MFiles/,/tudelft.net/staff-bulk/ewi/insy/mmc/XinchaoLi/SanFran/feats/SIFTUPRightINRIA2_QDPCIVW20k_SA_SanFran_DPCI/ index/ _SanFran_DPCI_QDPCIVW_SIFTUPRightINRIA2_VW20K 20000 _iniR-1vw1match@20@12_reR@0@1000_reRHE@20@12_1vs1@true@true VisualRank/RunTest_MA true dataSet/SanFrancisco_groundTruth_onlyPCI_inSInd_corr2014.hashMap noUse noUse dataSet/SanFrancisco_docsPCI_transIndex_S_to_S.hashMap noUse 
 */
public class ImageR{
		
	ImageDataManager imgManager_Q;
	//for rank
	ImageIndex index_docIDInS;
	ScoreDoc scoreDoc;
	//setup makeRank
	int rerank_len, toprank_len;
	MakeRank<Integer> makeRank_ini;
	MakeRank<DID_Score_ImageRegionMatch_ShortArr> makeRank_final;
	IndexTrans indexTrans;
	//optional
	ShowMatches showMatches;
	ImageR_Evaluation imageR_eval;
	
	public ImageR(ImageDataManager imgManager_Q, ImageIndex index_docIDInS, ScoreDoc scoreDoc, ShowMatches showMatches, RankModel rankModel, IndexTrans indexTrans, ImageR_Evaluation imageR_eval) throws InterruptedException{
		super();
		this.imgManager_Q=imgManager_Q;
		this.index_docIDInS=index_docIDInS;
		this.scoreDoc=scoreDoc;
		//setup makeRank
		rerank_len=rankModel.reRankTopRank.rerankLen; toprank_len=rankModel.reRankTopRank.topRankLen;
		this.makeRank_ini=new MakeRank<Integer>(Disp.getNotDisp(), Math.max(rerank_len, toprank_len), false, 10000);
		this.makeRank_final=new MakeRank<DID_Score_ImageRegionMatch_ShortArr>(Disp.getNotDisp(), toprank_len, false, 10000);//concateTwoList
		this.indexTrans=indexTrans;
		//optional
		this.showMatches=showMatches;
		this.imageR_eval=imageR_eval;
	}
	
	public String getComputingTimeReport(){
		return scoreDoc.matchingInfo==null?"":scoreDoc.matchingInfo.getComputingTimeReport();
	}
	
	public void retrieval(int queryID, Disp disp) throws IOException, InterruptedException, ClassNotFoundException{
		Disp disp_this=Disp.makeHardCopyAddSpacer(disp, "\t");
		PhotoAllFeats_orgVW queryFeat=imgManager_Q.getPhoFeat(queryID, Disp.getNotDisp());
		//0. build ini rank: search through TVector
		float[] merge=new float[scoreDoc.indexInfo.maxDocID+1];
//		LinkedList<Int_Float> debugOneDoc_vws_scores=new LinkedList<Int_Float>(); //debug
//		boolean debugTarget=(queryID==-7001);
		for (Entry<Integer, SURFfeat_ShortArr_AggSig> vw_sigs_Q : queryFeat.VW_Sigs.entrySet()) {
			int vw=vw_sigs_Q.getKey();
			ArrayList<DID_Score> oneVWMacthDocs=scoreDoc.scoreDocs_inOneTVector(vw_sigs_Q.getValue().feats, vw_sigs_Q.getValue().aggSig, vw, index_docIDInS.getOneTVector(vw));
			//matches_vw_mFeat and oneVWMacthDocs are in the same order of docIDs.
			for (DID_Score dID_score: oneVWMacthDocs) {
				merge[dID_score.docID]+=dID_score.score;
//				if (debugTarget && dID_score.docID==4553) {
//					debugOneDoc_vws_scores.add(new Int_Float(vw, dID_score.score));
//				}
			}
		}
		//*debug show
//		General.dispInfo_ifNeed(debugTarget, "\t debugInfo: debugOneDoc_vws_scores: ", debugOneDoc_vws_scores.toString());
		makeRank_ini.clearDocScores();
		for (int docID = 0; docID < merge.length; docID++) {
			if(merge[docID]>0){
				float docNorm=scoreDoc.indexInfo.docNorms[docID];
				makeRank_ini.addOneDoc_onlyMainRank(docID, merge[docID]/docNorm);
			}
		}
		ArrayList<Integer> iniRank_docs=new ArrayList<>(); ArrayList<Float> iniRank_scores=new ArrayList<>();
		makeRank_ini.getRes(iniRank_docs, iniRank_scores);
//		disp.disp("Q:"+queryID+", its rank: "+iniRank_docs+"\n\t its rankScore: "+iniRank_scores
//				+"\n\t 1stRanked doc, oriScore:"+merge[iniRank_docs.get(0).DocID].docScore+", doc_BoVWVectorNorm:"+scoreDoc.doc_BoVWVectorNorm[iniRank_docs.get(0).DocID]);
		//1. rerank
		IntList_FloatList rank_score_final=new IntList_FloatList();
		int toprank_len_act=Math.min(iniRank_docs.size(), toprank_len);
		if(rerank_len==0){
			for (int i = 0; i <toprank_len_act; i++) {
				rank_score_final.getIntegers().add(iniRank_docs.get(i));
				rank_score_final.getFloats().add(iniRank_scores.get(i));
			}
		}else{
			makeRank_final.clearDocScores(); LinkedList<DID_Score_ImageRegionMatch_ShortArr> docMatches=new LinkedList<>();
			IDF idf=new IDF(); ImageBlock queryImageBlock=new ImageBlock(30); queryImageBlock.iniForOneImage(queryFeat.getPhotoPointsLoc());
			LinkedList<DocAllMatchFeats> docIniMatches=collectMatches(queryFeat, iniRank_docs.subList(0, Math.min(iniRank_docs.size(), rerank_len)));
			for (DocAllMatchFeats docAllMatchFeats:docIniMatches) {
				//spatial verification
				if (docAllMatchFeats.getMatchNum()>0) {
					LinkedList<ImageRegionMatch> finalMatches=new LinkedList<ImageRegionMatch>();
					float[] scores=scoreDoc.scoreOneDoc(docAllMatchFeats, queryFeat.getIntersetPoint(), queryID, queryFeat.getMaxDim(), finalMatches, null, null, false);
					//distinctve visual elements
					if (finalMatches.size()>0) {
						HashMap<Short, ArrayList<DID_FeatInds_Score>> oneDoc_matchCandidates=new HashMap<Short, ArrayList<DID_FeatInds_Score>>();
						for (ImageRegionMatch oneMatch : finalMatches) {
							short queryFeatID_key=(short)queryImageBlock.getBlockID(oneMatch.src);
							General.updateMap(oneDoc_matchCandidates, queryFeatID_key, new DID_FeatInds_Score(docAllMatchFeats.DocID, oneMatch.src, queryFeatID_key, oneMatch.dst, oneMatch.dst, oneMatch.matchScore,0));
						}
						ArrayList<DID_FeatInds_Score> goodMatches=General_BoofCV.select1V1Match_for1vsM_basedOnScore(oneDoc_matchCandidates);
						for (DID_FeatInds_Score did_FeatInds_Score : goodMatches) {
							idf.updateOneIterm(did_FeatInds_Score.blockInd_Q);
						}
						docMatches.add(new DID_Score_ImageRegionMatch_ShortArr(docAllMatchFeats.DocID, scores[0],finalMatches));
					}
				}
			}
			idf.makeIDFTable(docMatches.size(), true, false, docMatches.size());
			for (DID_Score_ImageRegionMatch_ShortArr oneDoc : docMatches) {
				//sum score
				float thisDocScore=0;
				for (ImageRegionMatch oneMatch : oneDoc.matches.ObjArr) {
					//*** make final score for each match **
					float thisIDF=idf.getIDF(queryImageBlock.getBlockID(oneMatch.src));
					oneMatch.matchScore=thisIDF*thisIDF*smoothMatchingScore(oneMatch.matchScore);
					//*** add to thisLocScore **
					thisDocScore+=oneMatch.matchScore;
				}
				makeRank_final.addOneDoc_onlyMainRank(oneDoc, thisDocScore);//final result rank should be docIDs in L
			}
			ArrayList<DID_Score_ImageRegionMatch_ShortArr> finalRank = makeRank_final.getRes_onlyRank();
			for (DID_Score_ImageRegionMatch_ShortArr oneDoc : finalRank) {
				rank_score_final.getIntegers().add(oneDoc.getDID());
				rank_score_final.getFloats().add(oneDoc.getScore());
			}
			if (showMatches!=null) {
				showMatches.addOneQueryRes(queryID, indexTrans.translateOneDocMatchList(finalRank), 10);
			}
			//2. add lastPartOf ini_rank, (makeRank_ini.topRank-makeRank_final.topRank) to the final rank
			if (rerank_len<toprank_len_act) {//rerank<top
				for (int i = rerank_len; i < toprank_len_act; i++) {
					rank_score_final.getIntegers().add(iniRank_docs.get(i));
					rank_score_final.getFloats().add(iniRank_scores.get(i));
				}
			}
		}
		//3. imageR_eval
		if (imageR_eval!=null) {
			imageR_eval.add_oneRank(queryID, indexTrans.translateOneList(rank_score_final), disp_this);
		}
	}
	
	public LinkedList<DocAllMatchFeats> collectMatches(PhotoAllFeats_orgVW queryFeat, List<Integer> iniRank_docs) throws IllegalArgumentException, IOException, InterruptedException{
		//sort top ranked docs by docID in ASC!
		int[] topDocs_inSortedDocIDs=General.ListToIntArr(iniRank_docs);
		Arrays.sort(topDocs_inSortedDocIDs);
		//loop-over the TVector of each query's VW
		Score_MatchFeats[] merge=Score_MatchFeats.makeNewArray(scoreDoc.indexInfo.maxDocID+1);
		for (Entry<Integer, SURFfeat_ShortArr_AggSig> vw_sigs_Q : queryFeat.VW_Sigs.entrySet()) {
			int vw=vw_sigs_Q.getKey();
			//get match link
			TVector tVector = index_docIDInS.getOneTVector(vw);
			int[] docIDs_inTVector=General.ListToIntArr(tVector.getDocList());
			LinkedList<Int_MatchFeatArr> docIDs_matches = scoreDoc.collectMatches_inOneTVector(topDocs_inSortedDocIDs, vw_sigs_Q.getValue(), docIDs_inTVector, tVector);
			if (docIDs_matches!=null) {
				for (Int_MatchFeatArr docID_matches : docIDs_matches) {
					merge[docID_matches.Integer].mergeOneVWMatchGroup(1f, new Int_MatchFeatArr(vw, docID_matches.feats));
				}
			}
		}
		LinkedList<DocAllMatchFeats> res=new LinkedList<>();
		for (int i = 0; i < merge.length; i++) {
			if (merge[i].docScore>0) {//this doc has matches
				res.add(new DocAllMatchFeats(i, new Int_MatchFeatArr_Arr(merge[i].feats)));
			}
		}
		return res;
	}
	
	private float smoothMatchingScore(float oriScore){//0~1
		return (float) (1-Math.exp(-Math.pow(oriScore/10, 2)));
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		//0. runRetrieval in windows
		String basePath="O:/ImageRetrieval/Oxford5K/";
//		String basePath="F:/Experiments/SanFrancisco/";
		//_ImgDataL: no distractor
//		runRetrieval(new String[]{basePath,"Herve_querys_L_to_L.hashMap", "Herve_ori1.5K_SelPhos_L_to_S.hashMap", "Herve_ori1.5K_SelPhos_S_to_L.intArr", 
//				"CVPR15/MimicHPM/SURFFeat_VW100k_SA_Herve_1.5K/", "CVPR15/MimicHPM/SURFFeat_VW100k_SA_Herve_1.5K/", "CVPR15/", "_Herve_1.5K_100K-VW_SURF", "100",
//				"_BOF_iniR-noBurst_reR@1000@1000_1vs1AndHistAndAngle@0.52@0.2@1@0@0@0@0@0@0@0", "CVPR15/runTimeTest1", "true", "Herve_groundTruth.hashMap", "", ""});
		runRetrieval(new String[]{"_InMemIndex","_ImgDataL",basePath,"Oxford_querys_L_to_L.hashMap", 
				"O:/ImageRetrieval/Oxford5K/OxfordBuilding_CutQ.mapFile/,O:/ImageRetrieval/Oxford5K/feats/SURF_VW20k_MA_Oxford_5K_CutQ/", "O:/ImageRetrieval/Oxford5K/OxfordBuilding_CutQ.mapFile/,O:/ImageRetrieval/Oxford5K/feats/SURF_VW20k_SA_Oxford_5K_CutQ/", 
				"CVPR15/index/", "_Oxford_5K_20K-VW_SURF", "20000", 
				"_iniR-BurstIntraInter@18@12_reR@0@1000_reRHE@18@12_1vs1AndHistAndAngle@true@true@false@0.52@0.2@1@0@0@0@0@0@0@0", 
//				"_iniR-ASMK@0.2@3_reR@1000@1000_reRHE@20@12_1vs1AndHist@true@true@0.52@0.2", 
				"CVPR15/RunTest_MA", "true", "OxfordBuilding_groundTruth.hashMap", "OxfordBuilding_junks.hashMap", "OxfordBuilding_buildingInd_Name.hashMap",
				"Oxford_ori5K_SelPhos_L_to_S.hashMap", "Oxford_ori5K_SelPhos_S_to_L.intArr"});
		//with distractor
//		runRetrieval(new String[]{"",basePath+"Herve_querys_L_to_L.hashMap", basePath+"Herve_10K_SelPhos_L_to_S.hashMap", basePath+"Herve_10K_SelPhos_S_to_L.intArr", 
//				"CVPR15/MimicHPM/SURFFeat_VW100k_SA_Herve_1.5K/", "N:/ewi/insy/MMC/XinchaoLi/CVPR15/mimicHPM/SURFFeat_VW100k_SA_CVPR15UniDistra_10M_Inter100K/,"+basePath+"CVPR15/MimicHPM/SURFFeat_VW100k_SA_Herve_1.5K/", basePath+"CVPR15/", "_Herve_10K_100K-VW_SURF", "100", 
//				"_BOF_iniR-noBurst_reR@1000@1000_HistAnd1vs1AndAngle@0.52@0.2@0.8@0@0@0@0@0@0@0", basePath+"CVPR15/runTimeTest", "true", basePath+"Herve_groundTruth.hashMap", "", ""});
//		runRetrieval(new String[]{"",basePath+"Oxford_querys_L_to_L.hashMap", basePath+"Oxford_10K_SelPhos_L_to_S.hashMap", basePath+"Oxford_10K_SelPhos_S_to_L.intArr", 
//				"CVPR15/MimicHPM/SURFFeat_VW100k_SA_Oxford_5K_CutQ/", "N:/ewi/insy/MMC/XinchaoLi/CVPR15/mimicHPM/SURFFeat_VW100k_SA_CVPR15UniDistra_10M_Inter100K/,"+basePath+"CVPR15/MimicHPM/SURFFeat_VW100k_SA_Oxford_5K_CutQ/", basePath+"CVPR15/", "_Oxford_10K_100K-VW_SURF", "100", 
//				"_BOF_iniR-noBurst_reR@1000@1000_1vs1AndHistAndAngle@0.52@0.2@1@0@0@0@0@0@0@0", basePath+"CVPR15/runTimeTest1", "true", basePath+"OxfordBuilding_groundTruth.hashMap", basePath+"OxfordBuilding_junks.hashMap", basePath+"OxfordBuilding_buildingInd_Name.hashMap"});
//		//_ImgDataS
//		runRetrieval(new String[]{"_InMemIndex","_ImgDataS",basePath,"dataSet/SanFrancisco_querys_transIndex_S_to_S.hashMap", 
//				"N:/ewi/insy/MMC/XinchaoLi/PhotoDataBase/SanFrancisco_StreetView/SanFrancisco_inSInd_MFiles/,F:/Experiments/SanFrancisco/feats/SIFTUPRightINRIA2_QDPCIVW20k_MA_SanFran_Q/", "N:/ewi/insy/MMC/XinchaoLi/PhotoDataBase/SanFrancisco_StreetView/SanFrancisco_inSInd_MFiles/,F:/Experiments/SanFrancisco/feats/SIFTUPRightINRIA2_QDPCIVW20k_SA_SanFran_DPCI/", 
//				"index/", "_SanFran_DPCI_QDPCIVW_SIFTUPRightINRIA2_VW20K", "20000",
//				"_iniR-1vw1match@20@12_reR@0@1000_reRHE@20@12_1vs1AndHistAndAngle@true@true@false@0.52@0.2@1@0@0@0@0@0@0@0", 
//				"VisualRank/RunTest_MA", "true", "dataSet/SanFrancisco_groundTruth_onlyPCI_inSInd_corr2014.hashMap", "", "",
//				"dataSet/SanFrancisco_docsPCI_transIndex_S_to_S.hashMap", ""});
		
		//1. testComputingTime in Linux
//		runRetrieval(args);
	}
	
	/**
	 * runRetrieval:
	 * ID in imageFeat and groudTruth should be the same: either L or S, denotes as X
	 * e.g., 	for Oxford,  all IDs are in L (X=L), so isImgDataL==true and needs docID_S_to_L_path for evaluation
	 * 			for SanFran, all IDs are in S, so isImgDataL==false.
	 * 
	 */
	@SuppressWarnings("unchecked")
	public static void runRetrieval(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		//set workModel
		String workModel_index=args[0];//_InMemIndex, _HadoopIndex
		String workModel_imgData=args[1];//_ImgDataL, _ImgDataS
		boolean isInMemIndex=workModel_index.equalsIgnoreCase("_InMemIndex");
		boolean isImgDataL=workModel_imgData.equalsIgnoreCase("_ImgDataL");//ID in imageFeat and groudTruth should be the same: either L or S, denotes as X
		//set common path
		String basePath=args[2];
		String queryPath_X_to_X=basePath+args[3];//ID here must be the same with X
		String[] imageMapFilesPath_Q=args[4].split(",");//image and feat: maybe different with db photos if multiple assignment
		String[] imageMapFilesPath_DB=args[5].split(",");//image and feat: distractor,imagROriDataSet
		String indexPath=basePath+args[6];
		String indexLabel=args[7]; //"_Herve_1.5K_100K-VW_SURF"
		int vwNum=Integer.valueOf(args[8]);//65536
		RankModel rankModel=new RankModel(args[9]); //"_iniR-noBurst@18@12_reR@1000@1000_reRHE@18@12_1vs1AndHistAndAngle@true@false@0.52@0.2@1@0@0@0@0@0@0@0"
		String reportPath=basePath+args[10];
		boolean isEvalMatchingEfficiency=Boolean.valueOf(args[11]);
		String grondTruthPath=basePath+args[12];
		String junkPath=basePath+args[13];
		String buildingInd_NamePath=basePath+args[14];
		PreProcessImage preProcessImage=null;
		//set workModel specific resources
		String docID_X_to_S_path = null, docID_S_to_L_path = null;
		if (isInMemIndex) {
			docID_X_to_S_path=basePath+args[15];
		}		
		if (isImgDataL) {
			docID_S_to_L_path=basePath+args[16];
		}
		//set report
		PrintWriter outputStream_report=new PrintWriter(new OutputStreamWriter(new FileOutputStream(reportPath+workModel_index+indexLabel+rankModel+".report", false), "UTF-8"),true); 
		long startTime=System.currentTimeMillis();
		Disp disp=new Disp(true, "", outputStream_report);
		//setup imageDataManager: mapFiles feats
		ImageDataManager imageDataManager_Q=new ImageDataManager(100*1000, imageMapFilesPath_Q[0], 100*1000, imageMapFilesPath_Q[1],preProcessImage);
//		ImageDataManager imageDataManager_Q=new ImageDataManager("MapFile", 100*1000, null, new String[]{imageMapFilesPath_Q[0]}, 100*1000, null, ComparePhotos_runLocal.setupExtractFeat_SURF(disp, null, new MutiAssVW(true, 0.05)));
		ImageDataManager imageDataManager_D=new ImageDataManager(100*1000, imageMapFilesPath_DB[0], 100*1000, imageMapFilesPath_DB[1],preProcessImage);
		//load query
		HashMap<Integer, Integer> query_X_to_X=(HashMap<Integer, Integer>) General.readObject(queryPath_X_to_X);
		//set index
		ImageIndex imgIndex;
		if (isInMemIndex) {			
			//load docID_L_to_S
			HashMap<Integer, Integer> docID_X_to_S=(HashMap<Integer, Integer>) General.readObject(docID_X_to_S_path);	
			//setup indexPath
			String saveIndexPath=indexPath+"Index"+indexLabel;
			//buildIndexFromFeat
			ImageIndex_InMemory imageIndexInMem=new ImageIndex_InMemory(rankModel.iniR_weight); 
			imageIndexInMem.buildIndexFromFeat(vwNum, docID_X_to_S, imageDataManager_D, saveIndexPath, disp);
			imgIndex = imageIndexInMem;
		}else {
			ImageIndex_Disk imgIndexDisk=new ImageIndex_Disk(rankModel.iniR_weight, new Configuration(), indexPath+"docInfo"+indexLabel+"/part-r-00000", indexPath+"TVectorInfo"+indexLabel, indexPath+"TVector"+indexLabel, vwNum/1000);
			imgIndex = imgIndexDisk;
		}
		//setup scoreDoc
		ScoreDoc scoreDoc=new ScoreDoc(disp, rankModel.iniR_weight, rankModel.rerank_scoreFlag, rankModel.rerank_HEPara, imgIndex.indexInfo, isEvalMatchingEfficiency);
		//setup IndexTrans
		IndexTrans indexTrans=new IndexTrans(disp,docID_S_to_L_path);
		//setup ImageR_Evaluation
		ImageR_Evaluation imageR_Evaluation=isEvalMatchingEfficiency?new ImageR_Evaluation(disp, query_X_to_X.size(), indexLabel, rankModel.toString(), grondTruthPath, junkPath, buildingInd_NamePath, null, null, null):null;
		//setup showMatches
		ShowMatches showMatches=new ShowMatches(true, imageDataManager_Q, imageDataManager_D, 0, 3);
		//setup imagR
		ImageR imagR=new ImageR(imageDataManager_Q, imgIndex, scoreDoc, showMatches, rankModel, indexTrans, imageR_Evaluation);
		//pre load or extract all query feats
		if (isInMemIndex) {		
			imageDataManager_Q.loadPhoFeat_InMemory(new LinkedList<>(query_X_to_X.keySet()), disp);
			disp.disp("load index into memory done!  "+General.memoryInfo());
		}
		//do matching
		disp.disp("start do retrieval for "+query_X_to_X.size()+" queries  .... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
		int q_i=0; int dispInter=1; long queryTime_all=0;
		for (Integer queryID : query_X_to_X.keySet()) {
			disp.disp=(q_i%dispInter==0);
			//run
			long startTime_oneQ=System.currentTimeMillis();
			imagR.retrieval(queryID, disp);
			queryTime_all+=System.currentTimeMillis()-startTime_oneQ;
			disp.disp(q_i+"th query: "+queryID+" finished! ..... "+General.dispTime(queryTime_all, "s")+"\n"+imagR.getComputingTimeReport());
//			showMatches.disp();
			q_i++;
		}
		General.dispInfo(outputStream_report, "all queries are done! "+(isEvalMatchingEfficiency?imageR_Evaluation.getRes():"")+"..... "+General.dispTime(queryTime_all, "s")+", per query:"+queryTime_all/query_X_to_X.size()+"ms");
		//get run-time info
		General.dispInfo(outputStream_report, imagR.getComputingTimeReport());
		//clean up
		imageDataManager_Q.cleanUp();
		imageDataManager_D.cleanUp();
	}
}
