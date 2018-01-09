package MyAPI.General.BoofCV;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

import MyAPI.General.General;
import MyAPI.Obj.Disp;
import MyAPI.imagR.ExtractFeat;
import MyAPI.imagR.ImageDataManager;
import MyAPI.imagR.ImageR_Evaluation;
import MyAPI.imagR.MakeRank;
import MyAPI.imagR.MutiAssVW;
import MyAPI.imagR.PhotoAllFeats_orgVW;
import MyAPI.imagR.PreProcessImage;
import MyAPI.imagR.ScoreDoc;
import MyAPI.imagR.ShowMatches;
import MyAPI.imagR.ShowMatches.OnePairOfImages;
import MyCustomedHaoop.ValueClass.ImageRegionMatch;
import MyCustomedHaoop.ValueClass.IntList_FloatList;
import MyCustomedHaoop.ValueClass.SURFpoint;

public class ComparePhotos_runLocal {
		
	ImageDataManager imgManager_Q;
	ImageDataManager imgManager_D;
	ScoreDoc scoreDoc;
	//optional
	ShowMatches showMatches;
	MakeRank<Integer> makeRank;
	ImageR_Evaluation imageR_eval;
	HashMap<Integer,Integer> docID_L_to_S;
	
	@SuppressWarnings("unchecked")
	public ComparePhotos_runLocal(ImageDataManager imgManager_Q, ImageDataManager imgManager_D, ScoreDoc scoreDoc, String docID_L_to_S_path, ShowMatches showMatches, MakeRank<Integer> makeRank, ImageR_Evaluation imageR_eval) throws InterruptedException{
		super();
		this.imgManager_Q=imgManager_Q;
		this.imgManager_D=imgManager_D;
		this.scoreDoc=scoreDoc;
		this.docID_L_to_S=(HashMap<Integer, Integer>) General.readObject(docID_L_to_S_path);
		this.showMatches=showMatches;
		this.makeRank=makeRank;
		this.imageR_eval=imageR_eval;
	}
	
	public String getComputingTimeReport(){
		return scoreDoc.matchingInfo==null?"":scoreDoc.matchingInfo.getComputingTimeReport();
	}
	
 	public float[] compareTwoPhoto(int query, int docID, Disp disp) throws IOException, InterruptedException, ClassNotFoundException{
		PhotoAllFeats_orgVW queryFeat=imgManager_Q.getPhoFeat(query, disp);
		PhotoAllFeats_orgVW docFeat=imgManager_D.getPhoFeat(docID, disp);		
		//transfer docID in docFeat from L to S
		docFeat.ID=docID_L_to_S==null?docID:docID_L_to_S.get(docID);
		//run
		float[] res;
		if(showMatches!=null){
			LinkedList<ImageRegionMatch> finalMatches= new LinkedList<ImageRegionMatch>();
			LinkedList<SURFpoint> selPoints_l= new LinkedList<>();
			LinkedList<SURFpoint> selPoints_r= new LinkedList<>();
			res=scoreDoc.scoreOneDoc(queryFeat, docFeat, finalMatches, selPoints_l, selPoints_r, disp.disp);
			showMatches.addOneImgPair(new OnePairOfImages(query, docID, finalMatches, selPoints_l, selPoints_r, res));
		}else{
			res=scoreDoc.scoreOneDoc(queryFeat, docFeat, null, null, null, disp.disp);
		}
		disp.disp("\n");
		return res;
	}
	
	public IntList_FloatList compareOneQueryWithMutDoc(int query, List<Integer> docs, Disp disp) throws IOException, InterruptedException, ClassNotFoundException{
		Disp disp_this=Disp.makeHardCopy(disp);
		disp_this.spacer="\t";
		for (int oneDoc : docs) {
			float[] docScores= compareTwoPhoto(query, oneDoc, disp_this);
			if (makeRank!=null) {
				makeRank.addOneDoc(oneDoc, docScores);
			}
		}
		if (makeRank!=null){
			IntList_FloatList rank_score=new IntList_FloatList();
			makeRank.getRes(rank_score.getIntegers(), rank_score.getFloats());
			if (imageR_eval!=null) {
				imageR_eval.add_oneRank(query, rank_score, disp_this);
			}
			makeRank.clearDocScores();
			return rank_score;
		}else{
			return null;
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		testOneQ_fromFeat();
		
//		testOneQ_fromImage();
		
		//************** testComputingTime in windows ****************
//		String basePath="O:/ImageRetrieval/Herve1.5K/";
//		testComputingTime(new String[]{basePath+"Herve_querys_L_to_L.hashMap", basePath+"Herve_ori1.5K_SelPhos_L_to_S.hashMap", 
//				basePath+"CVPR15/MimicHPM/SURFFeat_VW100k_SA_Herve_1.5K/", basePath+"CVPR15/", "_Herve_1.5K_100K-VW_SURF", 
//				"_1vs1AndHistAndAngle@0.52@0.2@0@0@0@0@0@0@0", basePath+"CVPR15/runTimeTest", "false", "true", basePath+"Herve_groundTruth.hashMap", "", ""});
//		//************** testComputingTime in Linux **************
//		testComputingTime(args);
	}
	
	@SuppressWarnings("unchecked")
	public static void testComputingTime(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf=new Configuration();
		String queryPath=args[0];
		String docID_L_to_S_path=args[1];
		String imageFeatMapFilesPath=args[2]; //docID in L
		String basePathForScoreDoc=args[3];
		String indexLabel=args[4]; //"_Herve_10K_20K-VW_SURF"
		String rankModel=args[5]; //"_1vs1AndHistAndAngle@true@0.52@0.2@0@0@0@0@0@0@0"
		String reportPath=args[6];
		boolean isHE=Boolean.valueOf(args[7]);
		boolean isEvalMatchingEfficiency=Boolean.valueOf(args[8]);
		String grondTruthPath=args[9];
		String junkPath=args[10];
		String buildingInd_NamePath=args[11];
		PreProcessImage preProcessImage=null;
		//set HE or BOF
		String HEPara=isHE?"HE@20@12":"HE@64@0";
		//set report
		PrintWriter outputStream_report=new PrintWriter(new OutputStreamWriter(new FileOutputStream(reportPath+indexLabel+rankModel+
				(isHE?"_"+HEPara:"_BOF")+".report", false), "UTF-8"),true); 
		long startTime=System.currentTimeMillis();
		Disp disp=new Disp(true, "", outputStream_report);
		//setup imageDataManager: mapFiles feats
		ImageDataManager imageDataManager_Q=new ImageDataManager("noImages", 0, null, null, 100*1000, new String[]{imageFeatMapFilesPath}, preProcessImage, null);
		ImageDataManager imageDataManager_D=new ImageDataManager("noImages", 0, null, null, 100*1000, new String[]{imageFeatMapFilesPath}, preProcessImage, null);
		//load query
		HashMap<Integer, Integer> query_L_to_L=(HashMap<Integer, Integer>) General.readObject(queryPath);
		//setup scoreDoc
		String docInfoPath=basePathForScoreDoc+"docInfo"+indexLabel+"/part-r-00000";
		String TVectorInfoPath=basePathForScoreDoc+"TVectorInfo"+indexLabel;
		ScoreDoc scoreDoc=new ScoreDoc(disp, "_iniR-noBurst", rankModel, HEPara, conf, docInfoPath, TVectorInfoPath, isEvalMatchingEfficiency);
		//setup makeRank
		MakeRank<Integer> makeRank=isEvalMatchingEfficiency?new MakeRank<Integer>(disp, 10000, true, 10000):null;
		//setup ImageR_Evaluation
		ImageR_Evaluation imageR_Evaluation=isEvalMatchingEfficiency?new ImageR_Evaluation(disp, query_L_to_L.size(), indexLabel, rankModel, grondTruthPath, junkPath, buildingInd_NamePath, null, null, null):null;
		//setup ComparePhotos_runLocal
		ComparePhotos_runLocal comparePhotos_runLocal=new ComparePhotos_runLocal(imageDataManager_Q, imageDataManager_D, scoreDoc, docID_L_to_S_path, null, makeRank, imageR_Evaluation);
		//load docID_L_to_S
		HashMap<Integer, Integer> docID_L_to_S=(HashMap<Integer, Integer>) General.readObject(docID_L_to_S_path);	
		disp.disp("setup finished, current memory:"+General.memoryInfo()+" ..... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
		//pre extract all doc and query feats
		imageDataManager_Q.loadPhoFeat_InMemory(new LinkedList<>(query_L_to_L.keySet()), disp);
		imageDataManager_D.loadPhoFeat_InMemory(new LinkedList<>(docID_L_to_S.keySet()), disp);
		//do matching
		LinkedList<Integer> docIDs=new LinkedList<Integer>(docID_L_to_S.keySet());
		disp.disp("start do matching for "+query_L_to_L.size()+" queries, each against "+docIDs.size()+" docs .... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
		int q_i=0; int dispInter=20; long queryTime_all=0;
		for (Integer query : query_L_to_L.keySet()) {
			disp.disp=(q_i%dispInter==0);
			//run
			long startTime_oneQ=System.currentTimeMillis();
			comparePhotos_runLocal.compareOneQueryWithMutDoc(query, docIDs, new Disp(false, "", null));
			queryTime_all+=System.currentTimeMillis()-startTime_oneQ;
			disp.disp(q_i+"th query: "+query+" finished! ..... "+General.dispTime(queryTime_all, "s")+"\n"+comparePhotos_runLocal.getComputingTimeReport());
			q_i++;
		}
		General.dispInfo(outputStream_report, "all queries are done! "+(isEvalMatchingEfficiency?imageR_Evaluation.getRes():"")+"..... "+General.dispTime(queryTime_all, "s")+", per query:"+queryTime_all/query_L_to_L.size()+"ms");
		//get run-time info
		General.dispInfo(outputStream_report, comparePhotos_runLocal.getComputingTimeReport());
		//clean up
		imageDataManager_Q.cleanUp();
		imageDataManager_D.cleanUp();
	}
	
	@SuppressWarnings("unchecked")
	public static void testOneQ_fromFeat() throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf=new Configuration();
		String indexPath="F:/Experiments/SanFrancisco/index/";//O:/ImageRetrieval/Herve1.5K/
		String indexLabel="_SanFran_DPCI_QDPCIVW_SIFTUPRightOxford1_VW65K";//_Herve_1.5K_20K-VW_SURF
		String rankModel="_1vs1AndHistAndAngle@true@false@true@0.52@0.2@1@0@0@0@0@0@0@0"; //"_1vs1AndHistAndAngle@true@true@true@0.52@0.2@1@0@0@0@0@0@0@0" 
//		String rankModel="_1vs1AndHist@true@0.52@0.2"; //"_1vs1AndHistAndAngle@true@0.52@0.2@0@0@0@0@0@0@0" 
		String HEPara="@50@40";
		Disp disp=new Disp(true, "", null);
		
		PreProcessImage preProcessImage=new PreProcessImage(disp, null, 1024*768, true);
		
//		int query=1209; List<Integer> docs=Arrays.asList(1926306,3817212,3812681); 
		HashMap<Integer, HashSet<Integer>> groundTruth=(HashMap<Integer, HashSet<Integer>>) General.readObject("F:/Experiments/SanFrancisco/dataSet/SanFrancisco_groundTruth_onlyPCI_inSInd_corr2014.hashMap");
		ArrayList<Entry<Integer, HashSet<Integer>>> gTh = new ArrayList<>(groundTruth.entrySet());
		int query_i=30; int topGTruth=10;
		int query=gTh.get(query_i).getKey(); List<Integer> docs=new ArrayList<>(gTh.get(query_i).getValue()).subList(0, Math.min(topGTruth, gTh.size()));
		
		
		//setup imageDataManager: mapFiles feats
		ImageDataManager imageDataManager_Q=new ImageDataManager("MapFile", 100*1000, null, new String[]{"F:/Experiments/SanFrancisco/dataSet/SanFrancisco_inSInd_MFiles/"}, 100*1000, new String[]{"F:/Experiments/SanFrancisco/feats/SIFTUPRightOxford1_QDPCIVW65k_MA_SanFran_Q/"}, preProcessImage,null);
		ImageDataManager imageDataManager_D=new ImageDataManager("MapFile", 100*1000, null, new String[]{"F:/Experiments/SanFrancisco/dataSet/SanFrancisco_inSInd_MFiles/"}, 100*1000, new String[]{"F:/Experiments/SanFrancisco/feats/SIFTUPRightOxford1_QDPCIVW65k_SA_SanFran_DPCI/"}, preProcessImage, null);
		imageDataManager_Q.loadPhoFeat_InMemory(Arrays.asList(query), disp);
		imageDataManager_D.loadPhoFeat_InMemory(docs, disp);
		//setup scoreDoc
		String docInfoPath=indexPath+"docInfo"+indexLabel+"/part-r-00000";
		String TVectorInfoPath=indexPath+"TVectorInfo"+indexLabel;
		ScoreDoc scoreDoc=new ScoreDoc(disp, "_iniR-noBurst"+HEPara, rankModel, "reRHE"+HEPara, conf, docInfoPath, TVectorInfoPath, true);
		//setup ShowMatches
		ShowMatches showMatches=new ShowMatches(true, imageDataManager_Q, imageDataManager_D, 0, 2);
		//setup ComparePhotos_runLocal
		ComparePhotos_runLocal comparePhotos_runLocal=new ComparePhotos_runLocal(imageDataManager_Q, imageDataManager_D, scoreDoc, null, showMatches, null, null);
		//run
		comparePhotos_runLocal.compareOneQueryWithMutDoc(query, docs, disp);
		//disp
		disp.disp(comparePhotos_runLocal.getComputingTimeReport());
		showMatches.disp();
		//clean up
		imageDataManager_Q.cleanUp();
		imageDataManager_D.cleanUp();
	}
	
	public static void testOneQ_fromImage() throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf=new Configuration();
		String basePath="O:/GVM/VisualRank/ME13/";//O:/ImageRetrieval/Herve1.5K/
		String indexLabel="_MEva13_9M_20K-VW_SURF";//_Herve_1.5K_20K-VW_SURF
		String rankModel="_1vs1AndHistAndAngle@0.52@0.2@1@0@0@0@0@0@0@0"; //"_1vs1AndHistAndAngle@0.52@0.2@0@0@0@0@0@0@0"
		String HEPara="HE@20@12";
		PreProcessImage preProcessImage=null;
		int query=8642947; List<Integer> docs=Arrays.asList(1926306,3817212,3812681); 
		
		Disp disp=new Disp(true, "", null);
		//setup extractFeat
		ExtractFeat extractFeat_Q=setupExtractFeat_SURF(disp, new MutiAssVW(true, 10,1.2,0.05));
		ExtractFeat extractFeat_D=setupExtractFeat_SURF(disp, new MutiAssVW(false, 10,1.2,0.05));
		//setup imageDataManager: mapFiles feats
		ImageDataManager imageDataManager_Q=new ImageDataManager("MapFile", 100*1000, null, new String[]{"N:/ewi/insy/MMC/XinchaoLi/Photos_MEva13_9M_MapFiles/"}, 100*1000, new String[]{"Q:/SURFFeat_VW20k_MA_ME13_Q/"}, preProcessImage, extractFeat_Q);
		ImageDataManager imageDataManager_D=new ImageDataManager("MapFile", 100*1000, null, new String[]{"N:/ewi/insy/MMC/XinchaoLi/Photos_MEva13_9M_MapFiles/"}, 100*1000, new String[]{"Q:/SURFFeat_VW20k_SA_ME13_D/"}, preProcessImage, extractFeat_D);
		imageDataManager_Q.loadPhoFeat_InMemory(Arrays.asList(query), disp);
		imageDataManager_D.loadPhoFeat_InMemory(docs, disp);
		//setup scoreDoc
		String docInfoPath=basePath+"docInfo"+indexLabel+"/part-r-00000";
		String TVectorInfoPath=basePath+"TVectorInfo"+indexLabel;
		ScoreDoc scoreDoc=new ScoreDoc(disp, "_iniR-noBurst", rankModel, HEPara, conf, docInfoPath, TVectorInfoPath, true);
		//setup ShowMatches
		ShowMatches showMatches=new ShowMatches(true, imageDataManager_Q, imageDataManager_D, 0, 5);
		//setup ComparePhotos_runLocal
		ComparePhotos_runLocal comparePhotos_runLocal=new ComparePhotos_runLocal(imageDataManager_Q, imageDataManager_D, scoreDoc, basePath+"Herve_ori1.5K_SelPhos_L_to_S.hashMap", showMatches, null, null);
		//run
		comparePhotos_runLocal.compareOneQueryWithMutDoc(query, docs, disp);
		//disp
		disp.disp(comparePhotos_runLocal.getComputingTimeReport());
		showMatches.disp();
		//clean up
		imageDataManager_Q.cleanUp();
		imageDataManager_D.cleanUp();
	}
	
	public static ExtractFeat setupExtractFeat_SURF(Disp disp, MutiAssVW mutiAssVW) throws IOException, InterruptedException, ClassNotFoundException {
		//setup targetFeat
		String targetFeat="SURF"; //SURF, SIFT-binTool-Oxford2, SIFT-binTool-INRIA2, SIFT-binTool-VLFeat
		String tempFilesPath=null;
		String binaryPath_Detector=null;
		//setup for extractFeat
		String basePathForExtractFeat="O:/ImageRetrieval/SURFVW/"; // O:/ImageRetrieval/Oxford5K/VWs/, O:/ImageRetrieval/SURFVW/, O:/ImageRetrieval/SIFTVW/
		String vwCenters_path=basePathForExtractFeat+"SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000";//randFeat30000_AllOxford5K_KMean_VW1000k_loop-30/part-r-00000, SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k
		String PMatrix_path=basePathForExtractFeat+"HE_ProjectionMatrix64-64"; //HE_ProjectionMatrix64-64, HE_ProjectionMatrix128-64
		String HE_Thresholds_path=basePathForExtractFeat+"SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000";//randFeat30000_AllOxford5K_HEThr64_VW1000k_KMloop30/part-r-00000, SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99
		String middleNodes_path=basePathForExtractFeat+"MiddleNode1000_onVW20k/loop-199/part-r-00000";//randFeat30000_AllOxford5K_MiddleNode1000_loop-199/part-r-00000, MiddleNode1000_onVW20k/loop-199/part-r-00000
		String node_vw_links_path=basePathForExtractFeat+"D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet";// randFeat30000_AllOxford5Knode_vw_links_M1000_VW1000k_I200.ArrayList_HashSet, D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet
		//make extractFeat
		return new ExtractFeat(disp, targetFeat, binaryPath_Detector, tempFilesPath, vwCenters_path, PMatrix_path, HE_Thresholds_path, middleNodes_path, node_vw_links_path, mutiAssVW, new Configuration());
	}
	
	public static ExtractFeat setupExtractFeat_SIFT_VLFeat(Disp disp, MutiAssVW mutiAssVW) throws IOException, InterruptedException, ClassNotFoundException {
		//setup targetFeat
		String targetFeat="SIFT-binTool-VLFeat"; //SURF, SIFT-binTool-Oxford2, SIFT-binTool-INRIA2, SIFT-binTool-VLFeat
		String tempFilesPath="D:/xinchaoli/Desktop/My research/Code_Tools/SIFT_binary/VLFeat/vlfeat-0.9.18-bin/vlfeat-0.9.18/bin/win64/";
		String binaryPath_Detector=tempFilesPath+"sift.exe"; 
		//setup for extractFeat
		String basePathForExtractFeat="O:/ImageRetrieval/SIFTVW/"; // O:/ImageRetrieval/Oxford5K/VWs/, O:/ImageRetrieval/SURFVW/, O:/ImageRetrieval/SIFTVW/
		String vwCenters_path=basePathForExtractFeat+"SIFT_VLFeat/SIFT-binTool-VLFeat_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000";//randFeat30000_AllOxford5K_KMean_VW1000k_loop-30/part-r-00000, SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k
		String PMatrix_path=basePathForExtractFeat+"HE_ProjectionMatrix128-64"; //HE_ProjectionMatrix64-64, HE_ProjectionMatrix128-64
		String HE_Thresholds_path=basePathForExtractFeat+"SIFT_VLFeat/SIFT-binTool-VLFeat_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000";//randFeat30000_AllOxford5K_HEThr64_VW1000k_KMloop30/part-r-00000, SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99
		String middleNodes_path=basePathForExtractFeat+"SIFT_VLFeat/MiddleNode1000_onVW20k_maxLoop200/part-r-00000";// randFeat30000_AllOxford5K_MiddleNode1000_loop-199/part-r-00000, middleNodes_M1000_VW20000_I200.seq
		String node_vw_links_path=basePathForExtractFeat+"SIFT_VLFeat/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet";// randFeat30000_AllOxford5Knode_vw_links_M1000_VW1000k_I200.ArrayList_HashSet, node_vw_links_M1000_VW20000_I200_learned30M.ArrayList_HashSet
		//make extractFeat
		return new ExtractFeat(disp, targetFeat, binaryPath_Detector, tempFilesPath, vwCenters_path, PMatrix_path, HE_Thresholds_path, middleNodes_path, node_vw_links_path, mutiAssVW, new Configuration());
	}
	
}
