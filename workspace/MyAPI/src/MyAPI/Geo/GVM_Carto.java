package MyAPI.Geo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;

import MyAPI.General.General;
import MyAPI.Geo.groupDocs.CartoDocs;
import MyAPI.Geo.groupDocs.CartoListProc;
import MyAPI.Geo.groupDocs.GroupDocs;
import MyAPI.Geo.groupDocs.GroupEstResult;
import MyAPI.Geo.groupDocs.ShowCartoMatches;
import MyAPI.Obj.PatternBoolean;
import MyAPI.imagR.ImageDataManager;
import MyAPI.imagR.ShowMatches_rank;
import MyAPI.imagR.ShowImgBlocks;
import MyCustomedHaoop.ValueClass.ComposeWritableClassCollection.PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr;
import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr;

public class GVM_Carto extends GVM<CartoDocs<DID_Score_ImageRegionMatch_ShortArr>, CartoListProc<DID_Score_ImageRegionMatch_ShortArr>, ShowCartoMatches>{
	
	//data with buildingID
	HashSet<Integer>[] cartoIDs_Q;
	
	public GVM_Carto(String task_cartoList_Label, HashSet<Integer>[] cartoIDs_Q, int[] cartoIDs_db, HashMap<Integer, HashSet<Integer>> gTruth, int num_topCartos, int num_topGroups_toShow, ShowMatches_rank showGTMatches, ShowCartoMatches showCartoMatches, 
			int V_ForGTSize, ShowImgBlocks showFeatPoints) throws InterruptedException {//for data with GPS
		super(task_cartoList_Label.split("_")[1], num_topCartos, num_topGroups_toShow, showGTMatches, new CartoListProc<DID_Score_ImageRegionMatch_ShortArr>(cartoIDs_Q, cartoIDs_db, gTruth, task_cartoList_Label.split("_")[2], V_ForGTSize), showCartoMatches, showFeatPoints);
		this.cartoIDs_Q=cartoIDs_Q;
	}
	
	@Override
	protected CartoDocs<DID_Score_ImageRegionMatch_ShortArr> makeGroupMatch(GroupDocs<DID_Score_ImageRegionMatch_ShortArr> finalGroupMatch, CartoDocs<DID_Score_ImageRegionMatch_ShortArr> iniMatches){
		return new CartoDocs<>(finalGroupMatch, iniMatches.cartoID);
	}
	
	@Override
	protected void preProcessVisRank(int queryID,
			ArrayList<DID_Score_ImageRegionMatch_ShortArr> docs_scores_matches) {
		//cutOffIniMatchingScore
		cutOffIniMatchingScore(docs_scores_matches);
	}

	public static void main(String[] args) throws InterruptedException, IOException {
//		makeDevelopSet();
		
		//SanFrancisco_StreetView
		String basePath="F:/Experiments/SanFrancisco/";
		run(basePath+"dataSet/SanFrancisco_Q-DPCI_cartoIDs_q_corr2014.hashSetArr", //SanFrancisco_Q-DPCI_cartoIDs_q_corr2014.hashSetArr, SanFrancisco_Q-DPCI_cartoIDs_q.hashSetArr
				basePath+"dataSet/SanFrancisco_Q-DPCI_cartoIDs_db.intArr",
				basePath+"dataSet/SanFrancisco_groundTruth_onlyPCI_inSInd_corr2014.hashMap",//_corr2014
				basePath+"dataSet/SanFrancisco_inSInd_MFiles/", 
				basePath+"feats/SIFTUPRightOxford1_QDPCIVW65k_MA_SanFran_Q/", 
				basePath+"feats/SIFTUPRightOxford1_QDPCIVW65k_SA_SanFran_DPCI/", 
				basePath+"VisualRank/HR1000_SanFran_DPCI_QDPCIVW_SIFTUPRightOxford1_VW65K_iniR-BurstIntraInter@46@30_ReR1K_reRHE@46@30_Top1K_1vs1AndHistAndAngle@true@true@true@0.26@0.2@1@0@0@0@0@0@0@0_rankDocMatches");
	}
	
	public static void makeDevelopSet() throws FileNotFoundException, IOException, InterruptedException{
		String objPath="F:/Experiments/SanFrancisco/dataSet/Dev80Q.hashSet";
		HashSet<Integer> sels=General.randSelect(new Random(), 803, 80, 0);
		System.out.println(sels);
		if (new File(objPath).exists()) {
			throw new InterruptedException("already exist! "+objPath);
		}
		General.writeObject(objPath, sels);
	}
	
	@SuppressWarnings("unchecked")
	public static void run(String cartoIDs_Q_path, String cartoIDs_db_path, String gTruths_path, String photos, String Q_feat, String D_feat, String iniRank) throws InterruptedException, IOException {
		//data
		HashSet<Integer>[] cartoIDs_Q=(HashSet<Integer>[]) General.readObject(cartoIDs_Q_path);
		int[] cartoIDs_db=(int[]) General.readObject(cartoIDs_db_path);
		HashMap<Integer, HashSet<Integer>> gTruths=(HashMap<Integer, HashSet<Integer>>) General.readObject(gTruths_path);		
		//param
		int reRankScale=1000; int topVisScale=1000; int V_ForGTSize=1000;
		int num_topCartos=reRankScale; int num_topCartos_toShow=2; int maxNumPerLoc=10; int minGTSize=0; int maxGTSize=Integer.MAX_VALUE; int minGeoDensity=0;
		int RGBInd=0; int pointEnlargeFactor=1;
		ImageDataManager imageDataManager_Q=new ImageDataManager("MapFile", 100*1000, null, 
				new String[]{photos}, 
				100*1000, new String[]{Q_feat}, null, null);
		ImageDataManager imageDataManager_D=new ImageDataManager("MapFile", 100*1000, null, 
				new String[]{photos}, 
				100*1000, new String[]{D_feat}, null, null);
		int actQNum=803; String evalFlag="DivideQ@1,2,3,4,5,6,7,8,9,10@0,1,5,10,20,40,100@0,1,5,10,20,40,100";
		//set GVM, "_GVM-blockInd@9@30"
//		String[] schemes={"_VisNN@4","_GVR@4","_GVM@4@5@0@30@bestDoc@5","_GVM@4@5@20@10@1vs1@5","_GVM@4@5@0@20@sumDoc@2","_GVM@4@5@0@30@bestDocLocAsVec@5","_GVM@4@5@20@10@1vs1LocAsVec@5","_GVM@4@5@0@20@sumDocLocAsVec@2"}; PatternBoolean patternCorrect=new PatternBoolean(new String[]{"neutral","false","true","neutral","neutral","neutral","neutral","neutral"});//neutral, true, false
		String[] schemes={"_GVM@4@5@5@5@1vs1@5","_GVM@4@5@10@10@1vs1@5"}; PatternBoolean patternCorrect=new PatternBoolean(new String[]{"false","true"});//neutral, true, false
//		String[] schemes={"_VisNN@0"}; PatternBoolean patternCorrect=new PatternBoolean(new String[]{"neutral","neutral","neutral"});//neutral, true, false
//		//80 as turn parameters
		HashSet<Integer> sels=(HashSet<Integer>) General.readObject("F:/Experiments/SanFrancisco/dataSet/Dev80Q.hashSet");
		actQNum=sels.size();
//		PrintWriter turnParaRes=new PrintWriter(new OutputStreamWriter(new FileOutputStream("F:/Experiments/SanFrancisco/GVM/turnOnDev80Q.report",false), "UTF-8"),true); 
//		LinkedList<String> paras=new LinkedList<>();
//		for (int a : new int[]{0,10,20,30}) {
//			for (int b : new int[]{0,10,20,30}) {
//				for (int freqThr : new int[]{4,5,6,7}) {
//					paras.add("_GVM@4@5@"+a+"@"+b+"@1vs1@"+freqThr);
//				}
//			}
//		}
//		String[] schemes=paras.toArray(new String[0]); 
		//set system
		ShowMatches_rank showGTMatches=new ShowMatches_rank(6, true, imageDataManager_Q, imageDataManager_D, RGBInd, pointEnlargeFactor);
		ArrayList<GVM_Carto> GVMs=new ArrayList<>(schemes.length); ArrayList<GVM_Evaluator_Carto> evals=new ArrayList<>(schemes.length);
		for (int scheme_i = 0; scheme_i < schemes.length; scheme_i++) {
			String scheme = schemes[scheme_i];
			String cartoListLabel=CartoListProc.setGroupListParams(schemes[scheme_i].startsWith("_VisNN")?-1:reRankScale, topVisScale);
//			String cartoListLabel=CartoListProc.setGroupListParams(reRankScale, topVisScale);
			//pro_GVM
			ShowImgBlocks showFeatPoints=new ShowImgBlocks(imageDataManager_Q, RGBInd, pointEnlargeFactor, true);
			ShowCartoMatches showCartoMatch=new ShowCartoMatches(maxNumPerLoc, false, imageDataManager_Q, imageDataManager_D, RGBInd, pointEnlargeFactor, cartoIDs_Q);
			GVM_Carto proc_GVM=new GVM_Carto(scheme+cartoListLabel, cartoIDs_Q, cartoIDs_db, gTruths, num_topCartos, num_topCartos_toShow, scheme_i==0?showGTMatches:null, showCartoMatch, V_ForGTSize, showFeatPoints);
			GVMs.add(proc_GVM);
			//eval
			evals.add(new GVM_Evaluator_Carto(evalFlag, cartoIDs_Q_path));
		}
		//run
		MapFile.Reader rankReader=new MapFile.Reader(new Path(iniRank), new Configuration());
		IntWritable Key_queryName=new IntWritable();
		PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr Value_RankScores= new PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr();
		int query_i=0; int partenQ=0; int[] correctQ=new int[schemes.length]; int dispInter=10; LinkedList<Integer> partenQs=new LinkedList<>(); long startTime=System.currentTimeMillis();
		while (rankReader.next(Key_queryName, Value_RankScores)) {
			if (gTruths.get(Key_queryName.get()).size()>0 && sels.contains(Key_queryName.get())) {//&& !sels.contains(Key_queryName.get())
				boolean[] corPattern=new boolean[schemes.length]; GroupEstResult<CartoDocs<DID_Score_ImageRegionMatch_ShortArr>> res=null;
				for (int scheme_i = 0; scheme_i < GVMs.size(); scheme_i++) {//process Q for different schemes
					GroupEstResult<CartoDocs<DID_Score_ImageRegionMatch_ShortArr>> res_tmp=GVMs.get(scheme_i).ProcessOneQuery(Key_queryName.get(),Value_RankScores,false);
					if (res_tmp!=null) {
						res=res_tmp;
						boolean isCorrect_GVM=(res.firstMatch.rank==0);
						if (isCorrect_GVM) {
							correctQ[scheme_i]++;
							corPattern[scheme_i]=true;
						}
						evals.get(scheme_i).addOneQueryRank(
								Key_queryName.get(), 
								evals.get(scheme_i).evalOneQuery_carto(Key_queryName.get(), res.res), 
								res.queryReduncy, res.topVisRankedGTruthNum);
					}
				}
				//show Q
				boolean isShowThisQ=(query_i>-1) && (res!=null) && patternCorrect.isSamePattern(corPattern) && res.topVisRankedGTruthNum>=minGTSize && res.topVisRankedGTruthNum<=maxGTSize && res.queryReduncy>=minGeoDensity;
//				boolean isShowThisQ=(Key_queryName.get()==100 || Key_queryName.get()==307 || Key_queryName.get()==333|| Key_queryName.get()==373|| Key_queryName.get()==645|| Key_queryName.get()==684|| Key_queryName.get()==784);
//				boolean isShowThisQ=(Key_queryName.get()== 25);
				if(isShowThisQ){
//					for (GVM_Carto oneScheme : GVMs) {
//						oneScheme.showRes();
//					}
					partenQ++;
				}
			}
			//disp
			query_i++;
//			General.dispInfo_ifNeed(query_i%dispInter==0, "\n ", query_i+" quries, correctQ for shemes:"+General.StrArrToStr(schemes, ",")+" is "+General.IntArrToString(correctQ, "_")
//					+", for partens:"+patternCorrect.getTargetPattern()+", partenQ:"+partenQ+", "+General.dispTime(System.currentTimeMillis()-startTime, "s"));
		}
		rankReader.close();
		System.out.println("done! tot "+query_i+" quries, correctQ for shemes:"+General.StrArrToStr(schemes, ",")+" is "+General.IntArrToString(correctQ, "_")
				+", for partens:"+patternCorrect.getTargetPattern()+", partenQ:"+partenQ+", "+General.dispTime(System.currentTimeMillis()-startTime, "s"));
//		System.out.println("partenQs: "+partenQs);
		for (int scheme_i = 0; scheme_i < schemes.length; scheme_i++) {
			System.out.println(schemes[scheme_i]+"\n"+evals.get(scheme_i).getEvalationRes(actQNum));
		}
//		for (GVM one : GVMs) {
//			one.disp_elemLocDistri("SanFran");
//		}
//		//save res in turnParaRes
//		for (int scheme_i = 0; scheme_i < schemes.length; scheme_i++) {
//			turnParaRes.println(schemes[scheme_i]+"\n"+evals.get(scheme_i).getEvalationRes(actQNum));
//		}
//		turnParaRes.close();
	}

}
