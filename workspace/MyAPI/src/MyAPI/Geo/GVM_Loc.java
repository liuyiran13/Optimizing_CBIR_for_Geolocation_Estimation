package MyAPI.Geo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;

import MyAPI.General.General;
import MyAPI.Geo.groupDocs.GroupDocs;
import MyAPI.Geo.groupDocs.GroupEstResult;
import MyAPI.Geo.groupDocs.LocDocs;
import MyAPI.Geo.groupDocs.LocListProc;
import MyAPI.Geo.groupDocs.ShowLocMatches;
import MyAPI.Geo.groupDocs.UserIDs;
import MyAPI.Obj.PatternBoolean;
import MyAPI.imagR.ImageDataManager;
import MyAPI.imagR.ShowMatches_rank;
import MyAPI.imagR.ShowImgBlocks;
import MyCustomedHaoop.ValueClass.ComposeWritableClassCollection.PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr;
import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr;

public class GVM_Loc extends GVM<LocDocs<DID_Score_ImageRegionMatch_ShortArr>, LocListProc<DID_Score_ImageRegionMatch_ShortArr>, ShowLocMatches>{
	
	//data with GPS
	float[][] latlons;
	
	public GVM_Loc(String task_locList_Label, UserIDs userIDs, float[][] latlons, int num_topLocations, int num_topGroups_toShow, ShowMatches_rank showGTMatches, ShowLocMatches showLocMatches, 
			float G_ForGTSize, int V_ForGTSize, ShowImgBlocks showFeatPoints) throws InterruptedException {//for data with GPS
		super(task_locList_Label.split("_")[1], num_topLocations, num_topGroups_toShow, showGTMatches, new LocListProc<DID_Score_ImageRegionMatch_ShortArr>(userIDs, latlons, task_locList_Label.split("_")[2], G_ForGTSize, V_ForGTSize),showLocMatches, showFeatPoints);
		this.latlons=latlons;
	}
	
	@Override
	protected LocDocs<DID_Score_ImageRegionMatch_ShortArr> makeGroupMatch(GroupDocs<DID_Score_ImageRegionMatch_ShortArr> finalGroupMatch, LocDocs<DID_Score_ImageRegionMatch_ShortArr> iniMatches) throws InterruptedException{
//		LocDocs<DID_Score_ImageRegionMatch_ShortArr> res=new LocDocs<>(finalGroupMatch, new LatLon(General_geoRank.findCenterLoc(finalGroupMatch.docs, latlons)));
		LocDocs<DID_Score_ImageRegionMatch_ShortArr> res=new LocDocs<>(finalGroupMatch, iniMatches.latlon);//use the averaged centeroid is better than single photo
		return res;
	}
	
	@Override
	protected void preProcessVisRank(int queryID,
			ArrayList<DID_Score_ImageRegionMatch_ShortArr> docs_scores_matches) {
		//cutOffIniMatchingScore
		cutOffIniMatchingScore(docs_scores_matches);
		//remove Q&D SameUser
		groupListProc.preFilterRankByUser(queryID, docs_scores_matches);
	}

	public static void main(String[] args) throws InterruptedException, IOException {
		
		//SanFrancisco_StreetView
		String basePath="F:/Experiments/MediaEval15/";
		run(basePath+"DataSet/ME15_photos_latlons.floatArr", 
				basePath+"DataSet/Photos_MEva15_test_inSInd_MapFiles/", 
				basePath+"DataSet/Photos_MEva15_train_inSInd_MapFiles/", 
				basePath+"feats/SIFTUPRightOxford1_DVW65k_MA_ME15_Q/", 
				basePath+"feats/SIFTUPRightOxford1_DVW65k_SA_ME15_D/", 
				basePath+"ranks/SIFT/Q2014-0_HR1000_ME15_DVW_SIFTUPRightOxford1_VW65K_iniR-BurstIntraInter@46@30_ReR1K_reRHE@46@30_Top1K_1vs1AndHistAndAngle@true@true@true@0.52@0.2@1@0@0@0@0@0@0@0_rankDocMatches");

	}
	
	public static void run(String latlon, String photos_Q, String photos_D, String Q_feat, String D_feat, String iniRank) throws InterruptedException, IOException {
		float[][] latlons=(float[][]) General.readObject(latlon);
		float geoExpanSca=0.01f; int reRankScale=1000; int topVisScale=1000; float G_ForGTSize=1f; int V_ForGTSize=1000;
		int num_topLocations=reRankScale; int num_topGroups_toShow =3; int maxNumPerLoc=3; int minGTSize=0; int maxGTSize=Integer.MAX_VALUE; int minGeoDensity=0;
		int RGBInd=0; int pointEnlargeFactor=1;
		ImageDataManager imageDataManager_Q=new ImageDataManager("MapFile", 100*1000, null, 
				new String[]{photos_Q}, 
				100*1000, new String[]{Q_feat}, null, null);
		ImageDataManager imageDataManager_D=new ImageDataManager("MapFile", 100*1000, null, 
				new String[]{photos_D}, 
				100*1000, new String[]{D_feat}, null, null);
		//set GVM, GVM@thr_matchScore@smoothFactor@imBlockPortion@IDFBlockPortion@isExpan@isCocReg: _GVM@0@10@20@0@false@false
//		String[] schemes={"_VisNN@0","_GVR@0","_GVM@0@5@30@0@1vs1@6"}; PatternBoolean patternCorrect=new PatternBoolean(new String[]{"false","false","true"});//neutral, true, false
		String[] schemes={"_GVM@0@5@0@0@1vs1@6","_GVM@0@5@0@10@1vs1@6"}; PatternBoolean patternCorrect=new PatternBoolean(new String[]{"false","true"});//neutral, true, false
		ShowMatches_rank showGTMatches=new ShowMatches_rank(10, true, imageDataManager_Q, imageDataManager_D, RGBInd, pointEnlargeFactor);
		ArrayList<GVM_Loc> GVMs=new ArrayList<>(schemes.length);
		for (int scheme_i = 0; scheme_i < schemes.length; scheme_i++) {
			String scheme = schemes[scheme_i];
			String locListLabel=LocListProc.setLocListParams(schemes[scheme_i].startsWith("_VisNN")?-1:reRankScale, topVisScale, geoExpanSca, false, false);
			ShowImgBlocks showFeatPoints=new ShowImgBlocks(imageDataManager_Q, RGBInd, pointEnlargeFactor, true);
			ShowLocMatches showLocMatche=new ShowLocMatches(maxNumPerLoc, true, imageDataManager_Q, imageDataManager_D, RGBInd, pointEnlargeFactor, latlons);
			GVM_Loc proc_GVM=new GVM_Loc(scheme+locListLabel, null, latlons, num_topLocations, num_topGroups_toShow, scheme_i==0?showGTMatches:null, showLocMatche, G_ForGTSize, V_ForGTSize, showFeatPoints);
			GVMs.add(proc_GVM);
		}
		//run
		MapFile.Reader rankReader=new MapFile.Reader(new Path(iniRank), new Configuration());
		IntWritable Key_queryName=new IntWritable();
		PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr Value_RankScores= new PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr();
		int query_i=0; int partenQ=0; int[] correctQ=new int[schemes.length]; int dispInter=100; LinkedList<Integer> partenQs=new LinkedList<>(); long startTime=System.currentTimeMillis();
		while (rankReader.next(Key_queryName, Value_RankScores)) {
//			if (Key_queryName.get()==4706656 || Key_queryName.get()==5025695 || Key_queryName.get()==5436032 || Key_queryName.get()==5541462 || Key_queryName.get()==5583450
//					|| Key_queryName.get()==4765311|| Key_queryName.get()==4775462) {
				
			boolean[] corPattern=new boolean[schemes.length]; GroupEstResult<LocDocs<DID_Score_ImageRegionMatch_ShortArr>> res=null;
			if (Key_queryName.get()==4866259) {
				@SuppressWarnings("unused")
				int ii=0;
			}
			for (int scheme_i = 0; scheme_i < GVMs.size(); scheme_i++) {//process Q for different schemes
				res=GVMs.get(scheme_i).ProcessOneQuery(Key_queryName.get(),Value_RankScores,false);
				if (res!=null) {
					boolean isCorrect_GVM=(res.firstMatch.rank==0);
					if (isCorrect_GVM) {
						correctQ[scheme_i]++;
						corPattern[scheme_i]=true;
					}
				}
			}
			//show Q
			boolean isTargetQ=query_i>-1 && patternCorrect.isSamePattern(corPattern) && res!=null && res.topVisRankedGTruthNum>=minGTSize && res.topVisRankedGTruthNum<=maxGTSize && res.queryReduncy>=minGeoDensity;
			if(isTargetQ){
				for (GVM_Loc oneScheme : GVMs) {
					oneScheme.showRes();
				}
				partenQ++;
				partenQs.add(Key_queryName.get());
			}
			//disp
			query_i++;
			General.dispInfo_ifNeed(query_i%dispInter==0, "\n ", query_i+" quries, correctQ for shemes:"+General.StrArrToStr(schemes, ",")+" is "+General.IntArrToString(correctQ, "_")
					+", for partens:"+patternCorrect.getTargetPattern()+", partenQ:"+partenQ+", "+General.dispTime(System.currentTimeMillis()-startTime, "s"));
		
//			}
		}
		rankReader.close();
		System.out.println("done! tot "+query_i+" quries, correctQ for shemes:"+General.StrArrToStr(schemes, ",")+" is "+General.IntArrToString(correctQ, "_")
				+", for partens:"+patternCorrect.getTargetPattern()+", partenQ:"+partenQ+", "+General.dispTime(System.currentTimeMillis()-startTime, "s"));
		System.out.println("partenQs: "+partenQs);
		for (GVM one : GVMs) {
			one.disp_elemLocDistri("ME15");
		}
	}
}
