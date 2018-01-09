package MyAPI.Geo;

public class GVM_old {
//	
//	private class Vari{
//		
//		boolean isVisNN;
//		boolean isGVR;
//		boolean isGVM_featIndLevel;
//		boolean isGVM_blockIndLevel;
//		// paras for isGVM_blockIndLevel
//		ImageBlock queryImageBlock;
//		double matchingScoreSmooth;
//		//para
////		Statistic_MultiClass_1D_Distribution queryMatchSpeard;
//		//commons
//		ArrayList<Integer> location_inds; 
//		ArrayList<Float> location_Scores;
//		ArrayList<LocDocs<DID_Score_ImageRegionMatch_ShortArr>> location_Matches_ori;
//		ArrayList<ArrayList<DID_FeatInds_Score>> location_Matches_GVM;
//		
//
//		public Vari(String label, ImageDataManager imageDataManager_Q) throws InterruptedException{
//			//String label: GVM-blockInd@50@10, GVM-fInd@50
//			location_inds=new ArrayList<Integer>(); location_Scores=new ArrayList<Float>();
//			//get rerank scheme
//			String[] paras=label.split("@");
//			boolean labelCor=true;
//			if (paras[0].equalsIgnoreCase("VisNN")) {
//				this.isVisNN=true;
//			}else if(paras[0].equalsIgnoreCase("GVR")){
//				this.isGVR=true;
//			}else if(paras[0].startsWith("GVM")){
//				location_Matches_GVM=new ArrayList<ArrayList<DID_FeatInds_Score>>();
//				if(paras[0].equalsIgnoreCase("GVM-fInd")){
//					this.isGVM_featIndLevel=true;
//					matchingScoreSmooth=Double.valueOf(paras[1]);
//				}else if(paras[0].equalsIgnoreCase("GVM-blockInd")){
//					this.isGVM_blockIndLevel=true;
//					matchingScoreSmooth=Double.valueOf(paras[1]);
//					queryImageBlock=new ImageBlock(imageDataManager_Q, Integer.valueOf(paras[2]));
//				}else {
//					labelCor=false;
//				}
////				queryMatchSpeard=new Statistic_MultiClass_1D_Distribution(new String[]{"true","false"}, new float[]{0f,1.2f,0.1f}, "0");
//			}else {
//				labelCor=false;
//			}
//			General.Assert(labelCor, "label in GVM should contains VisNN, GVR, GVM-fInd, GVM-blockInd, here:"+label);
//		}
//		
//		public void scoreLocs(int queryID, ArrayList<LocDocs<DID_Score_ImageRegionMatch_ShortArr>> locationDocMatches) throws InterruptedException, IOException{
//			location_Matches_ori=locationDocMatches;
//			if (isVisNN) {
//				scoreLocs_VisNN(locationDocMatches);
//			}else if(isGVR){
//				scoreLocs_GVR(locationDocMatches);
//			}else if(isGVM_featIndLevel || isGVM_blockIndLevel){
//				if (isGVM_blockIndLevel) {
//					queryImageBlock.iniForOneImage(queryID);
//				}
//				scoreLocs_GVM(queryID, locationDocMatches);
//			}
//		}
//		
//		private void scoreLocs_VisNN(ArrayList<LocDocs<DID_Score_ImageRegionMatch_ShortArr>> locationDocMatches) {
//			location_inds.clear(); location_Scores.clear();
//			for (int loc_i = 0; loc_i < locationDocMatches.size(); loc_i++) {
//				location_inds.add(loc_i);
//				//use ori visual rank
//				location_Scores.add((float) (locationDocMatches.size()-loc_i));
//			}
//		}
//		
//		private void scoreLocs_GVR(ArrayList<LocDocs<DID_Score_ImageRegionMatch_ShortArr>> locationDocMatches) {
//			location_inds.clear(); location_Scores.clear();
//			for (int loc_i = 0; loc_i < locationDocMatches.size(); loc_i++) {
//				location_inds.add(loc_i);
//				//use ori matches within all photo's matches in this loc
//				List<DID_Score_ImageRegionMatch_ShortArr> locMatches=locationDocMatches.get(loc_i).docs;
//				//sum score
//				float thisLocScore=0;
//				for (DID_Score_ImageRegionMatch_ShortArr oneMatch : locMatches) {
//					thisLocScore+=oneMatch.getScore();
//				}
//				location_Scores.add(thisLocScore);
//			}
//		}
//		
//		private void scoreLocs_GVM(int queryID, ArrayList<LocDocs<DID_Score_ImageRegionMatch_ShortArr>> locationDocMatches) {
//			location_inds.clear(); location_Scores.clear(); location_Matches_GVM.clear(); 
//			//find 1vs1 matches in each loc, caculate qFeatInd_Locfreq
//			IDF idf=new IDF(locationDocMatches.size()); 
////			Normlise normlise_aveCentroidDist=new Normlise();
////			CocurrentRegionWeighting qRegWeighting=new CocurrentRegionWeighting(20,queryImageBlock.getTotBlockNum(), locationDocMatches.size());
//			for (int loc_i = 0; loc_i < locationDocMatches.size(); loc_i++) {
//				location_inds.add(loc_i);
//				//find 1vs1 matches within all photo's matches in this loc
//				ArrayList<DID_FeatInds_Score> locMatches=select1V1Match_forLocationPhoto(locationDocMatches.get(loc_i).docs);
//				//add matches
//				location_Matches_GVM.add(locMatches);
//				//update qFeatInd_Locfreq
////				AveCentroidDist aveCentroidDist_Q=new AveCentroidDist();
//				for (DID_FeatInds_Score oneMatch : locMatches) {
//					//use smoothed matching score
//					oneMatch.score=smoothMatchingScore(oneMatch.score);
//					//update qFeatInd_Locfreq
//					idf.updateOneIterm(oneMatch.blockInd_Q);
////					qRegWeighting.addOneLoc(loc_i, oneMatch.blockInd_Q);
////					//calculate QueryMatchSpeard
////					aveCentroidDist_Q.addOnePoint(queryImageBlock.getBlockCoordinate(oneMatch.blockInd_Q));
//				}
////				//add to normlise_aveCentroidDist
////				normlise_aveCentroidDist.addOneSample(aveCentroidDist_Q.getAveCentroidDist());
//			}
////			//add to statistics of QueryMatchSpeard
////			int loc_i = 0; LinkedList<Double> aveCentroidDist_norm=normlise_aveCentroidDist.normliseByMax();
////			for (double one : aveCentroidDist_norm) {
////				queryMatchSpeard.addOneSample(locationListProc.isCorrectLoc(queryID, locationDocMatches.get(loc_i).latlon)?0:1, (float) one);
////				loc_i++;
////			}
//			//make final scores for each loc
//			for (int loc_i = 0; loc_i < location_Matches_GVM.size(); loc_i++) {
//				ArrayList<DID_FeatInds_Score> locMatches= location_Matches_GVM.get(loc_i);
//				//sum score
//				float thisLocScore=0;
//				for (DID_FeatInds_Score oneMatch : locMatches) {
//					//*** make final score for each match **
//					oneMatch.score*=idf.getIDF(oneMatch.blockInd_Q);
//					//*** add to thisLocScore **
//					thisLocScore+=oneMatch.score;
//				}
////				thisLocScore=(float) (thisLocScore*0.9+aveCentroidDist_norm.get(loc_i)*0.1);
//				location_Scores.add(thisLocScore);
//			}
//		}
//		
//		private float smoothMatchingScore(float oriScore){//0~1
//			return (float) (1-Math.exp(-Math.pow(oriScore/matchingScoreSmooth, 2)));
//		}
//		
//		public void dispQueryMatchSpeard(int queryID){
////			if (isGVM_blockIndLevel || isGVM_featIndLevel) {
////				queryMatchSpeard.dispAsChart("queryMatchSpeard for Q:"+queryID, "aveCentDist", "percent");
////			}
//		}
//		
//		private ArrayList<DID_FeatInds_Score> select1V1Match_forLocationPhoto(List<DID_Score_ImageRegionMatch_ShortArr> matches) {
//			//make QFeatInd links
//			HashMap<Integer, ArrayList<DID_FeatInds_Score>> matchCandidates=new HashMap<Integer, ArrayList<DID_FeatInds_Score>>();
//			int global_docFeatInd=0;
//			for (DID_Score_ImageRegionMatch_ShortArr one : matches) {
//				int docID=one.getDID();
//				for (ImageRegionMatch oneMatch : one.matches.ObjArr) {
//					short QFeatInd=(short) oneMatch.src; //cast to short, please check!
//					//make keyID for isGVM_featIndLevel or isGVM_blockIndLevel
//					int keyID=0;
//					if (isGVM_featIndLevel) {
//						keyID=QFeatInd;
//					}else if (isGVM_blockIndLevel) {
//						keyID=queryImageBlock.getBlockID(QFeatInd);
//					}		
//					DID_FeatInds_Score oneLink=new DID_FeatInds_Score(docID, QFeatInd, keyID, oneMatch.dst, global_docFeatInd, oneMatch.matchScore);
//					if (matchCandidates.containsKey(keyID)) {
//						matchCandidates.get(keyID).add(oneLink);
//					}else {
//						ArrayList<DID_FeatInds_Score> temp=new ArrayList<DID_FeatInds_Score>();
//						temp.add(oneLink);
//						matchCandidates.put(keyID, temp);
//					}
//					global_docFeatInd++;
//				}
//			}
//			//find 1vs1 matches
//			ArrayList<DID_FeatInds_Score> goodMatches=General_BoofCV.select1V1Match_for1vsM_basedOnScore(matchCandidates);
//	    	return goodMatches;
//		}
//		
//		public LocDocs<DID_Score_ImageRegionMatch_ShortArr> getOneLocMatches(int locInd) throws InterruptedException{
//			if (isVisNN || isGVR) {//both isVisNN and isGVR do not change the mathes in one loc
//				return new LocDocs<DID_Score_ImageRegionMatch_ShortArr>(location_Matches_ori.get(locInd));
//			}else if(isGVM_featIndLevel || isGVM_blockIndLevel){
//				return new LocDocs<DID_Score_ImageRegionMatch_ShortArr>(GroupDocs.orgraniseMatchesForOneGroup(location_Matches_GVM.get(locInd)), location_Matches_ori.get(locInd).latlon);
//			}else{
//				return null;
//			}
//		}
//	}
//	
//	Vari vari;
//	float[][] latlons;
//	LocationListProc<DID_Score_ImageRegionMatch_ShortArr, LocDocs<DID_Score>> locationListProc;
//	int num_topLocations;
//	ShowLocMatches showLocMatches;
//	ShowMatches_rank showGTMatches;
//	
//	public GVM_old(String taskLabel, UserIDs userIDs, float[][] latlons, ImageDataManager imageDataManager, int num_topLocations, ShowMatches_rank showGTMatches, ShowLocMatches showLocMatches, 
//			float G_ForGTSize, int V_ForGTSize) throws InterruptedException {
//		this.vari=new Vari(taskLabel.split("_")[1], imageDataManager);
//		this.latlons=latlons;
//		this.locationListProc=new LocationListProc<>(userIDs, latlons, taskLabel, G_ForGTSize, V_ForGTSize);
//		this.num_topLocations=num_topLocations;
//		this.showLocMatches=showLocMatches;
//		this.showGTMatches=showGTMatches;
//	}
//
//	public fistMatch_GTruth_Docs_Locations ProcessOneQuery(int queryID, DID_Score_ImageRegionMatch_ShortArr_Arr temp, boolean disp) throws InterruptedException, IOException{
//		ArrayList<DID_Score_ImageRegionMatch_ShortArr> docs_scores_matches=new ArrayList<DID_Score_ImageRegionMatch_ShortArr>(Arrays.asList(temp.ObjArr));
//		locationListProc.preFilterRankByUser(queryID, docs_scores_matches);
//		int totRank_length=docs_scores_matches.size();
//		//calculate groundTruth
//		LinkedList<GTruth> gTruth= locationListProc.get_GTruth(queryID, docs_scores_matches);
//		if (showGTMatches!=null && GTruth.isExistGTruth(gTruth)) {
//			showGTMatches.addOneQuerySelRanks(queryID, temp.ObjArr, GTruth.getRanks(gTruth));
//		}
//		//calculate geoReduncy and saveInto gTruth
//		int geoReduncy=locationListProc.getGeoReduncy(queryID);//geo-neighbor num
//		//make locations
//		ArrayList<LocDocs<DID_Score_ImageRegionMatch_ShortArr>> res_locationDocMatches=locationListProc.get_topLocationDocList(docs_scores_matches);
//		//ini showLocMatches
//		if (showLocMatches!=null) {
//			showLocMatches.iniForOneQuery(queryID, new LatLon(latlons, queryID));
//		}
//		//rerank locations
//		LinkedList<LocDocs<DID_Score>> res=rerank_locations_byMatches(queryID, res_locationDocMatches);
//		//find rank of the first true math
//		int[] fistMatch = locationListProc.getFirstTrueMatch(queryID, res);
//		//**output
//		fistMatch_GTruth_Docs_Locations outValue=new fistMatch_GTruth_Docs_Locations(fistMatch,GTruth.getIntArrFormat(gTruth),res,geoReduncy);
//		if(disp==true){
//			System.out.println("QueryName:"+queryID+", visual rank-length:"+totRank_length);
//			System.out.println("outValue:"+outValue);
//			disp=false;
//		}
//		return outValue;
//	}
//	
//	private LinkedList<LocDocs<DID_Score>> rerank_locations_byMatches(int queryID, ArrayList<LocDocs<DID_Score_ImageRegionMatch_ShortArr>> locationDocMatches) throws InterruptedException, IOException{
//		//score locations
//		vari.scoreLocs(queryID, locationDocMatches);
//		//rank locations
//		ArrayList<Integer> location_inds_top=new ArrayList<Integer>(); ArrayList<Float> location_Scores_top=new ArrayList<Float>();
//		General_IR.rank_get_TopDocScores_PriorityQueue(vari.location_inds, vari.location_Scores, num_topLocations, location_inds_top, location_Scores_top, "DES", true, true);
//		//output
//		LinkedList<LocDocs<DID_Score>> res=new LinkedList<LocDocs<DID_Score>>();
//		for (Integer locInd : location_inds_top) {
//			//add doc, loc
//			ArrayList<DID_Score> thisLocDocScores=new ArrayList<DID_Score>();
//			for (DID_Score_ImageRegionMatch_ShortArr oneDoc : locationDocMatches.get(locInd).docs) {
//				thisLocDocScores.add(oneDoc.dID_score);
//			}
//			res.add(new LocDocs<DID_Score>(thisLocDocScores,locationDocMatches.get(locInd).latlon));
//			//add matches
//			if (showLocMatches!=null) {
//				showLocMatches.addOneGroup(vari.getOneLocMatches(locInd));
//			}
//		}
//		return res;
//	}
//
//	public static void main(String[] args) throws InterruptedException, IOException {
//		//SanFrancisco_StreetView
////		String basePath="O:/SanFrancisco_StreetView/";
////		process("Q:/PhotoDataBase/SanFrancisco_StreetView/SanFrancisco_Q-DPCI_latlons.floatArr", 
////				"N:/ewi/insy/MMC/XinchaoLi/PhotoDataBase/SanFrancisco_StreetView/SanFrancisco_inSInd_MFiles/", 
////				basePath+"SURFfeat/SURFFeat_VW20k_MA_SanFran_Q/", 
////				basePath+"SURFfeat/SURFFeat_VW20k_SA_SanFran_DPCI/", 
////				basePath+"VisualRank/R_SanFran_20K-VW_SURF_iniR-BurstIntraInter_HDs20-HMW12_ReR1K_HDr20_top1K_1vs1AndHistAndAngle@0.52@0.2@0@0@0@0@0@0@0_rankDocMatches");
//		
//		//MediaEval13
//		process("O:/MediaEval13/MEval13_latlons.floatArr", 
//				"N:/ewi/insy/MMC/XinchaoLi/Photos_MEva13_9M_MapFiles/", 
//				"Q:/SURFFeat_VW20k_MA_ME13_Q/", 
//				"Q:/SURFFeat_VW20k_SA_ME13_D/", 
//				"O:/GVM/VisualRank/0Q0_MEva13_9M_20K-VW_SURF_iniR-BurstIntraInter_HDs20-HMW12_ReR1K_HDr20_top1K_1vs1AndHistAndAngle@0.52@0.2@1@0@0@0@0@0@0@0_rankDocMatches");
//
//		
//	}
//	public static void process(String latlon, String photos, String Q_feat, String D_feat, String iniRank) throws InterruptedException, IOException {
//		float[][] latlons=(float[][]) General.readObject(latlon);
//		float geoExpanSca=0.01f; int reRankScale=100; int topVisScale=1000; float G_ForGTSize=0.01f;
//		int num_topLocations=3; int maxNumPerLoc=10; int minGTSize=1; int maxGTSize=Integer.MAX_VALUE; int minGeoDensity=0;
//		int RGBInd=0; int pointEnlargeFactor=5;
//		ImageDataManager imageDataManager_Q=new ImageDataManager("MapFile", 100*1000, null, 
//				new String[]{photos}, 
//				100*1000, new String[]{Q_feat}, null);
//		ImageDataManager imageDataManager_D=new ImageDataManager("MapFile", 100*1000, null, 
//				new String[]{photos}, 
//				100*1000, new String[]{D_feat}, null);
//		//set GVM
//		String[] schemes={"_VisNN","_GVM-blockInd@50@10"}; PatternBoolean patternCorrect=new PatternBoolean(new String[]{"true","neutral"});//neutral, true, false
//		ShowMatches_rank showGTMatches=new ShowMatches_rank(true, imageDataManager_Q, imageDataManager_D, RGBInd, pointEnlargeFactor);
//		ArrayList<ShowLocMatches> showLocMatches=new ArrayList<>(schemes.length); ArrayList<GVM_old> GVMs=new ArrayList<>(schemes.length);
//		for (int scheme_i = 0; scheme_i < schemes.length; scheme_i++) {
//			String scheme = schemes[scheme_i];
//			String locListLabel=scheme.equalsIgnoreCase("_VisNN")?LocationListProc.setLocListParams(0f, 100, 100, false, false):LocationListProc.setLocListParams(geoExpanSca, reRankScale, topVisScale, false, false);
//			ShowLocMatches showLocMatche=new ShowLocMatches(maxNumPerLoc, true, imageDataManager_Q, imageDataManager_D, RGBInd, pointEnlargeFactor);
//			GVM_old proc_GVM=new GVM_old(scheme+locListLabel, null, latlons, imageDataManager_Q, num_topLocations, scheme_i==0?showGTMatches:null, showLocMatche, G_ForGTSize, 100);
//			showLocMatches.add(showLocMatche);
//			GVMs.add(proc_GVM);
//		}
//		//run
//		MapFile.Reader rankReader=new MapFile.Reader(new Path(iniRank), new Configuration());
//		IntWritable Key_queryName=new IntWritable();
//		DID_Score_ImageRegionMatch_ShortArr_Arr Value_RankScores= new DID_Score_ImageRegionMatch_ShortArr_Arr();
//		int query_i=0; int partenQ=0; int[] correctQ=new int[schemes.length]; int dispInter=10; LinkedList<Integer> partenQs=new LinkedList<>();
//		while (rankReader.next(Key_queryName, Value_RankScores)) {
//			boolean[] corPattern=new boolean[schemes.length]; fistMatch_GTruth_Docs_Locations res=null;
//			for (int scheme_i = 0; scheme_i < GVMs.size(); scheme_i++) {//process Q for different schemes
//				res=GVMs.get(scheme_i).ProcessOneQuery(Key_queryName.get(),Value_RankScores,false);
//				boolean isCorrect_GVM=(res.fistMatch.getIntArr()[0]==0);
//				if (isCorrect_GVM) {
//					correctQ[scheme_i]++;
//					corPattern[scheme_i]=true;
//				}
//			}
//			//show Q
////			if(patternCorrect.isSamePattern(corPattern) && res.getGTSize()>=minGTSize && res.getGTSize()<=maxGTSize && res.geoDensity>=minGeoDensity){
//			if(query_i== 1065 || query_i== 3777 || query_i== 4746){
//				showGTMatches.disp();
//				for (ShowLocMatches showLocMatch : showLocMatches) {
//					showLocMatch.disp();
//				}
//				partenQ++;
//				partenQs.add(query_i);
//			}
//			//disp
//			query_i++;
//			General.dispInfo_ifNeed(query_i%dispInter==0, "\n ", query_i+" quries, correctQ for shemes:"+General.StrArrToStr(schemes, ",")+" is "+General.IntArrToString(correctQ, "_")
//					+", for partens:"+patternCorrect.getTargetPattern()+", partenQ:"+partenQ);
//		}
//		rankReader.close();
//		System.out.println("done! tot "+query_i+" quries, correctQ for shemes:"+General.StrArrToStr(schemes, ",")+" is "+General.IntArrToString(correctQ, "_")
//				+", for partens:"+patternCorrect.getTargetPattern()+", partenQ:"+partenQ);
//		System.out.println("partenQs: "+partenQs);
//		for (GVM_old one : GVMs) {
//			one.vari.dispQueryMatchSpeard(Key_queryName.get());
//		}
//	}

}
