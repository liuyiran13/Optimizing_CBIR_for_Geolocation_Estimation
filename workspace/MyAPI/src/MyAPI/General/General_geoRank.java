package MyAPI.General;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;

import MyAPI.General.myComparator.ValueComparator_Float_DES;
import MyAPI.General.myComparator.ValueComparator_MasterSlave_Float_DES;
import MyAPI.Geo.groupDocs.I_CartoID;
import MyAPI.Geo.groupDocs.I_LatLon;
import MyAPI.Interface.DID;
import MyCustomedHaoop.ValueClass.DID_Score;
import MyCustomedHaoop.ValueClass.IntArr;
import MyCustomedHaoop.ValueClass.IntList_FloatList;
import MyCustomedHaoop.ValueClass.FloatArr;


public class General_geoRank {
	
	public static int get_trueLocRank(int queryName, List<Integer> topLocationDocs, int topDoc, float isSameLocScale, float[][] latlons){
		int trueLocRank=-1;//rank from 0 !! noExistRank is -1
		int rank=0;
		for(int doc:topLocationDocs){
			if (rank<topDoc) {
				if(isOneLocation_GreatCircle(latlons[0][queryName],latlons[1][queryName],latlons[0][doc],latlons[1][doc],isSameLocScale)){
					trueLocRank=rank;
					break;
				}
			}else {
				break;
			}
			rank++;
		}
		return trueLocRank;
	}
	
	public static int get_trueLocRank(int queryName, int[] topLocationDocs, float isSameLocScale, float[][] latlons){
		int trueLocRank=-1;
		for(int i=0;i<topLocationDocs.length;i++){
			int doc=topLocationDocs[i];
			if(isOneLocation_GreatCircle(latlons[0][queryName],latlons[1][queryName],latlons[0][doc],latlons[1][doc],isSameLocScale)){
				trueLocRank=i; //rank from 0 !!
				break;
			}
		}
		return trueLocRank;
	}
	
	public static int get_trueLocRank_fromList(int queryName, ArrayList<ArrayList<Integer>> LocList, float isSameLocScale, float[][] latlons){
		//photoName from 1!!
		int trueLocRank=-1;
		for(int i=0;i<LocList.size();i++){
			for(int doc:LocList.get(i)){
				if(isOneLocation_GreatCircle(latlons[0][queryName],latlons[1][queryName],latlons[0][doc],latlons[1][doc],isSameLocScale)){
					trueLocRank=i; //rank from 0 !!
					return trueLocRank;
				}
			}
		}
		return trueLocRank;
	}
	
	public static int get_trueLocRank(float[][] topLocs, int queryName, int topLoc, float isSameLocScale, float[][] latlons){
		int trueLocRank=-1;
		for(int i=0;i<Math.min(topLocs.length, topLoc);i++){
			float[] latlon=topLocs[i];
			if(isOneLocation_GreatCircle(latlons[0][queryName],latlons[1][queryName],(float)latlon[0],(float)latlon[1],isSameLocScale)){
				trueLocRank=i; //rank from 0 !!
				break;
			}
		}
		return trueLocRank;
	}
	
	public static int get_trueLocRank(List<float[]> topLocs, int queryName, float isSameLocScale, float[][] latlons, int topLoc){
		int trueLocRank=-1;
		for(int i=0;i<Math.min(topLocs.size(), topLoc);i++){
			float[] latlon=topLocs.get(i);
			if(isOneLocation_GreatCircle(latlons[0][queryName],latlons[1][queryName],(float)latlon[0],(float)latlon[1],isSameLocScale)){
				trueLocRank=i; //rank from 0 !!
				break;
			}
		}
		return trueLocRank;
	}
	
	public static <T extends I_LatLon> int get_trueLocRankG(List<T> topLocs, int queryName, float isSameLocScale, float[][] latlons, int topLoc){
		int trueLocRank=-1;
		for(int i=0;i<Math.min(topLocs.size(), topLoc);i++){
			T latlon=topLocs.get(i);
			if(isOneLocation_GreatCircle(latlons[0][queryName],latlons[1][queryName],(float)latlon.getLatLon().lat,(float)latlon.getLatLon().lon,isSameLocScale)){
				trueLocRank=i; //rank from 0 !!
				break;
			}
		}
		return trueLocRank;
	}
	
	public static <T extends I_CartoID> int get_trueCartoRankG(List<T> topGroups, int queryName, HashSet<Integer>[] cartoIDs_Q, int topGroup){
		int trueLocRank=-1;
		for(int i=0;i<Math.min(topGroups.size(), topGroup);i++){
			T one=topGroups.get(i);
			if(cartoIDs_Q[queryName].contains(one.getCartoID())){
				trueLocRank=i; //rank from 0 !!
				break;
			}
		}
		return trueLocRank;
	}
	
	public static ArrayList<Float> transferDocScore(List<Float> docScores, Boolean isUseVisSim, Boolean isUseRankingScore, Boolean isUseNum){
		//transfer docScore
		ArrayList<Float> docScores_needed=new ArrayList<Float>(docScores.size());
		if (isUseVisSim) {//use visual score
			docScores_needed=new ArrayList<Float>(docScores);
			if ((docScores_needed.get(docScores_needed.size()-1)-docScores_needed.get(0))>0) {//score is dist, so after rank, last one is biggest
				float scaleFactor=50;
				General_IR.transferDist_to_Sim(docScores_needed,scaleFactor );
			}
		}else if (isUseRankingScore) {//use rank score
			for (int i = 0; i < docScores.size(); i++) {
				docScores_needed.add(General_IR.rankingScore_log(i+1));
			}
		}else if (isUseNum) {//use number, each doc same weight
			for (int i = 0; i < docScores.size(); i++) {
				docScores_needed.add((float) 1);
			}
		}
		return docScores_needed;
	}
	
	public static int get_GVSize(int queryName, float GVScale, List<Integer> visNeighbors, float[][] latlons){
		int GVSize=0;
		for(int visNeig:visNeighbors){
			if(isOneLocation_GreatCircle(latlons[0][queryName],latlons[1][queryName],latlons[0][visNeig],latlons[1][visNeig],GVScale)){
				GVSize++;
			}
		}
		return GVSize;
	}
	
	public static ArrayList<int[]> get_GTruth(int queryName, float GVScale, List<Integer> visNeighbors, float[][] latlons){
		ArrayList<int[]> gTruth=new ArrayList<int[]>();
		for (int i = 0; i < visNeighbors.size(); i++) {
			int visNeig=visNeighbors.get(i);
			if(isOneLocation_GreatCircle(latlons[0][queryName],latlons[1][queryName],latlons[0][visNeig],latlons[1][visNeig],GVScale)){
				int[] rank_photoName={i,visNeig};
				gTruth.add(rank_photoName);
			}
		}
		return gTruth;
	}
		
	public static ArrayList<Integer> get_topLocationDocs(int num_topLocationDocs, int[] topDocs, float isOneLocScale, float[][] latlons){
		ArrayList<Integer> topLocationDocs=new ArrayList<Integer>(num_topLocationDocs); 
		for(int doc: topDocs){
			if(!isLocExist(doc,topLocationDocs,latlons,isOneLocScale)){
				topLocationDocs.add(doc);
			}
			if(topLocationDocs.size()==num_topLocationDocs)
				break;
		}
		return topLocationDocs;
	}
	
	public static void get_conSimHist(int num_topDocs, int queryName, MapFile.Reader MapFileReader_concept, ArrayList<Integer> topDocs, float isOneLocScale, float[][] latlons, int[][] hist_TrueMatch, int[][] hist_ErrMatch, int[] hist_TrueMatch_allRank, int[] hist_ErrMatch_allRank, float[] bins, int[] selectedConcept) throws IOException, InterruptedException{
		General.Assert(hist_TrueMatch.length==num_topDocs, "error in get_conSimHist, hist_TrueMatch.length should equal to num_topDocs!! hist_TrueMatch.length:"+hist_TrueMatch.length+", num_topDocs:"+num_topDocs);
		General.Assert(hist_ErrMatch.length==num_topDocs, "error in get_conSimHist, hist_ErrMatch.length should equal to num_topDocs!! hist_ErrMatch.length:"+hist_ErrMatch.length+", num_topDocs:"+num_topDocs);
		FloatArr cs_query= new FloatArr();
		MapFileReader_concept.get(new IntWritable(queryName), cs_query);
		boolean queryExistCS=(cs_query.getFloatArr()!=null);
		float[] cs_query_arr=null;
		if (queryExistCS) {
			cs_query_arr=cs_query.getFloatArr();
			if (selectedConcept!=null) {//select concept
				cs_query_arr=General.selectArrFloat(cs_query.getFloatArr(),selectedConcept,0);
			}
		}
		if (queryExistCS) {//query has concept score
			for (int i = 0; i < Math.min(num_topDocs, topDocs.size()); i++) {
				float conSim=conceptSim( MapFileReader_concept, cs_query_arr,topDocs.get(i),selectedConcept);
				if (conSim!=0) {//doc has concept score
					int binInd=General.getBinInd_linear(bins, conSim);
					if(isOneLocation_approximate(latlons[0][queryName],latlons[1][queryName],latlons[0][topDocs.get(i)],latlons[1][topDocs.get(i)],isOneLocScale)){//true match
						hist_TrueMatch[i][binInd]++;
						hist_TrueMatch_allRank[binInd]++;
					}else {// error match
						hist_ErrMatch[i][binInd]++;
						hist_ErrMatch_allRank[binInd]++;
					}
				}
			}
		}
	}
	
	public static void get_conSimHist(int num_topDocs, int queryName, MapFile.Reader MapFileReader_concept, ArrayList<Integer> topDocs, float isOneLocScale, float[][] latlons, int[] hist_TrueMatch_allRank, int[] hist_ErrMatch_allRank, float[] bins, int[] selectedConcept) throws IOException, InterruptedException{
		FloatArr cs_query= new FloatArr();
		MapFileReader_concept.get(new IntWritable(queryName), cs_query);
		boolean queryExistCS=(cs_query.getFloatArr()!=null);
		float[] cs_query_arr=null;
		if (queryExistCS) {
			cs_query_arr=cs_query.getFloatArr();
			if (selectedConcept!=null) {//select concept
				cs_query_arr=General.selectArrFloat(cs_query.getFloatArr(),selectedConcept,0);
			}
		}
		if (queryExistCS) {//query has concept score
			for (int i = 0; i < Math.min(num_topDocs, topDocs.size()); i++) {
				float conSim=conceptSim( MapFileReader_concept, cs_query_arr,topDocs.get(i),selectedConcept);
				if (conSim!=0) {//doc has concept score
					int binInd=General.getBinInd_linear(bins, conSim);
					if(isOneLocation_approximate(latlons[0][queryName],latlons[1][queryName],latlons[0][topDocs.get(i)],latlons[1][topDocs.get(i)],isOneLocScale)){//true match
						hist_TrueMatch_allRank[binInd]++;
					}else {// error match
						hist_ErrMatch_allRank[binInd]++;
					}
				}
			}
		}
	}
	
	public static void get_conDominateHist(int num_topDocs, int queryName, MapFile.Reader MapFileReader_concept, ArrayList<Integer> topDocs, float isOneLocScale, float[][] latlons, int[] hist_TrueMatch_allRank, int[] hist_ErrMatch_allRank) throws IOException{
		for (int i = 0; i < Math.min(num_topDocs, topDocs.size()); i++) {
			int doc=topDocs.get(i);
			FloatArr cs_doc= new FloatArr();
			MapFileReader_concept.get(new IntWritable(doc), cs_doc);
			if (cs_doc.getFloatArr()!=null) {
				int dominateCon = General.getMax_ind(cs_doc.getFloatArr());
				if(isOneLocation_approximate(latlons[0][queryName],latlons[1][queryName],latlons[0][doc],latlons[1][doc],isOneLocScale)){//true match
					hist_TrueMatch_allRank[dominateCon]++;
				}else {// error match
					hist_ErrMatch_allRank[dominateCon]++;
				}
			}
		}
	}
	
	public static void get_conRankHist(int num_topDocs, int queryName, MapFile.Reader MapFileReader_concept, ArrayList<Integer> topDocs, float isOneLocScale, float[][] latlons, int[] hist_TrueMatch, int[] hist_ErrMatch, int[] bins, int[] selectedConcept) throws IOException, InterruptedException{
		FloatArr cs_query= new FloatArr();
		MapFileReader_concept.get(new IntWritable(queryName), cs_query);
		boolean queryExistCS=(cs_query.getFloatArr()!=null);
		float[] cs_query_arr=null;
		if (queryExistCS) {
			cs_query_arr=cs_query.getFloatArr();
			if (selectedConcept!=null) {//select concept
				cs_query_arr=General.selectArrFloat(cs_query.getFloatArr(),selectedConcept,0);
			}
		}
		if (queryExistCS) {//query has concept score
			//rank top-visul photo based on concept sim
			HashMap<Integer, Float> visRank_conSim=new HashMap<Integer, Float>();
			for (int i = 0; i < Math.min(num_topDocs, topDocs.size()); i++) {
				float conSim=conceptSim( MapFileReader_concept, cs_query_arr,topDocs.get(i),selectedConcept);
				visRank_conSim.put(i, conSim);
			}
			//***** sort visRank_conSim *********
			ValueComparator_Float_DES mvCompartor = new ValueComparator_Float_DES(visRank_conSim);
			TreeMap<Integer,Float> visRank_conSim_Des = new TreeMap<Integer,Float>(mvCompartor);
			visRank_conSim_Des.putAll(visRank_conSim);
			ArrayList<Integer> conRank_visRanks=new ArrayList<Integer>(visRank_conSim_Des.keySet());
			for (int conRank = 0; conRank < conRank_visRanks.size(); conRank++) {
				int binInd=General.getBinInd_linear(bins, conRank);
				int visRank=conRank_visRanks.get(conRank);
				if(isOneLocation_approximate(latlons[0][queryName],latlons[1][queryName],latlons[0][topDocs.get(visRank)],latlons[1][topDocs.get(visRank)],isOneLocScale)){//true match
					hist_TrueMatch[binInd]++;
				}else {// error match
					hist_ErrMatch[binInd]++;
				}
			}
		}
	}
	
	public static int[] get_distinctConcept(int queryName, MapFile.Reader MapFileReader_concept, float Thr) throws IOException{
		if (Thr<1) {
			FloatArr cs_query= new FloatArr();
			MapFileReader_concept.get(new IntWritable(queryName), cs_query);
			boolean queryExistCS=(cs_query.getFloatArr()!=null);
			float[] cs_query_arr=null;
			int[] selectedConcept=null;
			if (queryExistCS) {
				cs_query_arr=cs_query.getFloatArr();
				//select distinctConcept
				ArrayList<Integer> selCons=new ArrayList<Integer>();
				for (int i = 0; i < cs_query_arr.length; i++) {
					if (cs_query_arr[i]>Thr) {
						selCons.add(i);
					}
				}
				if (selCons.size()!=0) {//exist good concept
					selectedConcept=General.ListToIntArr(selCons);
				}
			}
			return selectedConcept;
		}else { // =1, no selection
			return null;
		}
		
		
	}
	
	public static void get_topLocationDocsList(ArrayList<ArrayList<DID_Score>> topLocationDocsList, ArrayList<float[]> topLocs, 
			int num_topLocations, ArrayList<Integer> topDocs, ArrayList<Float> topScores, float isOneLocScale, float[][] latlons, 
			boolean is1U1P, long[] userIDs_0, int[] userIDs_1) throws InterruptedException{
		topLocationDocsList.clear(); topLocs.clear();
		ArrayList<HashSet<Long>> topLocationUsersList_0=new ArrayList<HashSet<Long>>(); ArrayList<HashSet<Integer>> topLocationUsersList_1=new ArrayList<HashSet<Integer>>();		
		for(int di=0; di<topDocs.size(); di++){
			int doc=topDocs.get(di);
			float score=topScores.get(di);
			boolean docAdded=false;
			for(int i=0; i<topLocs.size(); i++){
				float[] oneLoc=topLocs.get(i);
				if(isOneLocation_approximate(oneLoc[0],oneLoc[1],latlons[0][doc],latlons[1][doc],isOneLocScale)){//this doc is assigned to this loc
					//check, 1 user only contribute 1 doc in one location
					if((!is1U1P) || topLocationUsersList_0.get(i).add(userIDs_0[doc])==true || topLocationUsersList_1.get(i).add(userIDs_1[doc])==true){//this user is not exist, add!
						//add this doc to this loc
						topLocationDocsList.get(i).add(new DID_Score(doc,score));
						//update center-loc
						oneLoc=findCenterLoc(topLocationDocsList.get(i), latlons);
					}
					docAdded=true;
				}
			}
			if(docAdded==false && topLocs.size()<num_topLocations){
				//add doc
				ArrayList<DID_Score> temp=new ArrayList<DID_Score>(); 
				temp.add(new DID_Score(doc,score));
				topLocationDocsList.add(temp);
				//add loc
				topLocs.add(new float[]{latlons[0][doc],latlons[1][doc]});
				//add user
				if (is1U1P) {
					HashSet<Long> users_0=new HashSet<Long>(); HashSet<Integer> users_1=new HashSet<Integer>();
					users_0.add(userIDs_0[doc]); users_1.add(userIDs_1[doc]);
					topLocationUsersList_0.add(users_0); topLocationUsersList_1.add(users_1);
				}
			}
		}
	}
	
	public static void get_topLocationDocsList_tunePhoNumPerLoc(ArrayList<ArrayList<DID_Score>> topLocationDocsList, ArrayList<float[]> topLocs, 
			int num_topLocations, ArrayList<Integer> topDocs, ArrayList<Float> topScores, float isOneLocScale, int PhoNumPerLoc, float[][] latlons, 
			boolean is1U1P, long[] userIDs_0, int[] userIDs_1) throws InterruptedException{
		topLocationDocsList.clear(); topLocs.clear();
		ArrayList<HashSet<Long>> topLocationUsersList_0=new ArrayList<HashSet<Long>>(); ArrayList<HashSet<Integer>> topLocationUsersList_1=new ArrayList<HashSet<Integer>>();		
		for(int di=0; di<topDocs.size(); di++){
			int doc=topDocs.get(di);
			float score=topScores.get(di);
			boolean docAdded=false;
			for(int i=0; i<topLocs.size(); i++){
				float[] oneLoc=topLocs.get(i);
				if(isOneLocation_approximate(oneLoc[0],oneLoc[1],latlons[0][doc],latlons[1][doc],isOneLocScale)){//this doc is assigned to this loc
					if(topLocationDocsList.get(i).size()<PhoNumPerLoc){//control PhoNumPerLoc
						//check, 1 user only contribute 1 doc in one location
						if((!is1U1P) || topLocationUsersList_0.get(i).add(userIDs_0[doc])==true || topLocationUsersList_1.get(i).add(userIDs_1[doc])==true){//this user is not exist, add!
							//add this doc to this loc
							topLocationDocsList.get(i).add(new DID_Score(doc,score));
							//update center-loc
							oneLoc=findCenterLoc(topLocationDocsList.get(i), latlons);
						}
					}
					docAdded=true;
				}
			}
			if(docAdded==false && topLocs.size()<num_topLocations){
				//add doc
				ArrayList<DID_Score> temp=new ArrayList<DID_Score>(); 
				temp.add(new DID_Score(doc,score));
				topLocationDocsList.add(temp);
				//add loc
				topLocs.add(new float[]{latlons[0][doc],latlons[1][doc]});
				//add user
				if (is1U1P) {
					HashSet<Long> users_0=new HashSet<Long>(); HashSet<Integer> users_1=new HashSet<Integer>();
					users_0.add(userIDs_0[doc]); users_1.add(userIDs_1[doc]);
					topLocationUsersList_0.add(users_0); topLocationUsersList_1.add(users_1);
				}
			}
		}
	}
	
	public static float[][] LocDocList_to_Locs(ArrayList<ArrayList<DID_Score>> LocDocList, float[][] latlons) throws InterruptedException{
		int locNum=LocDocList.size();
		float[][] Locs= new float[locNum][2];
		for (int i = 0; i < locNum; i++) {
			Locs[i]=findCenterLoc(LocDocList.get(i),latlons);
		}
		return Locs;
	}
	
	public static <T extends DID> float[] findCenterLoc(List<T> Docs, float[][] latlons) throws InterruptedException{
		int docNum=Docs.size();
		double[][] Cartesians=new double[docNum][3];
		for (int i = 0; i < docNum; i++) {
			Cartesians[i]=General.latlonToCartesian(latlons[0][Docs.get(i).getDID()],latlons[1][Docs.get(i).getDID()]);
		}
		double[] colSum=General.sumRowCol(Cartesians,1)[1];
		double[] center_Cartesian=new double[colSum.length];
		for (int i = 0; i < colSum.length; i++) {
			center_Cartesian[i]=colSum[i]/docNum;
		}
		float[] center_latlon=General.CartesianTolatlon(center_Cartesian);
		
		return center_latlon;
	}
	
	public static ArrayList<Integer> findGeoNeighbors(int queryName, float isSameLocScale, float[][] latlons){
		ArrayList<Integer> geoNeighbors=new ArrayList<Integer>();
		for (int i = 0; i < latlons[0].length; i++) {
			if (isOneLocation_approximate(latlons[0][queryName],latlons[1][queryName],latlons[0][i],latlons[1][i],isSameLocScale)) {
				geoNeighbors.add(i);
			}
		}
		return geoNeighbors;
	}
	
	public static ArrayList<Integer> get_topLocationDocs(int num_topLocationDocs, int[] topDocs, float isOneLocScale, float[][] latlons, ArrayList<Integer> topLocationIndex){
		ArrayList<Integer> topLocationDocs=new ArrayList<Integer>(num_topLocationDocs); 
		for(int i=0;i<topDocs.length;i++){
			int doc=topDocs[i];
			if(!isLocExist(doc,topLocationDocs,latlons,isOneLocScale)){
				topLocationDocs.add(doc);
				topLocationIndex.add(i); //topLocationDocs's index in topDocs
			}
			if(topLocationDocs.size()==num_topLocationDocs)
				break;
		}
		return topLocationDocs;
	}
	
	public static float[] get_topLocScoreNormalized(float[] docScores, ArrayList<Integer> topLocationIndex, int topN){
		Float[] topLocScore=new Float[Math.max(topN,topLocationIndex.size())];
		//initialize topLocScore
		for(int i=0;i<topLocScore.length;i++)
			topLocScore[i]=(float) 0;
		//assign 
		for(int i=0;i<topLocationIndex.size();i++){
			topLocScore[i]=docScores[topLocationIndex.get(i)];
		}
		return General.normliseArr(topLocScore,-1);
	}
	
	public static float[] get_topLocGVSizeNormalized(int[] docGVSizes, ArrayList<Integer> topLocationIndex, int topN){
		Integer[] topLocScore=new Integer[Math.max(topN,topLocationIndex.size())];
		//initialize topLocScore
		for(int i=0;i<topLocScore.length;i++)
			topLocScore[i]=0;
		//assign 
		for(int i=0;i<topLocationIndex.size();i++){
			topLocScore[i]=docGVSizes[topLocationIndex.get(i)];
		}
		return General.normliseArr(topLocScore,-1);
	}
	
	public static float[] get_topLocScoreAccumPerc(float[] docScores, ArrayList<Integer> topLocationIndex){
		Float[] topLocScore=new Float[topLocationIndex.size()];
		for(int i=0;i<topLocScore.length;i++){
			topLocScore[i]=docScores[topLocationIndex.get(i)];
		}
		return General.mkAccumPercent(topLocScore);
	}
	
	public static ArrayList<float[]> get_topLocations(int num_topLocationDocs, float isOneLocScale, ArrayList<float[]> possibleLocs){
		ArrayList<float[]> topLocations=new ArrayList<float[]>(num_topLocationDocs);  
		for(float[] latlon: possibleLocs){
			if(!isLocExist_fromLocs(latlon,topLocations,isOneLocScale)){
				topLocations.add(latlon);
			}
			if(topLocations.size()==num_topLocationDocs)
				break;
		}
		return topLocations;
	}
	
	public static boolean isLocExist_fromLocs(float[] latlon, ArrayList<float[]> topLocations,float isOneLocScale){
		boolean isExist=false;
		for(float[] oneLoc:topLocations){
			if(isOneLocation_approximate(oneLoc[0],oneLoc[1],latlon[0],latlon[1],isOneLocScale)){
				isExist=true;
				break;
			}
		}
		return isExist;
	}
	
	public static boolean isLocExist(int docName,ArrayList<Integer> topLocationDocs,float[][] latlons,float isOneLocScale){
		boolean isExist=false;
		for(int locDoc:topLocationDocs){
			if(isOneLocation_approximate(latlons[0][docName],latlons[1][docName],latlons[0][locDoc],latlons[1][locDoc],isOneLocScale)){
				isExist=true;
				break;
			}
		}
		return isExist;
	}
	
	public static boolean isOneLocation_approximate(float lat1,float lon1,float lat2,float lon2, float isOneLocScale){
		float latABS=Math.abs(lat1-lat2);
		float lonABS=Math.abs(lon1-lon2);
		boolean isNeighbor=false;
		if(latABS<isOneLocScale && lonABS<isOneLocScale){// is neighbor 
			isNeighbor=true;
		}
		return isNeighbor;
	}
	
	public static boolean isOneLocation_GreatCircle(float lat1,float lon1,float lat2,float lon2, float isOneLocDisInKm){
		boolean isNeighbor=false;
		if (General.calculateGeoDistance(lat1, lon1, lat2, lon2, "GreatCircle")<isOneLocDisInKm) {
			isNeighbor=true;
		}
		return isNeighbor;
	}
	
	public static boolean isNeighbor_squre(float lat1,float lon1,float lat2,float lon2, float isOneLocScale){
		float latABS=Math.abs(lat1-lat2);
		float lonABS=Math.abs(lon1-lon2);
		boolean isNeighbor=false;
		if(latABS<isOneLocScale && lonABS<isOneLocScale){// is neighbor 
			isNeighbor=true;
		}
		return isNeighbor;
	}
	
	public static void rerank_locations_byScore(ArrayList<ArrayList<DID_Score>> LocationDocs, ArrayList<float[]> Locations, int num_topLocations, ArrayList<ArrayList<DID_Score>> topLocationDocs, ArrayList<float[]> topLocations, boolean isNorm) throws InterruptedException{
		//rerank location by each location summed doc scores
		ArrayList<Integer> location_inds=new ArrayList<Integer>(); ArrayList<Float> location_Scores=new ArrayList<Float>();
		for (int i = 0; i < LocationDocs.size(); i++) {
			location_inds.add(i);
			float thisLocScore=0;
			for (DID_Score oneDoc : LocationDocs.get(i)) {
				thisLocScore+=oneDoc.score;
			}
			location_Scores.add(isNorm?thisLocScore/LocationDocs.get(i).size():thisLocScore);
		}
		//rank locations
		ArrayList<Integer> location_inds_top=new ArrayList<Integer>(); ArrayList<Float> location_Scores_top=new ArrayList<Float>();
		General_IR.rank_get_TopDocScores_PriorityQueue(location_inds, location_Scores, num_topLocations, location_inds_top, location_Scores_top, "DES", true, true);
		//output
		for (Integer locInd : location_inds_top) {
			topLocationDocs.add(LocationDocs.get(locInd));
			topLocations.add(Locations.get(locInd));
		}
	}
	
	
	
	
	public static ArrayList<Integer> rerank_geoExpansion_neigOnly(ArrayList<Integer> oriRanks, ArrayList<Float> oriScores, HashMap<Integer,Float> topDocVisualScores, float[] lats, float[] lons, float geoExpanScale) throws IOException{
		//*** caculate docs geoNeighbour's visual score *******//
		HashMap<Integer,Float> reR_doc_score = new HashMap<Integer,Float>();
		for (int i=0;i<oriRanks.size();i++){
			int doc=oriRanks.get(i);
			//compute GeoNeigbor visualScore
			float GeoNeigScore=0; 
			for(int VDoc:topDocVisualScores.keySet()){
				if(VDoc!=doc){
					float latABS=Math.abs(lats[VDoc]-lats[doc]);//photoName from 1!!
					float lonABS=Math.abs(lons[VDoc]-lons[doc]);
					if(latABS<geoExpanScale && lonABS<geoExpanScale){//VDoc is 1km neighbor of doc
						GeoNeigScore=GeoNeigScore+topDocVisualScores.get(VDoc);
					}
				}
			}
			//compute finial Score
			float finialScore=oriScores.get(i)+GeoNeigScore; //this photo's own visual score plus GeoNeigbor visualScore
			reR_doc_score.put(doc, finialScore);
		}
		//***** sort doc_neigNum *********
		ValueComparator_Float_DES mvCompartor = new ValueComparator_Float_DES(reR_doc_score);
		TreeMap<Integer,Float> reR_doc_score_Des = new TreeMap<Integer,Float>(mvCompartor);
		reR_doc_score_Des.putAll(reR_doc_score);
		return new ArrayList<Integer>(reR_doc_score_Des.keySet());
	}
	
	public static void selGoodRankDocs(ArrayList<Integer> oriRanks, ArrayList<Float> oriScores, HashSet<Integer> goodDocs){
		for (int i = 0; i < oriRanks.size(); i++) {
			if (!goodDocs.contains(oriRanks.get(i))) {
				oriRanks.remove(i);
				oriScores.remove(i);
				i--;
			}
		}
	}

	public static void rerank_geoExpansion_VisOnly_returnRankScore(ArrayList<Integer> oriRanks, ArrayList<Float> oriScores, HashMap<Integer,Float> topDocVisualScores, 
			int topDocNum, ArrayList<int[]> topDocs, ArrayList<Float> topScores, float[] lats, float[] lons, float geoExpanScale) throws InterruptedException{
		//return Rank and Score
		//*** caculate docs geoNeighbour's visual score *******//
		ArrayList<int[]> docs=new ArrayList<int[]>(); ArrayList<Float> scores=new ArrayList<Float>();
		for (int i=0;i<oriRanks.size();i++){
			int[] doc_GVSize=new int[2];
			int doc=oriRanks.get(i);
			//compute GeoNeigbor visualScore
			float GeoNeigScore=0;  int GVSize=0;
			for(int VDoc:topDocVisualScores.keySet()){
				if(VDoc!=doc){
					float latABS=Math.abs(lats[VDoc]-lats[doc]);//photoName from 0!!
					float lonABS=Math.abs(lons[VDoc]-lons[doc]);
					if(latABS<geoExpanScale && lonABS<geoExpanScale){//VDoc is geo-neighbor of doc
						GeoNeigScore+=topDocVisualScores.get(VDoc);
						GVSize++;
					}
				}
			}
//			//normalize GeoNeigScore
//			if(GeoNeigScore!=0)
//				GeoNeigScore/=GVSize;
			//compute finial Score
			float finialScore=oriScores.get(i)+GeoNeigScore; //this photo's own visual score plus GeoNeigbor visualScore
			doc_GVSize[0]=doc; doc_GVSize[1]=GVSize; 
			//make compareScores
			docs.add(doc_GVSize);
			scores.add(finialScore);
		}
		//***** sort doc_neigNum *********
		General_IR.rank_get_TopDocScores_PriorityQueue(docs, scores, topDocNum, topDocs, topScores, "DES", true, true);
	}
	
	public static TreeMap<Integer[],float[]> rerank_geoExpansion_ConceptOnly_returnRankScore
	(ArrayList<Integer> oriRanks, ArrayList<Float> oriScores, MapFile.Reader conceptMapFile, int[] selectedConcept, float[] lats, float[] lons, float geoExpanScale, int queryName, ArrayList<Integer> topVisDocs) throws IOException, InterruptedException{
		//return Rank and Score
		FloatArr cs_query= new FloatArr();
		conceptMapFile.get(new IntWritable(queryName), cs_query);
		boolean queryExistCS=(cs_query.getFloatArr()!=null);
		float[] cs_query_arr=null;
		if (queryExistCS) {
			cs_query_arr=cs_query.getFloatArr();
			if (selectedConcept!=null) {//select concept
				cs_query_arr=General.selectArrFloat(cs_query.getFloatArr(),selectedConcept,0);
			}
		}
		//*** caculate docs geoNeighbour's concept score *******//
		HashMap<Integer[],float[]> reR_doc_GVSize_score = new HashMap<Integer[],float[]>();
		for (int i=0;i<oriRanks.size();i++){
			Integer[] doc_GVSize=new Integer[2];
			int doc=oriRanks.get(i);
			//compute this doc's concept score
			float cSim_thisDoc=oriScores.get(i);
			if (queryExistCS) {
				cSim_thisDoc=conceptSim(conceptMapFile,  cs_query_arr,  doc, selectedConcept);
			}
			//for one doc compute GeoNeigbor concept-Score
			float GeoNeigScore=0;  int GVSize=0;
			if (queryExistCS) {
				for(int VDoc:topVisDocs){
					if(VDoc!=doc){
						float latABS=Math.abs(lats[VDoc]-lats[doc]);
						float lonABS=Math.abs(lons[VDoc]-lons[doc]);
						if(latABS<geoExpanScale && lonABS<geoExpanScale){//VDoc is 1km neighbor of doc
							GeoNeigScore+=conceptSim(conceptMapFile,  cs_query_arr,  doc, selectedConcept);
							GVSize++;
						}
					}
				}
			}
			
//			//normalize GeoNeigScore
//			if(GeoNeigScore!=0)
//				GeoNeigScore/=GVSize;
			//compute finial Score
			float finialScore=cSim_thisDoc+GeoNeigScore; //this photo's own visual score plus GeoNeigbor visualScore
			doc_GVSize[0]=doc; doc_GVSize[1]=GVSize;
			//make compareScores
			float[] compareScores=new float[2]; 
			compareScores[0]=finialScore; //mast value: gvScore
			compareScores[1]=0-i; //slave value: rank
			reR_doc_GVSize_score.put(doc_GVSize, compareScores);
		}
		//***** sort doc_neigNum *********
		ValueComparator_MasterSlave_Float_DES mvCompartor = new ValueComparator_MasterSlave_Float_DES(reR_doc_GVSize_score);
		TreeMap<Integer[],float[]> reR_doc_GVSize_score_Des = new TreeMap<Integer[],float[]>(mvCompartor);
		reR_doc_GVSize_score_Des.putAll(reR_doc_GVSize_score);
		return reR_doc_GVSize_score_Des;
	}
	
	public static TreeMap<Integer[],float[]> rerank_geoExpansion_VisConcept_returnRankScore
	(ArrayList<Integer> oriRanks, ArrayList<Float> oriScores, HashMap<Integer,Float> topDocVisualScores, MapFile.Reader conceptMapFile, int[] selectedConcept,  float[] lats, float[] lons, float geoExpanScale, int queryName, float normScore) throws IOException, InterruptedException{
		//return Rank and Score
		FloatArr cs_query= new FloatArr();
		conceptMapFile.get(new IntWritable(queryName), cs_query);
		boolean queryExistCS=(cs_query.getFloatArr()!=null);
		float[] cs_query_arr=null;
		if (queryExistCS) {
			cs_query_arr=cs_query.getFloatArr();
			if (selectedConcept!=null) {//select concept
				cs_query_arr=General.selectArrFloat(cs_query.getFloatArr(),selectedConcept,0);
			}
		}
		//*** caculate docs geoNeighbour's concept score *******//
		HashMap<Integer[],float[]> reR_doc_GVSize_score = new HashMap<Integer[],float[]>();
		for (int i=0;i<oriRanks.size();i++){
			Integer[] doc_GVSize=new Integer[2];
			int doc=oriRanks.get(i);
			//compute this doc's concept score
			float cSim_thisDoc=oriScores.get(i)/normScore;
			if (queryExistCS) {
				cSim_thisDoc+=conceptSim(conceptMapFile,  cs_query_arr,  doc,selectedConcept);
			}
			//for one doc compute GeoNeigbor concept-Score
			float GeoNeigScore=0;  int GVSize=0;
			for(int VDoc:topDocVisualScores.keySet()){
				if(VDoc!=doc){
					float latABS=Math.abs(lats[VDoc]-lats[doc]);
					float lonABS=Math.abs(lons[VDoc]-lons[doc]);
					if(latABS<geoExpanScale && lonABS<geoExpanScale){//VDoc is 1km neighbor of doc
						if (queryExistCS) {
							GeoNeigScore+=conceptSim(conceptMapFile,  cs_query_arr,  VDoc,selectedConcept);
						}
						GeoNeigScore+=topDocVisualScores.get(VDoc)/normScore;
						GVSize++;
					}
				}
			}
//			//normalize GeoNeigScore
//			if(GeoNeigScore!=0)
//				GeoNeigScore/=GVSize;
			//compute finial Score
			float finialScore=cSim_thisDoc+GeoNeigScore; //this photo's own visual score plus GeoNeigbor visualScore
			doc_GVSize[0]=doc; doc_GVSize[1]=GVSize;
			//make compareScores
			float[] compareScores=new float[2]; 
			compareScores[0]=finialScore; //mast value: gvScore
			compareScores[1]=0-i; //slave value: rank
			reR_doc_GVSize_score.put(doc_GVSize, compareScores);
		}
		//***** sort doc_neigNum *********
		ValueComparator_MasterSlave_Float_DES mvCompartor = new ValueComparator_MasterSlave_Float_DES(reR_doc_GVSize_score);
		TreeMap<Integer[],float[]> reR_doc_GVSize_score_Des = new TreeMap<Integer[],float[]>(mvCompartor);
		reR_doc_GVSize_score_Des.putAll(reR_doc_GVSize_score);
		return reR_doc_GVSize_score_Des;
	}
	
	public static void rerank_geoExpansion_VisConceptSimThre_returnRankScore
	(ArrayList<Integer> oriRanks, ArrayList<Float> oriScores, HashMap<Integer,Float> topDocVisualScores, MapFile.Reader conceptMapFile, int[] selectedConcept, float[] lats, float[] lons, float geoExpanScale, int queryName, float conSim_thr, float normScoreForVis) throws IOException, InterruptedException{
//		//return Rank and Score
//		
//		FloatArr cs_query= new FloatArr();
//		conceptMapFile.get(new IntWritable(queryName), cs_query);
//		boolean queryExistCS=(cs_query.getFloatArr()!=null);
//		
//		//filter out oriRanks, oriScores, topDocVisualScores
//		ArrayList<Integer> oriRanks_filtered=new ArrayList<Integer>(oriRanks.size());
//		ArrayList<Float> oriScores_filtered=new ArrayList<Float>(oriScores.size());
//		HashMap<Integer,Float> topDocVisualScores_filtered=new HashMap<Integer, Float>(topDocVisualScores.size());
//		
//		if (queryExistCS) {//query has concept score
//			float[] cs_query_arr=cs_query.getFloatArr();
//			if (selectedConcept!=null) {//select concept
//				cs_query_arr=General.selectArrFloat(cs_query.getFloatArr(),selectedConcept,0);
//			}
//			//filter out oriRanks, oriScores
//			for (int i=0;i<oriRanks.size();i++){
//				int doc=oriRanks.get(i);
//				float conSim=conceptSim(conceptMapFile,  cs_query_arr,  doc, selectedConcept);
//				float score_re=oriScores.get(i);
//				if (conSim!=0 && conSim<conSim_thr) {//doc has concept score and concept sim is low
//					score_re=0;
//				}
//				oriRanks_filtered.add(doc); oriScores_filtered.add(score_re);
//			}
//			//filter out topDocVisualScores
//			for (int doc :topDocVisualScores.keySet()) {
//				float conSim=conceptSim(conceptMapFile,  cs_query_arr,  doc, selectedConcept);
//				float score_re=topDocVisualScores.get(doc);
//				if (conSim!=0 && conSim<conSim_thr) {//doc has concept score and concept sim is low
//					score_re=0;
//				}
//				topDocVisualScores_filtered.put(doc,score_re);
//			}
//		}else {//query does not have concept score
//			oriRanks_filtered=oriRanks;
//			oriScores_filtered=oriScores;
//			topDocVisualScores_filtered=topDocVisualScores;
//		}
//		
//		if (normScoreForVis<0) {//only use vis 
//			return rerank_geoExpansion_VisOnly_returnRankScore(oriRanks_filtered, oriScores_filtered, topDocVisualScores_filtered,  lats,  lons,  geoExpanScale);
//		}else {
//			return rerank_geoExpansion_VisConcept_returnRankScore(oriRanks_filtered, oriScores_filtered, topDocVisualScores_filtered, conceptMapFile, selectedConcept, lats,  lons,  geoExpanScale, queryName,normScoreForVis);
//		}
	}
	
	public static void rerank_geoExpansion_VisConceptRankThre_returnRankScore
	(ArrayList<Integer> oriRanks, ArrayList<Float> oriScores, HashMap<Integer,Float> topDocVisualScores, HashSet<Integer> selectedDocs_basedon_concept, float[] lats, float[] lons, float geoExpanScale, int queryName) throws IOException{
//		//return Rank and Score
//		
//		
//		//filter out oriRanks, oriScores, topDocVisualScores
//		ArrayList<Integer> oriRanks_filtered=new ArrayList<Integer>(selectedDocs_basedon_concept.size());
//		ArrayList<Float> oriScores_filtered=new ArrayList<Float>(selectedDocs_basedon_concept.size());
//		HashMap<Integer,Float> topDocVisualScores_filtered=new HashMap<Integer, Float>(selectedDocs_basedon_concept.size());
//		
//		
//		//filter out oriRanks, oriScores
//		for (int i=0;i<oriRanks.size();i++){
//			int doc=oriRanks.get(i);
//			if(selectedDocs_basedon_concept.contains(doc)){
//				oriRanks_filtered.add(doc); oriScores_filtered.add(oriScores.get(i));
//			}
//		}
//		//filter out topDocVisualScores
//		for (int doc :topDocVisualScores.keySet()) {
//			if(selectedDocs_basedon_concept.contains(doc)){
//				topDocVisualScores_filtered.put(doc,topDocVisualScores.get(doc));
//			}
//		}
//		//only use vis 
//		return rerank_geoExpansion_VisOnly_returnRankScore(oriRanks_filtered, oriScores_filtered, topDocVisualScores_filtered,  lats,  lons,  geoExpanScale);
	}

	public static HashSet<Integer> get_GoodDoc_basedOn_conceptRank(ArrayList<Integer> oriRanks, int conRank_at_Top, MapFile.Reader conceptMapFile, int[] selectedConcept, int queryName, int conRank_thr) throws IOException, InterruptedException{
		
		FloatArr cs_query= new FloatArr();
		conceptMapFile.get(new IntWritable(queryName), cs_query);
		boolean queryExistCS=(cs_query.getFloatArr()!=null);
		float[] cs_query_arr=null;
		if (queryExistCS) {
			cs_query_arr=cs_query.getFloatArr();
			if (selectedConcept!=null) {//select concept
				cs_query_arr=General.selectArrFloat(cs_query.getFloatArr(),selectedConcept,0);
			}
		}
		if (queryExistCS) {//query has concept score
			//rank top-visul photo based on concept sim
			HashMap<Integer, Float> doc_conSim=new HashMap<Integer, Float>();
			for (int i = 0; i < Math.min(conRank_at_Top, oriRanks.size()); i++) {
				float conSim=conceptSim( conceptMapFile, cs_query_arr,oriRanks.get(i),selectedConcept);
				doc_conSim.put(oriRanks.get(i), conSim);
			}
			//***** sort doc_conSim *********
			ValueComparator_Float_DES mvCompartor = new ValueComparator_Float_DES(doc_conSim);
			TreeMap<Integer,Float> doc_conSim_Des = new TreeMap<Integer,Float>(mvCompartor);
			doc_conSim_Des.putAll(doc_conSim);
			ArrayList<Integer> topConDocs=new ArrayList<Integer>(doc_conSim_Des.keySet());
			
			return new HashSet<Integer>(topConDocs.subList(0, Math.min(conRank_thr,topConDocs.size())));
		}
		
		return new HashSet<Integer>(oriRanks.subList(0, Math.min(conRank_thr,oriRanks.size())));
	}
	
	public static HashSet<Integer> get_GoodDoc_basedOn_globalFeatRank(ArrayList<Integer> oriRanks, int Rank_at_Top, MapFile.Reader featMapFile, int queryName, int RankThr, String targetFeatClassName) throws IOException, InterruptedException{
		//for lire global feat
		BytesWritable value_feat=new BytesWritable();
		featMapFile.get(new IntWritable(queryName), value_feat);
		boolean queryExistFeat=(value_feat.getBytes()!=null);
		if (queryExistFeat) {//query has feat
			byte[] queryFeat_byteArr_r=new byte[value_feat.getLength()];
			System.arraycopy(value_feat.getBytes(), 0, queryFeat_byteArr_r, 0, value_feat.getLength()); //The data in value is only valid between 0 and getLength() - 1.!! when call getBytes() , it will return the byte[] by 1.5*ori_size,
			//rank top-visul photo based on feat-dist
			ArrayList<Integer> docs=new ArrayList<Integer>();  ArrayList<Float> dists=new ArrayList<Float>(); 
			ArrayList<Integer> freeDocs=new ArrayList<Integer>();
			for (int i = 0; i < Math.min(Rank_at_Top, oriRanks.size()); i++) {
				if(featMapFile.get(new IntWritable(oriRanks.get(i)), value_feat)!=null){
					byte[] docFeat_byteArr_r=new byte[value_feat.getLength()];
			        System.arraycopy(value_feat.getBytes(), 0, docFeat_byteArr_r, 0, value_feat.getLength()); //The data in value is only valid between 0 and getLength() - 1.!! when call getBytes() , it will return the byte[] by 1.5*ori_size,
			        float dist;
					try {
						dist = (float) General_Lire.getFeatDistance_lire136(docFeat_byteArr_r,queryFeat_byteArr_r, targetFeatClassName);
					} catch (Exception e) {
						e.printStackTrace();
						throw new InterruptedException(e.getMessage());
					} 
					docs.add(oriRanks.get(i));dists.add(dist);
				}else {//this doc do not have gist feat, add into free-docs
					freeDocs.add(oriRanks.get(i));
				}
			}
			//***** sort doc_featDist *********
			ArrayList<Integer> topDocs=new ArrayList<Integer>(RankThr); ArrayList<Float> topScores=new ArrayList<Float>(RankThr);
			General_IR.rank_get_TopDocScores_PriorityQueue(docs,dists, RankThr, topDocs, topScores,"ASC",false, true);	
			// add freeDocs
			topDocs.addAll(freeDocs);
			return new HashSet<Integer>(topDocs);
		}else {
			return new HashSet<Integer>(oriRanks.subList(0, Math.min(RankThr,oriRanks.size())));
		}
		
	}
	
	public static HashSet<Integer> get_GoodDoc_basedOn_globalFeatRank(ArrayList<Integer> oriRanks, int Rank_at_Top, MapFile.Reader featMapFile, int queryName, int RankThr) throws IOException, InterruptedException{
		FloatArr value_feat=new FloatArr();
		featMapFile.get(new IntWritable(queryName), value_feat);
		boolean queryExistFeat=(value_feat.getFloatArr()!=null);
		if (queryExistFeat) {//query has feat
			float[] queryFeat=value_feat.getFloatArr();
			//rank top-visul photo based on feat-dist
			ArrayList<Integer> docs=new ArrayList<Integer>();  ArrayList<Float> dists=new ArrayList<Float>(); 
			ArrayList<Integer> freeDocs=new ArrayList<Integer>();
			for (int i = 0; i < Math.min(Rank_at_Top, oriRanks.size()); i++) {
				if(featMapFile.get(new IntWritable(oriRanks.get(i)), value_feat)!=null){
					float dist=General.squaredEuclidian(queryFeat, value_feat.getFloatArr());
					docs.add(oriRanks.get(i));dists.add(dist);
				}else {//this doc do not have gist feat, add into free-docs
					freeDocs.add(oriRanks.get(i));
				}
			}
			//***** sort doc_featDist *********
			ArrayList<Integer> topDocs=new ArrayList<Integer>(RankThr); ArrayList<Float> topScores=new ArrayList<Float>(RankThr);
			General_IR.rank_get_TopDocScores_PriorityQueue(docs,dists, RankThr, topDocs, topScores,"ASC",false, true);	
			// add freeDocs
			topDocs.addAll(freeDocs);
			return new HashSet<Integer>(topDocs);
		}else {
			return new HashSet<Integer>(oriRanks.subList(0, Math.min(RankThr,oriRanks.size())));
		}
	}

	public static HashSet<Integer> get_GoodDoc_basedOn_globalFeatRank(MapFile.Reader globalReranked_MapFile, int queryName, int RankThr, ArrayList<Integer> oriRanks) throws IOException, InterruptedException{
		IntArr rank=new IntArr();
		if (globalReranked_MapFile.get(new IntWritable(queryName), rank)!=null) {
			HashSet<Integer> selectedDocs=new HashSet<Integer>();
			for (int doc : rank.getIntArr()) {
				selectedDocs.add(doc);
			}
			return selectedDocs;
		}else {
			return new HashSet<Integer>(oriRanks.subList(0, Math.min(RankThr,oriRanks.size())));
		}
	}

	public static float conceptSim(MapFile.Reader conceptMapFile, float[] cs_query_arr, int VDoc, int[] selectedConcept) throws IOException, InterruptedException{
		//cs_query_arr should be processed according to selectedConcept
		FloatArr cs_doc= new FloatArr();
		conceptMapFile.get(new IntWritable(VDoc), cs_doc);
		if (cs_doc.getFloatArr()!=null) {
			float[] cs_doc_arr=cs_doc.getFloatArr();
			if (selectedConcept!=null) {//select concept
				cs_doc_arr=General.selectArrFloat(cs_doc.getFloatArr(),selectedConcept,0);
			}
			return General_EJML.cosSim(cs_query_arr, cs_doc_arr);
//			return General.suqaredEuclidian(cs_query_arr, cs_doc_arr);
		}else {
			return 0;
		}
	}

	public static ArrayList<Integer> rerank_geoExpansion_neig_Prior(ArrayList<Integer> usedDocs, HashMap<Integer,Float> topDocVisualScores, float[] lats, float[] lons, float geoExpanScale, int[] geoNeighbourNums) throws IOException{
		//*** caculate docs geoNeighbour's visual score and geo-neighbour-num *******//
		int docsNum=usedDocs.size();
		float[] docScore_Vis=new float[docsNum];
		float[] docScore_Geo=new float[docsNum];
		for (int i=0;i<docsNum;i++){
			int doc=usedDocs.get(i);
			//compute GeoNeigbor visualScore
			float GeoNeigScore=0;
			for(int VDoc:topDocVisualScores.keySet()){
				float latABS=Math.abs(lats[VDoc]-lats[doc]);
				float lonABS=Math.abs(lons[VDoc]-lons[doc]);
				if(latABS<geoExpanScale && lonABS<geoExpanScale){//VDoc is 1km neighbor of doc
					GeoNeigScore=GeoNeigScore+topDocVisualScores.get(VDoc);
				}
			}
			docScore_Vis[i]=GeoNeigScore;
			//compute GeoNeigbor num
			docScore_Geo[i]=geoNeighbourNums[doc-1];//use geineigbour num in the whole dataset, //photoName from 1!!
		}
		//*** sum docs geoNeighbour's visual score and geo-neighbour-num *******//
		float sum_vis=0; float sum_geo=0;
		for (int i=0;i<docsNum;i++){
			sum_vis+=docScore_Vis[i];
			sum_geo+=docScore_Geo[i];
		}
		//*** nomarlize docs geoNeighbour's visual score and geo-neighbour-num *******//
		HashMap<Integer,Float> reR_doc_score = new HashMap<Integer,Float>();
		for (int i=0;i<docsNum;i++){
			int doc=usedDocs.get(i);
			//compute finial Score
//			float finialScore=weight*docScore_Vis[i]/sum_vis+(1-weight)*docScore_Geo[i]/sum_geo;
			float finialScore=(docScore_Vis[i]/sum_vis)*(docScore_Geo[i]/sum_geo);
			reR_doc_score.put(doc, finialScore);
		}
		//***** sort doc_neigNum *********
		ValueComparator_Float_DES mvCompartor = new ValueComparator_Float_DES(reR_doc_score);
		TreeMap<Integer,Float> reR_doc_score_Des = new TreeMap<Integer,Float>(mvCompartor);
		reR_doc_score_Des.putAll(reR_doc_score);
		return new ArrayList<Integer>(reR_doc_score_Des.keySet());
	}
	
	public static boolean isSameUser(int queryName, int docName, long[] userIDs_0, int[] userIDs_1) {	
		boolean isSame=false;
		if (userIDs_0[queryName]==userIDs_0[docName] && userIDs_1[queryName]==userIDs_1[docName])
			isSame=true;
		return isSame;
	}
	
	public static int[] removeSameUser_forTopDocs(int[] topDocs, int queryName, long[] userIDs_0, int[] userIDs_1) {	
		ArrayList<Integer> topDocs_noSameUser=new ArrayList<Integer>();
		for(int doc: topDocs){
			if(!isSameUser(queryName, doc, userIDs_0, userIDs_1))
				topDocs_noSameUser.add(doc);
		}
		int[] topDocs_noSameUser_r=new int[topDocs_noSameUser.size()];
		for(int i=0;i<topDocs_noSameUser_r.length;i++){
			topDocs_noSameUser_r[i]=topDocs_noSameUser.get(i);
		}
		return topDocs_noSameUser_r;
	}
	
	public static void removeSameUser_forTopDocsScores(ArrayList<Integer> topDocs, ArrayList<Float> topScores, int queryName, long[] userIDs_0, int[] userIDs_1) {	
		for(int i=0;i<topDocs.size();i++){
			int doc=topDocs.get(i);
			if(isSameUser(queryName, doc, userIDs_0, userIDs_1)){//same user, remove!
				topDocs.remove(i);
				topScores.remove(i);
				i--; //after remove, the array list is immediately changed!, so next one should still be i  
			}
		}
	}
	
	public static <T extends DID> void removeSameUser_forTopDocsInfos(List<T> topDocs, int queryName, long[] userIDs_0, int[] userIDs_1) {	
		for(int i=0;i<topDocs.size();i++){
			int doc=topDocs.get(i).getDID();
			if(isSameUser(queryName, doc, userIDs_0, userIDs_1)){//same user, remove!
				topDocs.remove(i);
				i--; //after remove, the array list is immediately changed!, so next one should still be i  
			}
		}
	}
	
	public static void make1U1P_forTopDocsScores(ArrayList<Integer> topDocs, ArrayList<Float> topScores, long[] userIDs_0, int[] userIDs_1) {	
		//1 user only contribute 1 photo in the rank list
		HashSet<Long> userIDs_0_exist=new HashSet<Long>(); HashSet<Integer> userIDs_1_exist=new HashSet<Integer>();
		for(int i=0;i<topDocs.size();i++){
			int doc=topDocs.get(i);
			if(userIDs_0_exist.add(userIDs_0[doc])==false && userIDs_1_exist.add(userIDs_1[doc])==false){//this user is already exist, remove!
				topDocs.remove(i);
				topScores.remove(i);
				i--; //after remove, the array list is immediately changed!, so next one should still be i  
			}
		}
	}
	
	public static void removeSameUser_forTopDocs(ArrayList<Integer> topDocs, int queryName, long[] userIDs_0, int[] userIDs_1) {	
		for(int i=0;i<topDocs.size();i++){
			int doc=topDocs.get(i);
			if(isSameUser(queryName, doc, userIDs_0, userIDs_1)){//same user, remove!
				topDocs.remove(i);
				i--; //after remove, the array list is immediately changed!, so next one should still be i  
			}
		}
	}

	public static void removeQueryItself_forTopDocsScores(ArrayList<Integer> topDocs, ArrayList<Float> topScores, int queryName) {	
		if(topDocs.get(0)==queryName){
			topDocs.remove(0); 
			topScores.remove(0);
		}else{//for some query, itself is not ranked first!!
			int queryRank=topDocs.indexOf(queryName);
			if(queryRank==-1){//do not handle bug-query
//				System.err.println("queryName:"+queryName+", queryRank:"+queryRank+", 1st doc:"+topDocs.get(0)+", rankList size:"+topDocs.size());
				return;
			}else{
				topDocs.remove(queryRank); topScores.remove(queryRank);
			}
		}
	}
	
	public static void removeQueryItself_forTopDocsScores(ArrayList<Integer> topDocs, int queryName) {	
		if(topDocs.get(0)==queryName){
			topDocs.remove(0); 
		}else{//for some query, itself is not ranked first!!
			int queryRank=topDocs.indexOf(queryName);
			if(queryRank==-1){//do not handle bug-query
				System.err.println("queryName:"+queryName+", queryRank:"+queryRank+", 1st doc:"+topDocs.get(0)+", rankList size:"+topDocs.size());
				return;
			}else{
				topDocs.remove(queryRank);
			}
		}
	}
	
	public static <T extends DID> void removeQueryItself_forTopDocsInfos(ArrayList<T> topDocs, int queryName) {	
		for(int i=0;i<topDocs.size();i++){
			int doc=topDocs.get(i).getDID();
			if(doc==queryName){//same as query, remove!
				topDocs.remove(i);
				i--; //after remove, the array list is immediately changed!, so next one should still be i  
			}
		}
	}
	
	public static boolean isContainSameUser(ArrayList<Integer> topDocs, int queryName, long[] userIDs_0, int[] userIDs_1) {	
		boolean isContainSameUser=false;
		for(int i=0;i<topDocs.size();i++){
			int doc=topDocs.get(i);
			if(isSameUser(queryName, doc, userIDs_0, userIDs_1)){//same user, remove!
				isContainSameUser=true;
				break;
			}
		}
		return isContainSameUser;
		
	}
	
	@SuppressWarnings("deprecation")
	public static void test_removeSameUser() throws InterruptedException, IOException {
		//read photos GPS, users info into memory
		String metaPath="D:/xinchaoli/Desktop/My research/Database/FlickrPhotos/image-meta/";
//		float[][] latlons=(float[][]) General.readObject(metaPath+"3M_latlon.float2");
		long[] userIDs_0=(long[]) General.readObject(metaPath+"3M_userIDs_0.long"); 
		int[] userIDs_1=(int[]) General.readObject(metaPath+"3M_userIDs_1.int"); 
		
		//*********read rank from MapFile*********//
		Configuration conf = new Configuration();
	    FileSystem hdfs  = FileSystem.get(conf);
		String rankPath="O:/ImageRetrieval/SearchResult_D3M_Q100K_ICMR13_HD16/part-r-00000/";	
		MapFile.Reader MapFileReader_rank=new MapFile.Reader(hdfs, rankPath, conf);
		System.out.println("MapFileReader_rank Key-Class: "+MapFileReader_rank.getKeyClass().getName());
		System.out.println("MapFileReader_rank Value-Class: "+MapFileReader_rank.getValueClass().getName());
		IntWritable Key_queryName= new IntWritable();
		IntList_FloatList docs_scores= new IntList_FloatList();
		
		while(MapFileReader_rank.next(Key_queryName, docs_scores)){ //loop over all queries, key-value(query-rank,score)
			int queryName=Key_queryName.get();
			removeSameUser_forTopDocsScores(docs_scores.getIntegers(), docs_scores.getFloats(), queryName, userIDs_0, userIDs_1);
			if(isContainSameUser(docs_scores.getIntegers(), queryName, userIDs_0, userIDs_1))
				System.out.println(queryName+"'s rank list still contains sameUser after removeSameUser_forTopDocsScores!");
		}
		MapFileReader_rank.close();
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		int comp=6;
		int job1RedNum=(comp<=5) ? 500:1000; //reducer number
		System.out.println(job1RedNum);
		
		ArrayList<String> CatchFilePaths=new ArrayList<String>();
		CatchFilePaths.add("#latlons.file"); //latlons path with symLink
		CatchFilePaths.add("#latlons.file"); //latlons path with symLink
		System.out.println(CatchFilePaths.toArray(new String[0]).length);
		
		String loopFlag="a_Concept_no";
		if(loopFlag.contains("_Concept")){
			System.out.println(loopFlag);
		}
		
		
		test_removeSameUser();
		
	}
	
}
