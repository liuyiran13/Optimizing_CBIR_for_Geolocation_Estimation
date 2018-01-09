package RankStudy_MapRed;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;












import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.General.General_IR;
import MyAPI.General.General_geoRank;
import MyAPI.Geo.groupDocs.UserIDs;
import MyCustomedHaoop.ValueClass.forTest;
import MyCustomedHaoop.ValueClass.GeoExpansionData.fistMatch_GTruth_Docs_Locations;

public class GVR_study {

	public static void main(String[] args) throws Exception {
//		GVR_performance_report();
		
//		trueMatchStatics();
		
//		queryCheck();
		
//		queryCheck_preciseQID();
		
//		showRanks_3M_SURF_GVR();
		
//		showRanks_3M_GlobalFeat_GVR();
		
//		showRanks_MediaEval13_GVR();
		
		showRanks_SanFrancisco_GVR();
	}
		
	public static void GVR_performance_report() throws Exception {
//	    String label, fileName;MapFile.Reader MapFileReader_rank;
//		
//		String servePath="O:/";
//		
//		String basePath=servePath+"ICMR2013/";	
//		String dataLabel="3M";
//		String rankPath=basePath+"GVR/"+dataLabel+"/";
//		int[] binsForGTSize={0,1,5,10,20,40,100};// grounTSize bins
//		int num_topLocations=20;  float isSameLocScale=(float) 0.01; float isOneLocScale=(float) (0.02);
//		String dir=rankPath+"isOneLoc"+isOneLocScale+"_sameLoc"+isSameLocScale+"/";
//		General.makeFolder(dir);
//		//set FileSystem
//		Configuration conf = new Configuration();
//        FileSystem hdfs  = FileSystem.get(conf);
//        //******* read latlons **************
//		float[][] latlons=(float[][]) General.readObject("D:/xinchaoli/Desktop/My research/Database/FlickrPhotos/image-meta/3M_latlon.float2");
////        float[][] latlons=(float[][]) General.readObject(basePath+"10M_selectedPhotos_LatLon.float2");
////		// read geoNeighbor number
////		int[] geoNeighbourNums=(int[]) General.readObject("O:/MediaEval_3185258Images/3M_GeoNeighbourNums_intArr");
////		// read userIDs
////		long[] userIDs_0=(long[]) General.readObject("D:/xinchaoli/Desktop/My research/Database/FlickrPhotos/image-meta/3M_userIDs_0.long");
////		int[] userIDs_1=(int[]) General.readObject("D:/xinchaoli/Desktop/My research/Database/FlickrPhotos/image-meta/3M_userIDs_1.int");
//
//
//		int topDoc=200; 
//		String GVR_label="_Vis";//_Vis _Concept _VisConceptScore _VisConceptThr
//		int HD=12;
//		//******* load ori-rank ********//
//		label="_topDoc"+topDoc+"_ori";
//		fileName="GVR"+GVR_label+"_D"+dataLabel+"_Q100K_HD"+HD+label;
//		// read rank from MapFile 
//		MapFileReader_rank=new MapFile.Reader(hdfs,rankPath+fileName+"/part-r-00000", conf);
////		//only for noUserCase, gtSize
////		MapFile.Reader MapFileReader_ap_forGTSize=new MapFile.Reader(hdfs,"D:/xinchaoli/Desktop/My research/My Code/DataSaved/ICMR2013/Q_AP_random100000/geoExpan_random100000_noSaUser_VisScal1000_expScal0.0100/"+"part-r-00000", conf);
////		GTSize_ap_topDocs Value_apRank_forGTSize= new GTSize_ap_topDocs();
//		
//		System.out.println("MapFileReader_rank Key-Class: "+MapFileReader_rank.getKeyClass().getName());
//		System.out.println("MapFileReader_rank Value-Class: "+MapFileReader_rank.getValueClass().getName());
//		IntWritable Key_queryName= new IntWritable();
//		fistMatch_GTruth_Docs_Locations Value_Rank= new fistMatch_GTruth_Docs_Locations();
//		HashMap<Integer,int[]> groTSize_locHist_ori= new HashMap<Integer,int[]>();//groTSize, histogram of trueLoactin rank, fist one is "not in top"
//		HashMap<Integer,int[]> groTSize_locHist_ori_Group= new HashMap<Integer,int[]>();//groTSize, histogram of trueLoactin rank, fist one is "not in top"
//		int[] totlocHist_ori=new int[num_topLocations+1]; //fist one is "not in top"
//		int totalQueryNum=0;
//		while(MapFileReader_rank.next(Key_queryName, Value_Rank)){ //loop over all queries, key-value(query-rank)
//			int queryName=Key_queryName.get();
//			int grounTSize = Value_Rank.GTruth.getArrArr().length; 
////			int trueLocRank_oriVisList=Value_Rank.get_trueRank();
////			int grounTSize = geoNeighbourNums[queryName-1]; //use geoNum in the whole collection, queryName from 1 !!
//			int[] topDocs = Value_Rank.get_Docs();
//			//remove same user
////			topDocs= General_geoRank.removeSameUser_forTopDocs(topDocs, queryName, userIDs_0, userIDs_1);
//			//get top Location Doc
////			ArrayList<Integer> topLocationDocs=General_geoRank.get_topLocationDocs(num_topLocations, topDocs, isOneLocScale, latlons);
//			ArrayList<ArrayList<Integer>> LocList=General_geoRank.get_topLocationDocsList(num_topLocations, topDocs,  (float) isOneLocScale,  latlons);
//			//get True-Location rank
////			int trueLocRank=General_geoRank.get_trueLocRank(queryName, topLocationDocs, isSameLocScale, latlons)+1;
//			int trueLocRank=General_geoRank.get_trueLocRank_fromList(queryName, LocList, isSameLocScale, latlons)+1;
//			//set groTSize_locHist_ori
//			if(groTSize_locHist_ori.containsKey(grounTSize)){
//				groTSize_locHist_ori.get(grounTSize)[trueLocRank]++;
//			}else{
//				int[] queryNum_locHist =new int[num_topLocations+1];//fist one is "not in top"
//				queryNum_locHist[trueLocRank]=1;
//				groTSize_locHist_ori.put(grounTSize, queryNum_locHist);
//			}
//			//set groTSize_locHist_Group
//			int binInd=General.getBinIndex(binsForGTSize,grounTSize);
//			if(groTSize_locHist_ori_Group.containsKey(binInd)){
//				groTSize_locHist_ori_Group.get(binInd)[trueLocRank]++;
//			}else{
//				int[] queryNum_locHist =new int[num_topLocations+1];//fist one is "not in top"
//				queryNum_locHist[trueLocRank]=1;
//				groTSize_locHist_ori_Group.put(binInd, queryNum_locHist);
//			}
//			//set totlocHist
//			totlocHist_ori[trueLocRank]++;
//			totalQueryNum++;
//		}
//		System.out.println("totalQueryNum:"+totalQueryNum);
//		MapFileReader_rank.close();
//		// find largest grounTSize
//		int grounTSize_MAX=0;
//		for(int grounTSize:groTSize_locHist_ori.keySet()){
//			grounTSize_MAX=Math.max(grounTSize, grounTSize_MAX);
//		}
//		System.out.println("grounTSize_MAX:"+grounTSize_MAX);
//		
//		//*** load GVR *******
////		int[] visScales={10,50,100,500,1000,10000}; 
//		int[] visScales={100,200,500,1000}; 
////		float[] geoExpanScales={(float) 0.01,(float) 0.02,(float) 0.05,(float) 0.1};
////		float[] geoExpanScales={(float) 0.001,(float) 0.002,(float) 0.005};
////		float[] geoExpanScales={(float) 0.0001,(float) 0.0002,(float) 0.0005};
////		float[] geoExpanScales={(float) 0.00001,(float) 0.00002,(float) 0.00005};
////		float[] geoExpanScales={(float) 0.0001,(float) 0.001,(float) 0.005,(float) 0.01,(float) 0.05,(float) 0.1};
//		for (int visScale:visScales){ // loop over parameters
////		int visScale=500;
////			for(float geoExpanScale:geoExpanScales){
//			float geoExpanScale=(float) Math.min(isSameLocScale, (float) 0.01);
//				label="_topDoc"+topDoc+"_VisSca"+visScale+"_expSca"+geoExpanScale;
//				fileName="GVR"+GVR_label+"_D"+dataLabel+"_Q100K_HD"+HD+label;
//				MapFileReader_rank=new MapFile.Reader(hdfs,rankPath+fileName+"/part-r-00000", conf);
//				//*** set data for output *******//
//				PrintWriter outStr_trueLocHist_fExcel = new PrintWriter(new OutputStreamWriter(new FileOutputStream(dir+fileName+".TruHistfExcel_locList",false), "UTF-8")); 
//				//*** compute aps *******//
//				HashMap<Integer,int[]> groTSize_locHist_rerank= new HashMap<Integer,int[]>();//groTSize,  histogram of trueLoactin position, fist one is "not in top"
//				HashMap<Integer,int[]> groTSize_locHist_rerank_Group= new HashMap<Integer,int[]>();//groTSize, histogram of trueLoactin position, fist one is "not in top"
//				int[] totlocHist_rerank=new int[num_topLocations+1]; //fist one is "not in top"
//				totalQueryNum=0;
//				while(MapFileReader_rank.next(Key_queryName, Value_Rank)){ //loop over all queries, key-value(query-ap&rank)
//					int queryName=Key_queryName.get();
//					int grounTSize=Value_Rank.get_GTruth().size();
////					int grounTSize = geoNeighbourNums[queryName-1]; //use geoNum in the whole collection, queryName from 1 !!
//					int[] topDocs = Value_Rank.get_Docs();
//					//get top Location Doc
////					ArrayList<Integer> topLocationDocs=General_geoRank.get_topLocationDocs(num_topLocations, topDocs, isOneLocScale, latlons);
//					ArrayList<ArrayList<Integer>> LocList=General_geoRank.get_topLocationDocsList(num_topLocations, topDocs,  (float) isOneLocScale,  latlons);
////					float[][] topLocations=General_geoRank.get_topLocations(num_topLocations, topDocs, isOneLocScale, latlons);
//					//get True-Location rank
////					int trueLocRank=General_geoRank.get_trueLocRank(queryName, topLocationDocs, isSameLocScale, latlons)+1;
//					int trueLocRank=General_geoRank.get_trueLocRank_fromList(queryName, LocList, isSameLocScale, latlons)+1;
////					int trueLocRank=General_geoRank.get_trueLocRank(topLocations, queryName, isSameLocScale, latlons)+1;
//					//set groTSize_locHist
//					if(groTSize_locHist_rerank.containsKey(grounTSize)){
//						groTSize_locHist_rerank.get(grounTSize)[trueLocRank]++;
//					}else{
//						int[] queryNum_locHist =new int[num_topLocations+1];//fist one is "not in top"
//						queryNum_locHist[trueLocRank]=1;
//						groTSize_locHist_rerank.put(grounTSize, queryNum_locHist);
//					}
//					//set groTSize_locHist_Group
//					int binInd=General.getBinIndex(binsForGTSize,grounTSize);
//					if(groTSize_locHist_rerank_Group.containsKey(binInd)){
//						groTSize_locHist_rerank_Group.get(binInd)[trueLocRank]++;
//					}else{
//						int[] queryNum_locHist =new int[num_topLocations+1];//fist one is "not in top"
//						queryNum_locHist[trueLocRank]=1;
//						groTSize_locHist_rerank_Group.put(binInd, queryNum_locHist);
//					}
//					//set totlocHist
//					totlocHist_rerank[trueLocRank]++;
//					totalQueryNum++;
//				}
//				System.out.println("totalQueryNum in rerank:"+totalQueryNum);
//				assert groTSize_locHist_ori.size()==groTSize_locHist_rerank.size();
//				assert groTSize_locHist_ori_Group.size()==groTSize_locHist_rerank_Group.size();
//				// ** compute TrueLocHist for each grounTSize ***//
//				outStr_trueLocHist_fExcel.println("******** each grounTSize:  TrueLocHist:");
//				for(int grounTSize=0;grounTSize<=grounTSize_MAX;grounTSize++){
//					if(groTSize_locHist_rerank.containsKey(grounTSize)){
//						int[] trueLocHist_ori=groTSize_locHist_ori.get(grounTSize);
//						int[] trueLocHist_rerank=groTSize_locHist_rerank.get(grounTSize);
//						int qureyNum=General.sum_IntArr(trueLocHist_ori);
//						outStr_trueLocHist_fExcel.println(grounTSize+"\t"+qureyNum+"\t"+General.floatArrToString(General.normliseArr(trueLocHist_ori, -1), "\t", "0.0000")+"\t"+General.floatArrToString(General.normliseArr(trueLocHist_rerank, -1), "\t", "0.0000"));
//					}
//				}
//				// ** compute TrueLocHist for grouped grounTSize ***//
//				outStr_trueLocHist_fExcel.println("******** Group grounTSize:  TrueLocHist:");
//				for(int binIndex:groTSize_locHist_rerank_Group.keySet()){
//					int[] trueLocHist_ori=groTSize_locHist_ori_Group.get(binIndex);
//					int[] trueLocHist_rerank=groTSize_locHist_rerank_Group.get(binIndex);
//					int qureyNum=General.sum_IntArr(trueLocHist_ori);
//					if(binIndex==0){//groundTruth size==0
//						outStr_trueLocHist_fExcel.println(0+"\t"+qureyNum+"\t"+General.floatArrToString(General.normliseArr(trueLocHist_ori, -1), "\t", "0.0000")+"\t"+General.floatArrToString(General.normliseArr(trueLocHist_rerank, -1), "\t", "0.0000"));
//					}else{
//						if(binIndex==-1){//groundTruth size > bins' last value
//							outStr_trueLocHist_fExcel.println(">"+binsForGTSize[binsForGTSize.length-1]+"\t"+qureyNum+"\t"+General.floatArrToString(General.normliseArr(trueLocHist_ori, -1), "\t", "0.0000")+"\t"+General.floatArrToString(General.normliseArr(trueLocHist_rerank, -1), "\t", "0.0000"));
//						}else{
//							outStr_trueLocHist_fExcel.println((binsForGTSize[binIndex-1]+1)+"--"+binsForGTSize[binIndex]+"\t"+qureyNum+"\t"+General.floatArrToString(General.normliseArr(trueLocHist_ori, -1), "\t", "0.0000")+"\t"+General.floatArrToString(General.normliseArr(trueLocHist_rerank, -1), "\t", "0.0000"));
//						}
//					}
//				}
//				// ** compute accumulated TrueLocHist for grouped grounTSize ***//
//				outStr_trueLocHist_fExcel.println("******** Group grounTSize:  accumulated-TrueLocHist:");
//				int[] accumLevel={1,2,3,5,10,20}; 
//				outStr_trueLocHist_fExcel.println("accumLevel:"+"\t"+General.IntArrToString(accumLevel, "\t"));
//				for(int binIndex:groTSize_locHist_rerank_Group.keySet()){
//					int[] trueLocHist_ori=groTSize_locHist_ori_Group.get(binIndex);
//					int[] trueLocHist_rerank=groTSize_locHist_rerank_Group.get(binIndex);
//					int qureyNum=General.sum_IntArr(trueLocHist_ori);
//					int[] trueLocHistAccu_ori=General.makeAccum(accumLevel, trueLocHist_ori); //no "not in top" querys, so when compute percent, should ./ qureyNum
//					int[] trueLocHistAccu_rerank=General.makeAccum(accumLevel, trueLocHist_rerank);
//					if(binIndex==0){//groundTruth size==0
//						outStr_trueLocHist_fExcel.println(0+"\t"+qureyNum+"\t"+General.floatArrToString(General.normliseArr(trueLocHistAccu_ori, qureyNum), "\t", "0.0000")+"\t"+General.floatArrToString(General.normliseArr(trueLocHistAccu_rerank, qureyNum), "\t", "0.0000"));
//					}else{
//						if(binIndex==-1){//groundTruth size > bins' last value
//							outStr_trueLocHist_fExcel.println(">"+binsForGTSize[binsForGTSize.length-1]+"\t"+qureyNum+"\t"+General.floatArrToString(General.normliseArr(trueLocHistAccu_ori, qureyNum), "\t", "0.0000")+"\t"+General.floatArrToString(General.normliseArr(trueLocHistAccu_rerank, qureyNum), "\t", "0.0000"));
//						}else{
//							outStr_trueLocHist_fExcel.println((binsForGTSize[binIndex-1]+1)+"--"+binsForGTSize[binIndex]+"\t"+qureyNum+"\t"+General.floatArrToString(General.normliseArr(trueLocHistAccu_ori, qureyNum), "\t", "0.0000")+"\t"+General.floatArrToString(General.normliseArr(trueLocHistAccu_rerank, qureyNum), "\t", "0.0000"));
//						}
//					}
//				}
//				// ** compute accumulated TrueLocHist for totlocHist_ori, totlocHist_rerank ***//
//				int[] totlocHistAccu_ori=General.makeAccum(accumLevel, totlocHist_ori); //no "not in top" querys, so when compute percent, should ./ totalQueryNum
//				int[] totlocHistAccu_rerank=General.makeAccum(accumLevel, totlocHist_rerank);
//				outStr_trueLocHist_fExcel.println("******** total querys:  accumulated-TrueLocHist:");
//				outStr_trueLocHist_fExcel.println("accumLevel:"+"\t"+General.IntArrToString(accumLevel, "\t"));
//				outStr_trueLocHist_fExcel.println("ori: \t\t"+General.floatArrToString(General.normliseArr(totlocHistAccu_ori, totalQueryNum), "\t", "0.0000"));
//				outStr_trueLocHist_fExcel.println("geoExpan: \t"+General.floatArrToString(General.normliseArr(totlocHistAccu_rerank, totalQueryNum), "\t", "0.0000"));
//
//				outStr_trueLocHist_fExcel.close();
//				MapFileReader_rank.close();
////			}
//		}
//			
			
	}

	public static void trueMatchStatics() throws Exception {
		String label, fileName; MapFile.Reader MapFileReader_ori, MapFileReader_GVR;
		
		String servePath="O:/";
		
		String basePath=servePath+"ICMR2013/";	
		String dataLabel="3M";
		String rankPath=basePath+"GVR/"+dataLabel+"/";
		float isOneLocScale=(float) (0.01);

		//set FileSystem
		Configuration conf = new Configuration();
        FileSystem hdfs  = FileSystem.get(conf);
        //******* read latlons **************
		float[][] latlons=(float[][]) General.readObject("D:/xinchaoli/Desktop/My research/Database/FlickrPhotos/image-meta/3M_latlon.float2");

		int topDoc=200; 
		String GVR_label="_Vis";//_Vis _Concept _VisConceptScore _VisConceptThr
		int HD=12;
		
		// read ori-rank from MapFile 
		label="_topDoc"+topDoc+"_ori";
		fileName="GVR"+GVR_label+"_D"+dataLabel+"_Q100K_HD"+HD+label;
		MapFileReader_ori=new MapFile.Reader(hdfs,rankPath+fileName+"/part-r-00000", conf);
		// read GVR-rank from MapFile 
		label="_topDoc"+topDoc+"_VisSca"+500+"_expSca"+0.01;
		fileName="GVR"+GVR_label+"_D"+dataLabel+"_Q100K_HD"+HD+label;
		MapFileReader_GVR=new MapFile.Reader(hdfs,rankPath+fileName+"/part-r-00000", conf);
		IntWritable Key_queryName= new IntWritable();
		fistMatch_GTruth_Docs_Locations Value_Rank_ori= new fistMatch_GTruth_Docs_Locations();
		fistMatch_GTruth_Docs_Locations Value_Rank_GVR= new fistMatch_GTruth_Docs_Locations();
		
		float[] visSimBins_step={0,10,(float) 1};
		float[] visSimBins=General.makeRange(visSimBins_step); 
		int[] hist_visSimRat_TureMatch=new int[visSimBins.length]; int[] hist_visSimRat_ErrMatch=new int[visSimBins.length];
		int[] hist_visSimRat_OriT_GVRF=new int[visSimBins.length]; 
		int totalQueryNum=0; float visSimMAX=0;
		while(MapFileReader_ori.next(Key_queryName, Value_Rank_ori)){ //loop over all queries, key-value(query-rank)
			MapFileReader_GVR.next(Key_queryName, Value_Rank_GVR);
			int queryName=Key_queryName.get();
			int gTSize=Value_Rank_ori.GTruth.getArrArr().length;
			int fistMatchDoc_ori = Value_Rank_ori.fistMatch.getIntArr()[1];
			int fistMatchDoc_GVR = Value_Rank_GVR.fistMatch.getIntArr()[1];
			float fistVisSim = Value_Rank_ori.Docs.getArr()[0].getArr()[0].score;
			float secondVisSim=Value_Rank_ori.Docs.getArr()[1].getArr()[0].score;
			int binInd=General.getBinInd_linear(visSimBins, fistVisSim/secondVisSim);
			boolean isTrueMatch_ori=General_geoRank.isOneLocation_approximate(latlons[0][queryName],latlons[1][queryName],latlons[0][fistMatchDoc_ori],latlons[1][fistMatchDoc_ori],isOneLocScale);
			boolean isTrueMatch_GVR=General_geoRank.isOneLocation_approximate(latlons[0][queryName],latlons[1][queryName],latlons[0][fistMatchDoc_GVR],latlons[1][fistMatchDoc_GVR],isOneLocScale);
			if (binInd!=visSimBins.length) {
				if(isTrueMatch_ori){//true match
					hist_visSimRat_TureMatch[binInd]++;
				}else {// error match
					hist_visSimRat_ErrMatch[binInd]++;
				}
				if(isTrueMatch_ori&&!isTrueMatch_GVR){//ori true match, GVR err match
					hist_visSimRat_OriT_GVRF[binInd]++;
				}
			}else {
				if (!isTrueMatch_ori) {
					double geoDis=General.calculateGeoDistance(latlons[0][queryName],latlons[1][queryName],latlons[0][fistMatchDoc_ori],latlons[1][fistMatchDoc_ori],"Cartesian");
					System.out.println("binInd==-1, queryName:"+queryName+", gTSize:"+gTSize+", fistMatchDoc_ori:"
							+fistMatchDoc_ori+", geoDis:"+geoDis+", fistVisSim:"+fistVisSim+", secondVisSim:"+secondVisSim+", isTrueMatch_ori:"+isTrueMatch_ori+", isTrueMatch_GVR:"+isTrueMatch_GVR);
				}
			}
			visSimMAX=Math.max(visSimMAX, fistVisSim);
			totalQueryNum++;
		}
		System.out.println("totalQueryNum:"+totalQueryNum+", visSimMAX:"+visSimMAX);
		MapFileReader_GVR.close();
		MapFileReader_ori.close();
		
		//statics: hist_conSim_TureMatch, hist_conSim_ErrMatch
		System.out.println("in 1st match docs, visSimBins: "+General.floatArrToString(visSimBins, "_", "00"));
		
		System.out.println(" hist_visSim_TureMatch: "+General.IntArrToString(hist_visSimRat_TureMatch, "_")
				+"\n hist_visSim_ErrMatch: "+General.IntArrToString(hist_visSimRat_ErrMatch, "_"));
		
		System.out.println("hist_visSimRat_OriT_GVRF: "+General.IntArrToString(hist_visSimRat_OriT_GVRF, "_"));

	}

	public static void queryCheck() throws Exception {
		int saveInterval=100*1000; 
		
		//load image mapFiles
		String imageMapFilesPath="Q:/Photos_MEva13_9M_MapFiles/";
		MapFile.Reader[] imgMapFiles= General_Hadoop.openAllMapFiles(new String[]{imageMapFilesPath});
		System.out.println("load mapFiles finished! tot-"+imgMapFiles.length);
		
		String saveQueryImageDir="O:/GVR_TMM/GVR/checkQ/";
		
		String rankPathBase="O:/GVR_TMM/GVR/";
		String[] rankPaths={"rank_VisNN_Vis_noGlobFilter_noSameUser_1U1P_D9M_Q0t_SURFHD18-20-18_1vs1AndHPM6-1000",
							"rank_GVR_Vis_noGlobFilter_noSameUser_1U1P_reRank@20_visSca@100_expSca@0.01_D9M_Q0t_SURFHD18-20-18_1vs1AndHPM6-1000"};
		
		String[] rankLabels={"VisNN_Vis_noGlobFilter_noSameUser_1U1P","GVR_Vis_noGlobFilter_noSameUser_1U1P"};
		
		General.addStrArr_prefx(rankPathBase, rankPaths);
		General.addStrArr_append(rankPaths, "/part-r-00000");
		//******* read latlons **************
		float[][] latlons=(float[][]) General.readObject("O:/MediaEval13/MEval13_latlons.floatArr");
		long[] userIDs_0=(long[]) General.readObject("O:/MediaEval13/MEval13_userIDs_0.long");
		int[] userIDs_1=(int[]) General.readObject("O:/MediaEval13/MEval13_userIDs_1.int");
		UserIDs userID=new UserIDs(userIDs_0, userIDs_1);
		
		float isSameLoc=(float) 0.0001;//10m  
		float isOneLoc=(float) 0.001;//100m, to find uni locs  
		float isPredRightScale=(float) 0.01;//1km, to check query prediction
		int randomLocNum=100;
		int miniLocSize=5;
		int randomQNumPerLoc=10;
		saveQueryImageDir+="Q0_isSameLoc"+new DecimalFormat("0.0000").format(isSameLoc)+"_isOneLoc"+new DecimalFormat("0.0000").format(isOneLoc)+"/";
		General.makeORdelectFolder(saveQueryImageDir);
		//load all querys
    	MapFile.Reader[] MapFileR_Rank=General_Hadoop.openAllMapFiles(rankPaths);//
    	IntWritable Key_queryName= new IntWritable();
    	fistMatch_GTruth_Docs_Locations Value_RankScores= new fistMatch_GTruth_Docs_Locations();
		ArrayList<Integer> allQuery=new ArrayList<Integer>(); 
		while (MapFileR_Rank[0].next(Key_queryName, Value_RankScores)) {
			//for ranks should be good
    		int queryName=Key_queryName.get();
    		allQuery.add(queryName);
		}
		General_Hadoop.closeAllMapFiles(MapFileR_Rank);
		int totQueryNum=allQuery.size();
		System.out.println("tot query num:"+totQueryNum);
		//make query locations
		float[][] query_latlons=new float[2][totQueryNum];
		for (int j = 0; j < allQuery.size(); j++) {
			query_latlons[0][j]=latlons[0][allQuery.get(j)];
			query_latlons[1][j]=latlons[1][allQuery.get(j)];
		}
		//get dense query locs
		ArrayList<Integer> denseLocQ=new ArrayList<Integer>(); MapFileR_Rank=General_Hadoop.openAllMapFiles(rankPaths);//for check prediction
		ArrayList<Integer> denseLocQ_correctQ=new ArrayList<Integer>(); 
		for (int j = 0; j < allQuery.size(); j++) {
			if (!General_geoRank.isLocExist(j, denseLocQ, query_latlons, isOneLoc)) {
				ArrayList<Integer> oneLoc= General_geoRank.findGeoNeighbors(j, isSameLoc, query_latlons);
				int geoNum=oneLoc.size();
				if (geoNum>=miniLocSize) {
					for (Integer oneQ_sInd : oneLoc) {//check whether this loca own correct predicted query,
						boolean isPreRight=false;
						int oneQ_LInd=allQuery.get(oneQ_sInd);
						for (MapFile.Reader oneRank : MapFileR_Rank) {
							oneRank.get(new IntWritable(oneQ_LInd), Value_RankScores);
							//get top Locations
							float[][] topLocations=Value_RankScores.topLocations.getArrArr();
							//get True-Location rank
							int trueLocRank=General_geoRank.get_trueLocRank(topLocations, oneQ_LInd, 10, isPredRightScale, latlons);//from 0, not exist ==-1
							isPreRight=isPreRight||(trueLocRank==0);
						}
						if (isPreRight) {//use one correct predicted query as one loc
							denseLocQ.add(j);
							denseLocQ_correctQ.add(oneQ_sInd);
							break;
						}
					}
				}
			}
		}
		System.out.println("denseLocQ num:"+denseLocQ.size());
		//random select some locs
		int[] randInd=General.randIndex(denseLocQ.size()); 
		ArrayList<Integer> goodQuerys=new ArrayList<Integer>(); int totFinalSelQ=0;
		for (int j = 0; j < Math.min(randomLocNum, randInd.length); j++) {//each rand query is a loc, and epand to get nearby querys
			int thisQ=denseLocQ.get(randInd[j]);
			int thisQ_itsLocCorrectQ=denseLocQ_correctQ.get(randInd[j]);
			ArrayList<Integer> geoNeighbors=General_geoRank.findGeoNeighbors(thisQ, isSameLoc, query_latlons);
			//random select some querys, and save into thisLoc
			ArrayList<Integer> thisLoc=new ArrayList<Integer>(randomQNumPerLoc);
			int[] randInd_geoN=General.randIndex(geoNeighbors.size());
			for (int t = 0; t < Math.min(randomQNumPerLoc, randInd_geoN.length); t++) {
				thisLoc.add(allQuery.get(geoNeighbors.get(randInd_geoN[t])));
			}
			if (thisLoc.indexOf(allQuery.get(thisQ_itsLocCorrectQ))==-1) {//is thisQ is not added, then remove first one, and add thisQ
				thisLoc.remove(0);
				thisLoc.add(allQuery.get(thisQ_itsLocCorrectQ));
			}
			totFinalSelQ+=thisLoc.size();
			//show selected dense loc querys
			System.out.println(j+"-th loc its size: "+geoNeighbors.size()+", loc:"+query_latlons[0][thisQ]+", "+query_latlons[1][thisQ]);
			String loc_dir=saveQueryImageDir+"loc_"+j+"_"+geoNeighbors.size()+"_"+query_latlons[0][thisQ]+", "+query_latlons[1][thisQ]+"/";
			General.makeORdelectFolder(loc_dir);
			PrintWriter outStr = new PrintWriter(new OutputStreamWriter(new FileOutputStream(saveQueryImageDir+"loc_"+j+"_QueryNames",false), "UTF-8")); 
			for (int oneQ : thisLoc) {
				//save query photo file
				General_IR.addPhotoPath_MovePhoto("MapFile", oneQ, loc_dir, saveInterval, null, 0, imgMapFiles);
				//check prediction res
				String predRes=""; boolean isPreRight=false;
				for (MapFile.Reader oneRank : MapFileR_Rank) {
					oneRank.get(new IntWritable(oneQ), Value_RankScores);
					//get top Locations
					float[][] topLocations=Value_RankScores.topLocations.getArrArr();
					//get True-Location rank
					int trueLocRank=General_geoRank.get_trueLocRank(topLocations, oneQ, 10, isPredRightScale, latlons);//from 0, not exist ==-1
					predRes+=trueLocRank+"_";
					isPreRight=isPreRight||(trueLocRank==0);
				}
				outStr.println(j+"\t"+oneQ+"\t"+latlons[0][oneQ]+", "+latlons[1][oneQ]+"\t"+isPreRight+"\t"+predRes);
				if(isPreRight)
					goodQuerys.add(oneQ);
				System.out.println(oneQ+", isPreRight:"+isPreRight);
			}
			outStr.close();
		}
		//show goood querys
    	String rankShowPath=saveQueryImageDir+"showGoodQuerys_inHTML/";	
    	int selQueryNum=goodQuerys.size();
    	int actShowQueryNum=Math.min(goodQuerys.size(), 50);
    	//html title
    	String HtmlTitle="show ranks for goodQuerys: \n"+General.StrArrToStr(rankPaths, "\n")
    			+"G_ForGTSize=1km, V_ForGTSize=1,000 \n"
    			+"qualified query num:"+selQueryNum+" in total "+totFinalSelQ+" selected queries, here show "+actShowQueryNum+" queries";
    	System.out.println("HtmlTitle: "+HtmlTitle);
		General_IR.showGeoReleventRanks(goodQuerys, rankShowPath, HtmlTitle, rankLabels, rankPaths, 10, latlons, userID, isPredRightScale, saveInterval, "MapFile", null, 0, imgMapFiles);
		//clean-up
    	General_Hadoop.closeAllMapFiles(imgMapFiles);
    	General_Hadoop.closeAllMapFiles(MapFileR_Rank);
		
		System.out.println("done!"); 	
    	
	}
	
	public static void queryCheck_preciseQID() throws Exception {
		int saveInterval=100*1000; 
		
		//load image mapFiles
		String imageMapFilesPath="Q:/Photos_MEva13_9M_MapFiles/";
		MapFile.Reader[] imgMapFiles= General_Hadoop.openAllMapFiles(new String[]{imageMapFilesPath});
		System.out.println("load mapFiles finished! tot-"+imgMapFiles.length);
		
		String saveQueryImageDir="O:/GVR_TMM/GVR/checkQ/";
		
		String rankPathBase="O:/GVR_TMM/GVR/";
		String[] rankPaths={"rank_VisNN_Vis_noGlobFilter_noSameUser_no1U1P_D9M_SURFHD18-20-18_1vs1AndHPM6-1000",
							"rank_MeanShift_Num_noGlobFilter_noSameUser_no1U1P_reRank@100_maxI@100_bandSca@1.0E-5_D9M_SURFHD18-20-18_1vs1AndHPM6-1000",
							"rank_GVR_Vis_noGlobFilter_noSameUser_1U1P_reRank@20_visSca@100_expSca@0.0010_D9M_SURFHD18-20-18_1vs1AndHPM6-1000"};
		
		String[] rankLabels={"VisNN_Vis_noGlobFilter_noSameUser_no1U1P", "MeanShift_Num_noGlobFilter_noSameUser_no1U1P", "GVR_Vis_noGlobFilter_noSameUser_1U1P"};
				
		General.addStrArr_prefx(rankPathBase, rankPaths);
		General.addStrArr_append(rankPaths, "/part-r-00000");
		//******* read latlons **************
		float[][] latlons=(float[][]) General.readObject("O:/MediaEval13/MEval13_latlons.floatArr");
		long[] userIDs_0=(long[]) General.readObject("O:/MediaEval13/MEval13_userIDs_0.long");
		int[] userIDs_1=(int[]) General.readObject("O:/MediaEval13/MEval13_userIDs_1.int");
		UserIDs userID=new UserIDs(userIDs_0, userIDs_1);
		
		float isPredRightScale=(float) 0.01;//1km, to check query prediction
		
		ArrayList<Integer> goodQuerys=new ArrayList<Integer>();
		goodQuerys.add(8651319);
		
		saveQueryImageDir+="Q"+goodQuerys+"_isSameLoc"+new DecimalFormat("0.000").format(isPredRightScale)+"/";
		General.makeORdelectFolder(saveQueryImageDir);
		
		//show goood querys
		int showTopLoc=20;
    	String rankShowPath=saveQueryImageDir;	
    	int selQueryNum=goodQuerys.size();
    	//html title
    	String HtmlTitle="show ranks for goodQuerys: \n"+General.StrArrToStr(rankPaths, "\n")
    			+"G_ForGTSize=1km, V_ForGTSize=1,000 \n"
    			+"qualified query num:"+selQueryNum+": "+goodQuerys;
    	System.out.println("HtmlTitle: "+HtmlTitle);
		General_IR.showGeoReleventRanks(goodQuerys, rankShowPath, HtmlTitle, rankLabels, rankPaths, showTopLoc, latlons, userID, isPredRightScale, saveInterval, "MapFile", null, 0, imgMapFiles);
		//clean-up
    	General_Hadoop.closeAllMapFiles(imgMapFiles);
		
		System.out.println("done!"); 	
    	
	}

	public static void showRanks_3M_GlobalFeat_GVR() throws Exception {
		int saveInterval=100*1000; int total_photos=3185258;  
		String imageBasePath="O:/MediaEval_3185258Images/trainImages_1-3185258/";
		String rankPathBase="O:/ICMR2013/GVR/3M/";
		ArrayList<String> rankPaths=new ArrayList<String>(), rankLabels=new ArrayList<String>();	
		//******* read latlons **************
		float[][] latlons=(float[][]) General.readObject("D:/xinchaoli/Desktop/My research/Database/FlickrPhotos/image-meta/3M_latlon.float2");
		long[] userIDs_0=(long[]) General.readObject("D:/xinchaoli/Desktop/My research/Database/FlickrPhotos/image-meta/3M_userIDs_0.long");
		int[] userIDs_1=(int[]) General.readObject("D:/xinchaoli/Desktop/My research/Database/FlickrPhotos/image-meta/3M_userIDs_1.int");
		UserIDs userID=new UserIDs(userIDs_0, userIDs_1);
		
    	int showTopRankDoc=10;  int showQueryNum=50; int minGTSize=-1, maxGTSize=Integer.MAX_VALUE;
    	float isSameLoc=(float) 0.01;
    	int[] rankInd_right,rankInd_wrong;
    	
//		String[] classArray = {"CEDD", "EdgeHistogram", "FCTH", "ColorLayout", "PHOG", "JCD", "Gabor", "JpegCoefficientHistogram", "Tamura", "LuminanceLayout", "OpponentHistogram", "ScalableColor"};
    	String feat;
    	
    	//****** show: feat rank   ***********// 
//    	feat=classArray[4];
    	feat="Concept";
    	rankPaths.add("rank_D3M_Q100K_"+feat+"_Vis_Ori_topDoc200_sameUser/part-r-00000");
    	rankPaths.add("rank_D3M_Q100K_"+feat+"_Vis_Ori_topDoc200_noSameUser/part-r-00000");
    	rankPaths.add("rank_D3M_Q100K_"+feat+"_Vis_GVR_topDoc200_reRankScale500_VisSca500_expSca0.01_sameUser/part-r-00000");
    	rankPaths.add("rank_D3M_Q100K_"+feat+"_Vis_GVR_topDoc200_reRankScale500_VisSca500_expSca0.01_noSameUser/part-r-00000");
    	rankLabels.add(feat+"_Ori_SamU"); rankLabels.add(feat+"_Ori_noSamU"); rankLabels.add(feat+"_GVR_SamU"); rankLabels.add(feat+"_GVR_noSamU");
    	//GVR_noSameU is right, Ori_noSameU is wrong
    	rankInd_right=new int[]{1};  rankInd_wrong=new int[]{3}; 
    	General_IR.showRanks_GoodVsWrong_geoRel( rankInd_right,  rankInd_wrong,  rankLabels.toArray(new String[0]),  rankPaths.toArray(new String[0]),  rankPathBase,  showTopRankDoc, 
    			 latlons, userID,  isSameLoc,  showQueryNum, 
    			 saveInterval,  "PhotoFile_3M",  imageBasePath, total_photos, null, minGTSize, maxGTSize);
    	//Ori_noSameU is right, GVR_noSameU is wrong
    	rankInd_right=new int[]{3};  rankInd_wrong=new int[]{1}; 
    	General_IR.showRanks_GoodVsWrong_geoRel( rankInd_right,  rankInd_wrong,  rankLabels.toArray(new String[0]),  rankPaths.toArray(new String[0]),  rankPathBase,  showTopRankDoc, 
    			 latlons, userID,  isSameLoc,  showQueryNum, 
    			 saveInterval,  "PhotoFile_3M",  imageBasePath, total_photos, null, minGTSize, maxGTSize);
    	
		System.out.println("done!");
	}
	
	public static void showRanks_3M_SURF_GVR() throws Exception {
		int saveInterval=100*1000; int total_photos=3185258;  
		String imageBasePath="O:/MediaEval_3185258Images/trainImages_1-3185258/";
		String rankPathBase="O:/ICMR2013/GVR/3M/";
		String[] rankPaths={"rank_D3M_Q100K_HD12_Vis_Ori_topDoc200_sameUser/part-r-00000","rank_D3M_Q100K_HD12_Vis_Ori_topDoc200_noSameUser/part-r-00000",
							"rank_D3M_Q100K_HD12_Vis_GVR_topDoc200_reRankScale500_VisSca500_expSca0.01_sameUser/part-r-00000","rank_D3M_Q100K_HD12_Vis_GVR_topDoc200_reRankScale500_VisSca500_expSca0.01_noSameUser/part-r-00000",
							"rank_D3M_Q100K_HD12_VisConRankThr_Ori_topDoc200_conRankThr200_sameUser/part-r-00000","rank_D3M_Q100K_HD12_VisConRankThr_Ori_topDoc200_conRankThr200_noSameUser/part-r-00000",
							"rank_D3M_Q100K_HD12_VisConRankThr_GVR_topDoc200_reRankScale500_VisSca500_expSca0.01_conRankThr200_sameUser/part-r-00000","rank_D3M_Q100K_HD12_VisConRankThr_GVR_topDoc200_reRankScale500_VisSca500_expSca0.01_conRankThr200_noSameUser/part-r-00000"};
		
		String[] rankLabels={"Ori_sameU","Ori_noSameU","GVR_sameU","GVR_noSameU","Ori_sameU_conRankThr","Ori_noSameU_conRankThr","GVR_sameU_conRankThr","GVR_noSameU_conRankThr"};
		
		//******* read latlons **************
		float[][] latlons=(float[][]) General.readObject("D:/xinchaoli/Desktop/My research/Database/FlickrPhotos/image-meta/3M_latlon.float2");
		long[] userIDs_0=(long[]) General.readObject("D:/xinchaoli/Desktop/My research/Database/FlickrPhotos/image-meta/3M_userIDs_0.long");
		int[] userIDs_1=(int[]) General.readObject("D:/xinchaoli/Desktop/My research/Database/FlickrPhotos/image-meta/3M_userIDs_1.int");
		UserIDs userID=new UserIDs(userIDs_0, userIDs_1);
    	
    	int showTopRankDoc=10;  int showQueryNum=50; int minGTSize=-1, maxGTSize=Integer.MAX_VALUE;
    	float isSameLoc=(float) 0.01;
    	int[] rankInd_right,rankInd_wrong;
    	
    	//****** show: GVR_noSameU is right, Ori_noSameU is wrong  ***********// 
    	rankInd_right=new int[]{3};  rankInd_wrong=new int[]{1}; 
    	General_IR.showRanks_GoodVsWrong_geoRel( rankInd_right,  rankInd_wrong,  rankLabels,  rankPaths,  rankPathBase,  showTopRankDoc, 
    			 latlons, userID,  isSameLoc,  showQueryNum, 
    			 saveInterval,  "PhotoFile_3M",  imageBasePath, total_photos, null, minGTSize, maxGTSize);
    	
    	//****** show: Ori_noSameU is right, GVR_noSameU is wrong  ***********// 
    	rankInd_right=new int[]{1};  rankInd_wrong=new int[]{3}; 
    	General_IR.showRanks_GoodVsWrong_geoRel( rankInd_right,  rankInd_wrong,  rankLabels,  rankPaths,  rankPathBase,  showTopRankDoc, 
   			 	latlons, userID,  isSameLoc,  showQueryNum, 
   			 saveInterval,  "PhotoFile_3M",  imageBasePath, total_photos, null, minGTSize, maxGTSize);
    	
    	//****** show: GVR_noSameU_conRankThr is right, GVR_noSameU is wrong  ***********// 
    	rankInd_right=new int[]{7};  rankInd_wrong=new int[]{3}; 
    	General_IR.showRanks_GoodVsWrong_geoRel( rankInd_right,  rankInd_wrong,  rankLabels,  rankPaths,  rankPathBase,  showTopRankDoc, 
   			 	latlons, userID,  isSameLoc,  showQueryNum, 
   			 	saveInterval,  "PhotoFile_3M",  imageBasePath, total_photos, null, minGTSize, maxGTSize);
    	
    	//****** show: GVR_noSameU is right, GVR_noSameU_conRankThr is wrong  ***********// 
    	rankInd_right=new int[]{3}; rankInd_wrong=new int[]{7}; 
    	General_IR.showRanks_GoodVsWrong_geoRel( rankInd_right,  rankInd_wrong,  rankLabels,  rankPaths,  rankPathBase,  showTopRankDoc, 
   			 	latlons, userID,  isSameLoc,  showQueryNum, 
   			 	saveInterval,  "PhotoFile_3M",  imageBasePath, total_photos, null, minGTSize, maxGTSize);
    	
		System.out.println("done!");
	}
	
	public static void showRanks_MediaEval13_GVR() throws Exception {
		int saveInterval=100*1000; 
		
		//load image mapFiles
		String imageMapFilesPath="Q:/Photos_MEva13_9M_MapFiles/";
		MapFile.Reader[] imgMapFiles= General_Hadoop.openAllMapFiles(new String[]{imageMapFilesPath});
		System.out.println("load mapFiles finished! tot-"+imgMapFiles.length);
		
		String rankPathBase="O:/GVR_TMM/GVR/";
		String[] rankPaths={"rank_VisNN_Vis_noGlobFilter_noSameUser_no1U1P_D9M_SURFHD18-20-18_1vs1AndHPM6-1000",
							"rank_MeanShift_Num_noGlobFilter_noSameUser_no1U1P_reRank@100_maxI@100_bandSca@1.0E-5_D9M_SURFHD18-20-18_1vs1AndHPM6-1000",
							"rank_GVR_Vis_noGlobFilter_noSameUser_1U1P_reRank@20_visSca@100_expSca@0.0010_D9M_SURFHD18-20-18_1vs1AndHPM6-1000"};
		
		String[] rankLabels={"VisNN_Vis_noGlobFilter_noSameUser_no1U1P", "MeanShift_Num_noGlobFilter_noSameUser_no1U1P", "GVR_Vis_noGlobFilter_noSameUser_1U1P"};
		
		General.addStrArr_prefx(rankPathBase, rankPaths);
		General.addStrArr_append(rankPaths, "/part-r-00000");
		//******* read latlons **************
		float[][] latlons=(float[][]) General.readObject("O:/MediaEval13/MEval13_latlons.floatArr");
		long[] userIDs_0=(long[]) General.readObject("O:/MediaEval13/MEval13_userIDs_0.long");
		int[] userIDs_1=(int[]) General.readObject("O:/MediaEval13/MEval13_userIDs_1.int");
		UserIDs userID=new UserIDs(userIDs_0, userIDs_1);
		
    	int showTopRankDoc=5;  int showQueryNum=50; int minGTSize=6, maxGTSize=10;
    	float isSameLoc=(float) 0.01;
    	int[] rankInd_right,rankInd_wrong;
    	
    	//****** show:  right vs is wrong  ***********// 
    	rankInd_right=new int[]{2};  rankInd_wrong=new int[]{0,1}; 
    	General_IR.showRanks_GoodVsWrong_geoRel( rankInd_right,  rankInd_wrong,  rankLabels,  rankPaths,  rankPathBase,  showTopRankDoc, 
    			 latlons, userID, isSameLoc,  showQueryNum, 
    			 saveInterval,  "MapFile",  null, 0, imgMapFiles, minGTSize, maxGTSize);
    	
    	//clean-up
    	General_Hadoop.closeAllMapFiles(imgMapFiles);
    	
		System.out.println("done!");
	}
	
	public static void showRanks_SanFrancisco_GVR() throws Exception {
		int saveInterval=100*1000; 
		
		//load image mapFiles
		String imageMapFilesPath="N:/ewi/insy/MMC/XinchaoLi/PhotoDataBase/SanFrancisco_StreetView/SanFrancisco_inSInd_MFiles/";
		MapFile.Reader[] imgMapFiles= General_Hadoop.openAllMapFiles(new String[]{imageMapFilesPath});
		System.out.println("load mapFiles finished! tot-"+imgMapFiles.length);
		
		String rankPathBase="O:/SanFrancisco_StreetView/Res/";
		String[] rankPaths={"rank_VisNN_Vis_noGlobFilter_SameUser_no1U1P_DSanFran_SURFHD20-12-20_1vs1AndHistAndAngle@0.52@0.2-1000",
							"rank_GVR_Vis_noGlobFilter_SameUser_no1U1P_reRank@10_visSca@100_expSca@0.01_DSanFran_SURFHD20-12-20_1vs1AndHistAndAngle@0.52@0.2-1000"};
		
		String[] rankLabels={"VisNN","GVR"};
		
		General.addStrArr_prefx(rankPathBase, rankPaths);
		General.addStrArr_append(rankPaths, "/part-r-00000");
		//******* read latlons **************
		float[][] latlons=(float[][]) General.readObject("Q:/PhotoDataBase/SanFrancisco_StreetView/SanFrancisco_Q-DPCI_latlons.floatArr");
		UserIDs userID=new UserIDs(null, null);
    	
    	int showTopRankDoc=10;  int showQueryNum=5; int minGTSize=1, maxGTSize=Integer.MAX_VALUE;
    	float isSameLoc=(float) 0.005;
    	int[] rankInd_right=null,rankInd_wrong=null;
    	
    	//****** show:  right vs is wrong  ***********// 
//    	rankInd_right=new int[]{1}; 
    	rankInd_wrong=new int[]{0}; 
    	General_IR.showRanks_GoodVsWrong_geoRel( rankInd_right,  rankInd_wrong,  rankLabels,  rankPaths,  rankPathBase,  showTopRankDoc, 
    			 latlons, userID,  isSameLoc,  showQueryNum, 
    			 saveInterval,  "MapFile",  null, 0, imgMapFiles, minGTSize, maxGTSize);
    	
    	//clean-up
    	General_Hadoop.closeAllMapFiles(imgMapFiles);
    	
		System.out.println("done!");
	}
	
}
