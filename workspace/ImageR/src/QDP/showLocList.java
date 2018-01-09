//package QDP;
//
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.io.OutputStreamWriter;
//import java.io.PrintWriter;
//import java.util.ArrayList;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.MapFile;
//
//import MyAPI.General.General;
//import MyAPI.General.General_geoRank;
//import MyCustomedHaoop.ValueClass.GeoExpansionData.fistMatch_GTruth_Docs_GVSizes_docScores;
//import MyCustomedHaoop.ValueClass.GeoExpansionData.fistMatch_GTruth_Docs_GVSizes_docScores;
//
//public class showLocList {
//
//	public static void main(String[] args) throws IOException, ClassNotFoundException {
//		
//		String PhotoOriPath_3MFlickr="O:/MediaEval_3185258Images/trainImages_1-3185258/";
//		int saveInterval=100*1000; int total_photos=3185258;
//		
//		int random=100*1000; String label;MapFile.Reader MapFileR_oriRank, MapFileR_gvrRank;
////		ArrayList<String[]> photoPaths_list, photoDiscrptions_list;
//		
//		String servePath="D:/xinchaoli/Desktop/My research/My Code/DataSaved/";
//		
//		String basePath=servePath+"ICMR2013/QDP/10M/";	
//		
//		String Q_rank_Path=servePath+"ICMR2013/GVR/10M/";
//		
//		int num_topLocationDocs;  float isSameLocScale=(float) 0.01; 
//		int totalQueryNum;  int showTopScore=10;
//		//set FileSystem
//		Configuration conf = new Configuration();
//        FileSystem hdfs  = FileSystem.get(conf);
//        //******* read latlons **************
////      	float[][] latlons=(float[][]) General.readObject("D:/xinchaoli/Desktop/My research/Database/FlickrPhotos/image-meta/3M_latlon.float2");
//      	float[][] latlons=(float[][]) General.readObject("D:/xinchaoli/Desktop/My research/My Code/DataSaved/ICMR2013/10M_selectedPhotos_LatLon.float2");
//      			
// 		//******* load geoVisual-rank  ********//
// 		int visScale=500; double geoExpanScale=0.01;
// 		label="_VisScal_"+visScale+"_expScal_"+geoExpanScale;
//      	//set score-out put
////		PrintWriter locRank_gvrRight = new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"trueLocRank"+label+".gvrRight"), "UTF-8")); 
//		PrintWriter locRank_gvrRight = new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"trueLocRank_fromLocList"+label+".gvrRight"), "UTF-8")); 
//     	// read rank from MapFile 
//// 		MapFileR_gvrRank=new MapFile.Reader(hdfs,Q_rank_Path+"GVR_D3M_Q100K"+label+"/part-r-00000", conf);
// 		MapFileR_gvrRank=new MapFile.Reader(hdfs,Q_rank_Path+"GVR_D10M_Q100K"+label+"/part-r-00000", conf);
// 		System.out.println("MapFileR_gvrRank Key-Class: "+MapFileR_gvrRank.getKeyClass().getName());
// 		System.out.println("MapFileR_gvrRank Value-Class: "+MapFileR_gvrRank.getValueClass().getName());
// 		IntWritable Key_queryName= new IntWritable();
// 		fistMatch_GTruth_Docs_GVSizes_docScores Value_RankScore= new fistMatch_GTruth_Docs_GVSizes_docScores();
//// 		photoPaths_list=new ArrayList<String[]>(); photoDiscrptions_list=new ArrayList<String[]>();  
// 		
// 		double[] isOneLocScales={0,0.001,0.005,0.01,0.012,0.015,0.1}; int[] rankBins={1,10,20,50,100,200}; 
// 		for(double isOneLocScale:isOneLocScales){
//	 		int rank1=0; int rank2_=0; totalQueryNum=0;   StringBuffer trueLocRankStatic=new StringBuffer(); int[] hist=new int[rankBins.length];
//	 		while(MapFileR_gvrRank.next(Key_queryName, Value_RankScore)){ //loop over all queries, key-value(query-rank)
//	 			int queryName=Key_queryName.get();
//	 			int[] topDocs = Value_RankScore.get_Docs(); 
//	 			float[] docScores = Value_RankScore.get_docScores(); 
//	 			int GTSize=Value_RankScore.get_GTruth().size();
//	 			num_topLocationDocs=topDocs.length; //use all doc!
//	 			if (docScores.length!=0){ //some query do not have any match in the whole dataSet
//		 			//get top Location Doc
////	 				ArrayList<Integer> topLocationIndex=new ArrayList<Integer>(); //topLocationDocs's index in topDocs
////					ArrayList<Integer> topLocationDocs=General_geoRank.get_topLocationDocs_nameFrom1(num_topLocationDocs, topDocs, (float) isOneLocScale, latlons,topLocationIndex); //do not do location selection.
////					ArrayList<Integer> topLocationDocs=General_geoRank.get_topLocationDocs(num_topLocationDocs, topDocs, (float) isOneLocScale, latlons,topLocationIndex); //do not do location selection.
////					ArrayList<ArrayList<Integer>> LocList=General_geoRank.get_topLocationDocsList_nameFrom1(num_topLocationDocs, topDocs,  (float) isOneLocScale,  latlons);
//	 				ArrayList<ArrayList<Integer>> LocList=General_geoRank.get_topLocationDocsList(num_topLocationDocs, topDocs,  (float) isOneLocScale,  latlons);
//					//get True-Location rank
////					int trueLocRank=General_geoRank.get_trueLocRank_nameFrom1(queryName, topLocationDocs, isSameLocScale, latlons);
////					int trueLocRank=General_geoRank.get_trueLocRank_fromList_nameFrom1(queryName, LocList, isSameLocScale, latlons);
//					int trueLocRank=General_geoRank.get_trueLocRank_fromList(queryName, LocList, isSameLocScale, latlons);
//					//get rank-position static
//					if(trueLocRank!=0){
//						int binInd=General.getBinIndex(rankBins, trueLocRank);
//						hist[binInd]++;
//						if(trueLocRank==1)
//							rank1++;
//						else
//							rank2_++;
//					}
//					//updata totalQueryNum
//					totalQueryNum++;
//	 			}else{
////	 				System.out.println("query:"+queryName+", do not have matches!!");
//	 			}
//	 		}
//	 		for(int i=0;i<hist.length;i++){
//	 			trueLocRankStatic.append(rankBins[i]+"_"+hist[i]+", ");
//	 		}
//	 		General.dispInfo(locRank_gvrRight, "isSameLocScale:"+isSameLocScale+", run for isOneLocScale"+isOneLocScale+", totalQueryNum: "+totalQueryNum+", rank1: "+rank1+", rank2_: "+rank2_);
//	 		General.dispInfo(locRank_gvrRight, trueLocRankStatic.toString());
//	 		General.dispInfo(locRank_gvrRight, "\n");
//	 		MapFileR_gvrRank.reset();
// 		}
// 		//clean-up
// 		locRank_gvrRight.close();
// 		MapFileR_gvrRank.close();
//
//	}
//
//}
