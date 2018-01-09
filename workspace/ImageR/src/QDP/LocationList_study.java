//package QDP;
//
//import java.io.FileOutputStream;
//import java.io.OutputStreamWriter;
//import java.io.PrintWriter;
//import java.text.DecimalFormat;
//import java.text.NumberFormat;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashMap;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.MapFile;
//
//import MyAPI.General.General;
//import MyAPI.General.General_geoRank;
//import MyCustomedHaoop.ValueClass.GeoExpansionData.fistMatch_GTruth_Docs_GVSizes_docScores;
//
//public class LocationList_study {
//
//	public static void main(String[] args) throws Exception {
//		DecimalFormat percformat = (DecimalFormat)NumberFormat.getPercentInstance();
//	    percformat.applyPattern("00.0%");
//	    double percentage;
//	    
//	    String PhotoOriPath_3MFlickr="O:/MediaEval_3185258Images/trainImages_1-3185258/";
//		int saveInterval=100*1000; int total_photos=3185258;
//		
//		int random=100*1000; String label;MapFile.Reader MapFileR_oriRank, MapFileR_gvrRank;
////		ArrayList<String[]> photoPaths_list, photoDiscrptions_list;
//		
//		String servePath="D:/xinchaoli/Desktop/My research/My Code/DataSaved/";
//		
//		String dataLabel="10M";
//		
//		String basePath=servePath+"ICMR2013/QDP/"+dataLabel+"/";	
//		
//		String Q_rank_Path=servePath+"ICMR2013/GVR/"+dataLabel+"/";
//		
//		float isSameLocScale=(float) 0.1; 
//		int totalQueryNum;  int showTopScore=10; 
//		//set FileSystem
//		Configuration conf = new Configuration();
//        FileSystem hdfs  = FileSystem.get(conf);
//        //******* read latlons **************
////      	float[][] latlons=(float[][]) General.readObject("D:/xinchaoli/Desktop/My research/Database/FlickrPhotos/image-meta/3M_latlon.float2");
//      	float[][] latlons=(float[][]) General.readObject("D:/xinchaoli/Desktop/My research/My Code/DataSaved/ICMR2013/10M_selectedPhotos_LatLon.float2");
//      			
// 		//******* load geoVisual-rank  ********//
// 		int visScale=500; double geoExpanScale=0.1;  int avelocLen_toploc=10;
// 		int[] topDocnums={100,200,300,400,500,1000};
//// 		for (int topDoc:topDocnums) {
// 			int topDoc=300;
//	 		label="_topDoc"+topDoc+"_VisScal_"+visScale+"_expScal_"+geoExpanScale;
//	      	//set score-out put
////			PrintWriter locRank_gvrRight = new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"trueLocRank"+label+".gvrRight"), "UTF-8")); 
//			PrintWriter locRank_gvrRight = new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"trueLocRank_fromLocList_isSameLoc"+isSameLocScale+label+".gvrRight"), "UTF-8")); 
//	     	// read rank from MapFile 
//	 		MapFileR_gvrRank=new MapFile.Reader(hdfs,Q_rank_Path+"GVR_D"+dataLabel+"_Q100K"+label+"/part-r-00000", conf);
//	 		System.out.println("MapFileR_gvrRank Key-Class: "+MapFileR_gvrRank.getKeyClass().getName());
//	 		System.out.println("MapFileR_gvrRank Value-Class: "+MapFileR_gvrRank.getValueClass().getName());
//	 		IntWritable Key_queryName= new IntWritable();
//	 		fistMatch_GTruth_Docs_GVSizes_docScores Value_RankScore= new fistMatch_GTruth_Docs_GVSizes_docScores();
//	// 		photoPaths_list=new ArrayList<String[]>(); photoDiscrptions_list=new ArrayList<String[]>();  
//	 		
//	// 		//set show photo HashMap
//	// 		HashMap<Integer,Integer> showPhotos= new HashMap<Integer,Integer>();  String folderName;
//	// 		int[] transIndex_StoL=(int[]) General.readObject("D:/xinchaoli/Desktop/My research/My Code/DataSaved/ICMR2013/10M_transIndex_StoL.intArr");
//	// 		int[] bins_GVSize={0,10,20,40,60,100};  int max_show_eachBin=40; 
//	// 		String showPhotoPath="Q:/ICMR2013/show_top10_location_photos/";
//	// 		MapFile.Reader MapFileR_showPhotos=new MapFile.Reader(hdfs, showPhotoPath+"MapFile", conf);
//	 		//***** process *********
//	 		int num_topLocations=100; 
//	 		double[] isOneLocScales={0,0.001,0.01,0.1,1,10}; int[] rankBins={1,10,20,30,40,50,100}; 
//	 		for(double isOneLocScale:isOneLocScales){
////	 		double isOneLocScale=10;
//		 		int rank1=0; int rank2_=0; totalQueryNum=0; double avelocPhotoNum=0; double maxlocPhotoNum=0; ArrayList<Integer> locSizes=new ArrayList<Integer>(); StringBuffer trueLocRankStatic=new StringBuffer(); int[] hist=new int[rankBins.length]; 
//	//	 		int[] showNum_rightInTop=new int[bins_GVSize.length+1]; int[] showNum_notInTop=new int[bins_GVSize.length+1];
//		 		while(MapFileR_gvrRank.next(Key_queryName, Value_RankScore)){ //loop over all queries, key-value(query-rank)
//		 			int queryName=Key_queryName.get();
//		 			int[] topDocs = Value_RankScore.Docs; 
//		 			float[] docScores = Value_RankScore.docScores; 
//		 			int GTSize=Value_RankScore.GTruth.size();
//		 			int[] ori_fistMatch=Value_RankScore.fistMatch; //two elements, 1st: firstMatch's Rank, 2nd: firstMatch's photoName
//		 			int firstMatch_rank=ori_fistMatch[0]; int firstMatch_name=ori_fistMatch[1];
//		 			if (docScores.length!=0){ //some query do not have any match in the whole dataSet
//			 			//get top Location Doc
//	//	 				ArrayList<Integer> topLocationIndex=new ArrayList<Integer>(); //topLocationDocs's index in topDocs
//	//					ArrayList<Integer> topLocationDocs=General_geoRank.get_topLocationDocs_nameFrom1(num_topLocationDocs, topDocs, (float) isOneLocScale, latlons,topLocationIndex); //do not do location selection.
//	//					ArrayList<Integer> topLocationDocs=General_geoRank.get_topLocationDocs(num_topLocationDocs, topDocs, (float) isOneLocScale, latlons,topLocationIndex); //do not do location selection.
//	//					ArrayList<ArrayList<Integer>> LocList=General_geoRank.get_topLocationDocsList_nameFrom1(num_topLocationDocs, topDocs,  (float) isOneLocScale,  latlons);
//		 				ArrayList<ArrayList<Integer>> LocList=General_geoRank.get_topLocationDocsList(num_topLocations, topDocs,  (float) isOneLocScale,  latlons);
//						//get ave-locationList length
//		 				double aveLen=0; int avelocLen_toploc_act=Math.min(avelocLen_toploc, LocList.size());
//		 				for (int i = 0; i < avelocLen_toploc_act; i++) {
//		 					aveLen+=LocList.get(i).size();
//		 					maxlocPhotoNum=Math.max(maxlocPhotoNum, LocList.get(i).size());
//		 					locSizes.add(LocList.get(i).size());
//						}
//		 				aveLen/=avelocLen_toploc_act;
//		 				avelocPhotoNum+=aveLen;
//		 				//get True-Location rank
//	//					int trueLocRank=General_geoRank.get_trueLocRank_nameFrom1(queryName, topLocationDocs, isSameLocScale, latlons);
//	//					int trueLocRank=General_geoRank.get_trueLocRank_fromList_nameFrom1(queryName, LocList, isSameLocScale, latlons);
//						int trueLocRank=General_geoRank.get_trueLocRank_fromList(queryName, LocList, isSameLocScale, latlons);
//						//get rank-position static
//						if(trueLocRank!=0){
//							int binInd=General.getBinIndex(rankBins, trueLocRank);
//							hist[binInd]++;
//							if(trueLocRank==1)
//								rank1++;
//							else
//								rank2_++;
//						}
//	//					//make showPhotos hashMap
//	//					if(!showPhotos.containsKey(transIndex_StoL[queryName])) //add query
//	//						showPhotos.put(transIndex_StoL[queryName],queryName);
//	//					if(firstMatch_rank!=0) //add first-match photo, firstMatch_rank==0 means true match do not exist in the doc list.
//	//						if(!showPhotos.containsKey(transIndex_StoL[firstMatch_name])) 
//	//							showPhotos.put(transIndex_StoL[firstMatch_name],firstMatch_name);
//	//					for(int i=0;i<LocList.size();i++){ // add loc-photos
//	//						for(int photoID:LocList.get(i)){
//	//							if(!showPhotos.containsKey(transIndex_StoL[photoID]))
//	//								showPhotos.put(transIndex_StoL[photoID],photoID);
//	//						}
//	//					}
//	//					//show photos--make photo description
//	//					ArrayList<ArrayList<String>> descriptions=new ArrayList<ArrayList<String>>(); 
//	//					for(int i=0;i<LocList.size();i++){// make description for loclist photos
//	//						ArrayList<String> oneList=new ArrayList<String>();
//	//						for(int j=0;j<LocList.get(i).size();j++){
//	//							int photoName=LocList.get(i).get(j);
//	//							String trueLoc="_FLoc";
//	//							if(General_geoRank.isOneLocation(latlons[0][queryName],latlons[1][queryName],latlons[0][photoName],latlons[1][photoName],isSameLocScale)){
//	//								trueLoc="_TLoc"; //this is a true location
//	//							}
//	//							oneList.add("Q"+queryName+"_loc"+(i+1)+"_P"+j+trueLoc+"_ID"+photoName+"_lat"+new DecimalFormat("0.00").format(latlons[0][photoName])+"_lon"+new DecimalFormat("0.00").format(latlons[1][photoName]));
//	//						}
//	//						descriptions.add(oneList);
//	//					}
//	//					//show photos--add query, first match to locList and description list for show
//	//					ArrayList<String> oneDescrList_forQuery=new ArrayList<String>(); // make description for query and first match
//	//					ArrayList<Integer> onelocList_forQuery=new ArrayList<Integer>(); // add queryName and first match name to loclist
//	//					oneDescrList_forQuery.add("Q"+queryName+"_loc"+0+"_lat"+new DecimalFormat("0.00").format(latlons[0][queryName])+"_lon"+new DecimalFormat("0.00").format(latlons[1][queryName])+"_oriT"+ori_fistMatch);
//	//					onelocList_forQuery.add(queryName);
//	//					if(firstMatch_rank!=0) {//true match do exist in the doc list.
//	//						oneDescrList_forQuery.add("Q"+queryName+"_loc"+0+"_fistMatch_lat"+new DecimalFormat("0.00").format(latlons[0][queryName])+"_lon"+new DecimalFormat("0.00").format(latlons[1][queryName])+"_oriT"+ori_fistMatch);
//	//						onelocList_forQuery.add(firstMatch_name);
//	//					}
//	//					descriptions.add(oneDescrList_forQuery);
//	//					LocList.add(onelocList_forQuery);
//	//					//show photos--save photo to file
//	//					int binIndex=General.getBinIndex(bins_GVSize,GTSize);
//	//					if(trueLocRank<=10 && trueLocRank>=1){
//	//						folderName=showPhotoPath+"isSameLocScale_"+isSameLocScale+"/isOneLocScale_"+isOneLocScale+"/"+"right_in_top10/";
//	//						General.showLocationList_BinIndex(bins_GVSize, showNum_rightInTop, binIndex, max_show_eachBin, folderName, LocList, descriptions, MapFileR_showPhotos);
//	//					}else{
//	//						folderName=showPhotoPath+"isSameLocScale_"+isSameLocScale+"/isOneLocScale_"+isOneLocScale+"/"+"not_in_top10/";
//	//						General.showLocationList_BinIndex(bins_GVSize, showNum_notInTop, binIndex, max_show_eachBin, folderName, LocList, descriptions, MapFileR_showPhotos);
//	//					}
//						//updata totalQueryNum
//						totalQueryNum++;
//		 			}else{
//	//	 				System.out.println("query:"+queryName+", do not have matches!!");
//		 			}
//		 		}
//		 		//get loc-size statics
//		 		avelocPhotoNum/=totalQueryNum;
//		 		Integer[] locSizeIntegers=locSizes.toArray(new Integer[0]);
//		 		Arrays.sort(locSizeIntegers);
//		 		float[] hist_percent=General.normliseArr(hist, totalQueryNum);
//		 		General.dispInfo(locRank_gvrRight, "isSameLocScale:"+isSameLocScale+", run for isOneLocScale"+isOneLocScale+", totalQueryNum: "+totalQueryNum+", rank1: "+rank1+", rank2_: "+rank2_);
//		 		General.dispInfo(locRank_gvrRight, General.IntArrToString(rankBins, "\t")+"\n"+General.floatArrToString(hist_percent, "\t","0.000"));
//		 		General.dispInfo(locRank_gvrRight, "in top "+avelocLen_toploc+" locs, medianLocPhotoNum: "+locSizeIntegers[locSizeIntegers.length/2]+", avelocPhotoNum :"+new DecimalFormat("0.0").format(avelocPhotoNum)+", maxlocPhotoNum:"+maxlocPhotoNum);
//		 		General.dispInfo(locRank_gvrRight,"");
//		 		MapFileR_gvrRank.reset();
//	 		}
//	// 		General.dispInfo(locRank_gvrRight, "total unqiue photos in these query's top "+showTop+" locations:"+showPhotos.size()+"\n");
//	// 		General.writeObject(basePath+dataLabel+"_photosToShow_top"+num_topLocations+"_locations.hashMap", showPhotos);
//	 		//clean-up
//	 		locRank_gvrRight.close();
//	 		MapFileR_gvrRank.close();
//	// 		MapFileR_showPhotos.close();
//// 		}
//	}
//}
