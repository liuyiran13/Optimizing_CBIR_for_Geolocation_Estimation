package BenchMarkTest;

import java.awt.Color;
import java.awt.List;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.General.General_IR;
import MyAPI.General.General_geoRank;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
import MyCustomedHaoop.ValueClass.IntArr;
import MyCustomedHaoop.ValueClass.IntList_FloatList;

public class stuy_SearchResult {


	public static void main(String[] args) throws Exception {
//		test_AP_function();
		
//		test_HerveImage();
		
//		test_Oxford();
		
//		test_Barcelona();
		
//		test_SanFrancisco();
		
//		test_3M_randQ_geoRelevance();
		
		test();
	}
	
	@SuppressWarnings("unchecked")
	public static void test_AP_function() throws Exception {
		/*
		 * test AP funtion with herver's O:\ImageRetrieval\HerveImage\ori_Data\eval_holidays\holidays_map.py
		 */
		String saveBasePath="O:/ImageRetrieval/HerveImage/";
		HashMap<Integer, HashSet<Integer>> groundTrue=(HashMap<Integer, HashSet<Integer>>) General.readObject(saveBasePath+"HerverImage_groundTruth.hashMap");
		
		BufferedReader inputStreamFeat = new BufferedReader(new InputStreamReader(new FileInputStream(saveBasePath+"ori_Data/eval_holidays/he_wgc_result_200806.dat"), "UTF-8"));
		String line1; int queryNum=0; float MAP=0;
		while((line1=inputStreamFeat.readLine())!=null){
			String[] info=line1.split(" "); 
			int listNum=(info.length-1)/2;
			if (listNum>0) {
				int queryName=Integer.valueOf(info[0].split(".j")[0]);
				int rankSize=Integer.valueOf(info[info.length-2])+1;
				System.out.println("Query:"+queryName);
				System.out.println("Rank Size:"+rankSize);
				ArrayList<Integer> oriIDs=new ArrayList<Integer>(rankSize);
				for (int i = 0; i < rankSize; i++) {
					oriIDs.add(0);
				}
				for (int i = 0; i < listNum; i++) {
					oriIDs.set(Integer.valueOf(info[2*i+1]),-Integer.valueOf(info[2*i+2].split(".j")[0]));
				}
				General_geoRank.removeQueryItself_forTopDocsScores(oriIDs, -queryName);
				float AP=General_IR.AP_smoothed(groundTrue.get(-queryName), oriIDs);
				System.out.println("queryName:"+queryName+", AP:"+AP);
				MAP+=AP;
			}
			queryNum++;	
		}
		MAP/=queryNum;
		System.out.println("total query-number:"+queryNum+", MAP:"+MAP);
	}
	
	@SuppressWarnings("unchecked")
	public static void test_HerveImage() throws Exception {

		//*********read rank from MapFile*********//
		Configuration conf = new Configuration();
	    FileSystem hdfs  = FileSystem.get(conf);
		String saveBasePath="O:/ImageRetrieval/Herve1.5K/";
	    String MapFile_path=saveBasePath+"CVPR15/mimicHPM/Rerank1000_Herve_1.5K.rank_top10000.mapFile";
		MapFile.Reader MapFileReader=new MapFile.Reader(hdfs, MapFile_path, conf); //MapFile_path+"/part-r-00000"
		System.out.println("MapFile Key-Class: "+MapFileReader.getKeyClass().getName());
		System.out.println("MapFile Value-Class: "+MapFileReader.getValueClass().getName());
		IntWritable Key= new IntWritable();
		IntList_FloatList Value= new IntList_FloatList();
		
		HashMap<Integer, HashSet<Integer>> groundTrue=(HashMap<Integer, HashSet<Integer>>) General.readObject(saveBasePath+"Herve_groundTruth.hashMap");
		int[] s_to_l=(int[]) General.readObject(saveBasePath+"Herve_ori1.5K_SelPhos_S_to_L.intArr");
		int queryNum=0; float MAP=0; ArrayList<Float> APs=new ArrayList<Float>();
		while(MapFileReader.next(Key, Value)){ //loop over all queries, key-value(query-rank)
			int queryName=Key.get();
			int listedNum=Value.getIntegers().size();
			System.out.println("Query:"+queryName);
			System.out.println("Rank Size:"+listedNum);
			ArrayList<Integer> oriIDs=new ArrayList<Integer>(Value.getIntegers().size());
			for (int i = 0; i < Value.getIntegers().size(); i++) {
				oriIDs.add(s_to_l[Value.getIntegers().get(i)]);
			}
			float AP=General_IR.AP_smoothed(groundTrue.get(queryName), oriIDs);
			System.out.println("queryName:"+queryName+", AP:"+AP);
			APs.add(AP);
			MAP+=AP;
			queryNum++;	
		}
		MAP/=queryNum;
		System.out.println("total query-number:"+queryNum+", MAP:"+MAP);
		MapFileReader.close();
		
		//*********setup: show rank in html *********//
		String imageMapFilesPath_oriData=saveBasePath+"HerverImage.mapFile/";//Q:/Photos_MEva13_9M_MapFiles/  O:/ImageRetrieval/Herve1.5K/HerverImage.mapFile/ Q:/PhotoDataBase/SanFrancisco_Landmark/SanFrancisco_MFiles/
//		String imageMapFilesPath_desctractors="Q:/Pho_UniDistra_10M_Flickr66M_Inter100K/";
		MapFile.Reader[] imgMapFiles= General_Hadoop.openAllMapFiles(new String[]{imageMapFilesPath_oriData});//Function readImgFromMFiles needs: the oriData must be labeled with negative phoID and only one mapFile and put into the last entry of imgMapFiles
		System.out.println("load mapFiles finished! tot-"+imgMapFiles.length);
		
		int saveInterval=100*1000; String dataFlag="_Herve_1.5K";

		int topShowQ=500; int topShowD=10; int topShowGT=5; DecimalFormat scoreFormat=new DecimalFormat("0.00");
		
		String rankPathBase=saveBasePath+"CVPR15/";
		String[] rankPaths={"mimicHPM/Rerank1000_Herve_1.5K.rank_top10000.mapFile/",
							"ranks_BOF/Show_Herve_1.5K_100K-VW_SURF_iniR-noBurst_HDs64-HMW0_ReR1K_HDr64_top1K_1vs1AndHistAndAngle@0.52@0.2@0@0@0@0@0@0@0_rankDocScore"};
		
		String[] rankLabels={"HPM", "1vs1AndHistAndAngle@0.52@0.2@0@0@0@0@0@0@0"};
		
		General.addStrArr_prefx(rankPathBase, rankPaths);
//		General.addStrArr_append(rankPaths, "/");// /, /part-r-00000
		
		String showRankDir=rankPathBase+"ranks_BOF/";
		
		int rankInd_right=1; int rankInd_wrong=0; float AP_thr=(float) 0.7; //if rankInd_wrong<0, then only show querys of rankInd_right that AP> AP_thr

		General_IR.showRanks_GoodVsWrong_APCompare(dataFlag, rankInd_right, rankInd_wrong, rankLabels, rankPaths, showRankDir, topShowD, topShowGT,
				groundTrue, s_to_l, null, scoreFormat, AP_thr, topShowQ, saveInterval, imgMapFiles);
		
		General_Hadoop.closeAllMapFiles(imgMapFiles);
	}
	
	@SuppressWarnings("unchecked")
	public static void test_Oxford() throws Exception {
		String saveBasePath="O:/ImageRetrieval/Oxford5K/";

//		//check query buildingName
//		HashMap<Integer, HashSet<Integer>> buildingInd_Name=(HashMap<Integer, HashSet<Integer>>) General.readObject(saveBasePath+"OxfordBuilding_buildingInd_Name.hashMap");
//		System.out.println(buildingInd_Name.get(-4));
//		HashMap<Integer, HashSet<Integer>> L_to_S=(HashMap<Integer, HashSet<Integer>>) General.readObject(saveBasePath+"Oxford_ori5K_SelPhos_L_to_S.hashMap");

		
		//*********setup: read rank from MapFile*********//
		Configuration conf = new Configuration();
	    FileSystem hdfs  = FileSystem.get(conf);
	    String MapFile_path=saveBasePath+"CVPR15/mimicHPM/Rerank1000_Oxford_5K_CutQ.rank_top10000.mapFile";
		MapFile.Reader MapFileReader=new MapFile.Reader(hdfs, MapFile_path, conf);
		System.out.println("MapFile Key-Class: "+MapFileReader.getKeyClass().getName());
		System.out.println("MapFile Value-Class: "+MapFileReader.getValueClass().getName());
		IntWritable Key= new IntWritable();
		IntList_FloatList Value= new IntList_FloatList();
//	
//		//calculate MAP
		HashMap<Integer, HashSet<Integer>> groundTrue=(HashMap<Integer, HashSet<Integer>>) General.readObject(saveBasePath+"OxfordBuilding_groundTruth.hashMap");
		HashMap<Integer, HashSet<Integer>> junks=(HashMap<Integer, HashSet<Integer>>) General.readObject(saveBasePath+"OxfordBuilding_junks.hashMap");
		HashMap<Integer, HashSet<Integer>> buildingInd_Name=(HashMap<Integer, HashSet<Integer>>) General.readObject(saveBasePath+"OxfordBuilding_buildingInd_Name.hashMap");
//		junks.get(-11).addAll(Arrays.asList(new Integer[]{-15184,-16105,-15034,-14897}));
//		junks.get(-9).addAll(Arrays.asList(new Integer[]{-12815}));
//		junks.get(-8).addAll(Arrays.asList(new Integer[]{-12204,-12205}));
//		junks.get(-6).addAll(Arrays.asList(new Integer[]{-15426,-12980}));
//		junks.get(-4).addAll(Arrays.asList(new Integer[]{-15744}));
//		junks.get(-3).addAll(Arrays.asList(new Integer[]{-14684,-12493,-12474}));
//		junks.get(-2).addAll(Arrays.asList(new Integer[]{-13226,-13255,-13214,-13216,-13221}));

		int[] s_to_l=(int[]) General.readObject(saveBasePath+"Oxford_ori5K_SelPhos_S_to_L.intArr");
		int queryNum=0; float MAP=0; ArrayList<Float> APs=new ArrayList<Float>();
		while(MapFileReader.next(Key, Value)){ //loop over all queries, key-value(query-rank)
			int queryName=Key.get();
			int listedNum=Value.getIntegers().size();
			System.out.println("Query:"+queryName);
			System.out.println("Rank Size:"+listedNum);
			ArrayList<Integer> oriIDs=new ArrayList<Integer>(Value.getIntegers().size());
			for (int i = 0; i < Value.getIntegers().size(); i++) {
				oriIDs.add(s_to_l[Value.getIntegers().get(i)]);
			}
			int buildingInd=queryName/1000;
			oriIDs.removeAll(junks.get(buildingInd));//remove junks
			float AP=General_IR.AP_smoothed(groundTrue.get(buildingInd), oriIDs);
			System.out.println(buildingInd_Name.get(buildingInd)+", queryName:"+queryName+", AP:"+AP);
			APs.add(AP);
			MAP+=AP;
			queryNum++;	
		}
		MAP/=queryNum;
		MapFileReader.close();
		System.out.println("total query-number:"+queryNum+", MAP:"+MAP);

		//*********setup: show rank in html *********//
		String imageMapFilesPath_oriData=saveBasePath+"OxfordBuilding.mapFile/";//Q:/Photos_MEva13_9M_MapFiles/  O:/ImageRetrieval/Herve1.5K/HerverImage.mapFile/ Q:/PhotoDataBase/SanFrancisco_Landmark/SanFrancisco_MFiles/
		MapFile.Reader[] imgMapFiles= General_Hadoop.openAllMapFiles(new String[]{imageMapFilesPath_oriData});//Function readImgFromMFiles needs: the oriData must be labeled with negative phoID and only one mapFile and put into the last entry of imgMapFiles
		System.out.println("load mapFiles finished! tot-"+imgMapFiles.length);
		int saveInterval=100*1000; String dataFlag="_Oxford_5K_AllCutQ";

		int topShowQ=100; int topShowD=20; int topShowGT=3; DecimalFormat scoreFormat=new DecimalFormat("0.00");
		
		String rankPathBase=saveBasePath+"CVPR15/";
		String[] rankPaths={"mimicHPM/Rerank1000_Oxford_5K_CutQ.rank_top10000.mapFile/",
							"ranks_BOF/Show_Oxford_5K_100K-VW_SURF_iniR-noBurst_HDs64-HMW0_ReR1K_HDr64_top1K_1vs1AndHistAndAngle@0.52@0.2@0@0@0@0@0@0@0_rankDocScore"};
		
		String[] rankLabels={"HPM", "1vs1AndHistAndAngle@0.52@0.2@0@0@0@0@0@0@0"};
		String showRankDir=rankPathBase+"ranks_BOF/";
		
		General.addStrArr_prefx(rankPathBase, rankPaths);
//		General.addStrArr_append(rankPaths, "");//part-r-00000
		
		int rankInd_right=0; int rankInd_wrong=1; float AP_thr=(float) 0.01; //if rankInd_wrong<0, then only show querys of rankInd_right that AP> AP_thr; if rankInd_right<0, AP_thr<0, then only show querys of rankInd_right that AP< AP_thr

		General_IR.showRanks_GoodVsWrong_APCompare(dataFlag, rankInd_right, rankInd_wrong, rankLabels, rankPaths, showRankDir, topShowD, topShowGT,
				groundTrue, s_to_l, junks, scoreFormat, AP_thr, topShowQ, saveInterval, imgMapFiles);
		
		
		General_Hadoop.closeAllMapFiles(imgMapFiles);
	}
	
	@SuppressWarnings("unchecked")
	public static void test_Barcelona() throws Exception {
		String saveBasePath="O:/ImageRetrieval/Barcelona1K/";
		//*********setup: read rank from MapFile*********//
		Configuration conf = new Configuration();
	    FileSystem hdfs  = FileSystem.get(conf);
	    String MapFile_path=saveBasePath+"CVPR15/mimicHPM/Rerank1000_Barcelona_1K.rank_top10000.mapFile";
		MapFile.Reader MapFileReader=new MapFile.Reader(hdfs, MapFile_path, conf);
		System.out.println("MapFile Key-Class: "+MapFileReader.getKeyClass().getName());
		System.out.println("MapFile Value-Class: "+MapFileReader.getValueClass().getName());
		IntWritable Key= new IntWritable();
		IntList_FloatList Value= new IntList_FloatList();

		//calculate MAP
		HashMap<Integer, HashSet<Integer>> groundTrue=(HashMap<Integer, HashSet<Integer>>) General.readObject(saveBasePath+"Barcelona_groundTruthBuildingID.hashMap");
		
		int[] s_to_l=(int[]) General.readObject(saveBasePath+"Barcelona_ori1K_SelPhos_S_to_L.intArr");
		int queryNum=0; float MAP=0; ArrayList<Float> APs=new ArrayList<Float>();
		while(MapFileReader.next(Key, Value)){ //loop over all queries, key-value(query-rank)
			int queryName=Key.get();
			int listedNum=Value.getIntegers().size();
			System.out.println("Query:"+queryName);
			System.out.println("Rank Size:"+listedNum);
			ArrayList<Integer> oriIDs=new ArrayList<Integer>(Value.getIntegers().size());
			for (int i = 0; i < Value.getIntegers().size(); i++) {
				oriIDs.add(s_to_l[Value.getIntegers().get(i)]);
			}
			int buildingInd=queryName/10000;
			float AP=General_IR.AP_smoothed(groundTrue.get(buildingInd), oriIDs);
			System.out.println("queryName:"+queryName+", AP:"+AP);
			APs.add(AP);
			MAP+=AP;
			queryNum++;	
		}
		MAP/=queryNum;
		MapFileReader.close();
		System.out.println("total query-number:"+queryNum+", MAP:"+MAP);

		//*********setup: show rank in html *********//
		String imageMapFilesPath_oriData=saveBasePath+"Barcelona1K.mapFile/";
//		String imageMapFilesPath_desctractors="Q:/Pho_UniDistra_10M_Flickr66M_Inter100K/";
		MapFile.Reader[] imgMapFiles= General_Hadoop.openAllMapFiles(new String[]{imageMapFilesPath_oriData});//Function readImgFromMFiles needs: the oriData must be labeled with negative phoID and only one mapFile and put into the last entry of imgMapFiles
		System.out.println("load mapFiles finished! tot-"+imgMapFiles.length);
		int saveInterval=100*1000; String dataFlag="_Barcelona_1K";

		int topShowQ=100; int topShowD=20;  int topShowGT=20; DecimalFormat scoreFormat=new DecimalFormat("0.00");
		
		String rankPathBase=saveBasePath+"CVPR15/";
		String[] rankPaths={"mimicHPM/Rerank1000_Barcelona_1K.rank_top10000.mapFile",
							"ranks_BOF/Show_Barcelona_1K_100K-VW_SURF_iniR-noBurst_HDs64-HMW0_ReR1K_HDr64_top1K_1vs1AndHistAndAngle@0.52@0.2@0@0@0@0@0@0@0_rankDocScore"};
		
		String[] rankLabels={"HPM", "1vs1AndHistAndAngle@0.52@0.2@0@0@0@0@0@0@0"};
		String showRankDir=rankPathBase+"ranks_BOF/";
		
		General.addStrArr_prefx(rankPathBase, rankPaths);
		General.addStrArr_append(rankPaths, "/");//  /, /part-r-00000
		
		int rankInd_right=0; int rankInd_wrong=1; float AP_thr=(float) 0.1; //if rankInd_wrong<0, then only show querys of rankInd_right that AP> AP_thr; if rankInd_right<0, AP_thr<0, then only show querys of rankInd_right that AP< AP_thr

		General_IR.showRanks_GoodVsWrong_APCompare(dataFlag, rankInd_right, rankInd_wrong, rankLabels, rankPaths, showRankDir, topShowD, topShowGT,
				groundTrue, s_to_l, null, scoreFormat, AP_thr, topShowQ, saveInterval, imgMapFiles);
		
		General_Hadoop.closeAllMapFiles(imgMapFiles);

	}

	@SuppressWarnings("unchecked")
	public static void test_SanFrancisco() throws Exception {

		//*********read rank from MapFile*********//
		Configuration conf = new Configuration();
	    FileSystem hdfs  = FileSystem.get(conf);
		String saveBasePath="Q:/PhotoDataBase/SanFrancisco_Landmark/";
		String MapFile_path=saveBasePath+"ranks/rank_SanFrancisco_20K-VW_HDs18-HMW20_ReR1000_HDr18_top1K_1vs1AndHPM6";
		MapFile.Reader MapFileReader=new MapFile.Reader(hdfs, MapFile_path+"/part-r-00000", conf);
		System.out.println("MapFile Key-Class: "+MapFileReader.getKeyClass().getName());
		System.out.println("MapFile Value-Class: "+MapFileReader.getValueClass().getName());
		IntWritable Key= new IntWritable();
		IntList_FloatList Value= new IntList_FloatList();
	
		HashMap<Integer, HashSet<Integer>> groundTrue=(HashMap<Integer, HashSet<Integer>>) General.readObject(saveBasePath+"SanFrancisco_groundTruth_onlyPCI_inSInd.hashMap");
		
		int queryNum=0; float MAP=0; ArrayList<Float> APs=new ArrayList<Float>(); float HR_1=0; 
		while(MapFileReader.next(Key, Value)){ //loop over all queries, key-value(query-rank)
			int queryName=Key.get();
			int listedNum=Value.getIntegers().size();
			System.out.println("Query:"+queryName);
			System.out.println("Rank Size:"+listedNum);
			ArrayList<Integer> oriIDs=Value.getIntegers();
			HashSet<Integer> relPhos=groundTrue.get(queryName);
			float AP=General_IR.AP_smoothed(relPhos, oriIDs);
			boolean cartoCorrect=relPhos.contains(oriIDs.get(0));
			System.out.println("queryName:"+queryName+", AP:"+AP+", cartoCorrect:"+cartoCorrect);
			APs.add(AP);
			MAP+=AP;
			HR_1+=cartoCorrect?1:0;
			queryNum++;	
		}
		MAP/=queryNum;
		HR_1/=queryNum;
		System.out.println("total query-number:"+queryNum+", MAP:"+MAP+", HR_1:"+HR_1);
		MapFileReader.close();
		
		//*********setup: show rank in html *********//
		//load image mapFiles
		String imageMapFilesPath="Q:/PhotoDataBase/SanFrancisco_Landmark/SanFrancisco_MFiles/";
		MapFile.Reader[] imgMapFiles= General_Hadoop.openAllMapFiles(new String[]{imageMapFilesPath});
		System.out.println("load mapFiles finished! tot-"+imgMapFiles.length);
		int saveInterval=100*1000; 
		String taskLabel="rank:"+MapFile_path+", MAP:"+MAP+", HR_1:"+HR_1;
		String rankShowPath=MapFile_path+"_inHTML/";//_inHTML, _inHTML_modifGTrueTop10
    	String rankShowPath_photos=rankShowPath+"photos/";
    	General.makeORdelectFolder(rankShowPath);
    	General.makeORdelectFolder(rankShowPath_photos);
		PrintWriter html = new PrintWriter(new OutputStreamWriter(new FileOutputStream(
				rankShowPath+"index.html",false), "UTF-8"),true);
		ArrayList<String> gtPath_ForHTML=new ArrayList<String>();
		ArrayList<String> gtCaption_ForHTML=new ArrayList<String>();
		ArrayList<String> gtColor_ForHTML=new ArrayList<String>();
		ArrayList<String> phoPath_ForHTML=new ArrayList<String>();
		ArrayList<String> phoCaption_ForHTML=new ArrayList<String>();
		ArrayList<String> phoColor_ForHTML=new ArrayList<String>();
		int topShowQ=50; int topShowD=10;  DecimalFormat scoreFormat=new DecimalFormat("0");
		//show ranks
		MapFileReader=new MapFile.Reader(hdfs, MapFile_path+"/part-r-00000/", conf); queryNum=0;
		while(MapFileReader.next(Key, Value)){ //loop over all queries, key-value(query-rank)
			int queryName=Key.get();
			int listedNum=Value.getIntegers().size();
			ArrayList<Integer> oriIDs=Value.getIntegers();
			//show rank
			if (queryNum<topShowQ) {
				//set query
				String queryFileName=General_IR.addPhotoPath_MovePhoto("MapFile", queryName, rankShowPath_photos, saveInterval, null, 0, imgMapFiles);
				String queryCap=""+queryName+"_AP:"+APs.get(queryNum)+"_listNum:"+listedNum;
				String queryCol="rgb("+General.floatArrToString_nolastDelimiter(General.mul(Color.black.getRGBColorComponents(null), 255), ",", "0")+")";
				//set query's ground truth
				gtPath_ForHTML.clear();
				gtCaption_ForHTML.clear();
				gtColor_ForHTML.clear();
				HashSet<Integer> groundTruths=groundTrue.get(queryName);
				int maxShowGTruth=9; int numShowGTruth=0;
				for(int gt: groundTruths){
					int rank=oriIDs.indexOf(gt);
					//set gTruth_photos
					gtPath_ForHTML.add(General_IR.addPhotoPath_MovePhoto("MapFile", gt, rankShowPath_photos, saveInterval, null, 0, imgMapFiles));
					//set gTruth_Caption
					gtCaption_ForHTML.add(rank+"_"+(rank==-1?-1:scoreFormat.format(Value.getFloats().get(rank))));
					//set gTruth_Color
					gtColor_ForHTML.add("rgb("+General.floatArrToString_nolastDelimiter(General.mul(Color.green.getRGBColorComponents(null), 255), ",", "0")+")");
					numShowGTruth++;
					if (numShowGTruth==maxShowGTruth) {//only show some ground truth
						break;
					}
				}
				//set docs
				phoPath_ForHTML.clear();
				phoCaption_ForHTML.clear();
				phoColor_ForHTML.clear();
				for (int i=0; i< Math.min(topShowD,oriIDs.size());i++) {
					int doc=oriIDs.get(i);
					//set photo
					phoPath_ForHTML.add(General_IR.addPhotoPath_MovePhoto("MapFile", doc, rankShowPath_photos, saveInterval, null, 0, imgMapFiles));
					//set photo_Caption
					phoCaption_ForHTML.add(i+"_"+doc+"_"+scoreFormat.format(Value.getFloats().get(i)));
					//set photo_Color
					if (groundTruths.contains(doc)) {
						phoColor_ForHTML.add("rgb("+General.floatArrToString_nolastDelimiter(General.mul(Color.green.getRGBColorComponents(null), 255), ",", "0")+")");
					}else{
						phoColor_ForHTML.add("rgb("+General.floatArrToString_nolastDelimiter(General.mul(Color.red.getRGBColorComponents(null), 255), ",", "0")+")");
					}
				}
				General.showPhoto_inHTML( html, taskLabel, queryNum==0?0:10,  rankShowPath_photos,  queryFileName, queryCap, queryCol,
						gtPath_ForHTML,gtCaption_ForHTML, gtColor_ForHTML, 3,
						phoPath_ForHTML, phoCaption_ForHTML, phoColor_ForHTML, null,null,null,null,0);
			}
			queryNum++;	
		}
		
		General.showPhoto_inHTML( html, taskLabel, 1,  rankShowPath_photos, null, null, null,
				null,null, null, 3,
				null, null, null, null,null,null,null,0);
		
		//clean-up
		html.close();
		MapFileReader.close();
	}
	
	public static void test_3M_randQ_geoRelevance() throws Exception {
		//read photos GPS into memory
		float[][] latlons=(float[][]) General.readObject("D:/xinchaoli/Desktop/My research/Database/FlickrPhotos/image-meta/3M_latlon.float2");
				
		//*********read rank from MapFile*********//
		Configuration conf = new Configuration();
	    FileSystem hdfs  = FileSystem.get(conf);
		String rankPath="O:/ImageRetrieval/SearchResult_D3M_Q100K_ICMR13_HD16/part-r-00000/";	
		MapFile.Reader MapFileReader_rank=new MapFile.Reader(hdfs, rankPath, conf);
		System.out.println("MapFileReader_rank Key-Class: "+MapFileReader_rank.getKeyClass().getName());
		System.out.println("MapFileReader_rank Value-Class: "+MapFileReader_rank.getValueClass().getName());
		IntWritable Key_queryName= new IntWritable();
		IntList_FloatList docs_scores= new IntList_FloatList();
		Integer[] firstTrueLocRank_bins={-1,0,5,10,50,100,200,1000};  Integer[] trueLocNum_bins={0,5,10,20,50,100,200,1000}; 
		int[] firstTrueLocRanks_hist=new int[firstTrueLocRank_bins.length+1]; int[] trueLocNum_hist_at200=new int[trueLocNum_bins.length+1];
		int queryNum=0;		
		while(MapFileReader_rank.next(Key_queryName, docs_scores)){ //loop over all queries, key-value(query-rank,score)
			int queryName=Key_queryName.get();
			//delete query itself
			if(docs_scores.getIntegers().get(0)==queryName){
				docs_scores.getIntegers().remove(0); 
				docs_scores.getFloats().remove(0);
			}else{//for some query, itself is not ranked first!!
				int queryRank=docs_scores.getIntegers().indexOf(queryName);
				if(queryRank==-1){//do not handle bug-query
					System.err.println("queryName:"+queryName+", queryRank:"+queryRank+", 1st doc:"+docs_scores.getIntegers().get(0)+", rankList size:"+docs_scores.getIntegers().size());
				}else{
					docs_scores.getIntegers().remove(queryRank); docs_scores.getFloats().remove(queryRank);
				}
			}
			//find first true math rank
			int firstTrueLocRank=General_geoRank.get_trueLocRank(queryName, docs_scores.getIntegers(), (float) 0.01, latlons);
			firstTrueLocRanks_hist[General.getBinInd_linear(firstTrueLocRank_bins, firstTrueLocRank)]++;
			//find trueLocNum
			int trueLocNum=General_geoRank.get_GVSize(queryName, (float) 0.01, docs_scores.getIntegers().subList(0, Math.min(200, docs_scores.getIntegers().size())), latlons);
			trueLocNum_hist_at200[General.getBinInd_linear(trueLocNum_bins, trueLocNum)]++;
			
			queryNum++;
		}
		System.out.println("queryNum:"+queryNum);	
		System.out.println("firstTrueLocRank_bins: "+General.ObjArrToString(firstTrueLocRank_bins, "_")+", firstTrueLocRanks_hist: "+General.IntArrToString(firstTrueLocRanks_hist, "_") );	
		System.out.println("trueLocNum_bins: "+General.ObjArrToString(trueLocNum_bins, "_")+", trueLocNum_hist_at200: "+General.IntArrToString(trueLocNum_hist_at200, "_") );	
	}

	public static void test() throws IOException{
		//*********read rank from MapFile*********//
		Configuration conf = new Configuration();
		SequenceFile.Reader reader=new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path("O:/R_SanFran_20K-VW_SURF_iniR-BurstIntraInter_HDs20-HMW12_ReR1K_HDr20_top1K_Q0_0_R0-0_H0-0_rankDocScore/part-r-00004")));
		
		IntWritable Key= new IntWritable();
		IntList_FloatList Value= new IntList_FloatList();
		while(reader.next(Key, Value)){
			System.out.println("flag:"+Key+", QID:"+Value.getIntegers().get(Value.getIntegers().size()-1));
		}
		reader.close();
	}
}
