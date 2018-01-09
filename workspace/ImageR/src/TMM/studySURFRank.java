package TMM;

import java.awt.Color;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.General.General_IR;
import MyCustomedHaoop.ValueClass.IntArr;
import MyCustomedHaoop.ValueClass.IntArrArr;
import MyCustomedHaoop.ValueClass.SURFfeat;

public class studySURFRank {

	public static void main(String[] args) throws Exception {		
//		showUniversalImagesInRankLists_MedEv9M();
		
		showRanks();
	}

	public static void showUniversalImagesInRankLists_MedEv9M() throws Exception {
//		int saveInterval=100*1000; 
//		//load image mapFiles
//		String imageMapFilesPath="Q:/Photos_MEva13_9M_MapFiles/";
//		MapFile.Reader[] imgMapFiles= General_Hadoop.openAllMapFiles(new String[]{imageMapFilesPath});
//		System.out.println("load mapFiles finished! tot-"+imgMapFiles.length);
//			
//		String workDir="O:/GVR_TMM/findUniverPhotos/";
//		int appearInTop=1;
//		String jobLabel="_AnalysisPhotIDFreq_D9M_Q250K_Q0_SURFHD12_1v1Match_appearInTop"+appearInTop;
//		BufferedReader inputStreamFeat = new BufferedReader(new InputStreamReader(new FileInputStream(workDir+"Report"+jobLabel), "UTF-8"));
//		//load mapFile_topPhotoIDQuRan
//      	Configuration conf = new Configuration();
//        FileSystem hdfs  = FileSystem.get(conf);
//        MapFile.Reader mapFile_topPhotoIDQuRan=new MapFile.Reader(hdfs,workDir+"mapF"+jobLabel+"/part-r-"+General.StrleftPad(0+"", 0, 5, "0"), conf);
//        IntArrArr queryNamRank=new IntArrArr();
//        
//        //set CompareTwoPhotos
//        String basePathForExtractFeat="O:/ImageRetrieval/SURFVW/";
//		String vwCenters_path=basePathForExtractFeat+"SURFVW_20K_I90";
//		String PMatrix_path=basePathForExtractFeat+"HE_ProjectionMatrix";
//		String HE_Thresholds_path=basePathForExtractFeat+"HE_Thresholds";
//		String middleNodes_path=basePathForExtractFeat+"middleNodes_M1000_VW20000_I200.ArrayList_HashSet";
//		String node_vw_links_path=basePathForExtractFeat+"node_vw_links_M1000_VW20000_I200_learned30M.ArrayList_HashSet";
//		
//		String docInfoPath="O:/GVR_TMM/imagR/docInfo_MEva13_9M/part-r-00000";
//		String TVectorInfoPath="O:/GVR_TMM/imagR/TVectorInfo_MEva13_9M";
//		
//		//setup CompareTwoPhotos
//		ComparePhotos_LocalFeature comparePhotos=new ComparePhotos_LocalFeature();
//		comparePhotos.setup_extractFeat(vwCenters_path,PMatrix_path,HE_Thresholds_path,middleNodes_path,node_vw_links_path,1,0,conf);
//		comparePhotos.setup_scoreDoc(20,20,6,docInfoPath,TVectorInfoPath,conf);
//		
//		HashMap<Integer, ArrayList<SURFfeat>> VW_Sigs=new HashMap<Integer, ArrayList<SURFfeat>>(); 
//		
//		String taskLabel="showUniverPhoto_1v1Match_appearInTop"+appearInTop;
//		String rankShowPath=workDir+taskLabel+"_inHTML/";
//    	String rankShowPath_photos=rankShowPath+"photos/";
//    	General.makeORdelectFolder(rankShowPath);
//    	General.makeORdelectFolder(rankShowPath_photos);
//		PrintWriter html = new PrintWriter(new OutputStreamWriter(new FileOutputStream(
//				rankShowPath+"index.html",false), "UTF-8"),true);
//		String line;
//		General.jumpLines(inputStreamFeat, 4);//first 4 lines are general info
//		ArrayList<String> phoPath_ForHTML=new ArrayList<String>();
//		ArrayList<String> phoCaption_ForHTML=new ArrayList<String>();
//		ArrayList<String> phoColor_ForHTML=new ArrayList<String>();
//		int phoInd=0; int topShow=10;
//		while ((line=inputStreamFeat.readLine())!=null) {
//			if (phoInd<topShow) {
//				int[] Freq_PhotoID=General.StrArrToIntArr(line.split("_"));
//				//set photoID
//				int[] featState=comparePhotos.extractRawFeat_makeVW_HESig(VW_Sigs, null, General_Hadoop.readImgageFromMFiles(Freq_PhotoID[1], saveInterval, imgMapFiles));
//				String photoIDFileName=General_IR.addPhotoPath_MovePhoto("MapFile", Freq_PhotoID[1], rankShowPath_photos, saveInterval, null, 0, imgMapFiles);
//				String photoIDCap="Pho"+Freq_PhotoID[1]+"_Freq"+Freq_PhotoID[0]+"_VW"+featState[1]+"_Uni"+featState[3];
//				String photoIDCol="rgb("+General.floatArrToString_nolastDelimiter(General.mul(Color.red.getRGBColorComponents(null), 255), ",", "0")+")";
//				//set queryName_rank
//				phoPath_ForHTML.clear();
//				phoCaption_ForHTML.clear();
//				phoColor_ForHTML.clear();
//				mapFile_topPhotoIDQuRan.get(new IntWritable(Freq_PhotoID[1]), queryNamRank);
//				for (IntArr Q_Rank : queryNamRank.getArrArr()) {
//					//set photo
//					phoPath_ForHTML.add(General_IR.addPhotoPath_MovePhoto("MapFile", Q_Rank.getIntArr()[0], rankShowPath_photos, saveInterval, null, 0, imgMapFiles));
//					//set photo_Caption
//					phoCaption_ForHTML.add("Q"+Q_Rank.getIntArr()[0]+"_R"+Q_Rank.getIntArr()[1]);
//					//set photo_Color
//					phoColor_ForHTML.add("rgb("+General.floatArrToString_nolastDelimiter(General.mul(Color.green.getRGBColorComponents(null), 255), ",", "0")+")");
//
//				}
//				General.showPhoto_inHTML( html, taskLabel.toString(), taskLabel, phoInd==0?0:10,  rankShowPath_photos,  photoIDFileName, photoIDCap, photoIDCol,
//						null,null, null, 3,
//						phoPath_ForHTML, phoCaption_ForHTML, phoColor_ForHTML, null,null,null,0);
//			}
//			phoInd++;
//		}
//		General.showPhoto_inHTML( html, taskLabel.toString(), taskLabel, 1,  rankShowPath_photos, null, null, null,
//				null,null, null, 3,
//				null, null, null, null,null,null,0);
//		
//		//clean-up
//		html.close();
//    	General_Hadoop.closeAllMapFiles(imgMapFiles);
	}

	public static void showRanks() throws Exception {
		int saveInterval=100*1000; 
		
		//load image mapFiles
		String imageMapFilesPath="Q:/Photos_MEva13_9M_MapFiles/";
		MapFile.Reader[] imgMapFiles= General_Hadoop.openAllMapFiles(new String[]{imageMapFilesPath});
		System.out.println("load mapFiles finished! tot-"+imgMapFiles.length);
		
		String showRankDir="O:/GVR_TMM/GVR/";
		String[] rankPaths={"O:/GVR_TMM/GVR/rank_VisNN_Vis_noGlobFilter_noSameUser_D9M_Q0_SURFHD18-20-18_1vs1AndHPM6-1000",
							"O:/GVR_TMM/GVR/rank_MeanShift_Vis_noGlobFilter_noSameUser_reRank@10_maxI@100_bandSca@1.0E-5_D9M_Q0_SURFHD18-20-18_1vs1AndHPM6-1000",
							"O:/GVR_TMM/GVR/rank_GVR_Vis_noGlobFilter_noSameUser_reRank@10_visSca@10_expSca@0.01_D9M_Q0_SURFHD18-20-18_1vs1AndHPM6-1000"
							};
		
		String[] rankLabels={"VisNN_Vis_noGlobFilter","MeanShift_Vis_noGlobFilter","GVR_Vis_noGlobFilter"};
		
		General.addStrArr_append(rankPaths, "/part-r-00000");
		//******* read latlons **************
		float[][] latlons=(float[][]) General.readObject("O:/MediaEval13/MEval13_latlons.floatArr");
				
    	
    	int showTopRankDoc=10;  int showQueryNum=50; 
    	float isSameLoc=(float) 0.01;
    	int[] rankInd_right,rankInd_wrong;
    	
    	//****** show:  right & wrong  ***********// 
    	rankInd_right=new int[]{2};  rankInd_wrong=new int[]{1}; //new int[]{2}
    	General_IR.showRanks_GoodVsWrong_geoRel( rankInd_right,  rankInd_wrong,  rankLabels,  rankPaths,  showRankDir,  showTopRankDoc, 
    			 latlons, null, isSameLoc,  showQueryNum, 
    			 saveInterval,  "MapFile",  null, 0, imgMapFiles, 0, Integer.MAX_VALUE);
    	
    	//clean-up
    	General_Hadoop.closeAllMapFiles(imgMapFiles);
    	
		System.out.println("done!");
	}

}
