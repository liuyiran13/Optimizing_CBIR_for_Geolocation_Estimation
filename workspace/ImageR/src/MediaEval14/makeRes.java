package MediaEval14;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;

import boofcv.gui.image.ShowImages;
import MyAPI.General.General;
import MyAPI.General.General_BoofCV;
import MyAPI.General.General_Hadoop;
import MyAPI.General.General_IR;
import MyAPI.General.General_geoRank;
import MyCustomedHaoop.ValueClass.DID_Score;
import MyCustomedHaoop.ValueClass.DID_Score_Arr;
import MyCustomedHaoop.ValueClass.GeoExpansionData.fistMatch_GTruth_Docs_Locations;
import MyCustomedHaoop.ValueClass.IntList_FloatList;

public class makeRes {
	public static void main(String[] args) throws Exception {
//		makeRankedLocations();
		
//		makeRunRes();
		
		makeIniVisualRankRes();
		
//		makeRunRes_forViderFramTest();
		
	}
	
	public static void makeRankedLocations() throws IOException, InterruptedException{
		String saveBasePath="O:/MediaEval14/";
		//set FileSystem
      	Configuration conf = new Configuration();
        FileSystem hdfs  = FileSystem.get(conf);
		//read s_to_PhotoID in memory
		long[] s_to_PhotoID=new long[55*100*1000];
		BufferedReader inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(saveBasePath+"MEval14_photos_s_to_photoID_md5_phoIndInL.txt"), "UTF-8"));
		String line1Photo; int ind=0;
		while((line1Photo=inStr_photoMeta.readLine())!=null){
			s_to_PhotoID[ind]=Long.valueOf(line1Photo.split("\t")[0]);
			ind++;
		}
		inStr_photoMeta.close();
		System.out.println("done for read s_to_PhotoID in memory! "+General.memoryInfo());
		//read latlons
		float[][] latlons=(float[][]) General.readObject(saveBasePath+"MEval14_photos_latlons.floatArr");
		//read rank, save result to txt
		PrintWriter outputStream_run=new PrintWriter(new OutputStreamWriter(new FileOutputStream(saveBasePath+"GVR/RankedLocations_visualBased.txt",false), "UTF-8"),true); 
        MapFile.Reader MapFileR_Rank=new MapFile.Reader(hdfs,saveBasePath+"GVR/rank_GVR_Vis_noGlobFilter_noSameUser_1U1P_reRank@10_visSca@100_expSca@0.01_D-ME14Pho5M_Q-Pho500K_SURFHD18-20-18_1vs1AndHPM6-1000/part-r-00000", conf);
        System.out.println("MapFileR_Rank Key-Class: "+MapFileR_Rank.getKeyClass().getName());
 		System.out.println("MapFileR_Rank Value-Class: "+MapFileR_Rank.getValueClass().getName());
 		IntWritable Key_queryName= new IntWritable();
 		fistMatch_GTruth_Docs_Locations Value_RankScores= new fistMatch_GTruth_Docs_Locations();
 		int queryNum=0; int queryNum_R=0;
 		float isSameLocScale=(float) 0.01;
    	while(MapFileR_Rank.next(Key_queryName, Value_RankScores)){ //loop over all queries, key-value(query-rank)
    		int queryName=Key_queryName.get(); float[][] topLocations=Value_RankScores.topLocations.getArrArr();
    		//judge whether is correct query
    		boolean correct=false;
    		if (General_geoRank.isOneLocation(latlons[0][queryName],latlons[1][queryName],topLocations[0][0],topLocations[0][1],isSameLocScale)) {
    			queryNum_R++;
    			correct=true;
//    			System.out.println("query-"+s_to_photoID[queryName]+": "+latlons[0][queryName]+"_"+latlons[1][queryName]
//    					+", estMatch-"+s_to_photoID[estMatch]+": "+latlons[0][estMatch]+"_"+latlons[1][estMatch]);
			}
    		outputStream_run.print(s_to_PhotoID[queryName]+"\t"+correct+"\t");
    		int rank_i=0;
    		for (DID_Score_Arr oneLocDocs : Value_RankScores.Docs.getArr()) {
    			float thisLocScore=0;
    			for (DID_Score oneDoc : oneLocDocs.getArr()) {
    				thisLocScore+=oneDoc.score;
				}
    			outputStream_run.print(General.floatArrToString(topLocations[rank_i],"_","0.0000")+thisLocScore+"\t");
    			rank_i++;
			}
    		outputStream_run.println();
    		queryNum++;
    	}
    	System.out.println("done ! in total queryNum:"+queryNum+", corrrect:"+queryNum_R+", "+new DecimalFormat("0.0%").format((double)queryNum_R/queryNum));
    	MapFileR_Rank.close();
    	outputStream_run.close();
	}
	
	public static void makeRunRes() throws IOException, InterruptedException{
		String saveBasePath="O:/MediaEval14/";
		//set FileSystem
      	Configuration conf = new Configuration();
        FileSystem hdfs  = FileSystem.get(conf);
		//read s_to_PhotoID in memory
		long[] s_to_PhotoID=new long[55*100*1000];
		BufferedReader inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(saveBasePath+"MEval14_photos_s_to_photoID_md5_phoIndInL.txt"), "UTF-8"));
		String line1Photo; int ind=0;
		while((line1Photo=inStr_photoMeta.readLine())!=null){
			s_to_PhotoID[ind]=Long.valueOf(line1Photo.split("\t")[0]);
			ind++;
		}
		inStr_photoMeta.close();
		System.out.println("done for read s_to_PhotoID in memory! "+General.memoryInfo());
		//read latlons
		float[][] latlons=(float[][]) General.readObject(saveBasePath+"MEval14_photos_latlons.floatArr");
		//read rank, save result to txt
		PrintWriter outputStream_run=new PrintWriter(new OutputStreamWriter(new FileOutputStream(saveBasePath+"GVR/RunRes_visualBased.txt",false), "UTF-8"),true); 
        MapFile.Reader MapFileR_Rank=new MapFile.Reader(hdfs,saveBasePath+"GVR/rank_GVR_Vis_noGlobFilter_noSameUser_1U1P_reRank@10_visSca@100_expSca@0.01_D-ME14Pho5M_Q-Pho500K_SURFHD18-20-18_1vs1AndHPM6-1000/part-r-00000", conf);
        System.out.println("MapFileR_Rank Key-Class: "+MapFileR_Rank.getKeyClass().getName());
 		System.out.println("MapFileR_Rank Value-Class: "+MapFileR_Rank.getValueClass().getName());
 		IntWritable Key_queryName= new IntWritable();
 		fistMatch_GTruth_Docs_Locations Value_RankScores= new fistMatch_GTruth_Docs_Locations();
 		int queryNum=0; int queryNum_R=0;
 		float isSameLocScale=(float) 0.01;
    	while(MapFileR_Rank.next(Key_queryName, Value_RankScores)){ //loop over all queries, key-value(query-rank)
    		int queryName=Key_queryName.get(); float[][] topLocations=Value_RankScores.topLocations.getArrArr();
    		//judge whether is correct query
    		if (General_geoRank.isOneLocation(latlons[0][queryName],latlons[1][queryName],topLocations[0][0],topLocations[0][1],isSameLocScale)) {
    			queryNum_R++;
//    			System.out.println("query-"+s_to_photoID[queryName]+": "+latlons[0][queryName]+"_"+latlons[1][queryName]
//    					+", estMatch-"+s_to_photoID[estMatch]+": "+latlons[0][estMatch]+"_"+latlons[1][estMatch]);
			}
    		outputStream_run.println(s_to_PhotoID[queryName]+";"+General.floatArrToString(topLocations[0],";","0.00000"));
    		queryNum++;
    	}
    	System.out.println("done ! in total queryNum:"+queryNum+", corrrect:"+queryNum_R+", "+new DecimalFormat("0.0%").format((double)queryNum_R/queryNum));
    	MapFileR_Rank.close();
    	outputStream_run.close();
	}
	
	public static void makeIniVisualRankRes() throws IOException, InterruptedException{
		String saveBasePath="O:/MediaEval14/";
		//set FileSystem
      	Configuration conf = new Configuration();
        FileSystem hdfs  = FileSystem.get(conf);
		//read s_to_PhotoID in memory
		long[] s_to_PhotoID=new long[55*100*1000];
		BufferedReader inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(saveBasePath+"MEval14_photos_s_to_photoID_md5_phoIndInL.txt"), "UTF-8"));
		String line1Photo; int ind=0;
		while((line1Photo=inStr_photoMeta.readLine())!=null){
			s_to_PhotoID[ind]=Long.valueOf(line1Photo.split("\t")[0]);
			ind++;
		}
		inStr_photoMeta.close();
		System.out.println("done for read s_to_PhotoID in memory! "+General.memoryInfo());
		//read latlons
		float[][] latlons_ME14_pho=(float[][]) General.readObject("O:/MediaEval14/MEval14_photos_latlons.floatArr");
		float[][] latlons=(float[][]) General.readObject(saveBasePath+"VideoFrameTest/2012TestVideoFrames_combinedME14_latlons.floatArr");//saveBasePath+"MEval14_photos_latlons.floatArr"
		int queryInd=latlons_ME14_pho[0].length;//queryId start from latlons_ME14_pho
		//read rank, save result to txt
		PrintWriter outputStream_run=new PrintWriter(new OutputStreamWriter(new FileOutputStream(saveBasePath+"VideoFrameTest/ranks/IniVisualRank_MEva14_5MPho.txt",false), "UTF-8"),true); 
        MapFile.Reader MapFileR_Rank=new MapFile.Reader(hdfs,saveBasePath+"VideoFrameTest/ranks/R_MEva14_5MPho_20K-VW_SURF_iniR-noBurst_HDs20-HMW12_ReR10K_HDr20_top10K_1vs1AndHPM@4@6_rankDocScore/part-r-00000", conf);
        System.out.println("MapFileR_Rank Key-Class: "+MapFileR_Rank.getKeyClass().getName());
 		System.out.println("MapFileR_Rank Value-Class: "+MapFileR_Rank.getValueClass().getName());
 		IntWritable Key_queryName= new IntWritable();
 		IntList_FloatList Value_RankScores= new IntList_FloatList();
 		int queryNum=0; int queryNum_R=0; int topDocNum=100;
 		float isSameLocScale=(float) 0.01; long startTime=System.currentTimeMillis(); 
    	while(MapFileR_Rank.next(Key_queryName, Value_RankScores)){ //loop over all queries, key-value(query-rank)
    		int queryName=Key_queryName.get(); 
    		int queryInd_inGT=queryName-queryInd+1;
    		ArrayList<Integer> docs=Value_RankScores.getIntegers();
    		ArrayList<Float> scores=Value_RankScores.getFloats();
    		//judge whether is correct query
    		if (General_geoRank.isOneLocation(latlons[0][queryName],latlons[1][queryName],latlons[0][docs.get(0)],latlons[1][docs.get(0)],isSameLocScale)) {
    			queryNum_R++;
    			System.out.println("query-"+queryInd_inGT+": "+latlons[0][queryName]+"_"+latlons[1][queryName]
    					+", estMatch-"+s_to_PhotoID[docs.get(0)]+": "+latlons[0][docs.get(0)]+"_"+latlons[1][docs.get(0)]);
			}
    		//output rank
    		StringBuffer rank=new StringBuffer();
    		for (int i = 0; i < Math.min(docs.size(), topDocNum); i++) {
    			rank.append(s_to_PhotoID[docs.get(i)]+"_"+scores.get(i)+",");
			}
    		outputStream_run.println(queryInd_inGT+","+rank.toString());
    		//disp
    		if (queryInd_inGT==233) {//fist one
				System.out.println(queryInd_inGT+","+latlons[0][queryName]+"_"+latlons[1][queryName]+", "+rank.toString());
			}
    		queryNum++;
    		if (queryNum%10000==0) {
    			System.out.println(queryNum+" queries done! ... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			}
    		
    	}
    	System.out.println("done ! in total queryNum:"+queryNum+", corrrect:"+queryNum_R+", "+new DecimalFormat("0.000%").format((double)queryNum_R/queryNum));
    	MapFileR_Rank.close();
    	outputStream_run.close();
	}

	public static void makeRunRes_forViderFramTest() throws IOException, InterruptedException{
		String saveBasePath="O:/MediaEval14/VideoFrameTest/";
 		float isSameLocScale=(float) 0.1;
		//set FileSystem
      	Configuration conf = new Configuration();
        FileSystem hdfs  = FileSystem.get(conf);
		//read latlon in memory
        float[][] latlons_ME14_pho=(float[][]) General.readObject("O:/MediaEval14/MEval14_photos_latlons.floatArr");
        float[][] latlons_combined=(float[][]) General.readObject(saveBasePath+"2012TestVideoFrames_combinedME14_latlons.floatArr");
        int queryInd=latlons_ME14_pho.length;//queryId start from latlons_ME14_pho
		System.out.println("done for read s_to_PhotoID in memory! "+General.memoryInfo());
		//read rank, save result to txt
		PrintWriter outputStream_run=new PrintWriter(new OutputStreamWriter(new FileOutputStream(saveBasePath+"GVR/RunRes_"+isSameLocScale+".txt",false), "UTF-8"),true); 
        MapFile.Reader MapFileR_Rank=new MapFile.Reader(hdfs,saveBasePath+"GVR/rank_GVR_Vis_noGlobFilter_SameUser_1U1P_noNorm_reRank@50_visSca@100_expSca@0.01_D-ME14Pho5M_Q-2012TestVideoFrames_SURFHD18-20-18_1vs1AndHPM6-1000/part-r-00000", conf);
        System.out.println("MapFileR_Rank Key-Class: "+MapFileR_Rank.getKeyClass().getName());
 		System.out.println("MapFileR_Rank Value-Class: "+MapFileR_Rank.getValueClass().getName());
 		IntWritable Key_queryName= new IntWritable();
 		fistMatch_GTruth_Docs_Locations Value_RankScores= new fistMatch_GTruth_Docs_Locations();
 		int queryNum=0; int queryNum_R=0;
    	while(MapFileR_Rank.next(Key_queryName, Value_RankScores)){ //loop over all queries, key-value(query-rank)
    		int queryName=Key_queryName.get(); float[][] topLocations=Value_RankScores.topLocations.getArrArr();
    		//judge whether is correct query
    		boolean isTrue=false;
    		if (General_geoRank.isOneLocation(latlons_combined[0][queryName],latlons_combined[1][queryName],topLocations[0][0],topLocations[0][1],isSameLocScale)) {
    			queryNum_R++;
    			isTrue=true;
    			System.out.println("query-"+(queryName-queryInd+1)+": "+latlons_combined[0][queryName]+"_"+latlons_combined[1][queryName]);
			}
    		outputStream_run.println((queryName-queryInd+1)+";"+isTrue+";"+General.floatArrToString(topLocations[0],",","0.00000"));
    		queryNum++;
    	}
    	General.dispInfo(outputStream_run, "done ! in total queryNum:"+queryNum+", corrrect:"+queryNum_R+", "+new DecimalFormat("0.000%").format((double)queryNum_R/queryNum));
    	MapFileR_Rank.close();
    	outputStream_run.close();
	}
	
}
