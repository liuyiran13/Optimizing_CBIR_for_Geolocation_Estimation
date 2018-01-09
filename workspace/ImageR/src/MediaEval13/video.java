package MediaEval13;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import javax.imageio.ImageIO;

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
import MyAPI.Geo.groupDocs.UserIDs;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
import MyCustomedHaoop.ValueClass.GeoExpansionData.fistMatch_GTruth_Docs_Locations;

public class video {
	/*
	 * for Jeayoung video test
	 */
	public static void mapTaskData() throws Exception{
		String SavePath="O:/MediaEval13/Jaeyoung_video/";
		//set FileSystem
		Configuration conf = new Configuration();
        FileSystem hdfs  = FileSystem.get(conf);
        int saveInter=100*1000;
        //set seqFile
//        SequenceFile.Writer seqFile_query=new SequenceFile.Writer(hdfs, conf, new Path(SavePath+"VideoFrameQuery.seq"), IntWritable.class, BufferedImage_jpg.class);  
		//data from MediaEval13
		float[][] latlons_MeEv13=(float[][]) General.readObject("O:/MediaEval13/MEval13_latlons.floatArr");
		long[] userIDs_0=(long[]) General.readObject("O:/MediaEval13/MEval13_userIDs_0.long"); 
		int[] userIDs_1=(int[]) General.readObject("O:/MediaEval13/MEval13_userIDs_1.int"); 
		//
		int startInd=(latlons_MeEv13[0].length/saveInter+1)*saveInter;
		System.out.println("startInd:"+startInd);
		int currentInd=startInd;
		//data from video
		LinkedList<float[]> latlons=new LinkedList<float[]>(); 
		LinkedList<Long> userIDs_0s=new LinkedList<Long>(); 
		LinkedList<Integer> userIDs_1s=new LinkedList<Integer>(); 
		HashMap<Integer, Integer> test_L_to_S=new HashMap<Integer, Integer>();
		BufferedReader intstr_data = new BufferedReader(new InputStreamReader(new FileInputStream(SavePath+"gt.txt"), "UTF-8"));
		String oneLine; int lineInd=0;
		while((oneLine=intstr_data.readLine())!=null){ //line1Photo: 00005-0001	21.275899	-157.825515
			String[] info=oneLine.split("\t");
			//phoInfo
			String phoName=info[0];
			float[] latlon=new float[]{Float.valueOf(info[1]),Float.valueOf(info[2])};
			long userID_0=Long.valueOf(info[3].split("@")[0]);
			int userID_1=Integer.valueOf(info[3].split("@")[1].substring(1,3));
//			BufferedImage img=ImageIO.read(new File(SavePath+"frames/"+phoName+(phoName.contains("panorama")?".JPG":".jpg")));
			//ind
			int queryInd_inS=currentInd;//continue the index of MedEval13
			int queryInd_inL=currentInd;
			//save
			test_L_to_S.put(queryInd_inL, queryInd_inS);//L is marked with negative, start from -1
			latlons.add(latlon);
			userIDs_0s.add(userID_0);
			userIDs_1s.add(userID_1);
//			seqFile_query.append(new IntWritable(queryInd_inL), new BufferedImage_jpg(img));
			lineInd++;
			currentInd++;
		}
//		seqFile_query.close();
		intstr_data.close();
		System.out.println("done! queryNum:"+lineInd);
		//combine with MediaEval13 set
		float[][] latlon_combined=new float[2][];
		latlon_combined[0]=new float[currentInd]; latlon_combined[1]=new float[currentInd];
		long[] userID0_combined=new long[currentInd];
		int[] userID1_combined=new int[currentInd];
		for (int i = 0; i < latlons_MeEv13[0].length; i++) {
			latlon_combined[0][i]=latlons_MeEv13[0][i];
			latlon_combined[1][i]=latlons_MeEv13[1][i];
			userID0_combined[i]=userIDs_0[i];
			userID1_combined[i]=userIDs_1[i];
		}
		currentInd= startInd; 
		Iterator<Long> Inter_userID0s = userIDs_0s.iterator(); Iterator<Integer> Inter_userID1s = userIDs_1s.iterator();
		for (float[] one : latlons) {
			latlon_combined[0][currentInd]=one[0];
			latlon_combined[1][currentInd]=one[1];
			userID0_combined[currentInd]=Inter_userID0s.next().longValue();
			userID1_combined[currentInd]=Inter_userID1s.next().intValue();
			currentInd++;
		}
//		General.writeObject(SavePath+"VideoFrameQuery_L_to_S", test_L_to_S);
//		General.writeObject(SavePath+"VideoFrameQueryAndME9m_Latlons", latlon_combined);
		General.writeObject(SavePath+"VideoFrameQueryAndME9m_userIDs_0.long", userID0_combined);
		General.writeObject(SavePath+"VideoFrameQueryAndME9m_userIDs_1.int", userID1_combined);
	}
		
	public static void showRanks() throws Exception {
		int saveInterval=100*1000; 
		
		//load image mapFiles
		String imageMapFilesPath="Q:/Photos_MEva13_9M_MapFiles/";
		MapFile.Reader[] imgMapFiles_ME13= General_Hadoop.openAllMapFiles(new String[]{imageMapFilesPath});
		ArrayList<MapFile.Reader> imgMapFiles=new ArrayList<MapFile.Reader>();
		imgMapFiles.addAll(Arrays.asList(imgMapFiles_ME13));
		imgMapFiles.addAll(Arrays.asList(General_Hadoop.openAllMapFiles(new String[]{"O:/MediaEval13/Jaeyoung_video/VideoFrameQuery.mapFile/"})));
		System.out.println("load mapFiles finished! tot-"+imgMapFiles.size());
		
		String rankPathBase="O:/MediaEval13/Jaeyoung_video/GVR/";
		String[] rankPaths={"rank_GVR_Vis_noGlobFilter_noSameUser_1U1P_reRank@20_visSca@100_expSca@0.01_D9M_Video_SURFHD18-20-18_1vs1AndHPM6-1000"};
		
		String[] rankLabels={"GVR_Vis_noGlobFilter_noSameUser_1U1P"};
		
		General.addStrArr_prefx(rankPathBase, rankPaths);
		General.addStrArr_append(rankPaths, "/part-r-00000");
		//******* read latlons **************
		float[][] latlons=(float[][]) General.readObject("O:/MediaEval13/Jaeyoung_video/VideoFrameQueryAndME9m_Latlons");
		long[] userIDs_0=(long[]) General.readObject("O:/MediaEval13/Jaeyoung_video/VideoFrameQueryAndME9m_userIDs_0.long"); 
		int[] userIDs_1=(int[]) General.readObject("O:/MediaEval13/Jaeyoung_video/VideoFrameQueryAndME9m_userIDs_1.int"); 
		UserIDs userID=new UserIDs(userIDs_0, userIDs_1);
		
    	
    	int showTopRankDoc=3;  int showQueryNum=1000; int minGTSize=0, maxGTSize=0;
    	float isSameLoc=(float) 0.01;
    	int[] rankInd_right,rankInd_wrong;
    	
    	//****** show:  right vs is wrong  ***********// 
    	rankInd_right=null;  rankInd_wrong=new int[]{0}; 
    	General_IR.showRanks_GoodVsWrong_geoRel( rankInd_right,  rankInd_wrong,  rankLabels,  rankPaths,  rankPathBase,  showTopRankDoc, 
    			 latlons, userID, isSameLoc,  showQueryNum, 
    			 saveInterval,  "MapFile",  null, 0, imgMapFiles.toArray(new MapFile.Reader[0]), minGTSize, maxGTSize);
    	
    	//clean-up
    	General_Hadoop.closeAllMapFiles(imgMapFiles.toArray(new MapFile.Reader[0]));
    	
//    	//judge query Right&Wrong
//    	IntWritable Key_queryName= new IntWritable();
//		fistMatch_GTruth_Docs_Locations Value_RankScores= new fistMatch_GTruth_Docs_Locations();
//		MapFile.Reader[] MapFileR_Rank=General_Hadoop.openAllMapFiles(rankPaths);
//		PrintWriter outStr_report=new PrintWriter(new OutputStreamWriter(new FileOutputStream(rankPathBase+"Res")));
//		while (MapFileR_Rank[0].next(Key_queryName, Value_RankScores)) {
//			//for ranks should be good
//    		int queryName=Key_queryName.get();
//    		//get top Locations
//			float[][] topLocations=Value_RankScores.topLocations.getArrArr();
//			outStr_report.println(General.floatArrToString(topLocations[0], ",", "0.0000"));
//		}
//		outStr_report.close();
//		General_Hadoop.closeAllMapFiles(MapFileR_Rank);
		
		System.out.println("done!");
	}
	
	public static void main(String[] args) throws Exception {
//		mapTaskData();
		
		showRanks();
	}

}
