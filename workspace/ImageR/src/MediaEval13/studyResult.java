package MediaEval13;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;

import MyAPI.General.General;
import MyAPI.General.General_geoRank;
import MyAPI.Obj.MakeHist_GVR;
import MyCustomedHaoop.ValueClass.GeoExpansionData.fistMatch_GTruth_Docs_GVSizes_docScores;

import com.almworks.sqlite4java.SQLiteException;

public class studyResult {

	public static void main(String[] args) throws IOException, SQLiteException, ClassNotFoundException, InterruptedException {
//		makeResultForRun();
		
		makeQueryGTSizes();
	}
	
	public static void makeResultForRun() throws IOException, SQLiteException, ClassNotFoundException, InterruptedException {
		String savePath="O:/MediaEval13/";
		
		float[][] latlons=(float[][]) General.readObject(savePath+"MEval13_latlons.floatArr");
		String[] s_to_photoID=(String[]) General.readObject(savePath+"MEval13_s_to_photoID.strArr");
		
		String[] rankPaths=new String[]{"rank_D9M_Q250K_Vis_SURFHD12_Ori_noSameUser",
				"rank_D9M_Q250K_Vis_SURFHD12_GVR_reRank300_VisSca300_expSca0.01_noSameUser",
				"rank_D9M_Q250K_Vis_GlobFilterTopLoc_SURFHD12_JCD_Ori_topDoc200_RankThr300_RankScale5_noSameUser",
				"rank_D9M_Q250K_Vis_GlobFilterTopLoc_SURFHD12_JCD_GVR_topDoc200_RankThr300_RankScale5_reRankScale300_VisSca300_expSca0.01_noSameUser",
				"rank_D9M_Q250K_Vis_SURFHD12_GVR_reRank500_VisSca500_expSca0.01_noSameUser"};
		
		float isSameLocScale=(float) 0.01;
		
 		IntWritable Key_queryName= new IntWritable();
 		fistMatch_GTruth_Docs_GVSizes_docScores Value_RankScores= new fistMatch_GTruth_Docs_GVSizes_docScores();
 		
		for (int i = 0; i < rankPaths.length; i++) {
			PrintWriter outputStream_run=new PrintWriter(new OutputStreamWriter(new FileOutputStream(savePath+"Runs/me13pt_TUD_"+i+".txt",false), "UTF-8"),true); 
			//set FileSystem
	      	Configuration conf = new Configuration();
	        FileSystem hdfs  = FileSystem.get(conf);
	    	
	        MapFile.Reader MapFileR_Rank=new MapFile.Reader(hdfs,savePath+"GVR/"+rankPaths[i]+"/part-r-00000", conf);
	        System.out.println("MapFileR_Rank Key-Class: "+MapFileR_Rank.getKeyClass().getName());
	 		System.out.println("MapFileR_Rank Value-Class: "+MapFileR_Rank.getValueClass().getName());
	 		
	 		int queryNum=0; int queryNum_R=0;
	    	while(MapFileR_Rank.next(Key_queryName, Value_RankScores)){ //loop over all queries, key-value(query-rank)
	    		int queryName=Key_queryName.get();
	    		int estMatch=Value_RankScores.Docs[0];
	    		if (General_geoRank.isOneLocation_approximate(latlons[0][queryName],latlons[1][queryName],latlons[0][estMatch],latlons[1][estMatch],isSameLocScale)) {
	    			queryNum_R++;
//	    			System.out.println("query-"+s_to_photoID[queryName]+": "+latlons[0][queryName]+"_"+latlons[1][queryName]
//	    					+", estMatch-"+s_to_photoID[estMatch]+": "+latlons[0][estMatch]+"_"+latlons[1][estMatch]);
				}
	    		outputStream_run.println(s_to_photoID[queryName]+";"+latlons[0][estMatch]+";"+latlons[1][estMatch]);
	    		queryNum++;
	    	}
	    	MapFileR_Rank.close();
	    	outputStream_run.close();
	    	System.err.println("run-"+i+", total queryNum in rank file:"+queryNum+", queryNum_R:"+queryNum_R+", "+new DecimalFormat("00.00%").format((float)queryNum_R/queryNum));
		}
	}

	public static void makeQueryGTSizes() throws IOException, SQLiteException, ClassNotFoundException, InterruptedException {
		String savePath="O:/MediaEval13/";
		
		float[][] latlons=(float[][]) General.readObject(savePath+"MEval13_latlons.floatArr");
		String[] s_to_photoID=(String[]) General.readObject(savePath+"MEval13_s_to_photoID.strArr");
		
		String rankPaths="rank_D9M_Q250K_Vis_SURFHD12_Ori_noSameUser";
		
		float isSameLocScale=(float) 0.01; int numTopLocations=10;
		MakeHist_GVR makeHist_forGTSize=new MakeHist_GVR("0,1,5,10,20,40,100", 1, numTopLocations);
		
 		IntWritable Key_queryName= new IntWritable();
 		fistMatch_GTruth_Docs_GVSizes_docScores Value_RankScores= new fistMatch_GTruth_Docs_GVSizes_docScores();
 		
		PrintWriter outputStream_run=new PrintWriter(new OutputStreamWriter(new FileOutputStream(savePath+"Query_GTSize.txt",false), "UTF-8"),true); 
		//set FileSystem
      	Configuration conf = new Configuration();
        FileSystem hdfs  = FileSystem.get(conf);
    	
        MapFile.Reader MapFileR_Rank=new MapFile.Reader(hdfs,savePath+"GVR/"+rankPaths+"/part-r-00000", conf);
        System.out.println("MapFileR_Rank Key-Class: "+MapFileR_Rank.getKeyClass().getName());
 		System.out.println("MapFileR_Rank Value-Class: "+MapFileR_Rank.getValueClass().getName());
 		
 		int queryNum=0; int queryNum_R=0;
    	while(MapFileR_Rank.next(Key_queryName, Value_RankScores)){ //loop over all queries, key-value(query-rank)
    		int queryName=Key_queryName.get();
    		int estMatch=Value_RankScores.Docs[0];
    		if (General_geoRank.isOneLocation_approximate(latlons[0][queryName],latlons[1][queryName],latlons[0][estMatch],latlons[1][estMatch],isSameLocScale)) {
    			queryNum_R++;
			}
    		int grounTSize=Value_RankScores.gTruth.size(); 
			if (grounTSize==1 && Value_RankScores.gTruth.get(0)[0]==-1) {//if query do not have ground truth, then in GVR, it mark this with {-1,-1}
				grounTSize=0;
			}
    		//get True-Location rank
			int trueLocRank=General_geoRank.get_trueLocRank(queryName, Value_RankScores.Docs, isSameLocScale, latlons)+1;
    		//add to hist
    		makeHist_forGTSize.addOneSample(grounTSize, 0, trueLocRank<=10?trueLocRank:0);
    		outputStream_run.println(s_to_photoID[queryName]+":"+grounTSize);
    		queryNum++;
    	}
    	// ** compute accumulated TrueLocHist for grouped grounTSize ***//
    	outputStream_run.println(makeHist_forGTSize.makeRes(0, new int[]{1}, "******** Group grounTSize:  accumulated-TrueLocHist: \n", queryNum_R));

    	MapFileR_Rank.close();
    	outputStream_run.close();
    	System.err.println("done! total queryNum in rank file:"+queryNum+", queryNum_R:"+queryNum_R+", "+new DecimalFormat("00.00%").format((float)queryNum_R/queryNum));
		
		
		
	}

}
