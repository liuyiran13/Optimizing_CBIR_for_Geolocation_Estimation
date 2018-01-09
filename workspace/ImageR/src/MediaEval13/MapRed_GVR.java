package MediaEval13;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.General.General_IR;
import MyAPI.General.General_geoRank;
import MyAPI.imagR.GTruth;
import MyCustomedHaoop.Mapper.SelectSamples;
import MyCustomedHaoop.Partitioner.Partitioner_random;
import MyCustomedHaoop.Reducer.Reducer_InOut.Reducer_InOut_1key_1value;
import MyCustomedHaoop.ValueClass.GeoExpansionData.fistMatch_GTruth_Docs_GVSizes_docScores;
import MyCustomedHaoop.ValueClass.IntList_FloatList;

public class MapRed_GVR extends Configured implements Tool{

	/** Different from MapRed_geoExpansion_getScore, no AP, but rank scores
	 * 
	 * job1: read query rank, and do geoExpansion, rerank, finally get query's groudTru-Size, top P doc (ori and reranked), their scores
	 * mapper: read and out, read query rank from MapFile(only use data), output: queryName_(docNames_scores)
	 * reducer: for one query, geoExpansion and rerank, output: sequence file queryName _ query's groudTru-Size, top P doc (ori and reranked), rank scores.
	 * 
	 * @param (Mapper_readRank):  "mapred.latlons" "mapred.userIDs_0" "mapred.userIDs_1" "mapred.topVisScale"  "mapred.geoExpanScale" "mapred.G_ForGTSize" "mapred.V_ForGTSize" "mapred.conceptThr" "mapred.conRank_thr" "mapred.num_topDocs" "mapred.FilterMapFile"
	 * 
	 * job2: combine results from job1
	 * mapper: read and output
	 * reducer: combine and save in MapFile, key:queryName
	 * 
	 * @param:  mapred.pho_S_to_L mapred.imgPath 
	 * 
	 * job3: analysis results from job1
	 * mapper: read and output
	 * reducer: analysis result and make report
	 * @param: "mapred.latlons" "mapred.reportPath" "mapred.isOneLocScales"  "mapred.isSameLocScales"  "mapred.num_topLocations" "mapred.binsForGTSize" "mapred.accumLevel" "mapred.InfoStrPath"
	 * 
	 * 
	 * @throws Exception 
	 * @command_example: 
	 * 3M:	hadoop jar MapRed_GeoVisualRanking_Vis.jar RankStudy_MapRed.MapRed_GeoVisualRanking -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,MyAPI.jar,JSAT_r413.jar,lire136.jar -Dmapred.task.timeout=6000000 -Dmapred.latlons=ICMR2013/3M_latlon.float2 -Dmapred.userIDs_0=ICMR2013/3M_userIDs_0.long -Dmapred.userIDs_1=ICMR2013/3M_userIDs_1.int -Dmapred.num_topDocs=200 -Dmapred.G_ForGTSize=0.01 -Dmapred.V_ForGTSize=10000 -Dmapred.reportPath=ICMR2013/GVR/ -Dmapred.isOneLocScales=0.01,0.1  -Dmapred.isSameLocScales=0.01,0.1  -Dmapred.num_topLocations=20 -Dmapred.binsForGTSize=0,1,5,10,20,40,100 -Dmapred.accumLevel=1,2,3,5,10,20 -Dmapred.saveRes=false -Dmapred.makeReport=true _D3M_Q100K_HD12_Vis ImageR/SearchResult_D3M_Q100K_ICMR13_HD12_topRank10K/part-r-00000/data ICMR2013/GVR/
	 * 		hadoop jar MapRed_GeoVisualRanking_VisConceptThr.jar RankStudy_MapRed.MapRed_GeoVisualRanking -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,MyAPI.jar,JSAT_r413.jar,lire136.jar -Dmapred.task.timeout=6000000 -Dmapred.latlons=ICMR2013/3M_latlon.float2 -Dmapred.userIDs_0=ICMR2013/3M_userIDs_0.long -Dmapred.userIDs_1=ICMR2013/3M_userIDs_1.int -Dmapred.num_topDocs=200 -Dmapred.G_ForGTSize=0.01 -Dmapred.V_ForGTSize=10000 -Dmapred.FilterMapFile=ConceptDetection/conceptScores/3M_conceptFeat_MapFile -Dmapred.reportPath=ICMR2013/GVR/ -Dmapred.isOneLocScales=0.01,0.1  -Dmapred.isSameLocScales=0.01,0.1  -Dmapred.num_topLocations=20 -Dmapred.binsForGTSize=0,1,5,10,20,40,100 -Dmapred.accumLevel=1,2,3,5,10,20 -Dmapred.saveRes=false -Dmapred.makeReport=true _D3M_Q100K_HD12_VisConceptThr0.85 ImageR/SearchResult_D3M_Q100K_ICMR13_HD12_topRank10K/part-r-00000/data ICMR2013/GVR/
	 * 		hadoop jar MapRed_GeoVisualRanking_VisConceptRankThr.jar RankStudy_MapRed.MapRed_GeoVisualRanking -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,MyAPI.jar,JSAT_r413.jar,lire136.jar -Dmapred.task.timeout=6000000 -Dmapred.latlons=ICMR2013/3M_latlon.float2 -Dmapred.userIDs_0=ICMR2013/3M_userIDs_0.long -Dmapred.userIDs_1=ICMR2013/3M_userIDs_1.int -Dmapred.num_topDocs=200 -Dmapred.G_ForGTSize=0.01 -Dmapred.V_ForGTSize=10000 -Dmapred.FilterMapFile=ConceptDetection/conceptScores/3M_conceptFeat_MapFile -Dmapred.reportPath=ICMR2013/GVR/ -Dmapred.isOneLocScales=0.01,0.1  -Dmapred.isSameLocScales=0.01,0.1  -Dmapred.num_topLocations=20 -Dmapred.binsForGTSize=0,1,5,10,20,40,100 -Dmapred.accumLevel=1,2,3,5,10,20 -Dmapred.saveRes=false -Dmapred.makeReport=true _D3M_Q100K_HD12_VisConceptRankThr ImageR/SearchResult_D3M_Q100K_ICMR13_HD12_topRank10K/part-r-00000/data ICMR2013/GVR/
	 * 		hadoop jar MapRed_GeoVisualRanking_VisConceptThr_ConSel.jar RankStudy_MapRed.MapRed_GeoVisualRanking -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,MyAPI.jar,JSAT_r413.jar,lire136.jar -Dmapred.task.timeout=6000000 -Dmapred.latlons=ICMR2013/3M_latlon.float2 -Dmapred.userIDs_0=ICMR2013/3M_userIDs_0.long -Dmapred.userIDs_1=ICMR2013/3M_userIDs_1.int -Dmapred.num_topDocs=200 -Dmapred.G_ForGTSize=0.01 -Dmapred.V_ForGTSize=10000 -Dmapred.Thr_distCon=0.1 -Dmapred.FilterMapFile=ConceptDetection/conceptScores/3M_conceptFeat_MapFile -Dmapred.reportPath=ICMR2013/GVR/ -Dmapred.isOneLocScales=0.01,0.1  -Dmapred.isSameLocScales=0.01,0.1  -Dmapred.num_topLocations=20 -Dmapred.binsForGTSize=0,1,5,10,20,40,100 -Dmapred.accumLevel=1,2,3,5,10,20 -Dmapred.saveRes=false -Dmapred.makeReport=true _D3M_Q100K_HD12_VisConceptThr0.85_ConSel ImageR/SearchResult_D3M_Q100K_ICMR13_HD12_topRank10K/part-r-00000/data ICMR2013/GVR/
	 * 		hadoop jar MapRed_GeoVisualRanking_VisConceptScore.jar RankStudy_MapRed.MapRed_GeoVisualRanking -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,MyAPI.jar,JSAT_r413.jar,lire136.jar -Dmapred.task.timeout=6000000 -Dmapred.latlons=ICMR2013/3M_latlon.float2 -Dmapred.userIDs_0=ICMR2013/3M_userIDs_0.long -Dmapred.userIDs_1=ICMR2013/3M_userIDs_1.int -Dmapred.num_topDocs=200 -Dmapred.G_ForGTSize=0.01 -Dmapred.V_ForGTSize=10000 -Dmapred.FilterMapFile=ConceptDetection/conceptScores/3M_conceptFeat_MapFile -Dmapred.reportPath=ICMR2013/GVR/ -Dmapred.isOneLocScales=0.01,0.1  -Dmapred.isSameLocScales=0.01,0.1  -Dmapred.num_topLocations=20 -Dmapred.binsForGTSize=0,1,5,10,20,40,100 -Dmapred.accumLevel=1,2,3,5,10,20 -Dmapred.saveRes=false -Dmapred.makeReport=true _D3M_Q100K_HD12_VisConceptScore ImageR/SearchResult_D3M_Q100K_ICMR13_HD12_topRank10K/part-r-00000/data ICMR2013/GVR/
	 * 		hadoop jar MapRed_GeoVisualRanking_ConceptScore.jar RankStudy_MapRed.MapRed_GeoVisualRanking -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,MyAPI.jar,JSAT_r413.jar,lire136.jar -Dmapred.task.timeout=6000000 -Dmapred.latlons=ICMR2013/3M_latlon.float2 -Dmapred.userIDs_0=ICMR2013/3M_userIDs_0.long -Dmapred.userIDs_1=ICMR2013/3M_userIDs_1.int -Dmapred.num_topDocs=200 -Dmapred.G_ForGTSize=0.01 -Dmapred.V_ForGTSize=10000 -Dmapred.FilterMapFile=ConceptDetection/conceptScores/3M_conceptFeat_MapFile -Dmapred.reportPath=ICMR2013/GVR/ -Dmapred.isOneLocScales=0.01,0.1  -Dmapred.isSameLocScales=0.01,0.1  -Dmapred.num_topLocations=20 -Dmapred.binsForGTSize=0,1,5,10,20,40,100 -Dmapred.accumLevel=1,2,3,5,10,20 -Dmapred.saveRes=false -Dmapred.makeReport=true _D3M_Q100K_HD12_ConceptScore ImageR/SearchResult_D3M_Q100K_ICMR13_HD12_topRank10K/part-r-00000/data ICMR2013/GVR/
	 * 		hadoop jar MapRed_GeoVisualRanking_VisConceptScoreConceptThr.jar RankStudy_MapRed.MapRed_GeoVisualRanking -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,MyAPI.jar,JSAT_r413.jar,lire136.jar -Dmapred.task.timeout=6000000 -Dmapred.latlons=ICMR2013/3M_latlon.float2 -Dmapred.userIDs_0=ICMR2013/3M_userIDs_0.long -Dmapred.userIDs_1=ICMR2013/3M_userIDs_1.int -Dmapred.num_topDocs=200 -Dmapred.G_ForGTSize=0.01 -Dmapred.V_ForGTSize=10000 -Dmapred.FilterMapFile=ConceptDetection/conceptScores/3M_conceptFeat_MapFile -Dmapred.reportPath=ICMR2013/GVR/ -Dmapred.isOneLocScales=0.01,0.1  -Dmapred.isSameLocScales=0.01,0.1  -Dmapred.num_topLocations=20 -Dmapred.binsForGTSize=0,1,5,10,20,40,100 -Dmapred.accumLevel=1,2,3,5,10,20 -Dmapred.saveRes=false -Dmapred.makeReport=true _D3M_Q100K_HD12_VisConceptScoreConceptThr0.8 ImageR/SearchResult_D3M_Q100K_ICMR13_HD12_topRank10K/part-r-00000/data ICMR2013/GVR/
	 * MEva13:	hadoop jar MapRed_GVR.jar MediaEval13.MapRed_GVR -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,MyAPI.jar,JSAT_r413.jar,lire136.jar -Dmapred.task.timeout=6000000 -Dmapred.job.priority=HIGH -Dmapred.latlons=MediaEval13/MEval13_latlons.floatArr -Dmapred.userIDs_0=MediaEval13/MEval13_userIDs_0.long -Dmapred.userIDs_1=MediaEval13/MEval13_userIDs_1.int -Dmapred.num_topDocs=200 -Dmapred.G_ForGTSize=0.01 -Dmapred.V_ForGTSize=10000 -Dmapred.FilterMapFile=MediaEval13/Global_PhoFeats/JCD_MFile -Dmapred.reportPath=MediaEval13/GVR/ -Dmapred.isOneLocScales=0.01,0.1  -Dmapred.isSameLocScales=0.01,0.1  -Dmapred.num_topLocations=20 -Dmapred.binsForGTSize=0,1,5,10,20,40,100 -Dmapred.accumLevel=1,2,3,5,10,20 -Dmapred.saveRes=false -Dmapred.makeReport=true -Dmapred.pho_S_to_L=MediaEval13/MEval13_S_to_L.intArr -Dmapred.imgPath=66M_Phos_Seqs/ _D9M_Q250K_Vis_GlobFilterTopLoc_SURFHD12_JCD MediaEval13/ranks/SURF_D9M_Q250K_HD12_topRank10K/part-r-00000/data MediaEval13/GVR/
	 * 			hadoop jar MapRed_GVR.jar MediaEval13.MapRed_GVR -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,MyAPI.jar,JSAT_r413.jar,lire136.jar -Dmapred.task.timeout=6000000 -Dmapred.job.priority=HIGH -Dmapred.latlons=MediaEval13/MEval13_latlons.floatArr -Dmapred.userIDs_0=MediaEval13/MEval13_userIDs_0.long -Dmapred.userIDs_1=MediaEval13/MEval13_userIDs_1.int -Dmapred.num_topDocs=200 -Dmapred.G_ForGTSize=0.01 -Dmapred.V_ForGTSize=1000 -Dmapred.FilterMapFile=MediaEval13/Global_PhoFeats/JCD_MFile -Dmapred.reportPath=TMM_GVR/GVR/ -Dmapred.isOneLocScales=0.01,0.1  -Dmapred.isSameLocScales=0.01,0.1  -Dmapred.num_topLocations=20 -Dmapred.binsForGTSize=0,1,5,10,20,40,100 -Dmapred.accumLevel=1,2,3,5,10,20 -Dmapred.saveRes=false -Dmapred.makeReport=true -Dmapred.pho_S_to_L=MediaEval13/MEval13_S_to_L.intArr -Dmapred.imgPath=66M_Phos_Seqs/ _D9M_Q250K_Vis_SURFHD12-20-20_1vs1AndHPM6-1000 TMM_GVR/imagR/ranks/SURF_D9M_Q250K_Rank_HDs12-HMW20_ReR1000_HDr20_top1K_1vs1AndHPM6_Q0_8 TMM_GVR/GVR/
	 * M13TMM:	hadoop jar MapRed_GVR.jar MediaEval13.MapRed_GVR -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,MyAPI.jar,JSAT_r413.jar,lire136.jar -Dmapred.task.timeout=6000000 -Dmapred.job.priority=HIGH -Dmapred.latlons=MediaEval13/MEval13_latlons.floatArr -Dmapred.userIDs_0=MediaEval13/MEval13_userIDs_0.long -Dmapred.userIDs_1=MediaEval13/MEval13_userIDs_1.int -Dmapred.num_topDocs=200 -Dmapred.G_ForGTSize=0.01 -Dmapred.V_ForGTSize=10000 -Dmapred.FilterMapFile=MediaEval13/Global_PhoFeats/JCD_MFile -Dmapred.isOneLocScales=0.01,0.1  -Dmapred.isSameLocScales=0.01,0.1  -Dmapred.num_topLocations=20 -Dmapred.binsForGTSize=0,1,5,10,20,40,100 -Dmapred.accumLevel=1,2,3,5,10,20 -Dmapred.saveRes=false -Dmapred.makeReport=true -Dmapred.pho_S_to_L=MediaEval13/MEval13_S_to_L.intArr -Dmapred.SelQuerys=MediaEval13/Querys_StoS_dev/ -Dmapred.imgPath=66M_Phos_Seqs/ _InTMM_D9M_Qdev_Vis_GlobFilterTopLoc_SURFHD12_JCD MediaEval13/ranks/SURF_D9M_Q250K_HD12_topRank10K/part-r-00000/data MediaEval13/GVR/inTMM/
	 * 
	 */
	
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new MapRed_GVR(), args);
		System.exit(ret);
	}

	@SuppressWarnings("unchecked")
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem hdfs=FileSystem.get(conf);
		String homePath="hdfs://p-head03.alley.sara.nl/user/yliu/";
		String[] otherArgs = args; //use this to parse args!
		String dateFormate="yyyy.MM.dd G 'at' HH:mm:ss z";
		//set common
		String label;     
		Boolean saveRes=Boolean.valueOf(conf.get("mapred.saveRes"));
		Boolean makeReport=Boolean.valueOf(conf.get("mapred.makeReport"));
		
		String jobLabel=otherArgs[0];//_D9M_Q250K_Vis_SURFHD12, _D9M_Q250K_RankScore_SURFHD12, _D9M_Q250K_Vis_GlobFilterTopLoc_SURFHD12_JCD, _D9M_Q250K_Vis_GlobFilterTopLoc_SURFHD12_Concept
		conf.set("mapred.jobLabel", jobLabel);
		String[] jobLabels=jobLabel.split("_");
		conf.set("mapred.targetFeatClassName",jobLabels[jobLabels.length-1]);
		
		PrintWriter outStr_report=null;
		if (makeReport) {
			outStr_report=new PrintWriter(new OutputStreamWriter(hdfs.create(new Path(homePath+otherArgs[2]+"Report"+jobLabel),false), "UTF-8"),true); 
		}
		
		General.dispInfo(outStr_report,"............... jobLabel:"+jobLabel+"   ............. ............. ");
		General.dispInfo(outStr_report,"num_topLocations:"+conf.get("mapred.num_topLocations")+", isOneLoc: "+conf.get("mapred.isOneLocScales")+"  \t  sameLoc: "+conf.get("mapred.isSameLocScales"));
		
		//set selected querys set
		String selQuery_combined_path=null;
		if (conf.get("mapred.SelQuerys")!=null) {//not use all query's rank, select some for tune parameters or test
			HashMap<Integer, Integer> selQuerys=new HashMap<Integer, Integer>(); 
			String queryHashMapPath=homePath+conf.get("mapred.SelQuerys");
			if (hdfs.isFile(new Path(queryHashMapPath))) {
				HashMap<Integer, Integer> oneSet=(HashMap<Integer, Integer>) General_Hadoop.readObject_HDFS(hdfs, queryHashMapPath);
				selQuerys.putAll(oneSet);
				selQuery_combined_path=queryHashMapPath+"_combined";
			}else {
				FileStatus[] files= hdfs.listStatus(new Path(queryHashMapPath));
				for (int i = 0; i < files.length; i++) {
					HashMap<Integer, Integer> oneSet=(HashMap<Integer, Integer>) General_Hadoop.readObject_HDFS(hdfs, files[i].getPath().toString());
					selQuerys.putAll(oneSet);
				}
				selQuery_combined_path=queryHashMapPath+"combined";
			}
			General_Hadoop.writeObject_HDFS(hdfs, selQuery_combined_path, selQuerys);
			General.dispInfo(outStr_report, "select "+selQuerys.size()+" queries");
		}
		
		General.dispInfo(outStr_report,General.dispTimeDate(System.currentTimeMillis(), dateFormate)+", start processing!  ..................");

		//set selectedConcept
//		conf.set("mapred.selectedConcept","3,4,6,10,13,14,16,18,19,30,33,35,41,46,48,50,53,59,60,62,68,69,71,73,76,84,101,126,128,140,145,150,153,180,182,188,191,197,203,205,217,222,224,226,239,242,246,283,286,295,298,309,324,329,348,350,353,357,359,365,367");
//		conf.set("mapred.selectedConcept","9,29,30,33,43,54,56,68,76,73,75,88,106,104,128,146,148,178,179,183,206,200,197,220,247,274,273,308,289,300,340,336,347,359");
//		General.dispInfo(outStr_report, "selectedConcept: "+conf.get("mapred.selectedConcept"));
//		General.dispInfo(outStr_report, "selectedConcept by each query, thr="+conf.get("mapred.Thr_distCon")+" for distinct concept");
		
		//******* run for reRank***********	
//		int[] reRankScales={100,200,500,1000,2000}; 
		int[] visScales={50,100}; //10,50,100,300,500
//		float[] geoExpanScales={(float)) 0.01,(float) 0.02,(float) 0.05,(float) 0.1};
		int[] RankThrs= jobLabel.contains("_GlobFilter")? new int[]{300}:new int[]{0}; //10,50,100,300,500
		int[] RankScales=jobLabel.contains("_GlobFilter")? new int[]{5}:new int[]{0};//5,10
		for (int RankThr:RankThrs){ // loop over parameters.
			for (int RankScale:RankScales){
//				int RankThr=500; int RankScale=5;
				conf.set("mapred.RankThr",RankThr+"");
				conf.set("mapred.RankScale",RankScale+"");
				String rankLoopLabel=jobLabel.contains("_GlobFilter")?"_RankThr"+RankThr+"_RankSca"+RankScale:"";
				//******** Ori ************
				label="_Ori"+rankLoopLabel;// 
//				oneJobsLoop("_SameUser", selQuery_combined_path, saveRes,  conf,  hdfs,  0,  
//						0,  0,  jobLabel,  label+"_sameUser",  homePath,  outStr_report, otherArgs[1], otherArgs[2]);
				oneJobsLoop("_noSameUser", selQuery_combined_path, saveRes,  conf,  hdfs,  0,  
						0,  0,  jobLabel,  label+"_noSameUser",  homePath,  outStr_report, otherArgs[1], otherArgs[2]);
				//******** GVR ************
//				for (int reRankScale:reRankScales){
					for (int visScale:visScales){ // loop over parameters
//						int visScale=300;
						int reRankScale=visScale;
						if (RankThr==0 || RankThr>=visScale) {
//							for(float geoExpanScale:geoExpanScales){
								float geoExpanScale=(float) 0.01;
								//*********  run jobs loop ***************
								label="_GVR"+rankLoopLabel+"_reRank"+reRankScale+"_VisSca"+visScale+"_expSca"+geoExpanScale;// +"_RankThr"+RankThr+"_RankScale"+RankSca
//								oneJobsLoop("_SameUser", selQuery_combined_path, saveRes,  conf,  hdfs,  reRankScale,  
//										visScale,  geoExpanScale,  jobLabel,  label+"_SameUser",  homePath,  outStr_report, otherArgs[1], otherArgs[2]); 
								oneJobsLoop("_noSameUser", selQuery_combined_path, saveRes,  conf,  hdfs,  reRankScale,  
										visScale,  geoExpanScale,  jobLabel,  label+"_noSameUser",  homePath,  outStr_report, otherArgs[1], otherArgs[2]);
//							}
						}
					}
//				}
			}
		}
						
		if (makeReport) {
			outStr_report.close();
		}
		General_Hadoop.deleteIfExist(selQuery_combined_path, hdfs);
		hdfs.close();
		return 0;
	}
	
	
//******************GVR*****************************	
	public void oneJobsLoop(String loopFlag, String selQuery_combined_path, Boolean saveRes, Configuration conf, FileSystem hdfs, int reRankScale, int visScale, float geoExpanScale, 
			String jobLabel, String taskLabel, String homePath, PrintWriter outStr_report, String In, String Out) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException{
		conf.set("mapred.topVisScale",visScale+"");
		conf.set("mapred.reRankScale",reRankScale+"");
		conf.set("mapred.geoExpanScale",geoExpanScale+"");
		//set label
		conf.set("mapred.taskLabel",taskLabel);
		General.dispInfo(outStr_report,"\n ...............................task label:"+taskLabel);
		//set commons
		ArrayList<Path> inputPaths=new ArrayList<Path>();
		ArrayList<String> cacheFilePaths=new ArrayList<String>();
		String out_reranked=Out+"rank"+jobLabel+taskLabel; //output path for reranked ranks

		//******* 1st job: query rerank  ******
		//set input/output path
		inputPaths.clear(); inputPaths.add(new Path(In));
		String job1_out=out_reranked+"_tempJob1Out";
		SelectSamples selectSamples=new SelectSamples(selQuery_combined_path, false);
		//set distributed cache, add latlons and concept Mapfile to Distributed cache
		cacheFilePaths.clear();
		cacheFilePaths.add(homePath+conf.get("mapred.latlons")+"#latlons.file"); //latlons path with symLink
		if(jobLabel.contains("_GlobFilter")){
			cacheFilePaths.add(homePath+conf.get("mapred.FilterMapFile")+"/part-r-00000/data#data"); //FilterMapFile data path with symLink
			cacheFilePaths.add(homePath+conf.get("mapred.FilterMapFile")+"/part-r-00000/index#index"); //FilterMapFile data path with symLink
		}
		if(loopFlag.contains("_noSameUser")){
			cacheFilePaths.add(homePath+conf.get("mapred.userIDs_0")+"#userIDs_0.file"); //userIDs_0
			cacheFilePaths.add(homePath+conf.get("mapred.userIDs_1")+"#userIDs_1.file"); //userIDs_1
		}
		selectSamples.addDistriCache_SelectSamples(cacheFilePaths);//selected queries
		//set reducer number
		int job1RedNum=visScale<=5000 ? 712:2000; //reducer number
		//run
		General_Hadoop.Job(conf, inputPaths.toArray(new Path[0]), job1_out, "GVR"+taskLabel, job1RedNum, 8, 2, true,
				MapRed_GVR.class, selectSamples.getMapper(), Partitioner_random.class,null, null,Reducer_processRank.class,
				IntWritable.class, IntList_FloatList.class, IntWritable.class,fistMatch_GTruth_Docs_GVSizes_docScores.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0L, 0,
				cacheFilePaths.toArray(new String[0]),null);
		
		//******* 2nd job: save, combine querys result, and group photos needed ******
		if (saveRes) {
			//set input/output path
			inputPaths.clear(); inputPaths.add(new Path(job1_out));
			//------------- combine querys result -----------------//
			General_Hadoop.Job(conf, inputPaths.toArray(new Path[0]), out_reranked, "GVR_save"+taskLabel, 1, 8, 2, true,
					MapRed_GVR.class, null, null,null, null,Reducer_InOut_1key_1value.class,
					IntWritable.class, fistMatch_GTruth_Docs_GVSizes_docScores.class, IntWritable.class,fistMatch_GTruth_Docs_GVSizes_docScores.class,
					SequenceFileInputFormat.class, MapFileOutputFormat.class, 0L, 10,
					null,null);
		}
		//******* 2nd job: analysis querys result******
		if (outStr_report!=null) {
			String InfoStrPath=homePath+out_reranked+".InfoStr";
			conf.set("mapred.InfoStrPath",InfoStrPath); //Job3 save MAPInfo as String object to InfoStrPath
			//set distributed cache, add latlons to Distributed cache
			cacheFilePaths.clear();
			cacheFilePaths.add(homePath+conf.get("mapred.latlons")+"#latlons.file"); //latlons path with symLink
			//set input/output path
			inputPaths.clear(); inputPaths.add(new Path(job1_out));
			//run
			General_Hadoop.Job(conf, inputPaths.toArray(new Path[0]), null, "GVR_analysis"+taskLabel, 1, 8, 2, false,
					MapRed_GVR.class, null, null,null, null,Reducer_makeReport.class,
					IntWritable.class, fistMatch_GTruth_Docs_GVSizes_docScores.class, IntWritable.class,fistMatch_GTruth_Docs_GVSizes_docScores.class,
					SequenceFileInputFormat.class, NullOutputFormat.class, 0L, 0,
					cacheFilePaths.toArray(new String[0]),null);
			String Info=(String) General_Hadoop.readObject_HDFS(hdfs, InfoStrPath);
			General.dispInfo(outStr_report,Info);
			outStr_report.flush();	
			//********* clean-up ***********//
			hdfs.delete(new Path(InfoStrPath), true);
		}
		//********* clean-up ***********//
		hdfs.delete(new Path(job1_out), true);
		General.dispInfo(outStr_report,General.dispTimeDate(System.currentTimeMillis(), "yyyy.MM.dd G 'at' HH:mm:ss z")+", finished!  ");	
	}
	
	//******* 1st job: query rerank  ******
	public static class Reducer_processRank extends Reducer<IntWritable,IntList_FloatList,IntWritable,fistMatch_GTruth_Docs_GVSizes_docScores>  {

		private String jobLabel;
		private String targetFeatClassName;
		private int queryNum;
		private float[][] latlons;
		private long[] userIDs_0;
		private int[] userIDs_1;
		private int topVisScale;
		private int reRankScale;
		private float geoExpanScale;
		private float G_ForGTSize;
		private int V_ForGTSize;
		private int RankThr;
		private int RankScale;
//		private int[] selectedConcept;
//		private float Thr_distCon;
		private MapFile.Reader FilterMFileReader;
		private int num_topDocs;
		private boolean disp;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//******* read jobLabel**************
			jobLabel=conf.get("mapred.jobLabel"); //_GlobFilter _Vis
			System.out.println("jobLabel: "+jobLabel);
			String taskLabel=conf.get("mapred.taskLabel");
			System.out.println("taskLabel: "+taskLabel);
			
			//******* read latlons, geoNeighbourNums, userIDs**************
			latlons=(float[][]) General.readObject("latlons.file");
			userIDs_0=(long[]) General.readObject("userIDs_0.file"); 
			userIDs_1=(int[]) General.readObject("userIDs_1.file"); 
			//check files in disCache
			File File_data=new File("latlons.file");
			String disCachePath=File_data.getAbsolutePath();
			String disCacheFolder=disCachePath.substring(0, disCachePath.indexOf(File_data.getName()));
			System.out.println("files in disCacheFolder in local hadoop-node: \n"+General.listFilesInfo(new File(disCacheFolder), -1));
			//******* read topVisScale, reRankScale **************
			topVisScale=Integer.valueOf(conf.get("mapred.topVisScale"));
			reRankScale=Integer.valueOf(conf.get("mapred.reRankScale"));
			//****** load FilterMapFile ************
			if(jobLabel.contains("_GlobFilter")){
				FilterMFileReader=General_Hadoop.openMapFileInNode("data", conf, true);
				//**** load featClassName ************//
				targetFeatClassName=conf.get("mapred.targetFeatClassName");
				System.out.println("targetFeatClassName: "+targetFeatClassName);
			}
			//******* read geoExpanScale  **************
			geoExpanScale=Float.valueOf(conf.get("mapred.geoExpanScale"));
			System.out.println("geoExpanScale: "+geoExpanScale);
			//******* read G_ForGTSize, V_ForGTSize for calculate GVSize  **************
			G_ForGTSize=Float.valueOf(conf.get("mapred.G_ForGTSize"));
			V_ForGTSize=Integer.valueOf(conf.get("mapred.V_ForGTSize"));
			System.out.println("G_ForGTSize:"+G_ForGTSize+", V_ForGTSize:"+V_ForGTSize);
			//******* read RankThr, RankScale for rankFilter  **************
			RankThr=Integer.valueOf(conf.get("mapred.RankThr"));
			RankScale=Integer.valueOf(conf.get("mapred.RankScale"));
			System.out.println("RankThr: "+RankThr+", RankScale: "+RankScale);
			//******* read selectedConcept**************
//			selectedConcept=null; //no selectedConcept, use all
//			if (conf.get("mapred.selectedConcept")!=null) {//with selectedConcept
//				selectedConcept=General.StrArrToIntArr(conf.get("mapred.selectedConcept").split(","));// selectedConcept, 0,1,2,3,5,10,20
//				System.out.println("selectedConcept: "+conf.get("mapred.selectedConcept"));
//			}
//			//select concept per query
//			Thr_distCon=Float.valueOf(conf.get("mapred.Thr_distCon"));
//			System.out.println("Thr_distCon: "+Thr_distCon);
			//******* read num_topDocs  **************
			num_topDocs=Integer.valueOf(conf.get("mapred.num_topDocs"));
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			disp=true;
			queryNum=0; //total point number in one reducer
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable QueryName, Iterable<IntList_FloatList> docs_scores_I, Context context) throws IOException, InterruptedException {
			//docs_scores: ranked docNames and Scores
			int queryName=QueryName.get(); IntList_FloatList docs_scores=new IntList_FloatList(); int loopNum=0;
			for(Iterator<IntList_FloatList> it=docs_scores_I.iterator();it.hasNext();){//only one element
				docs_scores=it.next();
				loopNum++;
			}
			General.Assert(loopNum==1, "error in Reducer_GVR! one photoName, one ranklist, loopNum should == 1, here loopNum="+loopNum);
			if (docs_scores.getIntegers().size()== docs_scores.getFloats().size()+1) {//in global feat, last doc is the feature ind! 
				docs_scores.getIntegers().remove(docs_scores.getIntegers().size()-1);
			}
			General.Assert(docs_scores.getIntegers().size()== docs_scores.getFloats().size(), 
					"err in Reducer_processRank! docs and scores are not equal length! docs:"+docs_scores.getIntegers().size()+", scores:"+docs_scores.getFloats().size());
			//check same user and delete query itself
			if (userIDs_0!=null && userIDs_1!=null) {//need delete same user, so query itself also deleted!
				General_geoRank.removeSameUser_forTopDocsScores(docs_scores.getIntegers(), docs_scores.getFloats(), queryName, userIDs_0, userIDs_1);
			}else if (userIDs_0==null && userIDs_1==null) {//no need to delete same user, but only query itself should be deleted!
				General_geoRank.removeQueryItself_forTopDocsScores(docs_scores.getIntegers(), docs_scores.getFloats(), queryName);
			}else {
				throw new InterruptedException("userIDs_0 and userIDs_1 should be both no-null for no same user; both null for with same user!");
			}
			
			int totRank_length=docs_scores.getIntegers().size();
			
			//calculate groundTruth
			ArrayList<Integer> visNeig_ForGTSize=new ArrayList<Integer>(docs_scores.getIntegers().subList(0, Math.min(totRank_length, V_ForGTSize)));
			ArrayList<int[]> gTruth= General_geoRank.get_GTruth(queryName, G_ForGTSize,visNeig_ForGTSize, latlons); //ArrayList<rank_photoName>
			if (gTruth.size()==0) {//no ground truth for this query, mark this use {-1,-1}
				int[] cell={-1,-1};
				gTruth.add(cell);
			}
			
			if(jobLabel.contains("_GlobFilter")){
				HashSet<Integer> selDocs=null;
				if(jobLabel.contains("_Concept")){//for concept-rank thr
					selDocs=General_geoRank.get_GoodDoc_basedOn_conceptRank(docs_scores.getIntegers(), Math.min(totRank_length,RankThr*RankScale), FilterMFileReader, null, queryName, RankThr);
				}else {//for lire global feat
					selDocs=General_geoRank.get_GoodDoc_basedOn_globalFeatRank(docs_scores.getIntegers(), Math.min(totRank_length,RankThr*RankScale), FilterMFileReader, queryName, RankThr, targetFeatClassName);
				}
				//pick out good docs
				General_geoRank.selGoodRankDocs(docs_scores.getIntegers(), docs_scores.getFloats(), selDocs);
				//update totRank_length
				totRank_length=docs_scores.getIntegers().size();
			}
			
			GTruth fistMatch; int[] topDocs_sel; int[] topDocGVSizes_sel; float[] topDocScores_sel; 
			if (reRankScale==0) {//no GVR, run for ori-rank
				//find rank of the first true math
				fistMatch = GTruth.get_firstTrueMatch(queryName, docs_scores.getIntegers(), G_ForGTSize, latlons);
							
				//make oriRanks, oriScores
				int usedRank_length=Math.min(num_topDocs,totRank_length);//some query do not have enough listed docs
				ArrayList<Integer> oriRanks=new  ArrayList<Integer>(docs_scores.getIntegers().subList(0, usedRank_length)); //use all, delete rank-0 query itself
				ArrayList<Float> oriScores=new ArrayList<Float>(docs_scores.getFloats().subList(0, usedRank_length)); //use all, delete rank-0 query itself

				//**get top-ranked docs' name
				int actNumTopDocs=oriRanks.size();
				topDocs_sel=new int[actNumTopDocs]; topDocGVSizes_sel=new int[actNumTopDocs]; topDocScores_sel=new float[actNumTopDocs]; 
				for(int i=0;i<actNumTopDocs;i++){
					topDocs_sel[i]=oriRanks.get(i);
					topDocScores_sel[i]=oriScores.get(i);
				}
			}else {//GVR
				int actVisScale=Math.min(topVisScale,totRank_length);//some query do not have enough listed docs
				int reRank_length=Math.min(reRankScale,totRank_length);//some query do not have enough listed docs
				
				//transfer docScore to similarity
				int maxNeedDocNum=Math.max(actVisScale, reRank_length);
				ArrayList<Float> docScores_needed=new ArrayList<Float>(maxNeedDocNum);
				if (jobLabel.contains("_Vis")) {//use visual score
					docScores_needed=new ArrayList<Float>(docs_scores.getFloats().subList(0, maxNeedDocNum));
					if ((docScores_needed.get(docScores_needed.size()-1)-docScores_needed.get(0))>0) {//score is dist, so after rank, last one is biggest
						float scaleFactor=50;
						General_IR.transferDist_to_Sim(docScores_needed,scaleFactor );
					}
				}else if (jobLabel.contains("_RankScore")) {//use rank score
					for (int i = 0; i < maxNeedDocNum; i++) {
						docScores_needed.add(General_IR.rankingScore_log(i+1));
					}
				}
				
				
				//a.0 for vis-rerank: make topDocVisualScores
				HashMap<Integer,Float> topDocVisualScores=new HashMap<Integer,Float>(actVisScale);
				for(int i=0;i<actVisScale;i++){//query itselft is deleted in oriRank, rank-0 is not query
					topDocVisualScores.put(docs_scores.getIntegers().get(i), docScores_needed.get(i));
				}
				//a.1 for rankScore-rerank: make topDocRankScores
//				HashMap<Integer,Float> topDocRankScores=new HashMap<Integer,Float>(actVisScale);
//				for(int i=0;i<actVisScale;i++){//query itselft is deleted in oriRank, rank-0 is not query
//					topDocRankScores.put(docs_scores.getIntegers().get(i), General_IR.rankingScore_log(i+1));
//				}
				//b. for concept score-rerank: 
//				ArrayList<Integer> topVisDocs = new  ArrayList<Integer>(docs_scores.getIntegers().subList(0, actVisScale));
				//c. for vis+concept-rerank: 
//				float normScore=docs_scores.getFloats().get(0);//use 1st ranked doc's sim score as the nomlizing score to normlize the sim to 0~1
				//d. for concept-rank thr
//				HashSet<Integer> selDocs_conRank=General_geoRank.get_GoodDoc_basedOn_conceptRank(docs_scores.getIntegers(), actVisScale, conceptMFile, null, queryName, conRank_thr);
				
				//make oriRanks, oriScores
				ArrayList<Integer> oriRanks=new  ArrayList<Integer>(docs_scores.getIntegers().subList(0, reRank_length)); //use all, delete rank-0 query itself
				ArrayList<Float> oriScores=new ArrayList<Float>(docScores_needed.subList(0, reRank_length)); //use all, delete rank-0 query itself
				//make ori_rankScore
//				ArrayList<Float> ori_rankScore=new ArrayList<Float>(oriRanks.size());
//				for(int i=0;i<oriRanks.size();i++){
//					ori_rankScore.add(General_IR.rankingScore_log(i+1));
//				}
				
				//concept selection by query
//				selectedConcept=General_geoRank.get_distinctConcept(queryName, conceptMFile, Thr_distCon);
				
				//******** rerank ********//
				//**re-rank--geo-neighbour
				ArrayList<int[]> reranked_doc_GVSize=new ArrayList<int[]>(num_topDocs*2);
				ArrayList<Float> reranked_score=new ArrayList<Float>(num_topDocs*2);
				//a.0 vis only
				General_geoRank.rerank_geoExpansion_VisOnly_returnRankScore(oriRanks,oriScores,topDocVisualScores,
						num_topDocs,reranked_doc_GVSize,reranked_score,latlons[0],latlons[1],geoExpanScale);
				//a.1 rankScore only
//				TreeMap<Integer[], float[]> reR_doc_scores= General_geoRank.rerank_geoExpansion_VisOnly_returnRankScore(oriRanks,ori_rankScore,topDocRankScores,latlons[0],latlons[1],geoExpanScale);
				//b. concept only
//				TreeMap<Integer[], float[]> reR_doc_scores = General_geoRank.rerank_geoExpansion_ConceptOnly_returnRankScore(oriRanks,oriScores,conceptMFile,selectedConcept,latlons[0],latlons[1],geoExpanScale,queryName,topVisDocs);
				//c. vis + concept, score
//				TreeMap<Integer[],float[]> 	reR_doc_scores = General_geoRank.rerank_geoExpansion_VisConcept_returnRankScore ( oriRanks,  oriScores,  topDocVisualScores,  conceptMFile, selectedConcept, latlons[0],latlons[1],  geoExpanScale,  queryName,  normScore);
				//d. use concept to filter out, vis
//				TreeMap<Integer[],float[]> 	reR_doc_scores = General_geoRank.rerank_geoExpansion_VisConceptThre_returnRankScore ( oriRanks,  oriScores,  topDocVisualScores,  conceptMFile, selectedConcept, latlons[0],latlons[1],  geoExpanScale,  queryName,  conceptThr, -1);
//				TreeMap<Integer[],float[]> 	reR_doc_scores = General_geoRank.rerank_geoExpansion_VisConceptRankThre_returnRankScore ( oriRanks,  oriScores,  topDocVisualScores,  selDocs_conRank, latlons[0],latlons[1],  geoExpanScale,  queryName);
				//e. use concept to filter out, vis+concept
//				TreeMap<Integer[],float[]> 	reR_doc_scores = General_geoRank.rerank_geoExpansion_VisConceptThre_returnRankScore ( oriRanks,  oriScores,  topDocVisualScores,  conceptMFile, selectedConcept, latlons[0],latlons[1],  geoExpanScale,  queryName,  conceptThr, normScore);

				//find rank of the first true math
				ArrayList<Integer> reranked_Docs=new ArrayList<Integer>(reranked_doc_GVSize.size());
				for (int i = 0; i < reranked_doc_GVSize.size(); i++) {
					reranked_Docs.add(reranked_doc_GVSize.get(i)[0]);
				}
				fistMatch = GTruth.get_firstTrueMatch(queryName, reranked_Docs, (float) 0.01, latlons);
				
				//**get top-ranked docs' name
				int actNumTopDocs=Math.min(reranked_doc_GVSize.size(),num_topDocs); //some query does not have enough docs in rank list
				topDocs_sel=new int[actNumTopDocs]; topDocGVSizes_sel=new int[actNumTopDocs]; topDocScores_sel=new float[actNumTopDocs]; 
				for(int i=0;i<actNumTopDocs;i++){
					topDocs_sel[i]=reranked_doc_GVSize.get(i)[0];
					topDocGVSizes_sel[i]=reranked_doc_GVSize.get(i)[1];
					topDocScores_sel[i]=reranked_score.get(i);
				}
			}
			
			
			//**output
			fistMatch_GTruth_Docs_GVSizes_docScores outValue=new fistMatch_GTruth_Docs_GVSizes_docScores(fistMatch,gTruth,topDocs_sel,topDocGVSizes_sel, topDocScores_sel);
			context.write(QueryName, outValue);
			if(disp==true){
				System.out.println("QueryName:"+QueryName.get());
				System.out.println("outValue.get_fistMatch():"+outValue.fistMatch);
				System.out.println("outValue.get_groudTSize():"+outValue.gTruth.size());
				System.out.println("outValue.get_Docs().length:"+outValue.Docs.length+", [0]:"+outValue.Docs[0]);
				System.out.println("outValue.get_GVSizes().length:"+outValue.GVSizes.length+", [0]:"+outValue.GVSizes[0]);
				System.out.println("outValue.get_docScores().length:"+outValue.docScores.length+", [0]:"+outValue.docScores[0]);
				disp=false;
			}		
			queryNum++;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
//			conceptMFile.close();
			System.out.println("one reducer finished! total query number: "+queryNum);
			super.setup(context);
	 	}
	}
	
	//******* 2nd job: analysis query result******
	public static class Reducer_makeReport extends Reducer<IntWritable,fistMatch_GTruth_Docs_GVSizes_docScores,IntWritable,fistMatch_GTruth_Docs_GVSizes_docScores>  {
		private Configuration conf;
		private FileSystem hdfs;
		private float[] isOneLocScales;
		private float[] isSameLocScales;
		private int num_evalRadius;
		private int num_topLocations;
		private float[][] latlons;
		private int[] binsForGTSize;
		private int[] accumLevel;
		private String InfoStrPath;
		private ArrayList<HashMap<Integer,int[]>>  groTSize_locHist_rerank_Groups;
		private int[][] totlocHist_reranks;
		private int queryNums;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			hdfs=FileSystem.get(conf);
			
			//******* read isOneLocScale, num_topLocations **************
			isOneLocScales=General.StrArrToFloatArr(conf.get("mapred.isOneLocScales").split(","));// isOneLocScales, 0.01,0.1,1.0
			isSameLocScales=General.StrArrToFloatArr(conf.get("mapred.isSameLocScales").split(","));// isSameLocScales, 0.01,0.1,1.0
			num_topLocations=Integer.valueOf(conf.get("mapred.num_topLocations"));
			
			//******* set num_evalRadius  **************
			General.Assert(isOneLocScales.length==isSameLocScales.length, "error in Reducer_makeReport, isOneLocScales and isSameLocScales should be equal length!");
			num_evalRadius=isOneLocScales.length;
			
			//******* read latlons, geoNeighbourNums, userIDs**************
			latlons=(float[][]) General.readObject("latlons.file");
//				geoNeighNums=(int[]) General_Hadoop.readObject_HDFS(hdfs, conf.get("mapred.geoNeighNums"));
//				userIDs_0=(long[]) General_Hadoop.readObject_HDFS(hdfs, conf.get("mapred.userIDs_0"));
//				userIDs_1=(int[]) General_Hadoop.readObject_HDFS(hdfs, conf.get("mapred.userIDs_1"));

			
			//******* read binsForGTSize**************
			binsForGTSize=General.StrArrToIntArr(conf.get("mapred.binsForGTSize").split(","));// grounTSize bins, 0,1,5,10,20,40,100
			
			//******* read binsForGTSize**************
			accumLevel=General.StrArrToIntArr(conf.get("mapred.accumLevel").split(","));// accumLevel, 1,2,3,5,10,20
			
			//**** set InfoStrPath ************//
			InfoStrPath=conf.get("mapred.InfoStrPath");
			
			//**** set groTSize_locHist_rerank_Group ************//
			groTSize_locHist_rerank_Groups= new ArrayList<HashMap<Integer,int[]>>(num_evalRadius);//groTSize, histogram of trueLoactin position, fist one is "not in top"
			for (int i = 0; i < num_evalRadius; i++) {
				groTSize_locHist_rerank_Groups.add(new HashMap<Integer, int[]>(binsForGTSize.length));
			}
			//**** set totlocHist_rerank ************//
			totlocHist_reranks=new int[num_evalRadius][num_topLocations+1]; //fist one is "not in top"
			
			queryNums=0;
			// ***** setup finsihed ***//
			System.out.println("only 1 reducer, combine result and analysize performance, save String obj to InfoStrPath: "+InfoStrPath);
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable Key_queryName, Iterable<fistMatch_GTruth_Docs_GVSizes_docScores> geoExpansionData, Context context) throws IOException, InterruptedException {
			//key: queryName, value: rank result
			
			//******** only one list in rank result! ************		
			int queryName=Key_queryName.get(); fistMatch_GTruth_Docs_GVSizes_docScores oneResult = null; int loopNum=0; 
			for(Iterator<fistMatch_GTruth_Docs_GVSizes_docScores> it=geoExpansionData.iterator();it.hasNext();){// loop over all HashMaps				
				oneResult= it.next();
				loopNum++;
			}
			General.Assert(loopNum==1, "error in Reducer_makeReport! one photoName, one rank, loopNum should == 1, here loopNum="+loopNum);
			
			//******* analysis this query's result by different evaluation radius *********************
			int grounTSize=oneResult.gTruth.size();
			if (grounTSize==1 && oneResult.gTruth.get(0)[0]==-1) {//if query do not have ground truth, then in GVR, it mark this with {-1,-1}
				grounTSize=0;
			}
			int[] topDocs = oneResult.Docs;
			
			for (int i = 0; i < num_evalRadius; i++) {
				float isOneLocScale=isOneLocScales[i];
				float isSameLocScale=isSameLocScales[i];
				int topDoc = 10;
				//get top Location Doc
				ArrayList<Integer> topLocationDocs=General_geoRank.get_topLocationDocs(num_topLocations, topDocs, isOneLocScale, latlons);
//				ArrayList<ArrayList<Integer>> LocList=General_geoRank.get_topLocationDocsList(num_topLocations, topDocs,  (float) isOneLocScale,  latlons);
				//get True-Location rank
				int trueLocRank=General_geoRank.get_trueLocRank(queryName, topLocationDocs, topDoc, isSameLocScale, latlons)+1;
//				int trueLocRank=General_geoRank.get_trueLocRank_fromList(queryName, LocList, isSameLocScale, latlons)+1;
				//set groTSize_locHist_Group
				int binInd=General.getBinInd_linear(binsForGTSize,grounTSize);
				if(groTSize_locHist_rerank_Groups.get(i).containsKey(binInd)){
					groTSize_locHist_rerank_Groups.get(i).get(binInd)[trueLocRank]++;
				}else{
					int[] queryNum_locHist =new int[num_topLocations+1];//fist one is "not in top"
					queryNum_locHist[trueLocRank]=1;
					groTSize_locHist_rerank_Groups.get(i).put(binInd, queryNum_locHist);
				}
				//set totlocHist
				totlocHist_reranks[i][trueLocRank]++;
			}

			queryNums++;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			//outPut as String
			StringBuffer outInfo=new StringBuffer();
			outInfo.append("accumLevel:"+"\t"+General.IntArrToString(accumLevel, "\t")+"\n");
			for (int i = 0; i < num_evalRadius; i++) {
				float isOneLocScale=isOneLocScales[i];
				float isSameLocScale=isSameLocScales[i];
				outInfo.append("isOneLocScale:"+isOneLocScale+", isSameLocScale:"+isSameLocScale+"\n");
				// ** compute accumulated TrueLocHist for grouped grounTSize ***//
				outInfo.append("******** Group grounTSize:  accumulated-TrueLocHist: \n");
				for(int binIndex:groTSize_locHist_rerank_Groups.get(i).keySet()){
					int[] trueLocHist_rerank=groTSize_locHist_rerank_Groups.get(i).get(binIndex);
					int qureyNum=General.sum_IntArr(trueLocHist_rerank);
					int[] trueLocHistAccu_rerank=General.makeAccum(accumLevel, trueLocHist_rerank);
					if(binIndex==0){//groundTruth size==0
						outInfo.append(0+"\t"+qureyNum+"\t"+General.floatArrToString(General.normliseArr(trueLocHistAccu_rerank, qureyNum), "\t", "0.0000")+"\n");
					}else{
						if(binIndex==binsForGTSize.length){//groundTruth size > bins' last value
							outInfo.append(">"+binsForGTSize[binsForGTSize.length-1]+"\t"+qureyNum+"\t"+General.floatArrToString(General.normliseArr(trueLocHistAccu_rerank, qureyNum), "\t", "0.0000")+"\n");
						}else{
							outInfo.append((binsForGTSize[binIndex-1]+1)+"--"+binsForGTSize[binIndex]+"\t"+qureyNum+"\t"+General.floatArrToString(General.normliseArr(trueLocHistAccu_rerank, qureyNum), "\t", "0.0000")+"\n");
						}
					}
				}
				// ** compute accumulated TrueLocHist for totlocHist_ori, totlocHist_rerank ***//
				int[] totlocHistAccu_rerank=General.makeAccum(accumLevel, totlocHist_reranks[i]); //no "not in top" querys, so when compute percent, should ./ totalQueryNum
				outInfo.append("******** total querys:  accumulated-TrueLocHist: \n");
				outInfo.append(General.floatArrToString(General.normliseArr(totlocHistAccu_rerank, queryNums), "\t", "0.0000")+"\n\n");
			}
			//outPut
			General_Hadoop.writeObject_HDFS(hdfs, InfoStrPath, outInfo.toString());
			
			// ***** setup finsihed ***//
			System.out.println("\n Reducer finished! total querys:"+queryNums);
			System.out.println("outInfo: \n"+ outInfo.toString());
			super.setup(context);
	 	}
	}
	
}
