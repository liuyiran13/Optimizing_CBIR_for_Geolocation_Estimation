package BuildRank;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import BuildRank.MapRed_buildRank.Mapper_GetQueryPoints;
import BuildRank.MapRed_buildRank.Mapper_GetQuerySize;
import BuildRank.MapRed_buildRank.Mapper_GetQueryVWHESig;
import BuildRank.MapRed_buildRank.Mapper_countVW_FeatNum;
import BuildRank.MapRed_buildRank.Reducer_ExtractQuerySize;
import BuildRank.MapRed_buildRank.Reducer_group_QDMatches;
import BuildRank.MapRed_buildRank.Reducer_makeVW_PartitionerIDs;
import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.Obj.Disp;
import MyAPI.SystemCommand.RenewKerberos;
import MyAPI.imagR.Conf_ImageR;
import MyAPI.imagR.ScoreDoc;
import MyAPI.imagR.TVector_Hadoop;
import MyCustomedHaoop.Combiner.Combiner_combine_IntArr_HESig_ShortArr_Arr;
import MyCustomedHaoop.Combiner.Combiner_sumValues;
import MyCustomedHaoop.KeyClass.Key_QID_DID;
import MyCustomedHaoop.MapRedFunction.MapRed_makeRawFeat_VW;
import MyCustomedHaoop.Partitioner.Partitioner_forSearchTVector;
import MyCustomedHaoop.Partitioner.Partitioner_random_sameKey;
import MyCustomedHaoop.Partitioner.Partitioner_random_sameKey_PartKey;
import MyCustomedHaoop.Reducer.Reducer_InOut.Reducer_InOut_1key_1value;
import MyCustomedHaoop.ValueClass.DID_Score;
import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr;
import MyCustomedHaoop.ValueClass.DocAllMatchFeats;
import MyCustomedHaoop.ValueClass.HESig;
import MyCustomedHaoop.ValueClass.ImageRegionMatch;
import MyCustomedHaoop.ValueClass.IntArr_HESig_ShortArr_Arr;
import MyCustomedHaoop.ValueClass.Int_MatchFeatArr;
import MyCustomedHaoop.ValueClass.PhotoSize;
import MyCustomedHaoop.ValueClass.SURFpoint_ShortArr;

public class MapRed_SpatialVerifyAll extends Configured implements Tool{

	/**Similar to MapRed_buildRank, but this one do spatial verification for all db photos, and output the mathchings
	 * 
	 * it takes 3 hours for spatial verify 9M db per query!
	 * 
	 * 
	 * @command_example: 
	 * 
	 * SURF(my own kmean VW):
	 * MEva13:	yarn jar MapRed_SpatialVerifyAll.jar BuildRank.MapRed_SpatialVerifyAll -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DSelQuerys=MediaEval13/DBAsQ_L_to_S_perSubSet100k -DindexPath=MM15/ImageR/ -DHMDistThr_selDoc=18 -DHMWeight_deta=12 -DisOnlyUseHMDistFor1Vs1=true -DHPM_ParaDim=4 -DHPM_level=6 -DhistRotation_binStep=0.52 -DhistScale_binStep=0.2 -DbinScaleRate=1 -DPointDisThr=0 -DbadPariWeight=0 -DweightThr=0 -DlineAngleStep=0 -DlineDistStep=0 -DsameLinePointNumThr=0 -DdocScoreThr=0 -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 -DisMultiAss=true -DvwSoftWeight=0.05 1000 1000 1000 66M_Phos_Seqs _MEva13_Rand10K MM15/ImageR/ranks/RandSelDB
	 */
	
	public static String[] oriArgs; //save all arguments
		
	public static void main(String[] args) throws Exception {		
//		prepareData();
		
		runHadoop(args);
	}
	
	public static void runHadoop(String[] args) throws Exception {
		oriArgs=args;
		int ret = ToolRunner.run(new MapRed_SpatialVerifyAll(), args);
		System.exit(ret);
	}
	
	@SuppressWarnings("unchecked")
	public static void prepareData() throws Exception {
//		//***** random select photos from 9M dataset as sample db ************
//		int selNum=10000;
//		HashMap<Integer, Integer> db_L_to_S=(HashMap<Integer, Integer>) General.readObject("O:/MediaEval13/MEval13_L_to_S_train.hashMap");
//		System.out.println("read db_L_to_S done!");
//		ArrayList<Integer> db_L=new ArrayList<>(db_L_to_S.keySet());
//		LinkedList<Integer> selphos=General.randSelect(new Random(2), selNum, db_L);
//		HashMap<Integer,Integer> sel_L_to_S=new HashMap<Integer, Integer>();
//		for (Integer one_L : selphos) {
//			sel_L_to_S.put(one_L, db_L_to_S.get(one_L));
//		}
//		System.out.println("done! sel_L_to_S:"+sel_L_to_S.size());
//		General.writeObject("O:/MediaEval13/MEval13_L_to_S_train_rand"+selNum/1000+"K.hashMap", sel_L_to_S);
	
		//***** for making sub-query-set for 9M, as when build rank, doc_scores take a lot space if dataset is large, so decrease the query number ************
		int querySetSize=100*1000;//
		HashMap<Integer, Integer> totQ=(HashMap<Integer, Integer>) General.readObject("O:/MediaEval13/MEval13_L_to_S_train.hashMap");
		String subQSetFolder="O:/MediaEval13/DBAsQ_L_to_S_perSubSet"+querySetSize/1000+"k/";
		General.makeORdelectFolder(subQSetFolder);
		Random rand=new Random();
		ArrayList<HashMap<Integer, Integer>> Qsets =General.randSplitHashMap(rand, totQ, 0, querySetSize);
		int totQnum=0;
		for (int i = 0; i < Qsets.size(); i++) {
			General.writeObject(subQSetFolder+"Q"+i+".hashMap", Qsets.get(i));
			System.out.println(i+", "+Qsets.get(i).size());
			totQnum+=Qsets.get(i).size();
		}
		General.Assert(totQnum==totQ.size(), "err, totQnum:"+totQnum+", should =="+totQ.size());
		System.out.println("taget querySetSize:"+querySetSize+", totQnum:"+totQnum+", should =="+totQ.size());

	}

	@Override
	public int run(String[] args) throws Exception {//this args is already parsed by ToolRunner, so not all the oriArgs
		Configuration conf = getConf(); 
		FileSystem hdfs=FileSystem.newInstance(conf);
		String[] otherArgs = args; //use this to parse args!
		ArrayList<String> cacheFilePaths=new ArrayList<String>();
		String dateFormate="yyyy.MM.dd G 'at' HH:mm:ss z";
		RenewKerberos renewTicket=new RenewKerberos();
		//set rankLabel_common
		String rankLabel_common="_HDs"+conf.get("HMDistThr_selDoc")+"-HMW"+conf.get("HMWeight_deta");
		//get vw_num
		float[][] centers=General_Hadoop.readMatrixFromSeq_floatMatrix(conf, new Path(Conf_ImageR.hdfs_address+conf.get("vwCenters")));
	    int vw_num=centers.length;
	    //set VWFileInter
	    int VWFileInter=conf.get("VWFileInter")==null?vw_num/1000:Integer.valueOf(conf.get("VWFileInter"));//by default VWFileInter=vw_num/1000
	    conf.set("VWFileInter", VWFileInter+"");
		//set reducer number
		int job1_1RedNum=Integer.valueOf(otherArgs[0]); //reducer number for extract query's SURF
		int job2_3RedNum=Integer.valueOf(otherArgs[1]); //reducer number for organize initial matches
		int job3RedNum=Integer.valueOf(otherArgs[2]); //reducer number for spatial verification
		//set imagesPath
		String imagesPath=otherArgs[3]; //input path
		ArrayList<Path> imageSeqPaths = General_Hadoop.addImgPathsFromMyDataSet(imagesPath);
		//set Index label
		String indexLabel=otherArgs[4]; //_Oxford_1M
		indexLabel+="_"+vw_num/1000+"K-VW";
		indexLabel+="_"+conf.get("targetFeature");
		conf.set("indexLabel", indexLabel);
		conf.set("TVector", conf.get("indexPath")+"TVector"+indexLabel);
		conf.set("docInfo", conf.get("indexPath")+"docInfo"+indexLabel);
		conf.set("TVectorInfo", conf.get("indexPath")+"TVectorInfo"+indexLabel);
		//set iniR_weight
		conf.set("iniR_weight", "_iniR-noBurst");
		//set output path
		String out=otherArgs[5]+indexLabel; //output path
		//set HMDistThr_selDoc
		String[] HMDistThr_selDoc=conf.get("HMDistThr_selDoc").split(",");
		//set HMDistThr_selDoc
		String[] HMWeight_deta=conf.get("HMWeight_deta").split(",");
		//set rerankFlag
		ArrayList<String> rerankFlags=new ArrayList<String>(); 
//		rerankFlags.add("_1vs1");
//		for (String para0 : conf.get("HPM_ParaDim").split(",")) {//2,4
//			for (String para1 : conf.get("HPM_level").split(",")) {//1,2,3,4,5
//				rerankFlags.add("_1vs1AndHPM@"+para0+"@"+para1);
//			}
//		}
//		for (String para0 : conf.get("histRotation_binStep").split(",")) {
//			for (String para1 : conf.get("histScale_binStep").split(",")) {
//				rerankFlags.add("_1vs1AndHist@"+para0+"@"+para1);
////				rerankFlags.add("_HistAnd1vs1@"+para0+"@"+para1);
//			}
//		}
		for (String para0 : conf.get("histRotation_binStep").split(",")) {
			for (String para1 : conf.get("histScale_binStep").split(",")) {
				for (String binScaleRate : conf.get("binScaleRate").split(",")) {
					for (String para2 : conf.get("PointDisThr").split(",")) {
						for (String para3 : conf.get("badPariWeight").split(",")) {
							for (String para4 : conf.get("weightThr").split(",")) {
								for (String para6 : conf.get("lineDistStep").split(",")) {
									if (Float.valueOf(para6)>0.0001) {//needs line detection
										for (String para5 : conf.get("lineAngleStep").split(",")) {
											for (String para7 : conf.get("sameLinePointNumThr").split(",")) {
												for (String para8 : conf.get("docScoreThr").split(",")) {
		//											rerankFlags.add("_1vs1AndAngle@"+para0+"@"+para1+"@"+para2+"@"+para3+"@"+para4+"@"+para5+"@"+para6+"@"+para7);
													rerankFlags.add("_1vs1AndHistAndAngle@"+para0+"@"+para1+"@"+binScaleRate+"@"+para2+"@"+para3+"@"+para4+"@"+para5+"@"+para6+"@"+para7+"@"+para8);
//													rerankFlags.add("_HistAnd1vs1AndAngle@"+para0+"@"+para1+"@"+binScaleRate+"@"+para2+"@"+para3+"@"+para4+"@"+para5+"@"+para6+"@"+para7+"@"+para8);
												}
											}
										}
									}else {//no line detection
										String para5="0";
										String para7="0";
										for (String para8 : conf.get("docScoreThr").split(",")) {
	//										rerankFlags.add("_1vs1AndAngle@"+para0+"@"+para1+"@"+para2+"@"+para3+"@"+para4+"@"+para5+"@"+para6+"@"+para7);
											rerankFlags.add("_1vs1AndHistAndAngle@"+para0+"@"+para1+"@"+binScaleRate+"@"+para2+"@"+para3+"@"+para4+"@"+para5+"@"+para6+"@"+para7+"@"+para8);
//											rerankFlags.add("_HistAnd1vs1AndAngle@"+para0+"@"+para1+"@"+binScaleRate+"@"+para2+"@"+para3+"@"+para4+"@"+para5+"@"+para6+"@"+para7+"@"+para8);
										}
									}
								}
							}
						}
					}
				}
			}
		}
//		for (String para0 : conf.get("histRotation_binStep").split(",")) {
//			for (String para1 : conf.get("histScale_binStep").split(",")) {
//				for (String binScaleRate : conf.get("binScaleRate").split(",")) {
//					for (String para2 : conf.get("PointDisThr").split(",")) {
//						for (String para3 : conf.get("badPariWeight").split(",")) {
//							for (String para4 : conf.get("weightThr").split(",")) {
//								for (String para6 : conf.get("lineDistStep").split(",")) {
//									if (Float.valueOf(para6)>0.0001) {//needs line detection
//										for (String para5 : conf.get("lineAngleStep").split(",")) {
//											for (String para7 : conf.get("sameLinePointNumThr").split(",")) {
//												for (String para8 : conf.get("docScoreThr").split(",")) {
//													for (String para9 : conf.get("HPM_ParaDim").split(",")) {//2,4
//														for (String para10 : conf.get("HPM_level").split(",")) {//1,2,3,4,5
//															rerankFlags.add("_1vs1AndHistAndAngleWithHPM@"+para0+"@"+para1+"@"+binScaleRate+"@"+para2+"@"+para3+"@"+para4+"@"+para5+"@"+para6+"@"+para7+"@"+para8+"@"+para9+"@"+para10);
//														}
//													}
//												}
//											}
//										}
//									}else {//no line detection
//										String para5="0";
//										String para7="0";
//										for (String para8 : conf.get("docScoreThr").split(",")) {
//											for (String para9 : conf.get("HPM_ParaDim").split(",")) {//2,4
//												for (String para10 : conf.get("HPM_level").split(",")) {//1,2,3,4,5
//													rerankFlags.add("_1vs1AndHistAndAngleWithHPM@"+para0+"@"+para1+"@"+binScaleRate+"@"+para2+"@"+para3+"@"+para4+"@"+para5+"@"+para6+"@"+para7+"@"+para8+"@"+para9+"@"+para10);
//												}
//											}
//										}
//									}
//								}
//							}
//						}
//					}
//				}
//			}
//		}
		String rerankLabel="_1vs1_1vs1AndHPM@paraDim"+conf.get("HPM_ParaDim")+"@level"+conf.get("HPM_level")
				+"_1vs1AndHist@histRotation_binStep"+conf.get("histRotation_binStep")+"@histScale_binStep"+conf.get("histScale_binStep")+"@binScaleRate"+conf.get("binScaleRate")
				+"@PointDisThr"+conf.get("PointDisThr")+"@badPariWeight"+conf.get("badPariWeight")+"@weightThr"+conf.get("weightThr")
				+"@lineAngleStep"+conf.get("lineAngleStep")+"@lineDistStep"+conf.get("lineDistStep")+"@sameLinePointNumThr"+conf.get("sameLinePointNumThr")
				+"@docScoreThr"+conf.get("docScoreThr");
		//set selected querys set
		ArrayList<String> selQuerys=new ArrayList<String>(); 
		String queryHashMapPath=conf.get("SelQuerys");
		if (hdfs.isFile(new Path(queryHashMapPath))) {
			selQuerys.add(queryHashMapPath);
		}else {
			FileStatus[] files= hdfs.listStatus(new Path(queryHashMapPath));
			for (int i = 0; i < files.length; i++) {
				selQuerys.add(files[i].getPath().toString());
			}
		}
		//set Report
		PrintWriter outStr_report=null;
		General.dispInfo(outStr_report,General.dispTimeDate(System.currentTimeMillis(), dateFormate)+", start processing!  ..................");
		General.dispInfo(outStr_report,"oriArgs:"+General.StrArrToStr(oriArgs, " "));
		General.dispInfo(outStr_report, "indexLabel: "+indexLabel+", vw_num:"+vw_num+", VWFileInter:"+VWFileInter+"\n"
				+"for Query, imagesPath:"+imagesPath+", resulting "+imageSeqPaths.size()+" imageSeqPaths:"+imageSeqPaths+"\n"
				+selQuerys.size()+" selQuerys: "+selQuerys+"\n"
				+"TVectorPath:"+conf.get("TVector")+", TVectorInfoPath:"+conf.get("TVectorInfo")+", docInfoPath:"+conf.get("docInfo")+"\n"
				+"work dir:"+out+"\n"
				+"job1_1RedNum for extract query's SURF:"+job1_1RedNum+", job2_3RedNum for organise iniMatches:"+job2_3RedNum+", job3RedNum for spatialVerification:"+job3RedNum
				+"rankLabels:"+rerankLabel);
			

		//**********************    build rank  ************//
		for (String one_HMDistThr_selDoc : HMDistThr_selDoc) {//HMDistThr_selDoc
			for (String one_HMWeight_deta : HMWeight_deta) {
				//commons
				conf.set("HMDistThr_selDoc", one_HMDistThr_selDoc);
				conf.set("HMWeight_deta", one_HMWeight_deta);
				rankLabel_common="_HDs"+one_HMDistThr_selDoc+"-HMW"+one_HMWeight_deta;
				//paths
				Path[][] rankPaths_rerank=new Path[rerankFlags.size()][selQuerys.size()];
				//run
				for (int i = 0; i < selQuerys.size(); i++) {
					renewTicket.renewTicket(true);
					String queryloopLabel="_Q"+i+"_"+(selQuerys.size()-1);	
					General.dispInfo(outStr_report, "start process "+queryloopLabel+", "+General.dispTimeDate(System.currentTimeMillis(), dateFormate));
					long startTime=System.currentTimeMillis();
					conf.set("HMDistThr", one_HMDistThr_selDoc); 
					//******* job1_1: extract query's SURF raw feats ******
					String Out_job1_1=out+queryloopLabel+"_querySURFRaw";
//					MapRed_makeRawFeat_VW.runHadoop(conf, imagesPath, Out_job1_1, selQuerys.get(i), job1_1RedNum, 0);
					General.dispInfo(outStr_report, "\t\t job1_1: extract query's SURF feats and make VW done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
					//******* job1_2: read query's SURF raw feats, save query_SURFpoint into MapFile ******
					String Out_job1_2=out+queryloopLabel+"_querySURFpoint";
//					General_Hadoop.Job(conf, new Path[]{new Path(Out_job1_1)}, Out_job1_2, "Job1_2_getSURFpoint"+queryloopLabel, 1, 8, 10, true,
//							MapRed_SpatialVerifyAll.class, Mapper_GetQueryPoints.class, null,null,null,Reducer_InOut_1key_1value.class,
//							IntWritable.class, SURFpoint_ShortArr.class, IntWritable.class,SURFpoint_ShortArr.class,
//							SequenceFileInputFormat.class, MapFileOutputFormat.class, 1*1024*1024*1024L, 0,
//							null,null);
					General.dispInfo(outStr_report, "\t\t job1_2: read query's SURF raw feats, save query_SURFpoint into MapFile done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
					//******* job1_3: group HESig ******
					String Out_job1_3=out+queryloopLabel+"_queryHESig";
//					General_Hadoop.Job(conf, new Path[]{new Path(Out_job1_1)}, Out_job1_3, "Job1_3_groupHESig"+queryloopLabel, job1_1RedNum, 8, 10, true,
//							MapRed_SpatialVerifyAll.class, Mapper_GetQueryVWHESig.class, null,null,null,null,
//							IntWritable.class, IntArr_HESig_ShortArr_Arr.class, IntWritable.class,IntArr_HESig_ShortArr_Arr.class,
//							SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
//							null,null);
					General.dispInfo(outStr_report, "\t\t job1_3: read query's SURF feats, and group HESig done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
					//******* job1_4: extract query's sizeInfo ******
					String QuerySize_HashMap_Path=out+queryloopLabel+"_querySize.hashMap";
					conf.set("QuerySize_HashMap", QuerySize_HashMap_Path); 
//					General_Hadoop.Job(conf, new Path[]{new Path(Out_job1_1)}, null, "Job1_4_getQuerySizes"+queryloopLabel, 1, 8, 10, true,
//							MapRed_SpatialVerifyAll.class, Mapper_GetQuerySize.class, null,null,null,Reducer_ExtractQuerySize.class,
//							IntWritable.class, PhotoSize.class, IntWritable.class,PhotoSize.class,
//							SequenceFileInputFormat.class, NullOutputFormat.class, 1*1024*1024*1024L, 0,
//							null,null);
					General.dispInfo(outStr_report, "\t\t job1_4: extract query's size info done! no outPut, save QuerySize_HashMap to "+QuerySize_HashMap_Path+", "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
					//******* job2_1: make VW_PartitionerIDs for partition reducers in Search TVector, no outPut, save VW_PartitionerIDs to VW_PartitionerIDs_Path ******
					conf.set("VW_PartitionerIDs", out+"_VW_PartitionerIDs"+queryloopLabel); 
					//add Distributed cache
					cacheFilePaths.clear();
					Conf_ImageR.addDistriCache_TVectorInfo(conf, cacheFilePaths); //TVectorInfo with symLink
//					General_Hadoop.Job(conf, new Path[]{new Path(Out_job1_3)}, null, "Job2_1_makeVW_PartitionerIDs"+queryloopLabel, 1, 8, 10, true,
//							MapRed_SpatialVerifyAll.class, Mapper_countVW_FeatNum.class, null, Combiner_sumValues.class, null, Reducer_makeVW_PartitionerIDs.class,
//							IntWritable.class, IntWritable.class, IntWritable.class,IntWritable.class,
//							SequenceFileInputFormat.class, NullOutputFormat.class, 0, 0,
//							cacheFilePaths.toArray(new String[0]),null);
					General.dispInfo(outStr_report, "\t\t job2_1: make VW_PartitionerIDs for partition reducers in Search TVector, no outPut, save VW_PartitionerIDs to "+conf.get("VW_PartitionerIDs")+" done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
					//set job2_2RedNum,job3_2RedNum,job4RedNum based on VW_PartitionerIDs
					int[] PaIDs=(int[]) General_Hadoop.readObject_HDFS(hdfs, new Path(conf.get("VW_PartitionerIDs")).toString());
					int job2_2RedNum=General_Hadoop.getTotReducerNum_from_vwPartitionIDs(PaIDs); //reducer number for seachTVector, PaIDs: values from 0!
					//******* job2_2: Search TVector, get query_doc hmScore for each vw ******
					String Out_job2_2=out+rankLabel_common+queryloopLabel+"_iniDocMatches";
//					//add Distributed cache
//					cacheFilePaths.clear();
//					Conf_ImageR.addDistriCache_docInfo(conf, cacheFilePaths); //docInfo with symLink
//					Conf_ImageR.addDistriCache_TVectorInfo(conf, cacheFilePaths); //TVectorInfo with symLink
//					Conf_ImageR.addDistriCache_VWPaIDs(conf, cacheFilePaths); //PaIDs with symLink
//					conf.set("rerankFlag", "_OriHE");
//					General_Hadoop.Job(conf, new Path[]{new Path(Out_job1_3)}, Out_job2_2, "Job2_2_getIniMatches"+queryloopLabel, job2_2RedNum, 8, 10, true,
//							MapRed_SpatialVerifyAll.class, null, Partitioner_forSearchTVector.class, Combiner_combine_IntArr_HESig_ShortArr_Arr.class, null, Reducer_SearchTVector_getMatches.class,
//							IntWritable.class, IntArr_HESig_ShortArr_Arr.class, Key_QID_DID.class, Int_MatchFeatArr.class,
//							SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0, 0,
//							cacheFilePaths.toArray(new String[0]),null);
					General.dispInfo(outStr_report, "\t\t job2_2: Search TVector, get query_doc ini matches for each vw done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
					//******* job3: spatial verification ******
					for (int j = 0; j < rerankFlags.size(); j++) {//run for different rerank strategy
						String Out_job3=out+rankLabel_common+queryloopLabel+rerankFlags.get(j)+"_verifiedMatches";
						conf.set("rerankFlag", rerankFlags.get(j));
						conf.set("rerankFlagInd", j+"");
						//add Distributed cache
						cacheFilePaths.clear();
						cacheFilePaths.add(Out_job1_2+"/part-r-00000#queryFeat.mapFile"); //queryFeat_MapFile data path with symLink
						Conf_ImageR.addDistriCache_docInfo(conf, cacheFilePaths); //docInfo with symLink
						Conf_ImageR.addDistriCache_TVectorInfo(conf, cacheFilePaths); //TVectorInfo with symLink
						Conf_ImageR.addDistriCache_QuerySize(conf, cacheFilePaths); //QuerySize_HashMap with symLink
						General_Hadoop.Job(conf, new Path[]{new Path(Out_job2_2)}, Out_job3, "spatialVerification"+queryloopLabel+"_F"+j+"-"+(rerankFlags.size()-1), job3RedNum, 8, 10, true,
								MapRed_SpatialVerifyAll.class, null, Partitioner_random_sameKey_PartKey.class,null, null, Reducer_SpatialVerification_saveDocMatches.class,
								Key_QID_DID.class, Int_MatchFeatArr.class, IntWritable.class, DID_Score_ImageRegionMatch_ShortArr.class,
								SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
								cacheFilePaths.toArray(new String[0]),null);	
						rankPaths_rerank[j][i]=new Path(Out_job3);//j:rankFlagInd, i:queryInd
						General.dispInfo(outStr_report, "\t\t\t\t\t job3: spatial verification done! save to "+Out_job3+", "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
					}
					//clean-up
					hdfs.delete(new Path(Out_job1_1), true);
					hdfs.delete(new Path(Out_job1_2), true);
					hdfs.delete(new Path(Out_job1_3), true);
					hdfs.delete(new Path(conf.get("QuerySize_HashMap")), true);
					hdfs.delete(new Path(conf.get("VW_PartitionerIDs")), true);
					hdfs.delete(new Path(Out_job2_2), true);// iniDocMatches
				}
				General.dispInfo(outStr_report, "All querys are done! "+General.dispTimeDate(System.currentTimeMillis(), dateFormate));
				//******* job4: save all querys' result  ******
				for (int j = 0; j < rerankFlags.size(); j++) {//run for different rerank strategy
					String Out_job3=out+rankLabel_common+rerankFlags.get(j)+"_verifiedMatches";
					General_Hadoop.Job(conf, rankPaths_rerank[j], Out_job3, "combine&save_"+j+"-"+(rerankFlags.size()-1), job3RedNum, 8, 10, true,
							MapRed_SpatialVerifyAll.class, null, null, null, null, null,
							IntWritable.class, DID_Score_ImageRegionMatch_ShortArr.class, IntWritable.class, DID_Score_ImageRegionMatch_ShortArr.class,
							SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0, 10,
							null,null);	
				}
				General.dispInfo(outStr_report, "\n combine all query results are done! "+General.dispTimeDate(System.currentTimeMillis(), dateFormate));
				//clean-up rankPaths
				for (Path[] paths: rankPaths_rerank) {
					for (Path path: paths) {
						hdfs.delete(path, true);
					}
				}
			}	
		}
		
		hdfs.close();
		return 0;
	}

	public static class Reducer_SearchTVector_getMatches extends Reducer<IntWritable,IntArr_HESig_ShortArr_Arr,Key_QID_DID,Int_MatchFeatArr>  {
	
		private Configuration conf;
		private ScoreDoc scoreDoc;
		private int VWFileInter;
		private String TVectorPath;
		private int[] PaIDs;
		private StringBuffer vws_queryNums;
		private int reduceNum;
		private int dispInter_reduce;
		private boolean disp;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			FileSystem.get(conf);
			Conf_ImageR conf_ImageR=new Conf_ImageR(conf);
			//check file in distributted cache
			General.checkDir(new Disp(true, "", null),".");
			//***** set TVector SeqFile Path***//
			VWFileInter=conf_ImageR.sd_VWFileInter;
			TVectorPath = Conf_ImageR.hdfs_address+conf_ImageR.sd_TVectorPath;
			System.out.println("TVectorPath:"+TVectorPath+", VWFileInter:"+VWFileInter);
			//********** setup scoreDoc ***************
			System.out.println("current memory:"+General.memoryInfo());
			scoreDoc= new ScoreDoc(new Disp(true, "\t", null), conf_ImageR);
			System.out.println("current memory:"+General.memoryInfo());
			//** set PaIDs **//
			PaIDs= (int[]) General.readObject(Conf_ImageR.sd_VWPaIDs);
			System.out.println("PaIDs load finished, total partioned reducer number : "+General_Hadoop.getTotReducerNum_from_vwPartitionIDs(PaIDs)+", job.setNumReduceTasks(jobRedNum) should == this value!!");
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			vws_queryNums=new StringBuffer();
			reduceNum=0;
			dispInter_reduce=1;
			disp=false;
			
	 	}
		
		@Override
		public void reduce(IntWritable VW, Iterable<IntArr_HESig_ShortArr_Arr> QueryNames_feats, Context context) throws IOException, InterruptedException {
			//QueryNameSigs: QueryName-Integer, Sigs:-ByteArrList
	
			int progInter=1000;  int vw=VW.get();
	
			reduceNum++;
			if (reduceNum%dispInter_reduce==0) {
				disp=true;
			}
			General.dispInfo_ifNeed(disp, "", "\n this reduce is for VW: "+VW+", total allocated reducers for this vw: "+General_Hadoop.getVWReducerNum_from_vwPartitionIDs(PaIDs, vw));
			
			long startTime=System.currentTimeMillis();
			int queryNum_thisVW=0; int queryFeatNum_thisVW=0; HESig[] queryFeats; int queryNum_existMatch=0;
			
			//********read TVector(SeqFile) into memory************
			TVector_Hadoop tVector=new TVector_Hadoop(context.getConfiguration(),TVectorPath, VWFileInter,disp);
			int TVectorFeatNum=tVector.readTVectorIntoMemory(vw);			
	
			//**** process
			if (TVectorFeatNum>0) {
				General.dispInfo_ifNeed(disp, "", "read this TVector into memory finished! docNum: "+ tVector.tVector.docID_feats.size()+", featNum: "+TVectorFeatNum
						+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "s")+", current memory info: "+General.memoryInfo());
				//******* search TVector ***********
				HashSet<Integer> checkDupliQuerys=new HashSet<Integer>();
				startTime=System.currentTimeMillis();
				//process querys have this vw
	        	for(Iterator<IntArr_HESig_ShortArr_Arr> it=QueryNames_feats.iterator();it.hasNext();){
	        		IntArr_HESig_ShortArr_Arr Querys=it.next(); //can not: it.next().getInteger(), it.next().getByteArr()
	        		int queryNum_thisInterator=Querys.getIntegers().length;
	        		for(int query_i=0;query_i<queryNum_thisInterator;query_i++){
		        		//********* process one query *********
						int queryName=Querys.getIntegers()[query_i]; queryFeats=Querys.getFeats()[query_i].getArr();
						General.Assert(checkDupliQuerys.add(queryName), "err! duplicate querys for VW:"+vw+", duplicate query:"+queryName);
						//compare docs in TVector for this query
						LinkedList<Int_MatchFeatArr>  matches=new LinkedList<>();
						ArrayList<DID_Score> docIDs=scoreDoc.scoreDocs_inOneTVector(queryFeats, vw, tVector.tVector, matches);
						//outputfile: SequcenceFile; outputFormat: key->queryID  value->IntList_FloatList
						if (docIDs.size()>0) {
							int doc_i=0;
							for (Int_MatchFeatArr oneDocM: matches) {
								context.write(new Key_QID_DID(queryName, docIDs.get(doc_i).getDID()), oneDocM); 
								doc_i++;
							}
							queryNum_existMatch++;
						}
						//************ report progress ******************
	        			queryNum_thisVW++; queryFeatNum_thisVW+=queryFeats.length;
						if(disp==true && queryNum_thisVW%progInter==0){
							long time=System.currentTimeMillis()-startTime;
							System.out.println("\t --curent total finished querys:"+queryNum_thisVW+", queryFeatNum_thisVW:"+queryFeatNum_thisVW+", queryNum_existMatch:"+queryNum_existMatch
									+", time:"+General.dispTime(time, "min")+", average compare time per sig pair:"+(double)time/TVectorFeatNum/queryFeatNum_thisVW);
							System.out.println("\t ----current memory info: "+General.memoryInfo());
							System.out.println("\t --current query:"+queryName+", exist matched docs for this vw: "+docIDs.size());
						}
	        		}
		        }
			}
			
	    	//*** some statistic ********
	    	General.dispInfo_ifNeed(disp, "", "one reduce finished! total query number for this vw: "+queryNum_thisVW+", queryFeatNum_thisVW:"+queryFeatNum_thisVW+", queryNum_existMatch:"+queryNum_existMatch);
	    	disp=false;
	    	vws_queryNums.append(vw+"_"+queryNum_thisVW+"_"+queryFeatNum_thisVW+"_"+queryNum_existMatch+",");
	    	
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("\n Reducer finished! reduceNum(vws num): "+reduceNum+", vws_queryNums_queryFeatNums_queryNumExistMatch: "+vws_queryNums.toString());
			
	 	}
	}
	
	public static class Reducer_SpatialVerification_saveDocMatches extends Reducer<Key_QID_DID, Int_MatchFeatArr, IntWritable, DID_Score_ImageRegionMatch_ShortArr>  {
			
		private Conf_ImageR conf_ImageR;
		private int ImagePairNum;
		private long startTime;
		private int dispInter;
		
		private MapFile.Reader queryFeatReader;
		private HashMap<Integer, int[]> QuerySize_HashMap;
		
		private ScoreDoc scoreDoc;
		
		private HashMap<Integer, Integer> queryID_iniDocNum;
		private HashMap<Integer, Integer> queryID_passVerifiedDocNum;
		
		@SuppressWarnings({ "unchecked" })
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			conf_ImageR=new Conf_ImageR(conf);
			//check file in distributted cache
			General.checkDir(new Disp(true, "", null), ".");
			//***** read query feats ***//
			queryFeatReader=General_Hadoop.openMapFileInNode("queryFeat.mapFile", conf, true);
			System.out.println("open query-Feat-MapFile finished");
			//***** read query Size_HashMap ***//
			QuerySize_HashMap=(HashMap<Integer, int[]>) General.readObject(Conf_ImageR.ef_QuerySize_HashMap);
			System.out.println("read QuerySize_HashMap finished! tot-query num:"+QuerySize_HashMap.size());
			//********** setup scoreDoc, makeRank***************
			System.out.println("current memory:"+General.memoryInfo());
			//-setup_scoreDoc for "_OriHE", "_1vs1", "_1vs1AndHPM", "_1vs1AndAngle"
			scoreDoc= new ScoreDoc(new Disp(true, "\t", null), conf_ImageR);
			//***** set queryID_previous ***//
			queryID_iniDocNum=new HashMap<>();
			queryID_passVerifiedDocNum=new HashMap<>();
			// ***** setup finsihed ***//
			ImagePairNum=0;
			startTime=System.currentTimeMillis();
			dispInter=10000;
			System.out.println("setup finsihed!\n");
//			debugFail=false;
			
	 	}
			
		@Override
		public void reduce(Key_QID_DID QID_DID, Iterable<Int_MatchFeatArr> values, Context context) throws IOException, InterruptedException {
			/**
			 * 1 reduce: process docs for 1 query, key: queryID, value: docID and this doc's MatchFeats for this query
			 * 
			 */
			ImagePairNum++;
			boolean disp=(ImagePairNum%dispInter==0); 
			int queryID=QID_DID.queryID; int docID=QID_DID.docID;
			//get query feat
			SURFpoint_ShortArr queryFeats=new SURFpoint_ShortArr();
			if(queryFeatReader.get(new IntWritable(queryID), queryFeats)==null){
				throw new InterruptedException("err! no feat for query:"+queryID);
			}
			//get query size
			int[] querySize=QuerySize_HashMap.get(queryID);
			int queryMaxSize=Math.max(querySize[0], querySize[1]);
			//run
			LinkedList<Int_MatchFeatArr>  matches = new LinkedList<Int_MatchFeatArr>(); 
			for (Int_MatchFeatArr docAllMatchFeats : values) {
				matches.add(docAllMatchFeats);
			}
			LinkedList<ImageRegionMatch> finalMatches=new LinkedList<ImageRegionMatch>();
			float[] docScores=scoreDoc.scoreOneDoc(new DocAllMatchFeats(docID, matches.toArray(new Int_MatchFeatArr[0])), queryFeats.getArr(), queryID, queryMaxSize, finalMatches, null, null, disp);
			if (finalMatches.size()>0) {
				context.write(new IntWritable(queryID), new DID_Score_ImageRegionMatch_ShortArr(docID, docScores[0], finalMatches));
				General.updateMap(queryID_passVerifiedDocNum, queryID, 1);
			}
			General.updateMap(queryID_iniDocNum, queryID, 1);
			
			//done
			General.dispInfo_ifNeed(disp, "", ImagePairNum+" ImagePairs are done! current finished QID_DID: "+QID_DID+"\n"
					+ "queryID_iniDocNum:"+queryID_iniDocNum+"\n"
							+ "queryID_passVerifiedDocNum: "+queryID_passVerifiedDocNum+"\n"
					+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );						
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** finsihed ***//	
			System.out.println("this reducer done! total "+ImagePairNum+" ImagePairs \n"
					+ "queryID_iniDocNum:"+queryID_iniDocNum+"\n"
							+ "queryID_passVerifiedDocNum: "+queryID_passVerifiedDocNum+"\n"
					+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );			
	 	}
	
	}

}
