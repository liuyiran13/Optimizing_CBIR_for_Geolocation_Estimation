//package BuildRank;
//
//import java.awt.image.BufferedImage;
//import java.io.IOException;
//import java.io.OutputStreamWriter;
//import java.io.PrintWriter;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.PriorityQueue;
//import java.util.Map.Entry;
//import java.util.Random;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.MapFile;
//import org.apache.hadoop.io.SequenceFile;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//
//import MyAPI.General.General;
//import MyAPI.General.General_BoofCV;
//import MyAPI.General.General_Hadoop;
//import MyAPI.General.General_IR;
//import MyAPI.General.BoofCV.ComparePhotos_LocalFeature;
//import MyAPI.General.ComparableCls.slave_masterFloat_DES;
//import MyCustomedHaoop.Combiner.Combiner_combine_IntArr_HESig_ShortArr_Arr;
//import MyCustomedHaoop.KeyClass.Key_QID_DID;
//import MyCustomedHaoop.KeyClass.Key_QID_VW;
//import MyCustomedHaoop.KeyClass.Key_RankFlagID_QID;
//import MyCustomedHaoop.Mapper.selectSamples.Mapper_selectSamples_hashMap;
//import MyCustomedHaoop.Partitioner.Partitioner_KeyisPartID;
//import MyCustomedHaoop.Partitioner.Partitioner_equalAssign_keyFrom0;
//import MyCustomedHaoop.Partitioner.Partitioner_forSearchTVector;
//import MyCustomedHaoop.Partitioner.Partitioner_random;
//import MyCustomedHaoop.Partitioner.Partitioner_random_sameKey;
//import MyCustomedHaoop.Partitioner.Partitioner_random_sameKey_PartKey;
//import MyCustomedHaoop.Reducer.Reducer_InOut.Reducer_InOut_1key_1value;
//import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
//import MyCustomedHaoop.ValueClass.DID_Score;
//import MyCustomedHaoop.ValueClass.DID_Score_Arr;
//import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr;
//import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr_Arr;
//import MyCustomedHaoop.ValueClass.DocAllMatchFeats;
//import MyCustomedHaoop.ValueClass.DouArr_ShortArr;
//import MyCustomedHaoop.ValueClass.DouArr_ShortArr_SURFpoint_ShortArr;
//import MyCustomedHaoop.ValueClass.HESig;
//import MyCustomedHaoop.ValueClass.HESig_ShortArr;
//import MyCustomedHaoop.ValueClass.ImageRegionMatch;
//import MyCustomedHaoop.ValueClass.IntArr;
//import MyCustomedHaoop.ValueClass.IntArr_HESig_ShortArr_Arr;
//import MyCustomedHaoop.ValueClass.IntList_FloatList;
//import MyCustomedHaoop.ValueClass.Int_MatchFeatArr;
//import MyCustomedHaoop.ValueClass.Int_MatchFeatArr_Arr;
//import MyCustomedHaoop.ValueClass.MatchFeat_Arr;
//import MyCustomedHaoop.ValueClass.SURFfeat;
//import MyCustomedHaoop.ValueClass.SURFpoint;
//import MyCustomedHaoop.ValueClass.SURFpoint_ShortArr;
//import MyCustomedHaoop.ValueClass.VW_DID_Score_Arr;
//
//public class MapRed_buildRank_withSameLineDetect extends Configured implements Tool{
//
//	/**version-2014.09.30
//	 * In Reducer_SearchTVector_getHMScore, it generate one DID_ScoreList Per VW, so takes 50+ TB for 30k query against 8.8M dataset
//	 * 
//	 * job1_1: extract query's SURF raw feats
//	 * mapper:  selected query, 
//	 * reducer: extract feat, output double[][], SURFpoint[]
//	 * @param: "mapred.SelQuerys"
//	 * 
//	 * job1_2: read query's SURF raw feats, save query_SURFpoint into MapFile
//	 * 
//	 * job1_3: read query's SURF raw feats, make HESig
//	 * @param: "mapred.VWPath"  "mapred.pMatrixPath"  "mapred.HEThresholdPath" "mapred.middleNode" "mapred.nodeLink_learned"
//	 *
//	 * job2: Search TVector, get query_MatchFeat for each vw
//	 * @param: "mapred.TVectorPath"  "mapred.HMDistThr_rankDoc" "mapred.VW_PartitionerIDs_Path"
//	 * 
//	 * job3: combine query_MatchFeat from each vw, build final rank for query 
//	 * @param: "mapred.topRank" "mapred.docInfoPath" "mapred.HPM_level" "mapred.HSigBit" "mapred.HMDistThr_rankDoc"
//	 * 
//	 * job4: save all querys' rank into one MapFile
//	 * 
//	 * 
//	 * 
//	 * @throws Exception 
//	 * @command_example: 
//	 * 
//	 * SURF(old Mahout-kmean VW):
//	 * 3M:		hadoop jar BuildRank.jar BuildRank.MapRed_buildRank_withSameLineDetect -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/SURFVW_20K_I90.seq -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix64 -Dmapred.HEThresholdPath=ImageR/HE_Thresholds64_VW20k_KMloop90.seq -Dmapred.middleNode=ImageR/middleNodes_M1000_VW20000_I200.seq -Dmapred.nodeLink_learned=ImageR/node_vw_links_M1000_VW20000_I200_learned30M.ArrayList_HashSet -Dmapred.SelQuerys=ICMR2013/Querys_100K_LtoS_from_D3M_ICMR2013.hashMap -Dmapred.TVectorPath=ImageR/TVector_3M_MapFile -Dmapred.HMDistThr=12 -Dmapred.docInfoPath=ImageR/photoFeatNum_3M -Dmapred.topRank=10000 1000 1000 1 3M_Photos_SeqFiles ImageR/SearchResult_D3M_Q100K_ICMR13 _rankDocScore
//	 * MEva13:	hadoop jar BuildRank.jar BuildRank.MapRed_buildRank_withSameLineDetect -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/SURFVW_20K_I90.seq -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix64 -Dmapred.HEThresholdPath=ImageR/HE_Thresholds64_VW20k_KMloop90.seq -Dmapred.middleNode=ImageR/middleNodes_M1000_VW20000_I200.seq -Dmapred.nodeLink_learned=ImageR/node_vw_links_M1000_VW20000_I200_learned30M.ArrayList_HashSet -Dmapred.SelQuerys=MediaEval13/Querys/ -Dmapred.TVectorPath=MediaEval13/TVector_MEva13_9M_MapFile -Dmapred.HMDistThr=12 -Dmapred.docInfoPath=MediaEval13/photoFeatNum_MEva13_9M -Dmapred.topRank=1000 1000 1000 66M_Phos_Seqs MediaEval13/ranks/SURF_D9M_Q250K _rankDocScore
//	 * Herve:	hadoop jar BuildRank.jar BuildRank.MapRed_buildRank_withSameLineDetect -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/SURFVW_20K_I90.seq -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix64-64 -Dmapred.HEThresholdPath=ImageR/HE_Thresholds64_VW20k_KMloop90.seq -Dmapred.middleNode=ImageR/middleNodes_M1000_VW20000_I200.seq -Dmapred.nodeLink_learned=ImageR/node_vw_links_M1000_VW20000_I200_learned30M.ArrayList_HashSet -Dmapred.SelQuerys=ImageR/BenchMark/Herve/Herve_querys_L_to_L.hashMap -Dmapred.indexPath=ImageR/BenchMark/Herve/SURF_oldVW/ -Dmapred.HMDistThr_selDoc=18 -Dmapred.HMDistThr_rankDoc=18 -Dmapred.HMWeight_deta=20 -Dmapred.HPM_ParaDim=2,4 -Dmapred.HPM_level=3,4,5,6,7 -Dmapred.histRotation_binStep=0.26,0.52,0.78 -Dmapred.histScale_binStep=0.1,0.2,0.3,0.4 -Dmapred.PointDisThr=0.001,0.01,0.1 -Dmapred.badPariWeight=0.1,0.2,0.3 -Dmapred.weightThr=1,2,3,5 -Dmapred.lineAngleStep=0.28,0.57,0.84 -Dmapred.lineDistStep=0.001,0.01,0.1 -Dmapred.docScoreThr=0,5,10,20 -Dmapred.reRankLength=1000 -Dmapred.topRank=1000 -Dmapred.groundTrueForReport=ImageR/BenchMark/Herve/Herve_groundTruth.hashMap -Dmapred.s_to_lForReport=ImageR/BenchMark/Herve/Herve_ori1.5K_SelPhos_S_to_L.intArr -Dmapred.targetFeature=SURF -Dmapred.BinTool_SIFT= -Dmapred.targetImgSize=786432 -Dmapred.isParaQueries=false -Dmapred.saveRes=false -Dmapred.makeReport=true -Dmapred.thresholdsForPRCurve=100,80,50,40,30,20,10,5,0 50 50 50 ImageR/BenchMark/Herve/HerverImage.seq _Herve_1.5K ImageR/BenchMark/Herve/SURF_oldVW/ranks/R _rankDocScore
//	 * Oxford:	hadoop jar BuildRank.jar BuildRank.MapRed_buildRank_withSameLineDetect -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/SURFVW_20K_I90.seq -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix64-64 -Dmapred.HEThresholdPath=ImageR/HE_Thresholds64_VW20k_KMloop90.seq -Dmapred.middleNode=ImageR/middleNodes_M1000_VW20000_I200.seq -Dmapred.nodeLink_learned=ImageR/node_vw_links_M1000_VW20000_I200_learned30M.ArrayList_HashSet -Dmapred.SelQuerys=ImageR/BenchMark/Oxford/Oxford_querys_L_to_L.hashMap -Dmapred.indexPath=ImageR/BenchMark/Oxford/SURF_oldVW/ -Dmapred.HMDistThr_selDoc=18 -Dmapred.HMDistThr_rankDoc=18 -Dmapred.HMWeight_deta=20 -Dmapred.HPM_ParaDim=2,4 -Dmapred.HPM_level=3 -Dmapred.histRotation_binStep=0.52 -Dmapred.histScale_binStep=0.1 -Dmapred.PointDisThr=0.001 -Dmapred.badPariWeight=0.1 -Dmapred.weightThr=3 -Dmapred.lineAngleStep=0.52 -Dmapred.lineDistStep=0.01 -Dmapred.docScoreThr=10 -Dmapred.reRankLength=1000 -Dmapred.topRank=1000 -Dmapred.groundTrueForReport=ImageR/BenchMark/Oxford/OxfordBuilding_groundTruth.hashMap -Dmapred.s_to_lForReport=ImageR/BenchMark/Oxford/Oxford_ori5K_SelPhos_S_to_L.intArr -Dmapred.junksForReport=ImageR/BenchMark/Oxford/OxfordBuilding_junks.hashMap -Dmapred.buildingInd_NameForReport=ImageR/BenchMark/Oxford/OxfordBuilding_buildingInd_Name.hashMap -Dmapred.targetFeature=SURF -Dmapred.BinTool_SIFT= -Dmapred.targetImgSize=786432 -Dmapred.isParaQueries=false -Dmapred.saveRes=false -Dmapred.makeReport=true -Dmapred.thresholdsForPRCurve=100,80,50,40,30,20,10,5,0 10 10 10 ImageR/BenchMark/Oxford/OxfordBuilding.seq _Oxford_5K ImageR/BenchMark/Oxford/SURF_oldVW/ranks/SURF _rankDocScore
//	 * SanFran:	hadoop jar BuildRank.jar BuildRank.MapRed_buildRank_withSameLineDetect -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/SURFVW_20K_I90.seq -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix64 -Dmapred.HEThresholdPath=ImageR/HE_Thresholds64_VW20k_KMloop90.seq -Dmapred.middleNode=ImageR/middleNodes_M1000_VW20000_I200.seq -Dmapred.nodeLink_learned=ImageR/node_vw_links_M1000_VW20000_I200_learned30M.ArrayList_HashSet -Dmapred.SelQuerys=ImageR/BenchMark/SanFrancisco/SanFrancisco_querys_transIndex_L_to_S.hashMap -Dmapred.TVectorPath=ImageR/BenchMark/SanFrancisco/TVector -Dmapred.HMDistThr_selDoc=18 -Dmapred.HMDistThr_rankDoc=18 -Dmapred.HMWeight_deta=20 -Dmapred.docInfoPath=ImageR/BenchMark/SanFrancisco/docInfo -Dmapred.TVectorInfoPath=ImageR/BenchMark/SanFrancisco/TVectorInfo -Dmapred.HPM_level=6 -Dmapred.reRankLength=1000 -Dmapred.topRank=1000 -Dmapred.groundTrueForReport= -Dmapred.s_to_lForReport= -Dmapred.saveRes=true -Dmapred.makeReport=false 720 720 720 ImageR/BenchMark/SanFrancisco/SanFrancisco_MFile_inLindex/data _SanFrancisco ImageR/BenchMark/SanFrancisco/ranks/SURF _rankDocScore
//	 * TMM_GVR: hadoop jar BuildRank.jar BuildRank.MapRed_buildRank_withSameLineDetect -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/SURFVW_20K_I90.seq -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix64 -Dmapred.HEThresholdPath=ImageR/HE_Thresholds64_VW20k_KMloop90.seq -Dmapred.middleNode=ImageR/middleNodes_M1000_VW20000_I200.seq -Dmapred.nodeLink_learned=ImageR/node_vw_links_M1000_VW20000_I200_learned30M.ArrayList_HashSet -Dmapred.SelQuerys=MediaEval13/Querys/ -Dmapred.TVectorPath=TMM_GVR/imagR/TVector -Dmapred.HMDistThr_selDoc=18 -Dmapred.HMDistThr_rankDoc=18 -Dmapred.HMWeight_deta=20 -Dmapred.docInfoPath=TMM_GVR/imagR/docInfo -Dmapred.TVectorInfoPath=TMM_GVR/imagR/TVectorInfo -Dmapred.HPM_level=6 -Dmapred.reRankLength=1000 -Dmapred.topRank=1000 -Dmapred.groundTrueForReport= -Dmapred.s_to_lForReport= -Dmapred.isParaQueries=true -Dmapred.saveRes=true -Dmapred.makeReport=false 1000 2000 1000 66M_Phos_Seqs _MEva13_9M TMM_GVR/imagR/ranks/SURF _rankDocScore
//	 * JaeVid:	hadoop jar BuildRank.jar BuildRank.MapRed_buildRank_withSameLineDetect -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/SURFVW_20K_I90.seq -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix64 -Dmapred.HEThresholdPath=ImageR/HE_Thresholds64_VW20k_KMloop90.seq -Dmapred.middleNode=ImageR/middleNodes_M1000_VW20000_I200.seq -Dmapred.nodeLink_learned=ImageR/node_vw_links_M1000_VW20000_I200_learned30M.ArrayList_HashSet -Dmapred.SelQuerys=MediaEval13/Jaeyoung_video/VideoFrameQuery_L_to_S -Dmapred.TVectorPath=TMM_GVR/imagR/TVector -Dmapred.HMDistThr_selDoc=18 -Dmapred.HMDistThr_rankDoc=18 -Dmapred.HMWeight_deta=20 -Dmapred.docInfoPath=TMM_GVR/imagR/docInfo -Dmapred.TVectorInfoPath=TMM_GVR/imagR/TVectorInfo -Dmapred.HPM_level=6 -Dmapred.reRankLength=1000 -Dmapred.topRank=1000 -Dmapred.groundTrueForReport= -Dmapred.s_to_lForReport= -Dmapred.isParaQueries=false -Dmapred.saveRes=true -Dmapred.makeReport=false 1000 2000 1000 MediaEval13/Jaeyoung_video/VideoFrameQuery.seq _MEva13_9M MediaEval13/Jaeyoung_video/ranks/SURF _rankDocScore
//	 * 
//	 * SURF(my own kmean VW):
//	 * Herve:	hadoop jar BuildRank.jar      BuildRank.MapRed_buildRank_withSameLineDetect -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix64-64 -Dmapred.HEThresholdPath=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -Dmapred.middleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -Dmapred.nodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100knode_vw_links_M1000_VW20k_I200.ArrayList_HashSet -Dmapred.SelQuerys=ImageR/BenchMark/Herve/Herve_querys_L_to_L.hashMap -Dmapred.indexPath=ImageR/BenchMark/Herve/SURF/ -Dmapred.HMDistThr_selDoc=18 -Dmapred.HMDistThr_rankDoc=18 -Dmapred.HMWeight_deta=20 -Dmapred.HPM_ParaDim=2,4 -Dmapred.HPM_level=6 -Dmapred.histRotation_binStep=0.26,0.52,0.78 -Dmapred.histScale_binStep=0.1,0.2,0.3 -Dmapred.PointDisThr=0.0001,0.001,0.01 -Dmapred.badPariWeight=0.1,0.2 -Dmapred.weightThr=1,3,5 -Dmapred.lineAngleStep=0.52 -Dmapred.lineDistStep=0.01 -Dmapred.docScoreThr=0,5,10,15,20 -Dmapred.reRankLength=1000 -Dmapred.topRank=1000000 -Dmapred.groundTrueForReport=ImageR/BenchMark/Herve/Herve_groundTruth.hashMap -Dmapred.s_to_lForReport=ImageR/BenchMark/Herve/Herve_ori1.5K_SelPhos_S_to_L.intArr -Dmapred.targetFeature=SURF -Dmapred.BinTool_SIFT= -Dmapred.targetImgSize=786432 -Dmapred.isParaQueries=false -Dmapred.saveRes=false -Dmapred.makeReport=true -Dmapred.thresholdsForPRCurve=100,80,50,40,30,20,10,5,0 50 50 50 ImageR/BenchMark/Herve/HerverImage.seq _Herve_1.5K ImageR/BenchMark/Herve/SURF/ranks/SURF _rankDocScore
//	 * Oxford:	hadoop jar BuildRank.jar 	  BuildRank.MapRed_buildRank_withSameLineDetect -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix64-64 -Dmapred.HEThresholdPath=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -Dmapred.middleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -Dmapred.nodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100knode_vw_links_M1000_VW20k_I200.ArrayList_HashSet -Dmapred.SelQuerys=ImageR/BenchMark/Oxford/Oxford_querys_L_to_L.hashMap -Dmapred.indexPath=ImageR/BenchMark/Oxford/SURF/ -Dmapred.HMDistThr_selDoc=18 -Dmapred.HMDistThr_rankDoc=18 -Dmapred.HMWeight_deta=20 -Dmapred.HPM_ParaDim=2,4 -Dmapred.HPM_level=3,4,5,6,7 -Dmapred.histRotation_binStep=0.26,0.52,0.78 -Dmapred.histScale_binStep=0.1,0.2,0.3 -Dmapred.PointDisThr=0.0001,0.001,0.01 -Dmapred.badPariWeight=0.1,0.2 -Dmapred.weightThr=1,3,5 -Dmapred.lineAngleStep=0.52 -Dmapred.lineDistStep=0.01 -Dmapred.docScoreThr=0,5,10,15,20 -Dmapred.reRankLength=1000 -Dmapred.topRank=1000 -Dmapred.groundTrueForReport=ImageR/BenchMark/Oxford/OxfordBuilding_groundTruth.hashMap -Dmapred.s_to_lForReport=ImageR/BenchMark/Oxford/Oxford_ori5K_SelPhos_S_to_L.intArr -Dmapred.junksForReport=ImageR/BenchMark/Oxford/OxfordBuilding_junks.hashMap -Dmapred.buildingInd_NameForReport=ImageR/BenchMark/Oxford/OxfordBuilding_buildingInd_Name.hashMap -Dmapred.targetFeature=SURF -Dmapred.BinTool_SIFT= -Dmapred.targetImgSize=786432 -Dmapred.isParaQueries=false -Dmapred.saveRes=false -Dmapred.makeReport=true -Dmapred.thresholdsForPRCurve=100,80,50,40,30,20,10,5,0 10 10 10 ImageR/BenchMark/Oxford/OxfordBuilding.seq _Oxford_5K ImageR/BenchMark/Oxford/SURF/ranks/OriQ _rankDocScore
//	 * Oxford:	hadoop jar BuildRank.jar 	  BuildRank.MapRed_buildRank_withSameLineDetect -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix64-64 -Dmapred.HEThresholdPath=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -Dmapred.middleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -Dmapred.nodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100knode_vw_links_M1000_VW20k_I200.ArrayList_HashSet -Dmapred.SelQuerys=ImageR/BenchMark/Oxford/Oxford_querys_L_to_L.hashMap -Dmapred.indexPath=ImageR/BenchMark/Oxford/SURF/ -Dmapred.HMDistThr_selDoc=18 -Dmapred.HMDistThr_rankDoc=18 -Dmapred.HMWeight_deta=20 -Dmapred.HPM_ParaDim=2,4 -Dmapred.HPM_level=3,4,5,6,7 -Dmapred.histRotation_binStep=0.26,0.52,0.78 -Dmapred.histScale_binStep=0.1,0.2,0.3 -Dmapred.PointDisThr=0.0001,0.001,0.01 -Dmapred.badPariWeight=0.1,0.2 -Dmapred.weightThr=1,3,5 -Dmapred.lineAngleStep=0.52 -Dmapred.lineDistStep=0.01 -Dmapred.docScoreThr=0,5,10,15,20 -Dmapred.reRankLength=1000 -Dmapred.topRank=1000 -Dmapred.groundTrueForReport=ImageR/BenchMark/Oxford/OxfordBuilding_groundTruth.hashMap -Dmapred.s_to_lForReport=ImageR/BenchMark/Oxford/Oxford_ori5K_SelPhos_S_to_L.intArr -Dmapred.junksForReport=ImageR/BenchMark/Oxford/OxfordBuilding_junks.hashMap -Dmapred.buildingInd_NameForReport=ImageR/BenchMark/Oxford/OxfordBuilding_buildingInd_Name.hashMap -Dmapred.QueryPos_HashMap=ImageR/BenchMark/Oxford/QueryID_Postions.hashMap -Dmapred.targetFeature=SURF -Dmapred.BinTool_SIFT= -Dmapred.targetImgSize=786432 -Dmapred.isParaQueries=false -Dmapred.saveRes=false -Dmapred.makeReport=true -Dmapred.thresholdsForPRCurve=100,80,50,40,30,20,10,5,0 10 10 10 ImageR/BenchMark/Oxford/OxfordBuilding.seq _Oxford_5K ImageR/BenchMark/Oxford/SURF/ranks/CutQ _rankDocScore
//	 * Oxford:	hadoop jar BuildRank.jar 	  BuildRank.MapRed_buildRank_withSameLineDetect -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix64-64 -Dmapred.HEThresholdPath=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -Dmapred.middleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -Dmapred.nodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100knode_vw_links_M1000_VW20k_I200.ArrayList_HashSet -Dmapred.SelQuerys=ImageR/BenchMark/Oxford/Oxford_querys_L_to_L.hashMap -Dmapred.indexPath=ImageR/BenchMark/Oxford/SURF_AllCutQ/ -Dmapred.HMDistThr_selDoc=18 -Dmapred.HMDistThr_rankDoc=18 -Dmapred.HMWeight_deta=20 -Dmapred.HPM_ParaDim=2,4 -Dmapred.HPM_level=3,4,5,6,7 -Dmapred.histRotation_binStep=0.26,0.52,0.78 -Dmapred.histScale_binStep=0.1,0.2,0.3 -Dmapred.PointDisThr=0.0001,0.001,0.01 -Dmapred.badPariWeight=0.1,0.2 -Dmapred.weightThr=1,3,5 -Dmapred.lineAngleStep=0.52 -Dmapred.lineDistStep=0.01 -Dmapred.docScoreThr=0,5,10,15,20 -Dmapred.reRankLength=1000 -Dmapred.topRank=1000 -Dmapred.groundTrueForReport=ImageR/BenchMark/Oxford/OxfordBuilding_groundTruth.hashMap -Dmapred.s_to_lForReport=ImageR/BenchMark/Oxford/Oxford_ori5K_SelPhos_S_to_L.intArr -Dmapred.junksForReport=ImageR/BenchMark/Oxford/OxfordBuilding_junks.hashMap -Dmapred.buildingInd_NameForReport=ImageR/BenchMark/Oxford/OxfordBuilding_buildingInd_Name.hashMap -Dmapred.targetFeature=SURF -Dmapred.BinTool_SIFT= -Dmapred.targetImgSize=786432 -Dmapred.isParaQueries=false -Dmapred.saveRes=false -Dmapred.makeReport=true -Dmapred.thresholdsForPRCurve=100,80,50,40,30,20,10,5,0 10 10 10 ImageR/BenchMark/Oxford/OxfordBuilding_cutQ.seq _Oxford_5K ImageR/BenchMark/Oxford/SURF_AllCutQ/ranks/AllCutQ _rankDocScore
//	 * Barceln:	hadoop jar BuildRank.jar   BuildRank.MapRed_buildRank_withSameLineDetect -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix64-64 -Dmapred.HEThresholdPath=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -Dmapred.middleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -Dmapred.nodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100knode_vw_links_M1000_VW20k_I200.ArrayList_HashSet -Dmapred.SelQuerys=ImageR/BenchMark/Barcelona/Barcelona_querys_L_to_L.hashMap -Dmapred.indexPath=ImageR/BenchMark/Barcelona/SURF/ -Dmapred.HMDistThr_selDoc=18 -Dmapred.HMDistThr_rankDoc=18 -Dmapred.HMWeight_deta=20 -Dmapred.HPM_ParaDim=2,4 -Dmapred.HPM_level=4,5,6 -Dmapred.histRotation_binStep=0.26,0.52,0.78 -Dmapred.histScale_binStep=0.1,0.2,0.3,0.4 -Dmapred.PointDisThr=0.001,0.01 -Dmapred.badPariWeight=0.1,0.2 -Dmapred.weightThr=1,2,3,5 -Dmapred.lineAngleStep=0.28,0.52,0.84 -Dmapred.lineDistStep=0.001,0.01 -Dmapred.docScoreThr=0,5,10,15,20 -Dmapred.reRankLength=1000 -Dmapred.topRank=1000 -Dmapred.groundTrueForReport=ImageR/BenchMark/Barcelona/Barcelona_groundTruthBuildingID.hashMap -Dmapred.s_to_lForReport=ImageR/BenchMark/Barcelona/Barcelona_ori1K_SelPhos_S_to_L.intArr -Dmapred.targetFeature=SURF -Dmapred.BinTool_SIFT= -Dmapred.targetImgSize=786432 -Dmapred.isParaQueries=false -Dmapred.saveRes=false -Dmapred.makeReport=true -Dmapred.thresholdsForPRCurve=10,5,4,3,2,1,0 10 10 10 ImageR/BenchMark/Barcelona/Barcelona1K.seq _Barcelona_1K ImageR/BenchMark/Barcelona/SURF/ranks/SURF _rankDocScore
//	 * Barceln:	hadoop jar BuildRank.jar   BuildRank.MapRed_buildRank_withSameLineDetect -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix64-64 -Dmapred.HEThresholdPath=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -Dmapred.middleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -Dmapred.nodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100knode_vw_links_M1000_VW20k_I200.ArrayList_HashSet -Dmapred.SelQuerys=ImageR/BenchMark/Barcelona/Barcelona_allPhotoAsQuery_L_to_L.hashMap -Dmapred.indexPath=ImageR/BenchMark/Barcelona/SURF/ -Dmapred.HMDistThr_selDoc=18 -Dmapred.HMDistThr_rankDoc=18 -Dmapred.HMWeight_deta=20 -Dmapred.HPM_ParaDim=2,4 -Dmapred.HPM_level=4,5,6 -Dmapred.histRotation_binStep=0.26,0.52,0.78 -Dmapred.histScale_binStep=0.1,0.2,0.3,0.4 -Dmapred.PointDisThr=0.001,0.01 -Dmapred.badPariWeight=0.1,0.2 -Dmapred.weightThr=1,2,3,5 -Dmapred.lineAngleStep=0.28,0.52,0.84 -Dmapred.lineDistStep=0.001,0.01 -Dmapred.docScoreThr=0,5,10 -Dmapred.reRankLength=1000 -Dmapred.topRank=1000 -Dmapred.groundTrueForReport=ImageR/BenchMark/Barcelona/Barcelona_groundTruthBuildingID.hashMap -Dmapred.s_to_lForReport=ImageR/BenchMark/Barcelona/Barcelona_ori1K_SelPhos_S_to_L.intArr -Dmapred.targetFeature=SURF -Dmapred.BinTool_SIFT= -Dmapred.targetImgSize=786432 -Dmapred.isParaQueries=false -Dmapred.saveRes=false -Dmapred.makeReport=true -Dmapred.thresholdsForPRCurve=10,5,4,3,2,1,0 10 10 10 ImageR/BenchMark/Barcelona/Barcelona1K.seq _Barcelona_1K ImageR/BenchMark/Barcelona/SURF/ranks/SURF-PairWisePRCurve _rankDocScore
//	 * MEva14:	hadoop jar BuildRank_ME14.jar BuildRank.MapRed_buildRank_withSameLineDetect -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -Dmapred.pMatrixPath=ImageR/HE_ProjectionMatrix64-64 -Dmapred.HEThresholdPath=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -Dmapred.middleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -Dmapred.nodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100knode_vw_links_M1000_VW20k_I200.ArrayList_HashSet -Dmapred.SelQuerys=MediaEval14/Querys_perSubSet20k -Dmapred.indexPath=MediaEval14/ -Dmapred.HMDistThr_selDoc=18 -Dmapred.HMDistThr_rankDoc=18 -Dmapred.HMWeight_deta=20 -Dmapred.HPM_ParaDim=4 -Dmapred.HPM_level=6 -Dmapred.reRankLength=1000 -Dmapred.topRank=1000 -Dmapred.targetFeature=SURF -Dmapred.BinTool_SIFT= -Dmapred.targetImgSize=786432 -Dmapred.isParaQueries=true -Dmapred.saveRes=true -Dmapred.makeReport=false 1000 1000 1000 Webscope100M/ME14_Crawl/Photos _MEva14_5MPho MediaEval14/ranks/R _rankDocScore
//	 * 
//	 * SIFT:
//	 * Herve:	hadoop jar BuildRank.jar BuildRank.MapRed_buildRank_withSameLineDetect -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/forVW/SIFT/SIFT-binTool_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -Dmapred.pMatrixPath=ImageR/forVW/SIFT/HE_ProjectionMatrix128-64 -Dmapred.HEThresholdPath=ImageR/forVW/SIFT/SIFT-binTool_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -Dmapred.middleNode=ImageR/forVW/SIFT/MiddleNode1000_onVW20k_maxLoop200/part-r-00000 -Dmapred.nodeLink_learned=ImageR/forVW/SIFT/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -Dmapred.SelQuerys=ImageR/BenchMark/Herve/Herve_querys_L_to_L.hashMap -Dmapred.indexPath=ImageR/BenchMark/Herve/SIFT/ -Dmapred.HMDistThr_selDoc=18 -Dmapred.HMDistThr_rankDoc=18 -Dmapred.HMWeight_deta=20 -Dmapred.HPM_ParaDim=2,4 -Dmapred.HPM_level=5,6 -Dmapred.histRotation_binStep=0.52,0.78 -Dmapred.histScale_binStep=0.2,0.4 -Dmapred.PointDisThr=0.001,0.01 -Dmapred.badPariWeight=0.1,0.2 -Dmapred.weightThr=1,3 -Dmapred.lineAngleStep=0.52 -Dmapred.lineDistStep=0.001,0.01 -Dmapred.docScoreThr=0,5 -Dmapred.reRankLength=1000 -Dmapred.topRank=1000000 -Dmapred.groundTrueForReport=ImageR/BenchMark/Herve/Herve_groundTruth.hashMap -Dmapred.s_to_lForReport=ImageR/BenchMark/Herve/Herve_ori1.5K_SelPhos_S_to_L.intArr -Dmapred.targetFeature=SIFT-binTool-Oxford2 -Dmapred.BinTool_SIFT=ImageR/forVW/SIFT/Oxford2_extract_features_64bit.ln -Dmapred.targetImgSize=786432 -Dmapred.isParaQueries=false -Dmapred.saveRes=false -Dmapred.makeReport=true 50 50 50 ImageR/BenchMark/Herve/HerverImage.seq _Herve_1.5K ImageR/BenchMark/Herve/SIFT/ranks/R _rankDocScore
//	 * Herve:	hadoop jar BuildRank.jar BuildRank.MapRed_buildRank_withSameLineDetect -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/forVW/SIFT_INRIA2/SIFT-binTool-INRIA2_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -Dmapred.pMatrixPath=ImageR/forVW/SIFT/HE_ProjectionMatrix128-64 -Dmapred.HEThresholdPath=ImageR/forVW/SIFT_INRIA2/SIFT-binTool-INRIA2_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -Dmapred.middleNode=ImageR/forVW/SIFT_INRIA2/MiddleNode1000_onVW20k_maxLoop200/part-r-00000 -Dmapred.nodeLink_learned=ImageR/forVW/SIFT_INRIA2/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -Dmapred.SelQuerys=ImageR/BenchMark/Herve/Herve_querys_L_to_L.hashMap -Dmapred.indexPath=ImageR/BenchMark/Herve/SIFT_INRIA2/ -Dmapred.HMDistThr_selDoc=18 -Dmapred.HMDistThr_rankDoc=18 -Dmapred.HMWeight_deta=20 -Dmapred.HPM_ParaDim=2,4 -Dmapred.HPM_level=5,6 -Dmapred.histRotation_binStep=0.52,0.78 -Dmapred.histScale_binStep=0.2,0.4 -Dmapred.PointDisThr=0.001,0.01 -Dmapred.badPariWeight=0.1,0.2 -Dmapred.weightThr=1,3 -Dmapred.lineAngleStep=0.52 -Dmapred.lineDistStep=0.001,0.01 -Dmapred.docScoreThr=0,5 -Dmapred.reRankLength=1000 -Dmapred.topRank=1000000 -Dmapred.groundTrueForReport=ImageR/BenchMark/Herve/Herve_groundTruth.hashMap -Dmapred.s_to_lForReport=ImageR/BenchMark/Herve/Herve_ori1.5K_SelPhos_S_to_L.intArr -Dmapred.targetFeature=SIFT-binTool-INRIA2 -Dmapred.BinTool_SIFT=ImageR/forVW/SIFT/INRIA2_compute_descriptors_linux64 -Dmapred.targetImgSize=786432 -Dmapred.isParaQueries=false -Dmapred.saveRes=false -Dmapred.makeReport=true 50 50 50 ImageR/BenchMark/Herve/HerverImage.seq _Herve_1.5K ImageR/BenchMark/Herve/SIFT_INRIA2/ranks/R _rankDocScore
//	 * Herve:	hadoop jar BuildRank.jar BuildRank.MapRed_buildRank_withSameLineDetect -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.VWPath=ImageR/forVW/SIFT_VLFeat/SIFT-binTool-VLFeat_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -Dmapred.pMatrixPath=ImageR/forVW/SIFT/HE_ProjectionMatrix128-64 -Dmapred.HEThresholdPath=ImageR/forVW/SIFT_VLFeat/SIFT-binTool-VLFeat_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -Dmapred.middleNode=ImageR/forVW/SIFT_VLFeat/MiddleNode1000_onVW20k_maxLoop200/part-r-00000 -Dmapred.nodeLink_learned=ImageR/forVW/SIFT_VLFeat/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -Dmapred.SelQuerys=ImageR/BenchMark/Herve/Herve_querys_L_to_L.hashMap -Dmapred.indexPath=ImageR/BenchMark/Herve/SIFT_VLFeat/ -Dmapred.HMDistThr_selDoc=18 -Dmapred.HMDistThr_rankDoc=18 -Dmapred.HMWeight_deta=20 -Dmapred.HPM_ParaDim=2,4 -Dmapred.HPM_level=5,6 -Dmapred.histRotation_binStep=0.52,0.78 -Dmapred.histScale_binStep=0.2,0.4 -Dmapred.PointDisThr=0.001,0.01 -Dmapred.badPariWeight=0.1,0.2 -Dmapred.weightThr=1,3 -Dmapred.lineAngleStep=0.52 -Dmapred.lineDistStep=0.001,0.01 -Dmapred.docScoreThr=0,5 -Dmapred.reRankLength=1000 -Dmapred.topRank=1000000 -Dmapred.groundTrueForReport=ImageR/BenchMark/Herve/Herve_groundTruth.hashMap -Dmapred.s_to_lForReport=ImageR/BenchMark/Herve/Herve_ori1.5K_SelPhos_S_to_L.intArr -Dmapred.targetFeature=SIFT-binTool-VLFeat -Dmapred.BinTool_SIFT=ImageR/forVW/SIFT_VLFeat/VLFeat09_sift_linux64 -Dmapred.BinTool_libs=ImageR/forVW/SIFT_VLFeat/libvl.so -Dmapred.targetImgSize=786432 -Dmapred.isParaQueries=false -Dmapred.saveRes=false -Dmapred.makeReport=true 50 50 50 ImageR/BenchMark/Herve/HerverImage.seq _Herve_1.5K ImageR/BenchMark/Herve/SIFT_VLFeat/ranks/R _rankDocScore
//	 */
//	
//	public static void main(String[] args) throws Exception {
////		timeTest();
//		
////		prepareData();
//		
//		runHadoop(args);
//	}
//	
//	public static void runHadoop(String[] args) throws Exception {
//		int ret = ToolRunner.run(new MapRed_buildRank_withSameLineDetect(), args);
//		System.exit(ret);
//	}
//	
//	@SuppressWarnings("unchecked")
//	public static void prepareData() throws Exception {
////		//***** for 3M random sellected querys *********//
////		int totNum=3185258; int queryNum=100*1000;
////		int[] randInds=General.randIndex(totNum);
////		HashMap<Integer, Integer> query_transIndex_LtoS= new HashMap<Integer, Integer>(queryNum);
////		for (int i = 0; i < queryNum; i++) {
////			query_transIndex_LtoS.put(randInds[i], randInds[i]);
////		}
////		General.writeObject("D:/xinchaoli/Desktop/My research/My Code/DataSaved/ICMR2013/Querys_100K_LtoS_from_D3M.hashMap", query_transIndex_LtoS);
//		
////		//***** for ICMR2013 3M random sellected querys *********//
////		String quryFilePath="D:/xinchaoli/Desktop/My research/My Code/DataSaved/ICMR2013/QDP/3M/";
////		String[] fileNames={"topLocGVSize_ori.oriRight","topLocGVSize_ori.oriWrong"};
////		int queryNum=100*1000; String line1;
////		HashMap<Integer, Integer> query_transIndex_LtoS= new HashMap<Integer, Integer>(queryNum);
////		for (int i = 0; i < fileNames.length; i++) {
////			BufferedReader inputStreamFeat = new BufferedReader(new InputStreamReader(new FileInputStream(quryFilePath+fileNames[i]), "UTF-8"));
////			while((line1=inputStreamFeat.readLine())!=null){//Q259_G4_R1:	0	0	0	0	0	0	0	0	0	0	
////				int queryName=Integer.valueOf(line1.split(":")[0].split("_")[0].substring(1));
////				query_transIndex_LtoS.put(queryName, queryName);
////			}
////			inputStreamFeat.close();
////		}
////		System.out.println("total query num:"+query_transIndex_LtoS.size());
////		General.writeObject("D:/xinchaoli/Desktop/My research/My Code/DataSaved/ICMR2013/Querys_100K_LtoS_from_D3M_ICMR2013.hashMap", query_transIndex_LtoS);
//		
//		//***** check photoFeatNum *********//
////		int[] photoFeatNum=(int[]) General.readObject("D:/xinchaoli/Desktop/My research/My Code/DataSaved/ImageRetrieval/photoFeatNum_3M");
////		int[] photoFeatNum=(int[]) General.readObject("O:/MediaEval13/photoFeatNum_MEva13_9M");
////		int noFeatPhoNum=0;
////		for (int pho_i : photoFeatNum) {
////			if (pho_i==0) {
////				noFeatPhoNum++;
////			}
////		}
////		System.out.println("totPhoNum:"+photoFeatNum.length+", some photo no feat, noFeatPhoNum:"+noFeatPhoNum);
//		
//		//***** for making sub-query-set for 9M dataset, as when build rank, doc_scores take a lot space if dataset is large, so decrease the query number ************
//		int querySetSize=20*1000;//30*1000 for 9M
//		HashMap<Integer, Integer> totQ=(HashMap<Integer, Integer>) General.readObject("O:/MediaEval14/MEval14_photos_L_to_S_test.hashMap");
//		String subQSetFolder="O:/MediaEval14/Querys_perSubSet"+querySetSize/1000+"k/";
//		General.makeORdelectFolder(subQSetFolder);
//		Random rand=new Random();
//		ArrayList<HashMap<Integer, Integer>> Qsets =General.randSplitHashMap(rand, totQ, 0, querySetSize);
//		int totQnum=0;
//		for (int i = 0; i < Qsets.size(); i++) {
//			General.writeObject(subQSetFolder+"Q"+i, Qsets.get(i));
//			System.out.println(i+", "+Qsets.get(i).size());
//			totQnum+=Qsets.get(i).size();
//		}
//		General.Assert(totQnum==totQ.size(), "err, totQnum:"+totQnum+", should =="+totQ.size());
//		System.out.println("taget querySetSize:"+querySetSize+", totQnum:"+totQnum+", should =="+totQ.size());
//		
//	}
//	
//	public static void timeTest() throws Exception {
//		
////		//******** time test ***********/
////		int TVectorLength=10*1000*1000; int HMDistThr=12; int matchNum=0; int HElength=64;
////		//make HESig_Bytes
////		BitSet HESig=new BitSet(HElength);
////		for(int i=0;i<HElength;i++)
////			HESig.set(i);// set i-th == true 
////		System.out.println(HESig);
////		byte[] HESig_Bytes=General.BitSettoByteArray(HESig); 
////		//make HESig_Bytes_q
////		BitSet HESig_q=new BitSet(HElength);
////		for(int i=0;i<HElength;i+=3)
////			HESig_q.set(i);// set i-th == true 
////		System.out.println(HESig_q);
////		byte[] HESig_Bytes_q=General.BitSettoByteArray(HESig_q);
////		//test HammingDist Time
////		long startTime=System.currentTimeMillis(); //startTime
////		int hammingDist=0;
////		for(int j=0;j<TVectorLength;j++){
////			hammingDist=General.get_DiffBitNum(HESig_Bytes_q, HESig_Bytes);// computing time: 15% of BigInteger!!
//////			hammingDist=(new BigInteger(HESig_Bytes_q)).xor(new BigInteger(HESig_Bytes)).bitCount(); //slow!!
////			if(hammingDist<=HMDistThr){
////				matchNum++;
////			}
////		}
////		long endTime=System.currentTimeMillis(); //endTime
////		System.out.println("hammingDist:"+hammingDist+", make HammingDist times:"+TVectorLength+"...."+General.dispTime(endTime-startTime, "ms"));
//
//	}
//
//	@SuppressWarnings("rawtypes")
//	@Override
//	public int run(String[] args) throws Exception {
//		Configuration conf = getConf();
//		FileSystem hdfs=FileSystem.get(conf);
//		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); //use this to parse args!
//		String homePath="hdfs://p-head03.alley.sara.nl/user/yliu/";
//		ArrayList<String> cacheFilePaths=new ArrayList<String>();
//		String dateFormate="yyyy.MM.dd G 'at' HH:mm:ss z";
//		/*in job 5, paralise rerankFlags or queries, the fist one needs to put all query's top-k doc's matchFeats into distributed cache! so only for small query set,  
//		 * attention: for 500 query, rerank top-1000, HMDistThr_rankDoc=18, the size of Out_job4 is 700mb!
//		 * when the size is problem, try to divid query into small groups or use paralise queries!
//		 */
//		boolean isParaQueries=Boolean.valueOf(conf.get("mapred.isParaQueries"));
//		//save rank result or make performance report
//		boolean saveRes=Boolean.valueOf(conf.get("mapred.saveRes"));
//		boolean makeReport=Boolean.valueOf(conf.get("mapred.makeReport"));
//		PrintWriter outStr_report=null;
//		//set rankLabel_common
//		String[] rankLabel_common={"_HDs"+conf.get("mapred.HMDistThr_selDoc")+"-HMW"+conf.get("mapred.HMWeight_deta"), 
//				"_ReR--notValid-changInreRankLengthLoop",
//				"_HDr--notValid-changInreRankHDrLoop",
//				"_top"+General_IR.makeNumberLabel(Integer.valueOf(conf.get("mapred.topRank")))};
//		//get vw_num
//		float[][] centers=General_Hadoop.readMatrixFromSeq_floatMatrix(conf, homePath+conf.get("mapred.VWPath"),hdfs);
//	    int vw_num=centers.length;
//	    //set VWFileInter
//	    int VWFileInter=conf.get("mapred.VWFileInter")==null?vw_num/1000:Integer.valueOf(conf.get("mapred.VWFileInter"));//by default VWFileInter=vw_num/1000
//	    conf.set("mapred.VWFileInter", VWFileInter+"");
//		//set reducer number
//		int job1_1RedNum=Integer.valueOf(otherArgs[0]); //reducer number for extract query's SURF
//		int job2_3RedNum=Integer.valueOf(otherArgs[1]); //reducer number for build ini rank
//		int job5RedNum=Integer.valueOf(otherArgs[2]); //reducer number for combine query_MatchFeat from each vw, build final rank for query
//		//set imagesPath
//		String imagesPath=otherArgs[3]; //input path
//		ArrayList<Path> imageSeqPaths = General_Hadoop.addImgPathsFromMyDataSet(imagesPath);
//		//set Index label
//		String indexLabel=otherArgs[4]; //_Oxford_1M
//		indexLabel+="_"+vw_num/1000+"K-VW";
//		indexLabel+="_"+conf.get("mapred.targetFeature");
//		conf.set("mapred.indexLabel", indexLabel);
//		conf.set("mapred.TVectorPath", conf.get("mapred.indexPath")+"TVector"+indexLabel);
//		conf.set("mapred.docInfoPath", conf.get("mapred.indexPath")+"docInfo"+indexLabel);
//		conf.set("mapred.TVectorInfoPath", conf.get("mapred.indexPath")+"TVectorInfo"+indexLabel);
//		//set output path
//		String out=otherArgs[5]+indexLabel; //output path
//		//set output rank format
//		String saveRankFormat=otherArgs[6];//_rankDocScore, _rankDocMatches
//		boolean onlyScore=saveRankFormat.equalsIgnoreCase("_rankDocScore");//only save rank with docScore or save rank with doc's all matches with the query
//		Class<? extends Writable> saveRankClass=onlyScore?IntList_FloatList.class:DID_Score_ImageRegionMatch_ShortArr_Arr.class;
//		Class<? extends Reducer> saveRankReducer=onlyScore?Reducer_buildRank_final_ParaliseQuery_saveScore.class:Reducer_buildRank_final_ParaliseQuery_saveDocMatches.class;
//		//set reRankLength
//		int[] reRankLengths=General.StrArrToIntArr(conf.get("mapred.reRankLength").split(","));
//		//set maxIniRankLength
//		int maxIniRankLength=General.getMax_ind_val(reRankLengths)[1];
//		conf.set("mapred.maxIniRankLength", maxIniRankLength+"");
//		//set reRank HDr
//		String[] reRankHDrs=conf.get("mapred.HMDistThr_rankDoc").split(",");
//		//set rerankFlag
//		ArrayList<String> rerankFlags=new ArrayList<String>();
//		rerankFlags.add("_OriHE"); 
//		rerankFlags.add("_1vs1");
//		for (String para0 : conf.get("mapred.HPM_ParaDim").split(",")) {//2,4
//			for (String para1 : conf.get("mapred.HPM_level").split(",")) {//1,2,3,4,5
//				rerankFlags.add("_1vs1AndHPM@"+para0+"@"+para1);
//			}
//		}
//		for (String para0 : conf.get("mapred.histRotation_binStep").split(",")) {
//			for (String para1 : conf.get("mapred.histScale_binStep").split(",")) {
//				rerankFlags.add("_1vs1AndHist@"+para0+"@"+para1);
//			}
//		}
//		for (String para0 : conf.get("mapred.histRotation_binStep").split(",")) {
//			for (String para1 : conf.get("mapred.histScale_binStep").split(",")) {
//				for (String para2 : conf.get("mapred.PointDisThr").split(",")) {
//					for (String para3 : conf.get("mapred.badPariWeight").split(",")) {
//						for (String para4 : conf.get("mapred.weightThr").split(",")) {
//							for (String para5 : conf.get("mapred.lineAngleStep").split(",")) {
//								for (String para6 : conf.get("mapred.lineDistStep").split(",")) {
//									for (String para7 : conf.get("mapred.docScoreThr").split(",")) {
////										rerankFlags.add("_1vs1AndAngle@"+para0+"@"+para1+"@"+para2+"@"+para3+"@"+para4+"@"+para5+"@"+para6+"@"+para7);
//										rerankFlags.add("_1vs1AndHistAndAngle@"+para0+"@"+para1+"@"+para2+"@"+para3+"@"+para4+"@"+para5+"@"+para6+"@"+para7);
//									}
//								}
//							}
//						}
//					}
//				}
//			}
//		}
//		for (String para0 : conf.get("mapred.histRotation_binStep").split(",")) {
//			for (String para1 : conf.get("mapred.histScale_binStep").split(",")) {
//				for (String para2 : conf.get("mapred.PointDisThr").split(",")) {
//					for (String para3 : conf.get("mapred.badPariWeight").split(",")) {
//						for (String para4 : conf.get("mapred.weightThr").split(",")) {
//							for (String para5 : conf.get("mapred.lineAngleStep").split(",")) {
//								for (String para6 : conf.get("mapred.lineDistStep").split(",")) {
//									for (String para7 : conf.get("mapred.docScoreThr").split(",")) {
//										for (String para8 : conf.get("mapred.HPM_ParaDim").split(",")) {//2,4
//											for (String para9 : conf.get("mapred.HPM_level").split(",")) {//1,2,3,4,5
//												rerankFlags.add("_1vs1AndHistAndAngleWithHPM@"+para0+"@"+para1+"@"+para2+"@"+para3+"@"+para4+"@"+para5+"@"+para6+"@"+para7+"@"+para8+"@"+para9);
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
//		General.dispInfo(outStr_report, "rerankFlags: "+rerankFlags+"\n rerankFlags_num:"+rerankFlags.size());
//		String rerankLabel="_OriHE_1vs1_1vs1AndHPM@paraDim"+conf.get("mapred.HPM_ParaDim")+"@level"+conf.get("mapred.HPM_level")
//				+"_1vs1AndHist@histRotation_binStep"+conf.get("mapred.histRotation_binStep")+"@histScale_binStep"+conf.get("mapred.histScale_binStep")
//				+"@PointDisThr"+conf.get("mapred.PointDisThr")+"@badPariWeight"+conf.get("mapred.badPariWeight")+"@weightThr"+conf.get("mapred.weightThr")
//				+"@lineAngleStep"+conf.get("mapred.lineAngleStep")+"@lineDistStep"+conf.get("mapred.lineDistStep")
//				+"@docScoreThr"+conf.get("mapred.docScoreThr");
////		String rerankLabel="_OriHE_1vs1AndHPM@"+conf.get("mapred.HPM_level");
//		//set selected querys set
//		ArrayList<String> selQuerys=new ArrayList<String>(); 
//		String queryHashMapPath=homePath+conf.get("mapred.SelQuerys");
//		if (hdfs.isFile(new Path(queryHashMapPath))) {
//			selQuerys.add(queryHashMapPath);
//		}else {
//			FileStatus[] files= hdfs.listStatus(new Path(queryHashMapPath));
//			for (int i = 0; i < files.length; i++) {
//				selQuerys.add(files[i].getPath().toString());
//			}
//		}
//		//set Report
//		if (makeReport) {
//			General.Assert(onlyScore, "err! when make performance report, the save format should be _rankDocScore! here it is:"+saveRankFormat);
//			outStr_report=new PrintWriter(new OutputStreamWriter(hdfs.create(new Path(homePath+out
//					+"_Report"+rankLabel_common[0]+"_ReR"+conf.get("mapred.reRankLength")+"_HDr"+conf.get("mapred.HMDistThr_rankDoc")+rankLabel_common[3]),false), "UTF-8"),true); 
//			General.dispInfo(outStr_report,General.dispTimeDate(System.currentTimeMillis(), dateFormate)+", start processing!  ..................");
//			General.dispInfo(outStr_report, "indexLabel: "+indexLabel+", vw_num:"+vw_num+", VWFileInter:"+VWFileInter+"\n"
//					+"for Query, imagesPath:"+imagesPath+", resulting "+imageSeqPaths.size()+" imageSeqPaths:"+imageSeqPaths+"\n"
//					+selQuerys.size()+" selQuerys: "+selQuerys+"\n"
//					+"TVectorPath:"+conf.get("mapred.TVectorPath")+", TVectorInfoPath:"+conf.get("mapred.TVectorInfoPath")+", docInfoPath:"+conf.get("mapred.docInfoPath")+"\n"
//					+"work dir:"+out+", saveRankFormat:"+saveRankFormat+"\n"
//					+"job1_1RedNum for extract query's SURF:"+job1_1RedNum+", job2_3RedNum for build ini rank:"+job2_3RedNum+", job5RedNum for combine query_MatchFeat from each vw, build final rank for query:"+job5RedNum
//					+"reRankLengths:"+General.IntArrToString(reRankLengths, ",")+", maxIniRankLength:"+maxIniRankLength+"\n"
//					+"reRankHDrs:"+General.StrArrToStr(reRankHDrs, ",")+"\n"
//					+"rankLabels:"+rerankLabel);
//		}
//				
//		//**********************    build rank  ************//
//		String In_reRankFlags_job5_2=out+"_reRankFlags.seq";
//		String[][] Out_job5_2_all = new String[reRankLengths.length][reRankHDrs.length];
//		int job5_2_RedNum=rerankFlags.size()<2000?rerankFlags.size():Math.max(700, Math.min(rerankFlags.size()/10, 50000)); //if 1 rerankFlag need 2mins, one reducer process 20 rerankFlags
//		float job5_2_reducerInter=(float)rerankFlags.size()/job5_2_RedNum;
//		if (!isParaQueries) {
//			General_Hadoop.makeTextSeq_indIsKey(In_reRankFlags_job5_2, rerankFlags, hdfs, conf);//ind in rerankFlags is the reducer ind 
//		}
//		Path[][][][] rankPaths=new Path[reRankLengths.length][reRankHDrs.length][isParaQueries?rerankFlags.size():job5_2_RedNum][selQuerys.size()];
//		for (int i = 0; i < selQuerys.size(); i++) {
//			String queryloopLabel="_Q"+i+"_"+(selQuerys.size()-1);	
//			General.dispInfo(outStr_report, "start process "+queryloopLabel+", "+General.dispTimeDate(System.currentTimeMillis(), dateFormate));
//			long startTime=System.currentTimeMillis();
//			//******* job1_1: extract query's SURF raw feats ******
//			String Out_job1_1=out+queryloopLabel+"_querySURFRaw";
//			//Distributed cache, add selectPhotosPath
//			cacheFilePaths.clear();
//			cacheFilePaths.add(selQuerys.get(i)+"#SelSamples.file"); //SelSamples path with symLink
//			if (conf.get("mapred.QueryPos_HashMap")!=null) {//queries from Oxford has bounding box
//				cacheFilePaths.add(homePath+conf.get("mapred.QueryPos_HashMap")+"#PhotoPos_HashMap.file"); //QueryPos_HashMap with symLink
//			}
//			if(conf.get("mapred.targetFeature").startsWith("SIFT-binTool")){
//				cacheFilePaths.add(homePath+conf.get("mapred.BinTool_SIFT")+"#BinTool_SIFT.exe"); //BinTool_SIFT path with symLink
//				General_Hadoop.addToCacheListWithOriNameAsSymLink(cacheFilePaths, conf.get("mapred.BinTool_libs"), ",", homePath);//libs without symLink, needs keep original name
//			}
//			General_Hadoop.Job(conf, imageSeqPaths.toArray(new Path[0]), Out_job1_1, "Job1_1_getRawFeats"+queryloopLabel, job1_1RedNum, 8, 10, true,
//					MapRed_buildRank_withSameLineDetect.class, Mapper_selectSamples_hashMap.class, Partitioner_random.class,null,Reducer_ExtractSURF.class,
//					IntWritable.class, BufferedImage_jpg.class, IntWritable.class,DouArr_ShortArr_SURFpoint_ShortArr.class,
//					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
//					cacheFilePaths.toArray(new String[0]),null);
//			General.dispInfo(outStr_report, "\t\t job1_1: extract query's SURF raw feats done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
//			//******* job1_2: read query's SURF raw feats, save query_SURFpoint into MapFile ******
//			String Out_job1_2=out+queryloopLabel+"_querySURFpoint";
//			General_Hadoop.Job(conf, new Path[]{new Path(Out_job1_1)}, Out_job1_2, "Job1_2_getSURFpoint"+queryloopLabel, 1, 8, 10, true,
//					MapRed_buildRank_withSameLineDetect.class, null, null,null,Reducer_SaveQueryPoints.class,
//					IntWritable.class, DouArr_ShortArr_SURFpoint_ShortArr.class, IntWritable.class,SURFpoint_ShortArr.class,
//					SequenceFileInputFormat.class, MapFileOutputFormat.class, 1*1024*1024*1024L, 0,
//					null,null);
//			General.dispInfo(outStr_report, "\t\t job1_2: read query's SURF raw feats, save query_SURFpoint into MapFile done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
//			//******* job1_3: read query's SURF raw feats, make HESig ******
//			String Out_job1_3=out+queryloopLabel+"_queryHESig";
//			//Distributed cache, add VWPath, pMatrixPath, HEThresholdPath, middleNode, nodeLink_learned
//			cacheFilePaths.clear();
//			cacheFilePaths.add(homePath+conf.get("mapred.VWPath")+"#centers.file"); //VWs path with symLink
//			cacheFilePaths.add(homePath+conf.get("mapred.pMatrixPath")+"#pMatrix.file"); //VWs path with symLink
//			cacheFilePaths.add(homePath+conf.get("mapred.HEThresholdPath")+"#HEThreshold.file"); //VWs path with symLink
//			cacheFilePaths.add(homePath+conf.get("mapred.middleNode")+"#middleNodes.file"); //VWs path with symLink
//			cacheFilePaths.add(homePath+conf.get("mapred.nodeLink_learned")+"#nodeLink_learned.file"); //VWs path with symLink
//			General_Hadoop.Job(conf, new Path[]{new Path(Out_job1_1)}, Out_job1_3, "Job1_3_getHESig"+queryloopLabel, job1_1RedNum, 8, 10, true,
//					MapRed_buildRank_withSameLineDetect.class, null, Partitioner_random.class,null,Reducer_MakeHESig.class,
//					IntWritable.class, DouArr_ShortArr_SURFpoint_ShortArr.class, IntWritable.class,IntArr_HESig_ShortArr_Arr.class,
//					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
//					cacheFilePaths.toArray(new String[0]),null);
//			General.dispInfo(outStr_report, "\t\t job1_3: read query's SURF raw feats, make HESig done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
//			//******* job1_4: extract query's sizeInfo ******
//			String QuerySize_HashMap=out+queryloopLabel+"_querySize.hashMap";
//			conf.set("mapred.QuerySize_HashMap", QuerySize_HashMap);
//			//Distributed cache, add selectPhotosPath
//			cacheFilePaths.clear();
//			cacheFilePaths.add(selQuerys.get(i)+"#SelSamples.file"); //SelSamples path with symLink
//			if (conf.get("mapred.QueryPos_HashMap")!=null) {//queries from Oxford has bounding box
//				cacheFilePaths.add(homePath+conf.get("mapred.QueryPos_HashMap")+"#PhotoPos_HashMap.file"); //QueryPos_HashMap with symLink
//			}
//			if (indexLabel.contains("Oxford")) {
//				cacheFilePaths.add(homePath+conf.get("mapred.buildingInd_NameForReport")+"#buildingInd_Name.file"); //buildingInd_Name path with symLink
//			}
//			General_Hadoop.Job(conf, imageSeqPaths.toArray(new Path[0]), null, "Job1_4_getQuerySizes"+queryloopLabel, 1, 8, 10, true,
//					MapRed_buildRank_withSameLineDetect.class, Mapper_selectSamples_hashMap.class, null, null, Reducer_ExtractQuerySize.class,
//					IntWritable.class, BufferedImage_jpg.class, IntWritable.class,IntWritable.class,
//					SequenceFileInputFormat.class, NullOutputFormat.class, 1*1024*1024*1024L, 0,
//					cacheFilePaths.toArray(new String[0]),null);
//			General.dispInfo(outStr_report, "\t\t job1_4: extract query's size info done! no outPut, save QuerySize_HashMap to mapred.QuerySize_HashMap!"+General.dispTime(System.currentTimeMillis()-startTime, "min"));
//			//******* job2_1: make VW_PartitionerIDs for partition reducers in Search TVector, no outPut, save VW_PartitionerIDs to mapred.VW_PartitionerIDs_Path ******
//			conf.set("mapred.VW_PartitionerIDs_Path", out+"_VW_PartitionerIDs"+queryloopLabel);
//			//add Distributed cache
//			cacheFilePaths.clear();
//			cacheFilePaths.add(homePath+conf.get("mapred.TVectorInfoPath")+"#TVectorInfo.file"); //TVectorInfo with symLink
//			General_Hadoop.Job(conf, new Path[]{new Path(Out_job1_3)}, null, "Job2_1_makeVW_PartitionerIDs"+queryloopLabel, 1, 8, 10, true,
//					MapRed_buildRank_withSameLineDetect.class, Mapper_countVW_FeatNum.class, null, null, Reducer_makeVW_PartitionerIDs.class,
//					IntWritable.class, IntWritable.class, IntWritable.class,IntWritable.class,
//					SequenceFileInputFormat.class, NullOutputFormat.class, 0, 0,
//					cacheFilePaths.toArray(new String[0]),null);
//			General.dispInfo(outStr_report, "\t\t job2_1: make VW_PartitionerIDs for partition reducers in Search TVector, no outPut, save VW_PartitionerIDs to mapred.VW_PartitionerIDs_Path done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
//			//set job2_2RedNum,job3_2RedNum,job4RedNum based on VW_PartitionerIDs
//			int[] PaIDs=(int[]) General_Hadoop.readObject_HDFS(hdfs, new Path(homePath+conf.get("mapred.VW_PartitionerIDs_Path")).toString());
//			int job2_2RedNum=General_Hadoop.getTotReducerNum_from_vwPartitionIDs(PaIDs); //reducer number for seachTVector, PaIDs: values from 0!
//			int job4RedNum=job2_2RedNum; //reducer number for seachTVector, PaIDs: values from 0!
//			//******* job2_2: Search TVector, get query_doc hmScore for each vw ******
//			String Out_job2_2=out+rankLabel_common[0]+queryloopLabel+"_DocScores";
//			//add Distributed cache
//			cacheFilePaths.clear();
//			cacheFilePaths.add(homePath+conf.get("mapred.docInfoPath")+"/part-r-00000#docInfo.file"); //docInfo with symLink
//			cacheFilePaths.add(homePath+conf.get("mapred.TVectorInfoPath")+"#TVectorInfo.file"); //TVectorInfo with symLink
//			cacheFilePaths.add(homePath+conf.get("mapred.VW_PartitionerIDs_Path")+"#PaIDs.file"); //PaIDs with symLink
//			General_Hadoop.Job(conf, new Path[]{new Path(Out_job1_3)}, Out_job2_2, "Job2_2_getDocScore"+queryloopLabel, job2_2RedNum, 8, 10, true,
//					MapRed_buildRank_withSameLineDetect.class, null, Partitioner_forSearchTVector.class, Combiner_combine_IntArr_HESig_ShortArr_Arr.class, Reducer_SearchTVector_getHMScore.class,
//					IntWritable.class, IntArr_HESig_ShortArr_Arr.class, IntWritable.class, VW_DID_Score_Arr.class,
//					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0, 0,
//					cacheFilePaths.toArray(new String[0]),null);
//			General.dispInfo(outStr_report, "\t\t job2_2: Search TVector, get query_doc hmScore for each vw done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
//			//******* job2_3: combine query_MatchScore from each vw, build initial top ranked docs for query ******
//			String Out_job2_3=out+rankLabel_common[0]+queryloopLabel+"_allIniDocs"+maxIniRankLength+"_temp";
//			//add Distributed cache
//			cacheFilePaths.clear();			
//			cacheFilePaths.add(homePath+conf.get("mapred.docInfoPath")+"/part-r-00000#docInfo.file"); //docInfo with symLink
//			cacheFilePaths.add(homePath+conf.get("mapred.VW_PartitionerIDs_Path")+"#PaIDs.file"); //PaIDs with symLink for check duplicated VW
//			General_Hadoop.Job(conf, new Path[]{new Path(Out_job2_2)}, Out_job2_3, "job2_3_buildInitialRank"+queryloopLabel, job2_3RedNum, 8, 10, true,
//					MapRed_buildRank_withSameLineDetect.class, null, Partitioner_random_sameKey.class, null, Reducer_buildInitialRank_HE.class,
//					IntWritable.class, VW_DID_Score_Arr.class, IntWritable.class,IntArr.class,
//					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
//					cacheFilePaths.toArray(new String[0]),null);
//			General.dispInfo(outStr_report, "\t\t job2_3: combine query_MatchScore from each vw, build initial top ranked docs for query done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
//			//************** loop over different rerank length **************
//			for (int rer_i = 0; rer_i < reRankLengths.length; rer_i++) {
//				conf.set("mapred.reRankLength",reRankLengths[rer_i]+"");
//				rankLabel_common[1]="_ReR"+General_IR.makeNumberLabel(reRankLengths[rer_i]);
//				General.dispInfo(outStr_report, "\t\t start rerank! loop over different rerank length:"+reRankLengths.length);
//				//******* job3: combine result from job2_3, 1 reducer, mutiple reduce(vws), make vw_matchedDocs ******
//				int job3RedNum=PaIDs.length%VWFileInter==0?PaIDs.length/VWFileInter:PaIDs.length/VWFileInter+1;; //each reducer process multiple VWs
//				conf.set("mapred.reducerInter", VWFileInter+"");
//				conf.set("mapred.vw_iniDocsPath", out+rankLabel_common[0]+rankLabel_common[1]+queryloopLabel+"_iniDocs");//job3's result
//				General_Hadoop.Job(conf, new Path[]{new Path(Out_job2_3)}, null, "job3_groupVWQIDDocIDs"+queryloopLabel+"_R"+rer_i+"-"+reRankLengths.length, job3RedNum, 8, 10, false,
//						MapRed_buildRank_withSameLineDetect.class, Mapper_selectTopRankDocs.class, Partitioner_equalAssign_keyFrom0.class,null,Reducer_groupVW_QID_DocIDs.class,
//						IntWritable.class,IntArr.class,IntWritable.class,IntArr.class,
//						SequenceFileInputFormat.class, NullOutputFormat.class, 1*1024*1024*1024L, 0,
//						null,null);
//				General.dispInfo(outStr_report, "\t\t\t job3: combine result from job2_3, 1 reducer, mutiple reduce(vws), make vw_matchedDocs done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
//				//************** loop over different rerank HMDistThr **************
//				General.dispInfo(outStr_report, "\t\t\t loop over different rerank HMDistThr:"+reRankHDrs.length);
//				for (int rerHDr_i = 0; rerHDr_i < reRankHDrs.length; rerHDr_i++) {
//					conf.set("mapred.HMDistThr_rankDoc", reRankHDrs[rerHDr_i]);
//					rankLabel_common[2]="_HDr"+reRankHDrs[rerHDr_i];
//					String currentBasePath=out+rankLabel_common[0]+rankLabel_common[1]+rankLabel_common[2];
//					String rerankLoopLabel="_R"+rer_i+"-"+(reRankLengths.length-1)+"_H"+rerHDr_i+"-"+(reRankHDrs.length-1);
//					//******* job4: Search TVector, get query_selectedDoc MatchFeat for each vw ******
//					String Out_job4=currentBasePath+queryloopLabel+rerankLoopLabel+"_MatchFeat";
//					//add Distributed cache
//					cacheFilePaths.clear();
//					cacheFilePaths.add(homePath+conf.get("mapred.TVectorInfoPath")+"#TVectorInfo.file"); //TVectorInfo with symLink
//					cacheFilePaths.add(homePath+conf.get("mapred.VW_PartitionerIDs_Path")+"#PaIDs.file"); //PaIDs with symLink
//					General_Hadoop.Job(conf, new Path[]{new Path(Out_job1_3)}, Out_job4, "Job4_getMatchFeat"+queryloopLabel+rerankLoopLabel, job4RedNum, 8, 10, true,
//							MapRed_buildRank_withSameLineDetect.class, null, Partitioner_forSearchTVector.class, Combiner_combine_IntArr_HESig_ShortArr_Arr.class, Reducer_SearchTVector_getDocFeat.class,
//							IntWritable.class,IntArr_HESig_ShortArr_Arr.class,Key_QID_VW.class,Int_MatchFeatArr.class,
//							SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0, 0,
//							cacheFilePaths.toArray(new String[0]),null);
//					General.dispInfo(outStr_report, "\t\t\t\t job4: Search TVector, get query_selectedDoc MatchFeat for each vw done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
//					//******* job5: combine query_MatchFeat from each vw, build final rank for query ******
//					General.dispInfo(outStr_report, "\t\t\t\t loop over different rerank strategies:"+rerankFlags.size());
//					if (isParaQueries) {//paralise queries, each rerankFlags lunch a job
//						for (int j = 0; j < rerankFlags.size(); j++) {//run for different rerank strategy
//							String Out_job5=currentBasePath+rankLabel_common[3]+rerankFlags.get(j)+queryloopLabel+rerankLoopLabel+saveRankFormat;
//							conf.set("mapred.rerankFlag", rerankFlags.get(j));
//							conf.set("mapred.rerankFlagInd", j+"");
//							//add Distributed cache
//							cacheFilePaths.clear();
//							cacheFilePaths.add(homePath+Out_job1_2+"/part-r-00000/data#data"); //queryFeat_MapFile data path with symLink
//							cacheFilePaths.add(homePath+Out_job1_2+"/part-r-00000/index#index"); //queryFeat_MapFile index path with symLink
//							cacheFilePaths.add(homePath+conf.get("mapred.docInfoPath")+"/part-r-00000#docInfo.file"); //docInfo with symLink
//							cacheFilePaths.add(homePath+conf.get("mapred.TVectorInfoPath")+"#TVectorInfo.file"); //TVectorInfo with symLink
//							cacheFilePaths.add(homePath+conf.get("mapred.QuerySize_HashMap")+"#QuerySize_HashMap.file"); //QuerySize_HashMap with symLink
//							General_Hadoop.Job(conf, new Path[]{new Path(Out_job4)}, Out_job5, "buildRank"+rerankFlags.get(j)+queryloopLabel+rerankLoopLabel+"_F"+j+"-"+(rerankFlags.size()-1), job5RedNum, 8, 10, true,
//									MapRed_buildRank_withSameLineDetect.class, Mapper_transfer_finalRank.class, Partitioner_random_sameKey_PartKey.class,null,saveRankReducer,
//									Key_QID_DID.class, Int_MatchFeatArr.class, IntWritable.class,saveRankClass,
//									SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
//									cacheFilePaths.toArray(new String[0]),null);	
//							rankPaths[rer_i][rerHDr_i][j][i]=new Path(Out_job5);
//							General.dispInfo(outStr_report, "\t\t\t\t\t job5: combine query_MatchFeat from each vw, build final rank for query done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
//						}
//					}else {//paralise rerankFlags, only lunch one job, (one rerankFlag one reducer)
//						/*
//						 * group Out_job4 into one seqFile, and put it into distributed cache,
//						 * attention: for 500 query, rerank top-1000, HMDistThr_rankDoc=18, the size of Out_job4 is 700mb!
//						 * when the size is problem, try to divid query into small groups!
//						 */
//						//******* job5_1: group Out_job4 into one seqFile ******
//						String Out_job5_1=Out_job4+".seq";		
//						General_Hadoop.Job(conf, new Path[]{new Path(Out_job4)}, Out_job5_1, "job5_1_groupOut_job4"+queryloopLabel+rerankLoopLabel, 1, 8, 10, true,
//								MapRed_buildRank_withSameLineDetect.class, Mapper_transfer_finalRank.class, null, null, Reducer_group_QDMatches.class,
//								Key_QID_DID.class, Int_MatchFeatArr.class, Key_QID_DID.class, Int_MatchFeatArr_Arr.class,
//								SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
//								null,null);
//						General.dispInfo(outStr_report, "\t\t job5_1: group Out_job4 into one seqFile done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
//						//******* job5_2: paralise rerankFlags, do final ranking ******
//						String Out_job5_2=currentBasePath+rankLabel_common[3]+queryloopLabel+rerankLoopLabel+saveRankFormat;
//						Out_job5_2_all[rer_i][rerHDr_i]=Out_job5_2;
//						conf.set("mapred.reducerInter", job5_2_reducerInter+"");
//						//add Distributed cache
//						cacheFilePaths.clear();
//						cacheFilePaths.add(homePath+Out_job1_2+"/part-r-00000/data#data"); //queryFeat_MapFile data path with symLink
//						cacheFilePaths.add(homePath+Out_job1_2+"/part-r-00000/index#index"); //queryFeat_MapFile index path with symLink
//						cacheFilePaths.add(homePath+conf.get("mapred.docInfoPath")+"/part-r-00000#docInfo.file"); //docInfo with symLink
//						cacheFilePaths.add(homePath+conf.get("mapred.TVectorInfoPath")+"#TVectorInfo.file"); //TVectorInfo with symLink
//						cacheFilePaths.add(homePath+conf.get("mapred.QuerySize_HashMap")+"#QuerySize_HashMap.file"); //QuerySize_HashMap with symLink
//						cacheFilePaths.add(Out_job5_1+"/part-r-00000"+"#AllDocMatchs.file"); //Out_job5_1 with symLink
//						General_Hadoop.Job(conf, new Path[]{new Path(In_reRankFlags_job5_2)}, Out_job5_2, "buildRank"+queryloopLabel+rerankLoopLabel+"_F-"+rerankFlags.size(), job5_2_RedNum, 8, 10, true,
//								MapRed_buildRank_withSameLineDetect.class, null, Partitioner_equalAssign_keyFrom0.class, null, Reducer_buildRank_final_ParaliseFlag_saveScore.class,
//								IntWritable.class, Text.class, IntWritable.class, IntList_FloatList.class,
//								SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
//								cacheFilePaths.toArray(new String[0]),null);	
//						//add ranks paths
//						for (int j = 0; j < job5_2_RedNum; j++) {
//							rankPaths[rer_i][rerHDr_i][j][i]=new Path(Out_job5_2+"/part-r-"+General.StrleftPad(j+"", 0, 5, "0"));
//						}
//						General.dispInfo(outStr_report, "\t\t\t\t\t job5: combine query_MatchFeat from each vw, build final rank for query done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
//						//clean-up
//						hdfs.delete(new Path(Out_job5_1), true);
//					}
//					//clean-up
//					hdfs.delete(new Path(Out_job4), true);
//				}
//				//clean-up
//				hdfs.delete(new Path(homePath+conf.get("mapred.vw_iniDocsPath")), true);//job 3
//			}
//			//clean-up
//			hdfs.delete(new Path(Out_job1_1), true);
//			hdfs.delete(new Path(Out_job1_2), true);
//			hdfs.delete(new Path(Out_job1_3), true);
//			hdfs.delete(new Path(homePath+conf.get("mapred.QuerySize_HashMap")), true);
//			hdfs.delete(new Path(homePath+conf.get("mapred.VW_PartitionerIDs_Path")), true);
//			hdfs.delete(new Path(Out_job2_2), true);//docScores
//			hdfs.delete(new Path(Out_job2_3), true);
//		}
//		General.dispInfo(outStr_report, "All querys are done! "+General.dispTimeDate(System.currentTimeMillis(), dateFormate));
//		//clean
//		General_Hadoop.deleteIfExist(In_reRankFlags_job5_2, hdfs);//clean reRankFlags seqFile
//		//******* job6: save all querys' rank into one MapFile ******
//		String rankFlagsData=out+"_rankFlags.arrList";
//		if (makeReport) {
//			General_Hadoop.writeObject_HDFS(hdfs, rankFlagsData, rerankFlags);
//		}
//		for (int rer_i = 0; rer_i < reRankLengths.length; rer_i++) {
//			rankLabel_common[1]="_ReR"+General_IR.makeNumberLabel(reRankLengths[rer_i]);
//			for (int rerHDr_i = 0; rerHDr_i < reRankHDrs.length; rerHDr_i++) {
//				rankLabel_common[2]="_HDr"+reRankHDrs[rerHDr_i];
//				String loopLabel="_R"+rer_i+"-"+reRankLengths.length+"_H"+rerHDr_i+"-"+reRankHDrs.length;
//				Path[] rankPaths_allFlags=General.arrArrToArrList(rankPaths[rer_i][rerHDr_i], "rowFirst").toArray(new Path[0]);
//				String currentBasePath=out+rankLabel_common[0]+rankLabel_common[1]+rankLabel_common[2]+rankLabel_common[3];
//				if (saveRes) {//parilise saving for all rankFlags
//					if (isParaQueries) {
//						for (int j = 0; j < rerankFlags.size(); j++) {//run for different rerank strategy, each rankFlag lunch one job and save into mapFile
//							String finalRankPath=currentBasePath+rerankFlags.get(j)+saveRankFormat;
//							General_Hadoop.Job(conf, rankPaths[rer_i][rerHDr_i][j], finalRankPath, "combine&save_"+loopLabel, 1, 8, 10, true,
//									MapRed_buildRank_withSameLineDetect.class, null, null, null, Reducer_InOut_1key_1value.class,
//									IntWritable.class, saveRankClass, IntWritable.class, saveRankClass,
//									SequenceFileInputFormat.class, MapFileOutputFormat.class, 0, 10,
//									null,null);	
//						}
//					}else {
//						String combinedRanks=currentBasePath+saveRankFormat; //this is a dirtory, each part-r-000j is for one RankFlag j
//						General_Hadoop.Job(conf, rankPaths_allFlags, combinedRanks, "combine&save_"+loopLabel, rerankFlags.size(), 8, 10, true,
//									MapRed_buildRank_withSameLineDetect.class, null, Partitioner_KeyisPartID.class, null, Reducer_InOut_SaveRank.class,
//									IntWritable.class, saveRankClass, IntWritable.class, saveRankClass,
//									SequenceFileInputFormat.class, MapFileOutputFormat.class, 0, 10,
//									null,null);	
//						for (int j = 0; j < rerankFlags.size(); j++) {//run for different rerank strategy, each rankFlag lunch one job and save into mapFile
//							String finalRankPath=currentBasePath+rerankFlags.get(j)+saveRankFormat;
//							General.runSysCommand(Arrays.asList("hadoop", "fs", "-mv",
//									combinedRanks+"/part-r-"+General.StrleftPad(j+"", 0, 5, "0"), finalRankPath));
//						}
//						General_Hadoop.deleteIfExist(combinedRanks, hdfs);
//					}
//				}
//				if (makeReport) {
//					// ********* make report ************
//					String finalRankLabel=rankLabel_common[0]+rankLabel_common[1]+rankLabel_common[2]+rankLabel_common[3];
//					conf.set("mapred.finalRankLabel", finalRankLabel);
//					conf.set("mapred.reducerInter", job5_2_reducerInter+"");
//					//add Distributed cache
//					cacheFilePaths.clear();
//					cacheFilePaths.add(homePath+conf.get("mapred.groundTrueForReport")+"#groundTrue.file"); //groundTrue path with symLink
//					cacheFilePaths.add(homePath+conf.get("mapred.s_to_lForReport")+"#s_to_l.file"); //s_to_l path with symLink
//					cacheFilePaths.add(rankFlagsData+"#rankFlagsData.file"); //rankFlagsData path with symLink
//					if (indexLabel.contains("Oxford")) {
//						cacheFilePaths.add(homePath+conf.get("mapred.junksForReport")+"#junks.file"); //junks path with symLink
//						cacheFilePaths.add(homePath+conf.get("mapred.buildingInd_NameForReport")+"#buildingInd_Name.file"); //buildingInd_Name path with symLink
//					}
//					//run, parilise making report for all rankFlags
//					String reportPath=currentBasePath+"_reports";
//					General_Hadoop.Job(conf, rankPaths_allFlags, reportPath, "analysis_"+loopLabel, job5_2_RedNum, 8, 10, true,
//							MapRed_buildRank_withSameLineDetect.class, null, Partitioner_equalAssign_keyFrom0.class, null, Reducer_makeReport.class,
//							IntWritable.class, IntList_FloatList.class, IntWritable.class,Text.class,
//							SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0, 10,
//							cacheFilePaths.toArray(new String[0]),null);
//					// ********* combine report ************
//					//set info
//					String InfoStrPath=homePath+out+".ResAnaInfoStr";
//					conf.set("mapred.InfoStrPath",InfoStrPath); //Job3 save MAPInfo as String object to InfoStrPath
//					//run
//					General_Hadoop.Job(conf, new Path[]{new Path(reportPath)}, null, "combineReport_"+loopLabel, 1, 8, 10, true,
//							MapRed_buildRank_withSameLineDetect.class, null, null, null, Reducer_combineReport.class,
//							IntWritable.class, Text.class, IntWritable.class,Text.class,
//							SequenceFileInputFormat.class, NullOutputFormat.class, 0, 10,
//							cacheFilePaths.toArray(new String[0]),null);
//					//print Info
//					String Info=(String) General_Hadoop.readObject_HDFS(hdfs, InfoStrPath);
//					General.dispInfo(outStr_report,Info);
//					outStr_report.flush();	
//					//********* clean-up ***********//
//					hdfs.delete(new Path(reportPath), true);
//					hdfs.delete(new Path(InfoStrPath), true);
//				}
//			}
//		}
//		General.dispInfo(outStr_report, "\n combine all query results are done! "+General.dispTimeDate(System.currentTimeMillis(), dateFormate));
//		General_Hadoop.deleteIfExist(rankFlagsData, hdfs);//clean reRankFlags ArrayList
//		//clean-up rankPaths
//		if (isParaQueries) {//clean Out_job5_2
//			for (Path[][][] pathsss : rankPaths) {
//				for (Path[][] pathss: pathsss) {
//					for (Path[] paths: pathss) {
//						for (Path path: paths) {
//							hdfs.delete(path, true);
//						}
//					}
//				}
//			}
//		}else {
//			for (String[] paths: Out_job5_2_all) {
//				for (String path: paths) {
//					hdfs.delete(new Path(path), true);
//				}
//			}
//		}
//		hdfs.close();
//		if (outStr_report!=null) {
//			outStr_report.close();
//		}
//		return 0;
//	}
//	
//	//******** job1_1 **************	
//	public static class Reducer_ExtractSURF extends Reducer<IntWritable,BufferedImage_jpg,IntWritable,DouArr_ShortArr_SURFpoint_ShortArr>{
//		//Reducer_extractSURF: extract double[][] feats, and SURFfeat_noSig[]
//		private ComparePhotos_LocalFeature comparePhotos;
//		private boolean disp;
//		private int imgPhotos;
//		private int noImgPhotos;
//		private int noFeatPhotos;
//		private int totFeatNum;
//		private int dispInter;
//		private long startTime;
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			Configuration conf=context.getConfiguration();
//			//setup preproc image
//			int targetImgSize=Integer.valueOf(conf.get("mapred.targetImgSize")); //1024*768=786432 pixels
//			String imageBoundingBoxPath="PhotoPos_HashMap.file";
//			//setup targetFeat
//			String targetFeat=conf.get("mapred.targetFeature"); //SURF, SIFT_binTool-Oxford2, SIFT_binTool-INRIA2
//			String tempFilesPath="./";
//			String binaryPath_Detector="./BinTool_SIFT.exe";
//			//setup_extractFeat
//			comparePhotos=new ComparePhotos_LocalFeature();
//			String info_setup_extractFeat= comparePhotos.setup_extractFeat(targetImgSize, imageBoundingBoxPath, targetFeat, binaryPath_Detector, tempFilesPath, null, null, null, null, null, 1, 0, conf);
//			System.out.println(info_setup_extractFeat);
//			System.out.println("current memory:"+General.memoryInfo());
//			//set procPhotos
//			imgPhotos=0;
//			noImgPhotos=0;
//			noFeatPhotos=0;
//			totFeatNum=0;
//			//set dispInter
//			dispInter=30;
//			startTime=System.currentTimeMillis(); //startTime
//			
//			System.out.println("reducer setup finsihed!");
//			disp=true; 
//			super.setup(context);
//	 	}
//		
//		protected void reduce(IntWritable key, Iterable<BufferedImage_jpg> value, Context context) throws IOException, InterruptedException {
//			//key: photoName
//			//value: file content
//			int photoName=key.get();// photoName
//			//******** only one in value! ************	
//			int loopNum=0; BufferedImage_jpg photo=new BufferedImage_jpg();
//			for(Iterator<BufferedImage_jpg> it=value.iterator();it.hasNext();){// loop over all BufferedImage_jpg				
//				photo=it.next();
//				loopNum++;
//			}
//			General.Assert(loopNum==1, "error in Reducer_SURFVW_HE_selectQurey! one photoName, one photo, loopNum should == 1, here loopNum="+loopNum);
//			//get BufferedImage
//			BufferedImage img=null;
//			try{
//				img=photo.getBufferedImage();
//			}catch(Exception e){
//				System.err.println("Exception when 'photo.getBufferedImage()' in extractRawFeat_makeVW_HESig for photoName:"+photoName);
//				e.printStackTrace();
//			}
//			//process image
//			if (img!=null) {
//				//disp
//				imgPhotos++;
//	    		if((imgPhotos)%dispInter==0){ 							
//					System.out.println( "extractSURF photo feat, "+imgPhotos+" photos finished!! ......"+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
//					disp=true;
//				}
//	    		//check whether query has bounding box
//	    		BufferedImage photoImg=photo.getBufferedImage(); 
//	    		//cut and resize image
//				BufferedImage reSizedImg=comparePhotos.preProcImage(photoImg, photoName, true, true, disp);
//				//***classify visual feat to visual word***//
//				ArrayList<SURFpoint> interestPoints=new ArrayList<SURFpoint>();
//				double[][] photoFeat=comparePhotos.extractRawFeature(photoName+"", reSizedImg, interestPoints, disp);
//				if(photoFeat!=null){ // photo has feat(some photos are too small, do not have interest point)
//					//mapper outPut
//					context.write(key, new DouArr_ShortArr_SURFpoint_ShortArr(new DouArr_ShortArr(photoFeat), new SURFpoint_ShortArr(interestPoints)));
//					//debug disp info
//			        if (disp==true){ 
//						System.out.println("\t show one example: ");
//						System.out.println("\t mapout_Key, photoName: "+photoName);
//						System.out.println("\t mapout_Value, photoFeat: number, "+photoFeat.length);
//						disp=false;
//					}
//			        totFeatNum+=photoFeat.length;
//				}else{
//					noFeatPhotos++;
//					System.err.println("image exist, but no feat for photo: "+photoName);
//					return;
//				}
//			}else {
//				noImgPhotos++;
//			}
//			
//		}
//
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//			System.out.println("\n one reducer finished! in this reducer, imaged photos: "+imgPhotos+", noImgPhotos: "+noImgPhotos+", noFeatPhotos:"+noFeatPhotos+", totFeatNum:"+totFeatNum+" ....."+ General.dispTime(System.currentTimeMillis()-startTime, "min"));
//			super.setup(context);
//	 	}
//	}
//
//	//******** job1_2 **************	
//	public static class Reducer_SaveQueryPoints extends Reducer<IntWritable,DouArr_ShortArr_SURFpoint_ShortArr,IntWritable,SURFpoint_ShortArr>{
//		private boolean disp;
//		private int procPhotos;
//		private int dispInter;
//		private long startTime;
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			//set procPhotos
//			procPhotos=0;
//			//set dispInter
//			dispInter=10*1000;
//			startTime=System.currentTimeMillis(); //startTime
//			
//			System.out.println("reducer setup finsihed!");
//			disp=true; 
//			super.setup(context);
//	 	}
//		
//		protected void reduce(IntWritable key, Iterable<DouArr_ShortArr_SURFpoint_ShortArr> value, Context context) throws IOException, InterruptedException {
//			//key: photoName
//			//value: file content
//			//******** only one in value! ************	
//			int loopNum=0; DouArr_ShortArr_SURFpoint_ShortArr photoFeat_IOpoint=new DouArr_ShortArr_SURFpoint_ShortArr();
//			for(Iterator<DouArr_ShortArr_SURFpoint_ShortArr> it=value.iterator();it.hasNext();){// loop over all BufferedImage_jpg				
//				photoFeat_IOpoint=it.next();
//				loopNum++;
//			}
//			General.Assert(loopNum==1, "error in Reducer_SURFVW_HE_selectQurey! one photoName, one photo, loopNum should == 1, here loopNum="+loopNum);
//			//disp
//	        procPhotos++;
//    		if((procPhotos)%dispInter==0){ 							
//				System.out.println( "Save Query photo Points, "+procPhotos+" photos finished!! ......"+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
//				disp=true;
//    		}
//			//mapper outPut
//			IntWritable mapout_Key=key;//photoName
//			SURFpoint_ShortArr mapout_Value=photoFeat_IOpoint.points; //SURFpoints
//			context.write(mapout_Key, mapout_Value);
//			//debug disp info
//	        if (disp==true){ 
//	        	System.out.println("\t show one example: ");
//				System.out.println("\t mapout_Key, photoName: "+mapout_Key.toString());
//				System.out.println("\t mapout_Value, SURFpoints: number_"+mapout_Value.getArr().length);
//				disp=false;
//			}
//			
//		}
//		
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//			System.out.println("\n one reducer finished! total processed photos in this Reducer: "+procPhotos+" ....."+ General.dispTime ( System.currentTimeMillis()-startTime, "min"));
//			super.setup(context);
//	 	}
//	}
//		
//	//******** job1_3 **************	
//	public static class Reducer_MakeHESig extends Reducer<IntWritable,DouArr_ShortArr_SURFpoint_ShortArr,IntWritable,IntArr_HESig_ShortArr_Arr>{
//		private ComparePhotos_LocalFeature comparePhotos;
//		private boolean disp;
//		private float aveNum_vwNN;
//		private int procPhotos;
//		private int dispInter;
//		private long startTime, endTime;
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			Configuration conf=context.getConfiguration();
//			//check file in distributted cache
//			String[] info=General_Hadoop.checkFileInNode("centers.file");
//			System.out.println("in local node:"+info[1]+"\n"+info[2]);
//			//setup ComparePhotos_LocalFeature
//			comparePhotos=new ComparePhotos_LocalFeature();
//			System.out.println("current memory:"+General.memoryInfo());
//			//-setup_extractFeat
//			String info_setup_extractFeat= comparePhotos.setup_extractFeat(0,"noMatter", "noMatter", null, null, "centers.file","pMatrix.file","HEThreshold.file","middleNodes.file","nodeLink_learned.file",10,1.2,conf); //MutAssign
////			String info_setup_extractFeat= comparePhotos.setup_extractFeat(0,"noMatter", "noMatter", null, null, "centers.file","pMatrix.file","HEThreshold.file","middleNodes.file","nodeLink_learned.file",1,0,conf);//SingleAssign
//			System.out.println(info_setup_extractFeat);
//			System.out.println("current memory:"+General.memoryInfo());
//			//set aveNum_vwNN, aveNum of mult-assigned vwed for each feat
//			aveNum_vwNN=0;
//			//set procPhotos
//			procPhotos=0;
//			//set dispInter
//			dispInter=50;
//			startTime=System.currentTimeMillis(); //startTime
//			
//			System.out.println("reducer setup finsihed!");
//			disp=true; 
//			super.setup(context);
//	 	}
//		
//		protected void reduce(IntWritable key, Iterable<DouArr_ShortArr_SURFpoint_ShortArr> value, Context context) throws IOException, InterruptedException {
//			//key: photoName
//			//value: file content
//			int photoName=key.get();// photoName
//			//******** only one in value! ************	
//			int loopNum=0; DouArr_ShortArr_SURFpoint_ShortArr photoFeat_IOpoint=new DouArr_ShortArr_SURFpoint_ShortArr();
//			for(Iterator<DouArr_ShortArr_SURFpoint_ShortArr> it=value.iterator();it.hasNext();){// loop over all BufferedImage_jpg				
//				photoFeat_IOpoint=it.next();
//				loopNum++;
//			}
//			General.Assert(loopNum==1, "error in Reducer_SURFVW_HE_selectQurey! one photoName, one photo, loopNum should == 1, here loopNum="+loopNum);
//			//disp
//			procPhotos++;
//    		if(procPhotos%dispInter==0){ 							
//				System.out.println( "indexing photo feat, "+procPhotos+" photos finished!! ......"+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
//				disp=true;
//    		}
////    		//for debug
////    		disp=disp||(photoName==-149800);
//    		
//			IntWritable mapout_Key=new IntWritable(-1);//"clusterId":0~19999
//			IntArr_HESig_ShortArr_Arr mapout_Value=new IntArr_HESig_ShortArr_Arr(); //"photoName&HESings", when use combiner, combiner's input/output class should be the same and equal to mapper's output class!!
//			
//			//***classify visual feat to visual word***//
//			double[][] photoFeat=photoFeat_IOpoint.douArrs.getRawArrArr(); SURFpoint[] interestPoints=photoFeat_IOpoint.points.getArr();
//			HashMap<Integer,ArrayList<SURFfeat>> VW_Sigs=new HashMap<Integer,ArrayList<SURFfeat>>();
//			int[] featStat=comparePhotos.makeVW_HESig(VW_Sigs, photoFeat, interestPoints); //feat num, vw num, mutiAssNum, uniqueVW num
//			if (disp==true){ 
//	        	System.out.println("\t extract VW_Sigs for one Photo finished! photoName: "+photoName+", ori-feat num: "+featStat[0]+", vws num:"+featStat[1]
//	        			+", aveNum of mult-assigned vwed for each feat is:"+featStat[2]+", unique vw num: "+featStat[3]+",  "+General.dispTime(System.currentTimeMillis()-startTime, "s"));
//			}
//			aveNum_vwNN+=featStat[2];
//			//mapper outPut
//			for(Entry<Integer, ArrayList<SURFfeat>> oneEnt : VW_Sigs.entrySet()){
//				HESig[] sigs=General_BoofCV.SURFfeats_to_HESigs(oneEnt.getValue().toArray(new SURFfeat[0]));
//				//set key-value
//				mapout_Key.set(oneEnt.getKey());//vw
//				mapout_Value.set(new int[]{photoName}, new HESig[][]{sigs}); // photoName, SURFfeat
//		        context.write(mapout_Key, mapout_Value);
//			}
//			//debug disp info
//	        if (disp==true){ 
//				System.out.println("\t mapout_Key, vw: "+mapout_Key.toString());
//				System.out.println("\t mapout_Value, photoName: "+mapout_Value.getIntegers()[0]+", SURFfeats number: "+mapout_Value.getFeats()[0].getArr().length);
//				disp=false;
//			}
//	        
////	        //debug
////	        if (photoName==-149800) {
////	        	throw new InterruptedException("this is the debug target! fail this reducer!");
////			}
//			
//		}
//		
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//		    endTime=System.currentTimeMillis(); //end time 
//			System.out.println("\n one reducer finished! total processed photos in this Reducer: "+procPhotos+" ....."+ General.dispTime ( endTime-startTime, "min"));
//			aveNum_vwNN/=procPhotos;
//			System.out.println("aveNum of mult-assigned vws for each feat is:"+aveNum_vwNN+", with vws_NN:"+comparePhotos.vws_NN+", alph_NNDist:"+comparePhotos.alph_NNDist);
//			super.setup(context);
//	 	}
//	}
//
//	//******** job1_4 **************	
//	public static class Reducer_ExtractQuerySize extends Reducer<IntWritable,BufferedImage_jpg,IntWritable,IntWritable>{
//		//Reducer_ExtractQuerySize: extract query image size
//		private HashMap<Integer, int[]> querySizes;
//		private ComparePhotos_LocalFeature comparePhotos;
//		private String QuerySize_HashMap;
//		private boolean isOxfordBuilding;
//		private HashMap<Integer, HashSet<Integer>> buildingInd_Name;
//		private int totImagePhotos;
//		private int noImgPhotos;
//		private int dispInter;
//		private long startTime;
//		
//		@SuppressWarnings("unchecked")
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			Configuration conf = context.getConfiguration();
//			querySizes=new HashMap<Integer, int[]>();
//			//setup preproc image
//			int targetImgSize=Integer.valueOf(conf.get("mapred.targetImgSize")); //1024*768=786432 pixels
//			String imageBoundingBoxPath="PhotoPos_HashMap.file";
//			//setup ComparePhotos_LocalFeature
//			comparePhotos=new ComparePhotos_LocalFeature();
//			System.out.println("current memory:"+General.memoryInfo());
//			//setup_extractFeat
//			String info_setup_extractFeat= comparePhotos.setup_extractFeat(targetImgSize, imageBoundingBoxPath, "noMatter", null, null, null,null,null,null,null,1,0,conf);
//			System.out.println(info_setup_extractFeat);
//			System.out.println("current memory:"+General.memoryInfo());
//			//** set QueryPos_HashMap **//
//			isOxfordBuilding=conf.get("mapred.indexLabel").contains("Oxford");
//			buildingInd_Name=(HashMap<Integer, HashSet<Integer>>) General.readObject("buildingInd_Name.file");
//			//** set VW_PartitionerIDs_Path **//
//			QuerySize_HashMap=("hdfs://p-head03.alley.sara.nl/user/yliu/"+conf.get("mapred.QuerySize_HashMap"));
//			System.out.println("QuerySize_HashMap path setted: "+QuerySize_HashMap);
//			//set procPhotos
//			totImagePhotos=0;
//			noImgPhotos=0;
//			//set dispInter
//			dispInter=20;
//			startTime=System.currentTimeMillis(); //startTime
//			
//			System.out.println("reducer setup finsihed!");
//			super.setup(context);
//	 	}
//		
//		protected void reduce(IntWritable key, Iterable<BufferedImage_jpg> value, Context context) throws IOException, InterruptedException {
//			//key: photoName
//			//value: file content
//			int photoName=key.get();// photoName
//			//******** only one in value! ************	
//			int loopNum=0; BufferedImage_jpg photo=new BufferedImage_jpg();
//			for(Iterator<BufferedImage_jpg> it=value.iterator();it.hasNext();){// loop over all BufferedImage_jpg				
//				photo=it.next();
//				loopNum++;
//			}
//			General.Assert(loopNum==1, "error in Reducer_SURFVW_HE_selectQurey! one photoName, one photo, loopNum should == 1, here loopNum="+loopNum);
//			//get BufferedImage
//			BufferedImage img=null;
//			try{
//				img=photo.getBufferedImage();
//			}catch(Exception e){
//				System.err.println("Exception when 'photo.getBufferedImage()' in extractRawFeat_makeVW_HESig for photoName:"+photoName);
//				e.printStackTrace();
//			}
//			//process image
//			if (img!=null) {
//				//disp
//				totImagePhotos++; boolean disp=false;
//	    		if((totImagePhotos)%dispInter==0){ 		
//	    			disp=true;
//				}
//				//*** get size ***//
//				img=comparePhotos.preProcImage(img, photoName, true, true, disp);
//				int[] size_w_h=new int[]{img.getWidth(),img.getHeight()};
//	    		int[] previous=querySizes.put(photoName, size_w_h);
//				General.Assert(previous==null, "err, duplicate for pho-"+photoName+" in hashMap: querySizes!");
//				//disp
//				General.dispInfo_ifNeed(disp, "", "extractSURF photo size, "+totImagePhotos+" photos finished!! current photo:"+photoName+(isOxfordBuilding?", buildingName:"+buildingInd_Name.get(photoName/1000):"")
//						+", its oriSize width_height:"+photo.getBufferedImage().getWidth()+"_"+photo.getBufferedImage().getHeight()+", its now width_height:"+General.IntArrToString(size_w_h, "_")+"......"+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
//			}else {
//				noImgPhotos++;
//			}
//		}
//
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//			Configuration conf = context.getConfiguration();
//			FileSystem HDFS=FileSystem.get(conf);
//			General_Hadoop.writeObject_HDFS(HDFS, QuerySize_HashMap, querySizes);
//			System.out.println("\n one reducer finished! in this reducer, imaged photos: "+totImagePhotos+", noImgPhotos: "+noImgPhotos+" ....."+ General.dispTime(System.currentTimeMillis()-startTime, "min"));
//			super.setup(context);
//	 	}
//	}
//
//	//******** job2_1 **************	
//	public static class Mapper_countVW_FeatNum extends Mapper<IntWritable, IntArr_HESig_ShortArr_Arr, IntWritable, IntWritable>{
//
//		private int procSamples;
//		private int progInter;
//
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			procSamples=0;
//			progInter=1000;
//			// ***** setup finished ***//
//			System.out.println("Mapper_transfer setup finsihed!");
//			super.setup(context);
//	 	}
//		
//		@Override
//		protected void map(IntWritable key, IntArr_HESig_ShortArr_Arr value, Context context) throws IOException, InterruptedException {
//			//key: VW, value: queryIDs, Feats
//			int vw =key.get();
//			int queryFeatNum=0;
//			
//			for (HESig_ShortArr oneQueryFeats : value.getFeats()) {
//				queryFeatNum+=oneQueryFeats.getArr().length;
//			}
//			context.write(key, new IntWritable(queryFeatNum));
//			
//			procSamples++;
//			if(procSamples%progInter==0){ //debug disp info
//				System.out.println(procSamples+" vw_queryIDs_feats-samples finished: ");
//				System.out.println("--current finished vw: "+vw+", query num:"+value.getIntegers().length+", queryFeatNum:"+queryFeatNum);
//			}
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//			System.out.println("one mapper finished! total Samples in this Mapper: "+procSamples);
//			super.setup(context);
//	 	}
//	}
//	
//	public static class Reducer_makeVW_PartitionerIDs extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>  {
//		
//		private int[][] TVectorInfo;
//		private int[] vw_featNum;
//		private String VW_PartitionerIDs_Path;
//		private int procSamples;
//		private int progInter;
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			Configuration conf = context.getConfiguration();
//			//check file in distributted cache
//			String[] info=General_Hadoop.checkFileInNode("TVectorInfo.file");
//			System.out.println("in local node:"+info[1]+"\n"+info[2]);
//			//***** TVectorInfo ***//
//			TVectorInfo=(int[][]) General.readObject("TVectorInfo.file"); //photoNum,featNum
//			System.out.println("read int[][] TVectorInfo finished, total vw number:"+TVectorInfo.length);
//			//** set vw_featNum **//
//			vw_featNum=new int[TVectorInfo.length];
//			//** set VW_PartitionerIDs_Path **//
//			VW_PartitionerIDs_Path=("hdfs://p-head03.alley.sara.nl/user/yliu/"+conf.get("mapred.VW_PartitionerIDs_Path"));
//			System.out.println("VW_PartitionerIDs_Path setted: "+VW_PartitionerIDs_Path);
//			// ***** setup finsihed ***//
//			procSamples=0;
//			progInter=1000;
//			System.out.println("setup finsihed!");
//			super.setup(context);
//	 	}
//		
//		@Override
//		public void reduce(IntWritable VW, Iterable<IntWritable> queryFeatNums, Context context) throws IOException, InterruptedException {
//			//QueryNameSigs: QueryName-Integer, Sigs:-ByteArrList
//
//			int vw=VW.get();
//			
//			for (IntWritable one_queryFeatNum : queryFeatNums) {
//				vw_featNum[vw]+=one_queryFeatNum.get();
//			}
//			
//			procSamples++;
//			if(procSamples%progInter==0){ //debug disp info
//				System.out.println(procSamples+" vw_queryFeatNum-samples finished: ");
//				System.out.println("--current finished vw: "+vw+", queryFeatNum:"+vw_featNum[vw]);
//			}
//			
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** make  PaIDs ***//
//			int[] PaIDs=General_Hadoop.make_vwPartitionIDs_HESig(TVectorInfo, vw_featNum,700);
//			System.out.println("\n Reducer for make vwPartitionIDs finished, save it as int[] to VW_PartitionerIDs_Path! total partioned reducer number : "+General_Hadoop.getTotReducerNum_from_vwPartitionIDs(PaIDs)+", job.setNumReduceTasks(jobRedNum) should == this value!!");
//			int maxReducerNum=0; int maxReducerNum_vw=0;
//			for (int i = 0; i < PaIDs.length; i++) {
//				int reducerNum=General_Hadoop.getVWReducerNum_from_vwPartitionIDs(PaIDs, i);
//				if(maxReducerNum<reducerNum){
//					maxReducerNum=reducerNum;
//					maxReducerNum_vw=i;
//				}
//			}
//			System.out.println("vw:"+maxReducerNum_vw+" has max reducerNum:"+maxReducerNum+", its TVector-featNum:"+TVectorInfo[maxReducerNum_vw][1]+", querys-featNum:"+vw_featNum[maxReducerNum_vw]);
//			// ***** write-out ***//
//			Configuration conf = context.getConfiguration();
//			FileSystem HDFS=FileSystem.get(conf);
//			General_Hadoop.writeObject_HDFS(HDFS, VW_PartitionerIDs_Path, PaIDs);
//			super.setup(context);
//	 	}
//	}
//
//	//******** job2_2 **************	
//	public static class Reducer_SearchTVector_getHMScore extends Reducer<IntWritable,IntArr_HESig_ShortArr_Arr,IntWritable,VW_DID_Score_Arr>  {
//
//		private Configuration conf;
//		private FileSystem HDFS;
//		private ComparePhotos_LocalFeature comparePhotos;
//		private int VWFileInter;
//		private String TVectorPath;
//		private int[] PaIDs;
//		private StringBuffer vws_queryNums;
//		private int reduceNum;
//		private int dispInter_reduce;
//		private boolean disp;
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			conf = context.getConfiguration();
//			HDFS=FileSystem.get(conf);
//			//check file in distributted cache
//			String[] info=General_Hadoop.checkFileInNode("PaIDs.file");
//			System.out.println("in local node:"+info[1]+"\n"+info[2]);
//			//***** set TVector SeqFile Path***//
//			VWFileInter=Integer.valueOf(conf.get("mapred.VWFileInter"));
//			TVectorPath = "hdfs://p-head03.alley.sara.nl/user/yliu/"+conf.get("mapred.TVectorPath");
//			System.out.println("TVectorPath:"+TVectorPath+", VWFileInter:"+VWFileInter);
//			//********** setup ComparePhotos_LocalFeature ***************
//			comparePhotos=new ComparePhotos_LocalFeature();
//			System.out.println("current memory:"+General.memoryInfo());
//			//-setup_scoreDoc
//			String info_setup_scoreDoc= comparePhotos.setup_scoreDoc("_OriHE", Integer.valueOf(conf.get("mapred.HMDistThr_selDoc")), Double.valueOf(conf.get("mapred.HMWeight_deta")), 
//					"docInfo.file", "TVectorInfo.file", conf);
//			System.out.println(info_setup_scoreDoc);
//			System.out.println("current memory:"+General.memoryInfo());
//			//** set PaIDs **//
//			PaIDs= (int[]) General.readObject("PaIDs.file");
//			System.out.println("PaIDs load finished, total partioned reducer number : "+General_Hadoop.getTotReducerNum_from_vwPartitionIDs(PaIDs)+", job.setNumReduceTasks(jobRedNum) should == this value!!");
//			// ***** setup finsihed ***//
//			System.out.println("setup finsihed!");
//			vws_queryNums=new StringBuffer();
//			reduceNum=0;
//			dispInter_reduce=1;
//			disp=false;
//			super.setup(context);
//	 	}
//		
//		@Override
//		public void reduce(IntWritable VW, Iterable<IntArr_HESig_ShortArr_Arr> QueryNames_feats, Context context) throws IOException, InterruptedException {
//			//QueryNameSigs: QueryName-Integer, Sigs:-ByteArrList
//
//			int progInter=1000;  int vw=VW.get();
//
//			reduceNum++;
//			if (reduceNum%dispInter_reduce==0) {
//				disp=true;
//			}
//			
//			General.dispInfo_ifNeed(disp, "", "\n this reduce is for VW: "+VW+", total allocated reducers for this vw: "+General_Hadoop.getVWReducerNum_from_vwPartitionIDs(PaIDs, vw));
//			
//			long startTime=System.currentTimeMillis();
//			int queryNum_thisVW=0; int queryFeatNum_thisVW=0; HESig[] queryFeats; int queryNum_existMatch=0;
//			
//			//********read TVector(SeqFile) into memory************
//			String TVPath=General_Hadoop.getOneVWFilePath(TVectorPath, vw, VWFileInter);//1vw, 1 seqFile
//			if (HDFS.exists(new Path(TVPath))) {
//				General.dispInfo_ifNeed(disp, "", "TVPath exists: "+ TVPath+", before read TVector, current memory info: "+General.memoryInfo());
//				ArrayList<Integer> TVector_docIDs=new ArrayList<Integer>(1000*1000); ArrayList<SURFfeat[]> TVector_feats=new ArrayList<SURFfeat[]>(1000*1000);
//				int TVectorFeatNum=General_BoofCV.readTVectorIntoMemory(HDFS, TVPath, conf, vw, TVector_docIDs, TVector_feats);
//				General.dispInfo_ifNeed(disp, "", "read this TVector into memory finished! docNum: "+ TVector_docIDs.size()+", featNum: "+TVectorFeatNum
//						+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "s")+", current memory info: "+General.memoryInfo());
//				//******* search TVector ***********
//				HashSet<Integer> checkDupliQuerys=new HashSet<Integer>();
//				startTime=System.currentTimeMillis();
//				//process querys have this vw
//	        	for(Iterator<IntArr_HESig_ShortArr_Arr> it=QueryNames_feats.iterator();it.hasNext();){
//	        		IntArr_HESig_ShortArr_Arr Querys=it.next(); //can not: it.next().getInteger(), it.next().getByteArr()
//	        		int queryNum_thisInterator=Querys.getIntegers().length;
//	        		for(int query_i=0;query_i<queryNum_thisInterator;query_i++){
//		        		//********* process one query *********
//						int queryName=Querys.getIntegers()[query_i]; queryFeats=Querys.getFeats()[query_i].getArr();
//						General.Assert(checkDupliQuerys.add(queryName), "err! duplicate querys for VW:"+vw+", duplicate query:"+queryName);
//						//compare docs in TVector for this query
//						ArrayList<DID_Score> docs_scores=new ArrayList<DID_Score>(2*TVector_docIDs.size()); 
//						for(int doc_i=0;doc_i<TVector_docIDs.size();doc_i++){
//							//get match link and score
//							float hmScore=General_BoofCV.compare_HESigs(queryFeats, TVector_feats.get(doc_i), comparePhotos.HMDistThr, comparePhotos.hammingW);
//							if (hmScore>0) {
//								docs_scores.add(new DID_Score(TVector_docIDs.get(doc_i), hmScore*comparePhotos.idf_squre[vw]));
//							}
//						}
//						//outputfile: SequcenceFile; outputFormat: key->queryID  value->IntList_FloatList
//						if (docs_scores.size()>0) {
//							context.write(new IntWritable(queryName), new VW_DID_Score_Arr(vw, new DID_Score_Arr(docs_scores))); 
//							queryNum_existMatch++;
//						}
//						//************ report progress ******************
//	        			queryNum_thisVW++; queryFeatNum_thisVW+=queryFeats.length;
//						if(disp==true && queryNum_thisVW%progInter==0){
//							long time=System.currentTimeMillis()-startTime;
//							System.out.println("\t --curent total finished querys:"+queryNum_thisVW+", queryFeatNum_thisVW:"+queryFeatNum_thisVW+", queryNum_existMatch:"+queryNum_existMatch
//									+", time:"+General.dispTime(time, "min")+", average compare time per sig pair:"+(double)time/TVectorFeatNum/queryFeatNum_thisVW);
//							System.out.println("\t ----current memory info: "+General.memoryInfo());
//						}
//	        		}
//		        }
//			}else {
//				System.out.println("\n -Warning!!  for VW-"+vw+", query exist this vw, but no photo in dataset exist this vw, TVector not exist for this vw!");
//			}
//			
//        	
//        	//*** some statistic ********
//        	General.dispInfo_ifNeed(disp, "", "one reduce finished! total query number for this vw: "+queryNum_thisVW+", queryFeatNum_thisVW:"+queryFeatNum_thisVW+", queryNum_existMatch:"+queryNum_existMatch);
//        	disp=false;
//        	
//        	vws_queryNums.append(vw+"_"+queryNum_thisVW+"_"+queryFeatNum_thisVW+"_"+queryNum_existMatch+",");
//        	
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//			System.out.println("\n Reducer finished! reduceNum(vws num): "+reduceNum+", vws_queryNums_queryFeatNums_queryNumExistMatch: "+vws_queryNums.toString());
//			super.setup(context);
//	 	}
//	}
//
//	//******** job2_3 **************	
//	public static class Reducer_buildInitialRank_HE extends Reducer<IntWritable,VW_DID_Score_Arr,IntWritable,IntArr>  {
//		Configuration conf;
//		private ComparePhotos_LocalFeature comparePhotos;
//		
//		private int maxIniRankLength;
//		
//		private Path tempPathFor_VWDocs; //save vw_docs to seqFile on local node
//		
//		private int[] PaIDs;
//		private int totRedNumForSearchTVector;
//		private Partitioner_forSearchTVector<IntWritable> partitioner_forSearchTVector;
//		
//		private int processedQueryNum;
//		private long startTime;
//		private int dispInter;
//		private boolean disp;
//	
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			conf = context.getConfiguration();	
//			//setup ComparePhotos_LocalFeature
//			comparePhotos=new ComparePhotos_LocalFeature();
//			System.out.println("current memory:"+General.memoryInfo());
//			//-setup_scoreDoc
//			String info_setup_scoreDoc= comparePhotos.setup_scoreDoc("_OriHE", Integer.valueOf(conf.get("mapred.HMDistThr_selDoc")), Double.valueOf(conf.get("mapred.HMWeight_deta")), 
//					"docInfo.file", null, conf); //do not need TVectorInfo for idf, as the score here alreay have idf in job2_2:Reducer_SearchTVector_getHMScore
//			System.out.println(info_setup_scoreDoc);
//			System.out.println("current memory:"+General.memoryInfo());
//			//***** set maxIniRankLength for further rerank ***//
//			maxIniRankLength=Integer.valueOf(conf.get("mapred.maxIniRankLength")); //select top rank to do 1vs1 and HPM check
//			System.out.println("select top-"+maxIniRankLength+" in the initial rankList to do further rerank: 1vs1 and HPM check!");
//			//***** set tempPathFor_VWDocs for save vw_docs to SeqFile ***//
//			tempPathFor_VWDocs= new Path("vw_docs.seq");
//			String[] info=General_Hadoop.checkFileInNode("vw_docs.seq");//absoultPath, parentPath, files in parentPath
//			System.out.println("tempPathFor_VWDocs for save vw_docs setted: "+tempPathFor_VWDocs+", its absolut path in local-node: \n"+info[0]);
//			System.out.println("files in its parent path: \n"+info[2]);
//			// ***** setup PaID for check duplicate-VW ***//
//			PaIDs= (int[]) General.readObject("PaIDs.file");//each element in PaIDs is mutipled by 10!
//			totRedNumForSearchTVector=General_Hadoop.getTotReducerNum_from_vwPartitionIDs(PaIDs); //reducer number for seachTVector, PaIDs: values from 0!
//			System.out.println("PaIDs set finished for check duplicate-VW, total partioned reducer number : "+totRedNumForSearchTVector+", job.setNumReduceTasks(jobRedNum) should >= this value!!");
//			partitioner_forSearchTVector=new Partitioner_forSearchTVector<IntWritable>();
//			partitioner_forSearchTVector.setConf(conf);
//			// ***** setup finsihed ***//
//			processedQueryNum=0;
//			dispInter=5;
//			disp=true;
//			startTime=System.currentTimeMillis();
//			System.out.println("setup finsihed!");
//			super.setup(context);
//	 	}
//			
//		@Override
//		public void reduce(IntWritable QID, Iterable<VW_DID_Score_Arr> docs_scores_I, Context context) throws IOException, InterruptedException {
//			/**
//			 * 1 reduce: for 1 query, merge mutiple vw-list into one list, each list should be ordered in ASC by docID! 
//			 */
//			
//			//key: query, value: vw and this vw-mathed docs and scores for this query
//
//			int thisQueryID=QID.get();
//			
//			//disp progress
//			processedQueryNum++;
//			if (processedQueryNum%dispInter==0){ 
//				disp=true;
//				System.out.println("\n this reduce for "+processedQueryNum+" -th queries");
//			}
//			//********combine all vw_docs_scores for one query************
//			float[] mergResu=new float[comparePhotos.maxDocID+1]; //need to merge many times for every vw's DID_Score list, use DID as index in mergResu, so DID should start from 0!
//			IntWritable local_key=new IntWritable(); IntArr local_value=new IntArr();
//			SequenceFile.Writer seqFileWr_vw_Docs=new SequenceFile.Writer(FileSystem.getLocal(conf), conf, tempPathFor_VWDocs, local_key.getClass(),local_value.getClass());
//			int vwNum=0;  long startTimeInRed=System.currentTimeMillis(); HashSet<Integer> checkDupliVWs=new HashSet<Integer>();
//			for(Iterator<VW_DID_Score_Arr> it=docs_scores_I.iterator();it.hasNext();){
//				VW_DID_Score_Arr one=it.next(); //can not: it.next().getInteger(), it.next().getByteArr()
//				vwNum++;
//				boolean debugShow=(disp && vwNum%2000==0);
//				//add vw
//				int this_vw=one.vw;
//				//save to local seqFile, each DID_Score_Arr should be ordered in ASC by docID! this is guanteed by: ASC docID in TVector
//				local_key.set(this_vw);
//				seqFileWr_vw_Docs.append(local_key, new IntArr(one.getDocs()));
//				//merge
//				if (debugShow) {//show example for one Query
//					System.out.println("\t this is "+vwNum+"-th mergeSortedList_ASC(mergResu, Arr), this merge is for vw:"+this_vw+", before merge:"+General.dispTime((System.currentTimeMillis()-startTimeInRed), "ms"));
//			    	System.out.println("\t --list size:"+one.docs_scores.getArr().length+", top docs:"+General.selectArr(one.docs_scores.getArr(), null, 5));
//			    	System.out.println("\t --mergResu top docs:"+General.floatArrToString(General.selectArrFloat(mergResu, null, 5), "_", "0.0000") );
//			    	System.out.println("\t --current memory info: "+General.memoryInfo());
//				}
//				General_IR.mergeSortedList_ASC(mergResu, one.docs_scores.getArr());
//				if (debugShow) {//show example for one Query
//					System.out.println("\t "+vwNum+"-th mergeSortedList_ASC(mergResu, Arr) finished: "+General.dispTime((System.currentTimeMillis()-startTimeInRed), "ms"));
//			    	System.out.println("\t --mergResu top docs:"+General.floatArrToString(General.selectArrFloat(mergResu, null, 5), "_", "0.0000") );
//				}
//				General.Assert(checkDupliVWs.add(this_vw), "err! duplicate VWs for query:"+thisQueryID+", duplicate vw:"+this_vw
//						+", its allocated reducer num when searchTVector:"+General_Hadoop.getVWReducerNum_from_vwPartitionIDs(PaIDs, this_vw)
//						+", partitionID:"+partitioner_forSearchTVector.getPartition(new IntWritable(this_vw), QID, totRedNumForSearchTVector));
//			}
//			seqFileWr_vw_Docs.close();
//			if (disp){//show example for one Query
//				System.out.println("\t all mergeSortedList_ASC(mergResu, Arr) finished, vwNum: "+vwNum+", time:"+General.dispTime((System.currentTimeMillis()-startTimeInRed), "ms"));
//				System.out.println("\t --mergResu:"+General.floatArrToString(General.selectArrFloat(mergResu, null, 5), "_", "0.0000") );
//		    	System.out.println("\t --current memory info: "+General.memoryInfo());
//			}
//			//**************** find top ranked docs, and output them by vw, each vw has socred docs, and some of these are top ranked docs ****************
//			ArrayList<Integer> docs=new ArrayList<Integer>(mergResu.length); ArrayList<Float> scores=new ArrayList<Float>(mergResu.length); 
//			for (int docID = 0; docID < mergResu.length; docID++) {//docID is the index!
//				if (mergResu[docID]>0) {//this docID has match score
//					docs.add(docID);
//					scores.add(mergResu[docID]);
//				}
//			}
//			int scoredDocNum=docs.size();
//			//find top ranked docs
//			ArrayList<Integer> docs_top=new ArrayList<Integer>(maxIniRankLength*2); ArrayList<Float> scores_top=new ArrayList<Float>(maxIniRankLength*2); 
//			General_IR.rank_get_TopDocScores_PriorityQueue(docs, scores, maxIniRankLength, docs_top, scores_top, "DES", true, true);
//			//sort top ranked docs by docID in ASC!
//			ArrayList<Integer> docRanks=new ArrayList<Integer>(docs_top.size()*2); ArrayList<Integer> docIDs=new ArrayList<Integer>(docs_top.size()*2); 
//			for (int i = 0; i < docs_top.size(); i++) {
//				docRanks.add(i);
//				docIDs.add(docs_top.get(i));//use docID as ranking score
//			}
//			ArrayList<Integer> docRanks_sortbyDocID=new ArrayList<Integer>(docs_top.size()*2); ArrayList<Integer> sortedDocIDs=new ArrayList<Integer>(docs_top.size()*2); 
//			General_IR.rank_get_AllSortedDocIDs_treeSet(docRanks, docIDs, docRanks_sortbyDocID, sortedDocIDs, "ASC");
//			int[] topDocs_inSortedDocIDs=General.ArrListToIntArr(sortedDocIDs);
//			if (disp) {//for debug show
//				System.out.println("\t selected top docs:"+sortedDocIDs.size()+", top examples in sortedDocIDs: "+sortedDocIDs.subList(0, Math.min(sortedDocIDs.size(),10))
//						+", time:"+General.dispTime((System.currentTimeMillis()-startTimeInRed), "ms"));
//			}
//			//find topDocs' involved vws
//			SequenceFile.Reader seqFileRe_vw_Docs=new SequenceFile.Reader(FileSystem.getLocal(conf), tempPathFor_VWDocs, conf);
//			vwNum=0; int commonNumTot=0; 
//			while (seqFileRe_vw_Docs.next(local_key, local_value)) {
//				vwNum++;
//				boolean debugShow=(disp && vwNum%2000==0);
//				int thisVW=local_key.get();
//				int[] thisDocs=local_value.getIntArr();//must be ordered based on docID in ASC!
//				ArrayList<int[]> commons=General.findCommonElementInds_twoSorted_ASC_loopShotArr(topDocs_inSortedDocIDs, thisDocs);//find common elements in two sorted arr
//				if (commons!=null) {
//					HashSet<Integer> checkDuplicate=new HashSet<Integer>();
//					for (int[] one : commons) {
//						context.write(new IntWritable(thisVW), new IntArr(new int[]{thisQueryID, one[0], docRanks_sortbyDocID.get(one[1])})); //key_vw, value_QID_DID_rank
//						General.Assert(checkDuplicate.add(one[0]), "err! duplicate docs in commons! vw:"+thisVW+", query:"+thisQueryID
//								+", duplicate doc:"+one[0]+", rank in topDocs:"+one[1]);
//					}
//					General.Assert(checkDuplicate.size()==commons.size(), "err! duplicate docs in commons! checkDuplicate.size():"+checkDuplicate.size()+", commons.size():"+commons.size());
//					commonNumTot+=commons.size();
//				}
//				//debug show common elements
//				if (debugShow) {//for debug show
//					System.out.println("\t ---"+vwNum+"-th vw(finding common docs), time:"+General.dispTime((System.currentTimeMillis()-startTimeInRed), "ms")+", thisVW: "+thisVW+", top in thisDocs: "+General.IntArrToString(General.selectArrInt(thisDocs, null, Math.min(thisDocs.length,10)),"_"));
//					System.out.println("\t ---this vw:"+thisVW+", commonDocs: "+commons.size());
//				}
//			}
//			//clean-up seqFile in local node
//			seqFileRe_vw_Docs.close();
//			General.Assert(FileSystem.getLocal(conf).delete(tempPathFor_VWDocs,true), "err in delete tempPathFor_VWDocs, not successed!") ;
//
//			if (disp){//show example for one Query
//				int topToShow=Math.min(docs_top.size(),10);
//				System.out.println(processedQueryNum+" querys finished! time:"+General.dispTime(System.currentTimeMillis()-startTime, "min")
//						+", current finished queryName: "+thisQueryID
//						+", matched vwNum:"+vwNum+", total listed photos in its initial rank: "+scoredDocNum+", saved top doc numbers:"+docs_top.size()
//						+", top ranked Docs:"+docs_top.subList(0, topToShow)+", Scores: "+scores_top.subList(0, topToShow));
//				System.out.println("total common docs between topDocs and scoredDocs in all vws':"+commonNumTot+", on average, "+(float)commonNumTot/vwNum+" common docs per vw");
//		    	disp=false;
//			}
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			System.out.println("\n one Reducer finished! total querys in this reducer:"+processedQueryNum+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );
//			// ***** finsihed ***//			
//			super.setup(context);
//	 	}
//	}
//
//	//******** job3 **************
//	public static class Mapper_selectTopRankDocs extends Mapper<IntWritable,IntArr,IntWritable,IntArr>{
//		
//		private int reRankLength;
//		private int procSamples;
//		private int procSelSamples;
//		private int dispInter;
//		private long startTime, endTime;
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			Configuration conf = context.getConfiguration();
//			//***** set reRankLength for 1vs1&HPM rerank ***//
//			reRankLength=Integer.valueOf(conf.get("mapred.reRankLength")); //select top rank to do 1vs1 and HPM check
//			System.out.println("select top-"+reRankLength+" in the initial rankList to do 1vs1 and HPM check!");
//			//set procSamples
//			procSamples=0;
//			procSelSamples=0;
//			//set dispInter
//			dispInter=5000;
//			startTime=System.currentTimeMillis(); //startTime
//			
//			System.out.println("mapper setup finsihed!");
//			super.setup(context);
//	 	}
//		
//		@Override
//		protected void map(IntWritable key, IntArr value, Context context) throws IOException, InterruptedException {
//			//key: vw
//			//value: QID_DID_Rank
//			procSamples++;
//			if(value.getIntArr()[2]<reRankLength){//doc's rank is within the rerank scale
//				procSelSamples++;
//				context.write(key, value);
//			}
//			//disp
//			if((procSamples)%dispInter==0){ 							
//				endTime=System.currentTimeMillis(); //end time 
//				System.out.println( "select Samples, "+procSamples+" Samples finished!! selected: "+procSelSamples+" ......"+ General.dispTime (endTime-startTime, "min"));
//			}
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//		    endTime=System.currentTimeMillis(); //end time 
//			System.out.println("one mapper finished! total Samples in this Mapper: "+procSamples+", selected: "+procSelSamples+" ....."+ General.dispTime ( endTime-startTime, "min"));
//			super.setup(context);
//	 	}
//	}
//
//	public static class Reducer_groupVW_QID_DocIDs extends Reducer<IntWritable,IntArr,IntWritable,IntArr>  {
//		private Configuration conf;
//		private FileSystem hdfs;
//		private int VWFileInter;
//		private String vw_iniDocsPath;
//		private int reduceNum;
//		private StringBuffer vws;
//		private int dispInter;
//
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			String homePath="hdfs://p-head03.alley.sara.nl/user/yliu/";
//			conf=context.getConfiguration();
//			hdfs=FileSystem.get(conf);
//			VWFileInter=Integer.valueOf(conf.get("mapred.VWFileInter"));
//			vw_iniDocsPath=homePath+conf.get("mapred.vw_iniDocsPath");
//			System.out.println("vw_iniDocsPath:"+vw_iniDocsPath+", VWFileInter:"+VWFileInter);
//			// ***** setup finsihed ***//
//			reduceNum=0;
//			vws=new StringBuffer();
//			dispInter=50;
//			System.out.println("setup finsihed!");
//			super.setup(context);
//	 	}
//		
//		@Override
//		public void reduce(IntWritable VW, Iterable<IntArr> QID_DIDs, Context context) throws IOException, InterruptedException {
//			//key: VW, value: QID_DID_Rank
//
//			int vw=VW.get();
//			//******** matched  ************	
//			HashMap<Integer, ArrayList<Integer>> Q_Docs=new HashMap<Integer, ArrayList<Integer>>(); 
//			for(Iterator<IntArr> it=QID_DIDs.iterator();it.hasNext();){// loop over all HashMaps				
//				IntArr oneQ_D_R=it.next();
//				int thisQ=oneQ_D_R.getIntArr()[0]; int thisD=oneQ_D_R.getIntArr()[1]; 
//				ArrayList<Integer> thisQueryDocs=Q_Docs.get(thisQ);
//				if (thisQueryDocs==null) {
//					thisQueryDocs=new ArrayList<Integer>();
//					thisQueryDocs.add(thisD);
//					Q_Docs.put(thisQ, thisQueryDocs);
//				}else {
//					thisQueryDocs.add(thisD);
//				}
//			}		
//			//outPut the matchedDocs into seq on vw_iniDocsPath
//			SequenceFile.Writer VWFile_Writer=new SequenceFile.Writer(hdfs, conf, new Path(General_Hadoop.getOneVWFilePath(vw_iniDocsPath, vw, VWFileInter)), IntWritable.class, IntArr.class);
//			//--1st elemetn in the output is to mark vw!
//			VWFile_Writer.append(new IntWritable(vw), new IntArr(new int[]{vw,vw,vw})); //key_vw, value_[vw,vw,vw]
//			//--output query_docs
//			int queryNum=0; int docNum=0;
//			for (Entry<Integer, ArrayList<Integer>> oneQ_Docs : Q_Docs.entrySet()) {
//				HashSet<Integer> checkDuplicate=new HashSet<Integer>(oneQ_Docs.getValue());
//				General.Assert(checkDuplicate.size()==oneQ_Docs.getValue().size(), "err, duplicate docs in VW:"+vw+", Q:"+oneQ_Docs.getKey()+", its docs:"+oneQ_Docs.getValue());
//				int[] docs=General.ArrListToIntArr(oneQ_Docs.getValue());
//				Arrays.sort(docs);//sort docs in asc order
//				VWFile_Writer.append(new IntWritable(oneQ_Docs.getKey()), new IntArr(docs)); //key_queryID, value_matchedDocs
//				queryNum++;
//				docNum+=oneQ_Docs.getValue().size();
//			}
//			VWFile_Writer.close();
//				
//			//disp progress
//			reduceNum++;
//			if (reduceNum%dispInter==0){ 
//				System.out.println(reduceNum+" reduce(vw) finished! current finished vw: "+vw+", total queryNum: "+queryNum+", matched photos:"+docNum);
//			}
//			vws.append(vw+"_"+queryNum+"_"+docNum+", ");
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//			System.out.println("this reducer is for vw_matchedDocs, one reducer finished! total "+reduceNum+" vws in this Reducer, vw_queryNum_docNum: "+vws.toString());
//			super.setup(context);
//	 	}
//	}
//	
//	//******** job4 **************	
//	public static class Reducer_SearchTVector_getDocFeat extends Reducer<IntWritable,IntArr_HESig_ShortArr_Arr,Key_QID_VW,Int_MatchFeatArr>  {
//
//		private Configuration conf;
//		private FileSystem HDFS;
//		private int VWFileInter;
//		private String TVectorPath;
//		private String vw_iniDocsPath;
//		private int HMDistThr_rankDoc;
//		private int[] PaIDs;
//		private StringBuffer vws_queryNums;
//		private int reduceNum;
//		private int dispInter_reduce;
//		private boolean disp;
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			conf = context.getConfiguration();
//			HDFS=FileSystem.get(conf);
//			//check file in distributted cache
//			String[] info=General_Hadoop.checkFileInNode("PaIDs.file");
//			System.out.println("in local node:"+info[1]+"\n"+info[2]);
//			//***** set TVector SeqFiles Path***//
//			VWFileInter=Integer.valueOf(conf.get("mapred.VWFileInter"));
//			TVectorPath = "hdfs://p-head03.alley.sara.nl/user/yliu/"+conf.get("mapred.TVectorPath");
//			System.out.println("TVectorPath:"+TVectorPath+", VWFileInter:"+VWFileInter);
//			//***** set vw_matchedDocs MapFile Path***//
//			vw_iniDocsPath = conf.get("mapred.vw_iniDocsPath");
//			System.out.println("vw_iniDocsPath: "+vw_iniDocsPath);
//			//********* set Hamming distance threshold***//
//			int HMDistThr_selDoc=Integer.valueOf(conf.get("mapred.HMDistThr_selDoc"));
//			HMDistThr_rankDoc=Integer.valueOf(conf.get("mapred.HMDistThr_rankDoc"));
//			System.out.println("HMDistThr_selDoc:"+HMDistThr_selDoc+", HMDistThr_rankDoc: "+HMDistThr_rankDoc);
//			//********** set PaIDs **************//
//			PaIDs= (int[]) General.readObject("PaIDs.file");
//			System.out.println("PaIDs load finished, total partioned reducer number : "+(PaIDs[PaIDs.length-1]+1)+", job.setNumReduceTasks(jobRedNum) should == this value!!");
//			// ***** setup finsihed ***//
//			System.out.println("setup finsihed!");
//			vws_queryNums=new StringBuffer();
//			reduceNum=0;
//			dispInter_reduce=1;
//			disp=false;
//			super.setup(context);
//	 	}
//		
//		@Override
//		public void reduce(IntWritable VW, Iterable<IntArr_HESig_ShortArr_Arr> QueryNames_feats, Context context) throws IOException, InterruptedException {
//			//QueryNameSigs: QueryName-Integer, Sigs:-ByteArrList
//
//			int progInter=500;  int vw=VW.get();
//			
//			reduceNum++;
//			if (reduceNum%dispInter_reduce==0) {
//				disp=true;
//			}
//			
//			General.dispInfo_ifNeed(disp,"\n ", "this reduce is for VW: "+VW+", total allocated reducers for this vw: "+General_Hadoop.getVWReducerNum_from_vwPartitionIDs(PaIDs, vw));	
//			
//			//******** set output key,value ************
//			Key_QID_VW redOutKey=new Key_QID_VW();
//			Int_MatchFeatArr redOutValue =new Int_MatchFeatArr();
//			
//			long startTime=System.currentTimeMillis();
//
//			//******** read vw_matchedDocs into memory ************
//			String one_vw_iniDocsPath=General_Hadoop.getOneVWFilePath(vw_iniDocsPath, vw, VWFileInter);//1vw, 1 seqFile
//			General.dispInfo_ifNeed(disp,"\t ", "one_vw_iniDocsPath: "+ one_vw_iniDocsPath);
//			int queryNum_thisVW=0; int queryFeatNum_thisVW=0; int queryNum_existMatch=0; 
//			if (HDFS.exists(new Path(one_vw_iniDocsPath))) {
//				HashMap<Integer, int[]> QID_DIDs = new HashMap<Integer, int[]>();
//				int allQueryMatchedDocNum=General_BoofCV.readVW_MatchDocsIntoMemory(HDFS, one_vw_iniDocsPath, conf, vw, QID_DIDs);
//				General.dispInfo_ifNeed(disp,"\t ", "read this vw_matchedDocs into memory finished! queryNum: "+ QID_DIDs.size()+", allQueryMatchedDocNum: "+allQueryMatchedDocNum
//						+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "s")+", current memory info: "+General.memoryInfo());
//				//******** read TVector(SeqFile) into memory************
//				String TVPath=General_Hadoop.getOneVWFilePath(TVectorPath, vw, VWFileInter);//1vw, 1 seqFile
//				General.dispInfo_ifNeed(disp,"\t ", "TVPath: "+ TVPath+", before read TVector, current memory info: "+General.memoryInfo());
//				ArrayList<Integer> TVector_docIDs=new ArrayList<Integer>(); ArrayList<SURFfeat[]> TVector_feats=new ArrayList<SURFfeat[]>();
//				int TVectorFeatNum=General_BoofCV.readTVectorIntoMemory(HDFS, TVPath, conf, vw, TVector_docIDs, TVector_feats);
//				General.dispInfo_ifNeed(disp,"\t ", "read this TVector into memory finished! docNum: "+ TVector_docIDs.size()+", featNum: "+TVectorFeatNum
//						+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "s")+", current memory info: "+General.memoryInfo());
//				//******* search TVector ***********
//				HESig[] queryFeats; int[] matchedDocs=null; startTime=System.currentTimeMillis();
//				//process querys have this vw
//	        	for(Iterator<IntArr_HESig_ShortArr_Arr> it=QueryNames_feats.iterator();it.hasNext();){
//	        		IntArr_HESig_ShortArr_Arr Querys=it.next(); //can not: it.next().getInteger(), it.next().getByteArr()
//	        		int queryNum_thisInterator=Querys.getIntegers().length; boolean showThisQuery=false;
//	        		for(int query_i=0;query_i<queryNum_thisInterator;query_i++){
//		        		//********* process one query *********
//						int queryName=Querys.getIntegers()[query_i]; queryFeats=Querys.getFeats()[query_i].getArr(); 	 
//						queryNum_thisVW++; queryFeatNum_thisVW+=queryFeats.length;
//						if(disp && queryNum_thisVW%progInter==0){
//							showThisQuery=true;
//						}
//						int matchDocNum_right=0; int matchDocNum_act=0; int matchFeatNum=0; 
//						if((matchedDocs=QID_DIDs.get(queryName))!=null){//this vw exist top-selected docs for this query
//							redOutKey.set(queryName, vw);
//							//get selected docs' match feat in TVector for this query
//							matchDocNum_right=matchedDocs.length; boolean showOneExample=showThisQuery;
//							ArrayList<int[]> commonDocs=General.findCommonElementInds_twoSorted_ASC_loopShotArr(matchedDocs, General.ArrListToIntArr(TVector_docIDs));
//							if (commonDocs==null) {
//								throw new InterruptedException("err!! no common docs! should matchedDocs:"+matchedDocs.length+", "+matchedDocs[0]+"_"+matchedDocs[matchedDocs.length-1]
//										+", TVector_docIDs:"+TVector_docIDs.size()+", "+TVector_docIDs.get(0)+"_"+TVector_docIDs.get(TVector_docIDs.size()-1));
//							}else {
//								General.Assert(commonDocs.size()==matchDocNum_right, "err in Reducer_SearchTVector_getDocFeat, for queryName:"+queryName
//										+", matchDocNum_act:"+commonDocs.size()+", should == matchDocNum_right:"+matchDocNum_right+", docs should match:"+General.IntArrToString(matchedDocs, "_"));
//							}
//							for (int[] oneDoc : commonDocs) {
//								int docInd_inTV=oneDoc[2];
//								//get match link and score
//								MatchFeat_Arr oneDocMatches= General_BoofCV.compare_HESigs(queryFeats, TVector_feats.get(docInd_inTV), HMDistThr_rankDoc);
//								if (oneDocMatches!=null) {//when HDr < HDs, some doc that match in iniRank, may do not match when rerank!
//									matchDocNum_act++;
//									matchFeatNum+=oneDocMatches.getArr().length;
//									//outputfile: SequcenceFile; outputFormat: key->Key_QID_VW  value->Int_MatchFeat_ShortArr
//									redOutValue.set(TVector_docIDs.get(docInd_inTV), oneDocMatches);
//									context.write(redOutKey, redOutValue); 	       
//									if(showOneExample){//for debug
//										System.out.println("\t ---show one example: 1 doc's matches of 1 query --- queryName:"+queryName+", docID:"+TVector_docIDs.get(docInd_inTV)+", vw:"+vw+", its Matches:"+oneDocMatches.getArr().length);
//										for (int m_i = 0;m_i < oneDocMatches.getArr().length; m_i++) {
//											System.out.println("\t ------ match-"+m_i+": "+oneDocMatches.getArr()[m_i].toString());
//										}
//										showOneExample=false;
//									}
//								}
//							}
//							queryNum_existMatch++;
//						}
//						//************ report progress ******************
//						if(showThisQuery){
//							System.out.println("\t -curent total finished querys:"+queryNum_thisVW+", queryFeatNum_thisVW:"+queryFeatNum_thisVW+", queryNum_existMatch:"+queryNum_existMatch
//									+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min"));
//							System.out.println("\t ---show info for current query: "+queryName+", sig number:"+queryFeats.length);
//							System.out.println("\t ---selected matched docs number in iniRank under HDs: "+matchDocNum_right+", actual match docNum when rerank under HDr:"+matchDocNum_act+", tot matchFeatNum:"+matchFeatNum);
//							System.out.println("\t ---current memory info: "+General.memoryInfo());
//							showThisQuery=false;
//						}
//	        		}
//		        }
//			}else {
//				General.dispInfo_ifNeed(disp,"", "this vw do not have matched query-doc! just ignor!");
//			}
//        	//*** some statistic ********
//        	General.dispInfo_ifNeed(disp,"\t ", "one reduce finished! total query number for this vw: "+queryNum_thisVW+", queryFeatNum_thisVW:"+queryFeatNum_thisVW+", queryNum_existMatch:"+queryNum_existMatch);
//        	vws_queryNums.append(vw+"_"+queryNum_thisVW+"_"+queryFeatNum_thisVW+"_"+queryNum_existMatch+",");
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//			System.out.println("Reducer finished! reduceNum: "+reduceNum+", vws_queryNums_queryFeatNums_queryNumExistMatch: "+vws_queryNums.toString());
//			super.setup(context);
//	 	}
//	}
//
//	//******** job5 **************	
//	public static class Mapper_transfer_finalRank extends Mapper<Key_QID_VW,Int_MatchFeatArr, Key_QID_DID, Int_MatchFeatArr>{
//
//		private boolean disp;
//		private int procSamples;
//
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			disp=true; 
//			procSamples=0;
//			// ***** setup finished ***//
//			System.out.println("Mapper_transfer setup finsihed!");
//			super.setup(context);
//	 	}
//		
//		@Override
//		protected void map(Key_QID_VW key, Int_MatchFeatArr value, Context context) throws IOException, InterruptedException {
//			//key: Key_QID_VW, value: Int_MatchFeat_ShortArr
//			int queryID=key.queryID;
//			int vw =key.vw;
//			//make query-doc keys
//			int docID=value.Integer;
//			MatchFeat_Arr docFeats=value.feats;
//			context.write(new Key_QID_DID(queryID, docID), new Int_MatchFeatArr(vw,docFeats));
//			
//
//			if (disp==true){ //debug disp info
//				System.out.println("Mapper_transfer: ");
//				System.out.println("queryName: "+queryID+", vw: "+vw+", docID:"+docID+", docFeats:"+docFeats.getArr().length);
//				disp=false;
//			}
//			//** output, set key, value **//
//			procSamples++;
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//			System.out.println("one mapper finished! total Samples in this Mapper: "+procSamples);
//			super.setup(context);
//	 	}
//	}
//	
//	public static class Reducer_buildRank_final_ParaliseQuery_saveScore extends Reducer<Key_QID_DID, Int_MatchFeatArr, IntWritable, IntList_FloatList>  {
//		
//		private int topRank;
//		private int reduceNum;
//		private long startTime;
//		private int dispInter;
//		private int reRankLength;
//		
//		private MapFile.Reader queryFeatReader;
//		private HashMap<Integer, int[]> QuerySize_HashMap;
//		
//		private int rankFlagInd;
//		private ComparePhotos_LocalFeature comparePhotos;
//		
//		private ArrayList<Integer> docIDs_0;
//		private ArrayList<Float> docScores_0;
//		private ArrayList<Integer> docIDs_1;
//		private ArrayList<Float> docScores_1;
//		
//		private UpdateInReRankFunction upDates;
//		private int saveQueryNum;
//		private HashSet<Integer> processedQuery;
////		private boolean debugFail;
//		
//		@SuppressWarnings("unchecked")
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			Configuration conf = context.getConfiguration();
//			//check file in distributted cache
//			String[] info=General_Hadoop.checkFileInNode("docInfo.file");
//			System.out.println("in local node:"+info[1]+"\n"+info[2]);
//			//***** select top rank for output ***//
//			topRank=Integer.valueOf(conf.get("mapred.topRank")); //select top rank as output
//			System.out.println("select top-"+topRank+" in rankList as output");
//			//***** set rerankFlag ***********
//			String rerankFlag=conf.get("mapred.rerankFlag");
//			rankFlagInd=Integer.valueOf(conf.get("mapred.rerankFlagInd"));
//			System.out.println("rankFlagInd:"+rankFlagInd+", rerankFlag:"+rerankFlag);
//			//***** set reRankLength for 1vs1&HPM rerank ***//
//			reRankLength=Integer.valueOf(conf.get("mapred.reRankLength")); //select top rank to do 1vs1 and HPM check
//			System.out.println("select top-"+reRankLength+" in the initial rankList to do 1vs1 and HPM check!");
//			//***** read query feats ***//
//			queryFeatReader=General_Hadoop.openMapFileInNode("data", conf, true);
//			System.out.println("open query-Feat-MapFile finished");
//			//***** read query Size_HashMap ***//
//			QuerySize_HashMap=(HashMap<Integer, int[]>) General.readObject("QuerySize_HashMap.file");
//			System.out.println("read QuerySize_HashMap finished! tot-query num:"+QuerySize_HashMap.size());
//			//********** setup ComparePhotos_LocalFeature ***************
//			comparePhotos=new ComparePhotos_LocalFeature();
//			System.out.println("current memory:"+General.memoryInfo());
//			//-setup_scoreDoc for "_OriHE", "_1vs1", "_1vs1AndHPM", "_1vs1AndAngle"
//			String info_setup_scoreDoc= comparePhotos.setup_scoreDoc(rerankFlag, Integer.valueOf(conf.get("mapred.HMDistThr_rankDoc")), Double.valueOf(conf.get("mapred.HMWeight_deta")), 
//					"docInfo.file", "TVectorInfo.file", conf);
//			System.out.println(info_setup_scoreDoc);
//			System.out.println("current memory:"+General.memoryInfo());
//			//***** set queryID_previous ***//
//			docIDs_0=new ArrayList<Integer>(reRankLength*2); //save all docs' score for one query
//			docScores_0=new ArrayList<Float>(reRankLength*2); 
//			docIDs_1=new ArrayList<Integer>(reRankLength*2); //save all docs' score for one query
//			docScores_1=new ArrayList<Float>(reRankLength*2); 
//			upDates=new UpdateInReRankFunction(0, -1, 0, new SURFpoint_ShortArr(), true);
//			saveQueryNum=0;
//			processedQuery=new HashSet<Integer>();
//			// ***** setup finsihed ***//
//			reduceNum=0;
//			startTime=System.currentTimeMillis();
//			dispInter=10;
//			System.out.println("setup finsihed!");
////			debugFail=false;
//			super.setup(context);
//	 	}
//			
//		@Override
//		public void reduce(Key_QID_DID QID_DID, Iterable<Int_MatchFeatArr> vw_MatchFeats, Context context) throws IOException, InterruptedException {
//			/**
//			 * 1 reduce: process 1 doc for 1 query, 
//			 */
//			//key: query doc, value: vw and this doc's MatchFeats for this query
//			IntWritable returnQID=new IntWritable();
//			IntList_FloatList rank=new IntList_FloatList();
//			reRankFunction(reduceNum==0, false, 
//					QID_DID, vw_MatchFeats, null, 
//					queryFeatReader, QuerySize_HashMap, 
//					comparePhotos, topRank, dispInter, startTime,
//					docIDs_0, docIDs_1, docScores_0, docScores_1, 
//					returnQID, rank, upDates);
//			if (rank.getIntegers().size()!=0) {//one query's rank is finished
//				context.write(returnQID, rank);
//				General.Assert(processedQuery.add(returnQID.get()), "err! duplicated queryID:"+returnQID.get());
//				saveQueryNum++;
//			}
//			reduceNum++;
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			//**** process last query ********
//			if (reduceNum!=0) {//this reducer has key-value assigned!
//				IntWritable returnQID=new IntWritable();
//				IntList_FloatList rank=new IntList_FloatList();
//				reRankFunction(false, true, 
//						null, null, null, 
//						queryFeatReader, QuerySize_HashMap, 
//						comparePhotos, topRank, dispInter, startTime,
//						docIDs_0, docIDs_1, docScores_0, docScores_1, 
//						returnQID, rank, upDates);
//				context.write(returnQID, rank);
//				General.Assert(processedQuery.add(returnQID.get()), "err! duplicated queryID:"+returnQID.get());
//				saveQueryNum++;
//			}
//			General.Assert(upDates.processedQueryNum==saveQueryNum, "err! processedQueryNum != saveQueryNum, "+upDates.processedQueryNum+":"+saveQueryNum);
//			// ***** finsihed ***//			
//			System.out.println("one Reducer finished! total querys in this reducer:"+upDates.processedQueryNum+", reduceNum:"+reduceNum+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );
////			if (debugFail) {
////				throw new InterruptedException("this is the debug target! fail this reducer!");
////			}
//			super.setup(context);
//	 	}
//	
//	}
//
//	public static class Reducer_buildRank_final_ParaliseQuery_saveDocMatches extends Reducer<Key_QID_DID,Int_MatchFeatArr,Key_RankFlagID_QID,DID_Score_ImageRegionMatch_ShortArr_Arr>  {
//		
//		private int topRank;
//		private int processedQueryNum;
//		private int reduceNum;
//		private long startTime;
//		private int dispInter;
//		private int reRankLength;
//		
//		private MapFile.Reader queryFeatReader;
//		private HashMap<Integer, int[]> QuerySize_HashMap;
//		
//		private int rankFlagInd;
//		private ComparePhotos_LocalFeature comparePhotos;
//		
//		private PriorityQueue<slave_masterFloat_DES<DID_Score_ImageRegionMatch_ShortArr>> doc_scores_queue;
//		private float thr_min;
//		private int iniDocNum_thisQ;
//		private int queryID_previous;
//		private boolean disp;
//		
//		
//		@SuppressWarnings("unchecked")
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			Configuration conf = context.getConfiguration();
//			//check file in distributted cache
//			String[] info=General_Hadoop.checkFileInNode("docInfo.file");
//			System.out.println("in local node:"+info[1]+"\n"+info[2]);
//			//***** select top rank for output ***//
//			topRank=Integer.valueOf(conf.get("mapred.topRank")); //select top rank as output
//			System.out.println("select top-"+topRank+" in rankList as output");
//			//***** set rerankFlag ***********
//			String rerankFlag=conf.get("mapred.rerankFlag");
//			rankFlagInd=Integer.valueOf(conf.get("mapred.rerankFlagInd"));
//			System.out.println("rankFlagInd:"+rankFlagInd+", rerankFlag:"+rerankFlag);
//			//***** set reRankLength for 1vs1&HPM rerank ***//
//			reRankLength=Integer.valueOf(conf.get("mapred.reRankLength")); //select top rank to do 1vs1 and HPM check
//			System.out.println("select top-"+reRankLength+" in the initial rankList to do 1vs1 and HPM check!");
//			//***** read query feats ***//
//			queryFeatReader=General_Hadoop.openMapFileInNode("data", conf, true);
//			System.out.println("open query-Feat-MapFile finished");
//			//***** read query Size_HashMap ***//
//			QuerySize_HashMap=(HashMap<Integer, int[]>) General.readObject("QuerySize_HashMap.file");
//			System.out.println("read QuerySize_HashMap finished! tot-query num:"+QuerySize_HashMap.size());
//			//********** setup ComparePhotos_LocalFeature ***************
//			comparePhotos=new ComparePhotos_LocalFeature();
//			System.out.println("current memory:"+General.memoryInfo());
//			//-setup_scoreDoc for "_OriHE", "_1vs1", "_1vs1AndHPM", "_1vs1AndAngle"
//			String info_setup_scoreDoc= comparePhotos.setup_scoreDoc(rerankFlag, Integer.valueOf(conf.get("mapred.HMDistThr_rankDoc")), Double.valueOf(conf.get("mapred.HMWeight_deta")), 
//					"docInfo.file", "TVectorInfo.file", conf);
//			System.out.println(info_setup_scoreDoc);
//			System.out.println("current memory:"+General.memoryInfo());
//			//***** set queryID_previous ***//
//			doc_scores_queue=new PriorityQueue<slave_masterFloat_DES<DID_Score_ImageRegionMatch_ShortArr>>(topRank);
//			thr_min=Float.MAX_VALUE;
//			iniDocNum_thisQ=0;
//			queryID_previous=-1;
//			// ***** setup finsihed ***//
//			processedQueryNum=0;
//			reduceNum=0;
//			startTime=System.currentTimeMillis();
//			dispInter=10;
//			disp=true;
//			System.out.println("setup finsihed!");
//			super.setup(context);
//	 	}
//			
//		@Override
//		public void reduce(Key_QID_DID QID_DID, Iterable<Int_MatchFeatArr> vw_MatchFeats, Context context) throws IOException, InterruptedException {
//			/**
//			 * 1 reduce: process 1 doc for 1 query, 
//			 */
//			
//			//key: query doc, value: vw and this doc's MatchFeats for this query
//
//			int thisQueryID=QID_DID.queryID;
//			//get query feat
//			SURFpoint_ShortArr queryFeats=new SURFpoint_ShortArr(); 
//			if(queryFeatReader.get(new IntWritable(thisQueryID), queryFeats)==null){
//				throw new InterruptedException("err! no feat for query:"+thisQueryID);
//			}
//			//get query size
//			int[] querySize=QuerySize_HashMap.get(thisQueryID);
//			int queryMaxSize=Math.max(querySize[0], querySize[1]);
//			//**************** check if new query ****************
//			if ((thisQueryID!=queryID_previous && reduceNum!=0)) {//new query and not the first query
//				//rank 
//				ArrayList<slave_masterFloat_DES<DID_Score_ImageRegionMatch_ShortArr>> order= General_IR.get_topRanked_from_PriorityQueue(doc_scores_queue, doc_scores_queue.size());
//				ArrayList<DID_Score_ImageRegionMatch_ShortArr> topDocs=new ArrayList<DID_Score_ImageRegionMatch_ShortArr>(topRank);
//				for (int i = 0; i < order.size(); i++) {
//					topDocs.add(order.get(i).getSlave());
//				}			
//				context.write(new Key_RankFlagID_QID(rankFlagInd, queryID_previous), new DID_Score_ImageRegionMatch_ShortArr_Arr(topDocs));
//				//disp progress
//				processedQueryNum++;
//				if (processedQueryNum%dispInter==0){ 
//					System.out.println("\n"+processedQueryNum+" querys finished! time:"+General.dispTime(System.currentTimeMillis()-startTime, "min")
//							+", current finished queryName: "+queryID_previous+", total selected photos in its initial rank: "+iniDocNum_thisQ+", saved top doc numbers:"+topDocs.size()
//							+", top1Doc_Score:"+topDocs.get(0).dID_score.toString()+", top10Scores:"+order.subList(0, Math.min(10, topDocs.size())));
//					disp=true;
//				}
//				//prepareForNext
//				doc_scores_queue.clear(); thr_min=Float.MAX_VALUE;iniDocNum_thisQ=0;
//			}
//
//			//********combine all vw_MatchFeats for one doc************
//			int docID=QID_DID.docID;
//			ArrayList<Int_MatchFeatArr>  allMatches = new ArrayList<Int_MatchFeatArr>(); 
//			for(Iterator<Int_MatchFeatArr> it=vw_MatchFeats.iterator();it.hasNext();){
//				Int_MatchFeatArr oneVW_matches=it.next(); //can not: it.next().getInteger(), it.next().getByteArr()
//				allMatches.add(new Int_MatchFeatArr(oneVW_matches.Integer,oneVW_matches.feats));
//			}
//			
//			//**************** get matches' weights and final doc score ****************
//			ArrayList<ImageRegionMatch> goodMatches=new ArrayList<ImageRegionMatch>(); 
//			float docScore=comparePhotos.scoreOneDoc(new DocAllMatchFeats(docID,allMatches.toArray(new Int_MatchFeatArr[0])), queryFeats.getArr(), thisQueryID, queryMaxSize, goodMatches, null, null, disp)[0];
//			// if the array is not full yet:
//	        if (doc_scores_queue.size() < topRank) {
//	        	doc_scores_queue.add(new slave_masterFloat_DES<DID_Score_ImageRegionMatch_ShortArr>(new DID_Score_ImageRegionMatch_ShortArr(docID, docScore, goodMatches),docScore));
//	            if (docScore<thr_min) //update current thr in doc_scores_order
//	            	thr_min = docScore;
//	        } else if (docScore>thr_min) { // if it is "better" than the least one in the current doc_scores_order
//	            // remove the last one ...
//	        	doc_scores_queue.poll();
//	            // add the new one ...
//	        	doc_scores_queue.offer(new slave_masterFloat_DES<DID_Score_ImageRegionMatch_ShortArr>(new DID_Score_ImageRegionMatch_ShortArr(docID, docScore, goodMatches),docScore));
//	            // update new thr in doc_scores_order
//	        	thr_min = doc_scores_queue.peek().getMaster();
//	        }
//			
//	        //update queryID_previous
//			queryID_previous=thisQueryID;
//			iniDocNum_thisQ++;
//			reduceNum++;
//			disp=false;
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			//**** process last query ********
//			if (reduceNum!=0) {//this reducer has key-value assigned!
//				//rank 
//				ArrayList<slave_masterFloat_DES<DID_Score_ImageRegionMatch_ShortArr>> order= General_IR.get_topRanked_from_PriorityQueue(doc_scores_queue, doc_scores_queue.size());
//				ArrayList<DID_Score_ImageRegionMatch_ShortArr> topDocs=new ArrayList<DID_Score_ImageRegionMatch_ShortArr>(topRank);
//				for (int i = 0; i < order.size(); i++) {
//					topDocs.add(order.get(i).getSlave());
//				}			
//				context.write(new Key_RankFlagID_QID(rankFlagInd, queryID_previous), new DID_Score_ImageRegionMatch_ShortArr_Arr(topDocs));
//				System.out.println("last query: "+queryID_previous+", total selected photos in its initial rank: "+iniDocNum_thisQ+", saved top doc numbers:"+topDocs.size()
//						+", top10Docs:"+topDocs.subList(0, Math.min(10, topDocs.size()))+", top10Scores:"+order.subList(0, Math.min(10, topDocs.size())));
//				processedQueryNum++;
//			}
//			
//			// ***** finsihed ***//			
//			System.out.println("one Reducer finished! total querys in this reducer:"+processedQueryNum+", reduceNum:"+reduceNum+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );
//			super.setup(context);
//	 	}
//	}
//
//	public static class Reducer_group_QDMatches extends Reducer<Key_QID_DID,Int_MatchFeatArr,Key_QID_DID,Int_MatchFeatArr_Arr>  {
//		
//		private int reduceNum;
//		private long startTime;
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//
//			reduceNum=0;
//			startTime=System.currentTimeMillis();
//			System.out.println("setup finsihed!");
//			super.setup(context);
//	 	}
//			
//		@Override
//		public void reduce(Key_QID_DID QID_DID, Iterable<Int_MatchFeatArr> vw_MatchFeats, Context context) throws IOException, InterruptedException {
//			/**
//			 * 1 reduce: process 1 doc for 1 query, 
//			 */
//			
//			//********combine all vw_MatchFeats for one doc************
//			ArrayList<Int_MatchFeatArr>  matches = new ArrayList<Int_MatchFeatArr>(); 
//			for(Iterator<Int_MatchFeatArr> it=vw_MatchFeats.iterator();it.hasNext();){
//				Int_MatchFeatArr oneVW_matches=it.next(); //can not: it.next().getInteger(), it.next().getByteArr()
//				matches.add(new Int_MatchFeatArr(oneVW_matches.Integer,oneVW_matches.feats));
//			}
//			//**************** save this doc into doc_scores_order ****************
//			context.write(QID_DID, new Int_MatchFeatArr_Arr(matches));
//			reduceNum++;
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//
//			// ***** finsihed ***//			
//			System.out.println("one Reducer finished! reduceNum:"+reduceNum+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );
//			super.setup(context);
//	 	}
//	}
//	
//	public static class Reducer_buildRank_final_ParaliseFlag_saveScore extends Reducer<IntWritable,Text,IntWritable,IntList_FloatList>  {
//		
//		private int topRank;
//		private int reduceNum;
//		private long startTime;
//		private int dispInter;
//		private int reRankLength;
//		
//		private MapFile.Reader queryFeatReader;
//		private HashMap<Integer, int[]> QuerySize_HashMap;
//				
//		private ComparePhotos_LocalFeature comparePhotos;
//		
//		private ArrayList<Integer> docIDs_0;
//		private ArrayList<Float> docScores_0;
//		private ArrayList<Integer> docIDs_1;
//		private ArrayList<Float> docScores_1;
//		
//		private UpdateInReRankFunction upDates;
////		private boolean debugFail;
//		
//		@SuppressWarnings("unchecked")
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			Configuration conf = context.getConfiguration();
//			//check file in distributted cache
//			String[] info=General_Hadoop.checkFileInNode("docInfo.file");
//			System.out.println("in local node:"+info[1]+"\n"+info[2]);
//			//***** select top rank for output ***//
//			topRank=Integer.valueOf(conf.get("mapred.topRank")); //select top rank as output
//			System.out.println("select top-"+topRank+" in rankList as output");
//			//***** set reRankLength for 1vs1&HPM rerank ***//
//			reRankLength=Integer.valueOf(conf.get("mapred.reRankLength")); //select top rank to do 1vs1 and HPM check
//			System.out.println("select top-"+reRankLength+" in the initial rankList to do 1vs1 and HPM check!");
//			//***** read query feats ***//
//			queryFeatReader=General_Hadoop.openMapFileInNode("data", conf, true);
//			System.out.println("open query-Feat-MapFile finished");
//			//***** read query Size_HashMap ***//
//			QuerySize_HashMap=(HashMap<Integer, int[]>) General.readObject("QuerySize_HashMap.file");
//			System.out.println("read QuerySize_HashMap finished! tot-query num:"+QuerySize_HashMap.size());
//			// ***** setup finsihed ***//
//			reduceNum=0;
//			startTime=System.currentTimeMillis();
//			dispInter=QuerySize_HashMap.size()/2;
//			System.out.println("setup finsihed!");
//			super.setup(context);
//	 	}
//			
//		@Override
//		public void reduce(IntWritable rankFlagInd, Iterable<Text> rankFlags, Context context) throws IOException, InterruptedException {
//			/**
//			 * 1 reduce: process 1 reRankFlag for all query, 
//			 */
//			//******** only one value in list! ************		
//			int loopNum=0;  Text oneRankFlag=null;
//			for(Iterator<Text> it=rankFlags.iterator();it.hasNext();){// loop over all HashMaps				
//				oneRankFlag=it.next();
//				loopNum++;
//			}
//			General.Assert(loopNum==1, "error! one rankFlagInd, one rankFlag, loopNum should == 1, here loopNum="+loopNum);
//
//			//******** start rank ***********
//			Configuration conf = context.getConfiguration();
//			//setup ComparePhotos_LocalFeature
//			comparePhotos=new ComparePhotos_LocalFeature();
//			System.out.println("start process rank for "+oneRankFlag.toString()+", current memory:"+General.memoryInfo());
//			//setup_scoreDoc for "_OriHE", "_1vs1", "_1vs1AndHPM", "_1vs1AndAngle"
//			String info_setup_scoreDoc= comparePhotos.setup_scoreDoc(oneRankFlag.toString(), Integer.valueOf(conf.get("mapred.HMDistThr_rankDoc")), Double.valueOf(conf.get("mapred.HMWeight_deta")), 
//					"docInfo.file", "TVectorInfo.file", conf);
//			System.out.println(info_setup_scoreDoc);
//			System.out.println("current memory:"+General.memoryInfo());
//			//***** read docMatches ***//
//			SequenceFile.Reader DocMatchReader=General_Hadoop.openSeqFileInNode("AllDocMatchs.file", conf, true);
//			System.out.println("open AllDocMatchs.file finished");
//			//set queryID_previous ***//
//			docIDs_0=new ArrayList<Integer>(reRankLength*2); //save all docs' score for one query
//			docScores_0=new ArrayList<Float>(reRankLength*2); 
//			docIDs_1=new ArrayList<Integer>(reRankLength*2); //save all docs' score for one query
//			docScores_1=new ArrayList<Float>(reRankLength*2); 
//			upDates=new UpdateInReRankFunction(0, -1, 0, new SURFpoint_ShortArr(), true);
//			int saveQueryNum=0;
//			HashSet<Integer> processedQuery=new HashSet<Integer>();
//			//key: query doc, value: vw and this doc's MatchFeats for this query
//			Key_QID_DID QID_DID=new Key_QID_DID(); Int_MatchFeatArr_Arr vw_MatchFeats=new Int_MatchFeatArr_Arr();
//			int pairNum=0;
//			IntWritable returnQID=new IntWritable();
//			IntList_FloatList rank=new IntList_FloatList();
//			while (DocMatchReader.next(QID_DID, vw_MatchFeats)) {//each is for one Query-Doc
//				reRankFunction(pairNum==0, false, 
//						QID_DID, null, vw_MatchFeats, 
//						queryFeatReader, QuerySize_HashMap,
//						comparePhotos, topRank, dispInter, startTime,
//						docIDs_0, docIDs_1, docScores_0, docScores_1, 
//						returnQID, rank, upDates);
//				if (rank.getIntegers().size()!=0) {//one query's rank is finished
//					rank.getIntegers().add(returnQID.get());//add queryID to its ranks as the last one!
//					context.write(rankFlagInd, rank);
//					General.Assert(processedQuery.add(returnQID.get()), "err! duplicated queryID:"+returnQID.get());
//					saveQueryNum++;
//					rank.getFloats().clear();
//					rank.getIntegers().clear();
//				}
//				pairNum++;
//			}
//			DocMatchReader.close();
//			//last query
//			reRankFunction(false, true, 
//					null, null, null, 
//					queryFeatReader, QuerySize_HashMap,
//					comparePhotos, topRank, dispInter, startTime,
//					docIDs_0, docIDs_1, docScores_0, docScores_1, 
//					returnQID, rank, upDates);
//			rank.getIntegers().add(returnQID.get());//add queryID to its ranks as the last one!
//			context.write(rankFlagInd, rank);
//			General.Assert(processedQuery.add(returnQID.get()), "err! duplicated queryID:"+returnQID.get());
//			saveQueryNum++;
//			//done
//			General.Assert(upDates.processedQueryNum==saveQueryNum, "err! processedQueryNum != saveQueryNum, "+upDates.processedQueryNum+":"+saveQueryNum);
//			System.out.println("one reRankFlag done! "+oneRankFlag.toString()+", queryNum:"+upDates.processedQueryNum
//					+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );
//			reduceNum++;
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** finsihed ***//			
//			queryFeatReader.close();
//			System.out.println("one Reducer finished! total reduceNum:"+reduceNum+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );
////			if (debugFail) {
////				throw new InterruptedException("this is the debug target! fail this reducer!");
////			}
//			super.setup(context);
//	 	}
//		
//	}
//
//	public static class UpdateInReRankFunction{
//		public int queryMaxSize, queryID_previous, processedQueryNum;
//		public SURFpoint_ShortArr queryFeats;
//		public boolean disp;
//		
//		public UpdateInReRankFunction(int queryMaxSize, int queryID_previous, int processedQueryNum, SURFpoint_ShortArr queryFeats, boolean disp){
//			this.queryMaxSize=queryMaxSize;
//			this.queryID_previous=queryID_previous;
//			this.processedQueryNum=processedQueryNum;
//			this.queryFeats=queryFeats;
//			this.disp=disp;
//		}
//	}
//	
//	public static void reRankFunction(boolean isFirstQ, boolean isLastQ,
//			Key_QID_DID QID_DID, Iterable<Int_MatchFeatArr> vw_MatchFeats, Int_MatchFeatArr_Arr groupedMatches,
//			MapFile.Reader queryFeatReader, HashMap<Integer, int[]> QuerySize_HashMap, 
//			ComparePhotos_LocalFeature comparePhotos, int topRank, int dispInter, long startTime,
//			ArrayList<Integer> docIDs_0, ArrayList<Integer> docIDs_1, ArrayList<Float> docScores_0, ArrayList<Float> docScores_1, 
//			IntWritable returnQID, IntList_FloatList rank, UpdateInReRankFunction upDates) throws IOException, InterruptedException {
//		
//		int thisQueryID=isLastQ?-1:QID_DID.queryID;
//		if (isFirstQ) {//first query
//			//get query feat
//			if(queryFeatReader.get(new IntWritable(thisQueryID), upDates.queryFeats)==null){
//				throw new InterruptedException("err! no feat for query:"+thisQueryID);
//			}
//			//get query size
//			int[] querySize=QuerySize_HashMap.get(thisQueryID);
//			upDates.queryMaxSize=Math.max(querySize[0], querySize[1]);
//			//change queryID_previous
//			upDates.queryID_previous=thisQueryID;
//		}
//		//**************** check if new query ****************
//		if (thisQueryID!=upDates.queryID_previous || isLastQ) {//new query and not the first query
//			//do 1vs1 and HPM check on previous query's initial rank
//			ArrayList<Integer> topDocs=new ArrayList<Integer>(topRank); ArrayList<Float> topScores=new ArrayList<Float>();
//			General_IR.rank_get_TopDocScores_PriorityQueue(docIDs_0, docScores_0, topRank, topDocs, topScores, "DES", true, true);
//			General_IR.rank_get_TopDocScores_PriorityQueue(docIDs_1, docScores_1, topRank, topDocs, topScores, "DES", true, false); //concate two rank
//			returnQID.set(upDates.queryID_previous); rank.set(topDocs, topScores);
//			//disp progress
//			upDates.processedQueryNum++;
//			if (upDates.processedQueryNum%dispInter==0){ 
//				System.out.println("\n"+upDates.processedQueryNum+" querys finished! time:"+General.dispTime(System.currentTimeMillis()-startTime, "min")
//						+", current finished queryName: "+upDates.queryID_previous+", total selected photos in its initial rank-0: "+docIDs_0.size()+", rank-1: "+docIDs_1.size()+", saved top doc numbers:"+topDocs.size()
//						+", top10Docs:"+topDocs.subList(0, Math.min(10, topDocs.size()))+", top10Scores:"+topScores.subList(0, Math.min(10, topDocs.size())));
//				upDates.disp=true;
//			}
//			if (!isLastQ) {
//				//prepareForNext
//				docIDs_0.clear(); docScores_0.clear(); docIDs_1.clear(); docScores_1.clear();
//				//get query feat
//				if(queryFeatReader.get(new IntWritable(thisQueryID), upDates.queryFeats)==null){
//					throw new InterruptedException("err! no feat for query:"+thisQueryID);
//				}
//				//get query size
//				int[] querySize=QuerySize_HashMap.get(thisQueryID);
//				upDates.queryMaxSize=Math.max(querySize[0], querySize[1]);
//				//change queryID_previous
//				upDates.queryID_previous=thisQueryID;
//			}
//		}
//		if (!isLastQ) {
//			//********combine all vw_MatchFeats for one doc************
//			int docID=QID_DID.docID;
//			Int_MatchFeatArr[] docMatches=null;
//			if(vw_MatchFeats!=null){//for Reducer_buildRank_final_ParaliseQuery_saveScore
//				ArrayList<Int_MatchFeatArr>  matches = new ArrayList<Int_MatchFeatArr>(); 
//				for(Iterator<Int_MatchFeatArr> it=vw_MatchFeats.iterator();it.hasNext();){
//					Int_MatchFeatArr oneVW_matches=it.next(); //can not: it.next().getInteger(), it.next().getByteArr()
//					matches.add(new Int_MatchFeatArr(oneVW_matches.Integer,oneVW_matches.feats));
//				}
//				docMatches=matches.toArray(new Int_MatchFeatArr[0]);
//			}else if (groupedMatches!=null) {//for Reducer_buildRank_final_ParaliseFlag_saveScore
//				docMatches=groupedMatches.feats;
//			}else {
//				throw new InterruptedException("err, vw_MatchFeats and groupedMatches cannot be both null!");
//			}
//			//**************** save this doc into doc_scores_order ****************
//			float[] docScores=comparePhotos.scoreOneDoc(new DocAllMatchFeats(docID,docMatches), upDates.queryFeats.getArr(), thisQueryID, upDates.queryMaxSize, null, null, null, upDates.disp);
//			if (docScores[0]!=0) {
//				docScores_0.add(docScores[0]);
//				docIDs_0.add(docID);
//			}else {
//				docScores_1.add(docScores[1]);
//				docIDs_1.add(docID);
//			}
//			upDates.disp=false;
//		}
//	}
//	
//	//******** job6: analysis result ******
//	public static class Reducer_InOut_SaveRank extends Reducer<IntWritable,IntList_FloatList,IntWritable,IntList_FloatList>  {
//		
//		private int sampleNums;
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			sampleNums=0;
//			// ***** setup finsihed ***//
//			System.out.println("setup finsihed!");
//			super.setup(context);
//	 	}
//		
//		@Override
//		public void reduce(IntWritable RankFlagID, Iterable<IntList_FloatList> values, Context context) throws IOException, InterruptedException {
//			//key: sampleName, value: content
//
//			//******** only one list in values! ************		
//			for(Iterator<IntList_FloatList> it=values.iterator();it.hasNext();){// loop over all HashMaps				
//				IntList_FloatList oneSample=it.next();
//				int queryID=oneSample.getIntegers().get(oneSample.getFloats().size()); //add queryID to its ranks as the last one! so interger num = score num + 1
//				oneSample.getIntegers().remove(oneSample.getFloats().size());//remove queryID
//				sampleNums++;
//				context.write(new IntWritable(queryID), oneSample);
//			}
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ***** setup finsihed ***//
//			System.out.println("read and out finished! total sampleNums:"+sampleNums);
//			super.setup(context);
//	 	}
//	}
//
//	public static class Reducer_makeReport extends Reducer<IntWritable,IntList_FloatList,IntWritable,Text>  {
//		private Configuration conf;
//		private String indexLabel;
//		private boolean isOxfordBuilding;
//		private boolean isBarcelonaBuilding;
//		private ArrayList<String> rankFlagsData;
//		private String finalRankLabel;
//		private HashMap<Integer, HashSet<Integer>> groundTrue;
//		private int[] s_to_l;
//		private HashMap<Integer, HashSet<Integer>> junks;
//		private HashMap<Integer, HashSet<Integer>> buildingInd_Name;
//		private float[] thresholds;
//		private int tot_TruePositive;
//		private int reduceNum;
//		private int dispInter;
//		
//		@SuppressWarnings("unchecked")
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			conf = context.getConfiguration();
//			//***** read indexLabel ***//
//			indexLabel=conf.get("mapred.indexLabel");
//			System.out.println("read indexLabel finished! indexLabel: "+indexLabel);
//			isOxfordBuilding=indexLabel.contains("Oxford");
//			isBarcelonaBuilding=indexLabel.contains("Barcelona");
//			//***** read rankFlagsData ***//
//			rankFlagsData=(ArrayList<String>) General.readObject("rankFlagsData.file");
//			System.out.println("read rankFlagsData finished! tot-rankFlags num:"+rankFlagsData.size());
//			//******* read finalRankLabel**************			
//			finalRankLabel=conf.get("mapred.finalRankLabel");
//			System.out.println("load finalRankLabel: "+finalRankLabel);
//			//******* load groundTruth, s_to_l
//			groundTrue=(HashMap<Integer, HashSet<Integer>>) General.readObject("groundTrue.file");
//			s_to_l=(int[]) General.readObject("s_to_l.file");
//			if (isOxfordBuilding) {//Oxford dataset
//				junks=(HashMap<Integer, HashSet<Integer>>) General.readObject("junks.file");
//				buildingInd_Name=(HashMap<Integer, HashSet<Integer>>) General.readObject("buildingInd_Name.file");
//				System.out.println("this is Oxford dataset, read junks and buildingInd_Name finished!");
//			}
//			//***** load thresholds for pairwise PRCurve *****
//			thresholds=General.StrArrToFloatArr(conf.get("mapred.thresholdsForPRCurve").split(","));
//			tot_TruePositive=0;
//			for (HashSet<Integer> oneGroundTrue : groundTrue.values()) {
//				tot_TruePositive+=Math.pow(oneGroundTrue.size(), 2);
//			}
//			System.out.println("for pairwise PRCurve, tot_TruePositive:"+tot_TruePositive+", thresholds : "+conf.get("mapred.thresholdsForPRCurve"));
//			// ***** setup finsihed ***//
//			reduceNum=0;
//			dispInter=10;
//			System.out.println("combine result and analysize performance");
//			System.out.println("setup finsihed! \n");
//			super.setup(context);
//	 	}
//		
//		@Override
//		public void reduce(IntWritable RankFlagID, Iterable<IntList_FloatList> ranks, Context context) throws IOException, InterruptedException {
//			//key: queryName, value: rank result
//			//******** only one list in rank result! ************	
//			int queryNum=0; float MAP=0; float HR_1=0; float[][] truePositive_totPositive=new float[thresholds.length][];
//			//ini truePositive_totPositive
//			for (int i = 0; i < truePositive_totPositive.length; i++) {
//				truePositive_totPositive[i]=new float[2];
//			}
//			//run
//			for(Iterator<IntList_FloatList> it=ranks.iterator();it.hasNext();){// loop over all ranks				
//				IntList_FloatList oneRank= it.next();
//				int queryName=oneRank.getIntegers().get(oneRank.getFloats().size()); //add queryID to its ranks as the last one! so interger num = score num + 1
//				oneRank.getIntegers().remove(oneRank.getFloats().size());//remove queryID
//				//******* analysis this query's result *********************
//				ArrayList<Integer> oriIDs=new ArrayList<Integer>(oneRank.getIntegers().size());
//				for (int i = 0; i < oneRank.getIntegers().size(); i++) {
//					oriIDs.add(s_to_l[oneRank.getIntegers().get(i)]);
//				}
//				HashSet<Integer> relPhos=null;
//				String queryInfoToShow=", queryName:"+queryName;
//				if (isOxfordBuilding) {
//					int buildingInd=queryName/1000;//oxford use 1000 to group
//					relPhos=groundTrue.get(buildingInd);
//					oriIDs.removeAll(junks.get(buildingInd));//remove junks
//					queryInfoToShow+=", buildingName:"+buildingInd_Name.get(buildingInd);
//				}else if (isBarcelonaBuilding){
//					int buildingInd=queryName/10000;//Barcelona use 10000 to group
//					relPhos=groundTrue.get(buildingInd);
//					General.dispInfo_ifNeed(relPhos==null, "", "err! relPhos==null, queryName:"+queryName+", buildingInd:"+buildingInd+", groundTrue:"+groundTrue);
//					queryInfoToShow+=", buildingName:"+buildingInd;
//				}else {
//					relPhos=groundTrue.get(queryName);
//				}
//				float AP=General_IR.AP_smoothed(relPhos, oriIDs);
//				boolean cartoCorrect=relPhos.contains(oriIDs.get(0));
//				General.elementAdd_saveInA(truePositive_totPositive, General_IR.PR_Curve(relPhos, oriIDs, oneRank.getFloats(), thresholds, false));
//				MAP+=AP;
//				HR_1+=cartoCorrect?1:0;
//				queryNum++;	
//				General.dispInfo_ifNeed(queryNum%dispInter==0,"---", "current queryNums:"+queryNum+queryInfoToShow+", AP:"+AP+", 1NN-cartoCorrect:"+cartoCorrect);
//			}
//			
//			//outPut as String
//			StringBuffer outInfo=new StringBuffer();
//			outInfo.append("indexLabel:"+indexLabel+", finalRankLabel: "+finalRankLabel+rankFlagsData.get(RankFlagID.get())+"\n");
//			outInfo.append("\t tot "+queryNum+" querys, MAP:"+MAP/queryNum+", HR_1:"+HR_1/queryNum);
//			//make pairwise PRCurve
//			outInfo.append(", pairwise PRCurve: ");
//			for (float[] one : truePositive_totPositive) {
//				outInfo.append(one[0]/one[1]+"_"+one[0]/tot_TruePositive+", ");
//			}
//			//outPut
//			context.write(RankFlagID, new Text(outInfo.toString()+"\n"));
//			
//			// ***** setup finsihed ***//
//			System.out.println("one reduce finished! RankFlagID:"+RankFlagID+", total querys:"+queryNum);
//			System.out.println("outInfo: \n"+ outInfo.toString()+"\n");
//			reduceNum++;
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			
//			// ***** setup finsihed ***//
//			System.out.println("\n Reducer finished! total reduce num:"+reduceNum);
//			super.setup(context);
//	 	}
//	}
//	
//	public static class Reducer_combineReport extends Reducer<IntWritable,Text,IntWritable,Text>  {
//		
//		private Configuration conf;
//		private FileSystem hdfs;
//		private StringBuffer outInfo;
//		private String InfoStrPath;
//		private int reRankFlag_num;
//		private int dispInter;
//		
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			conf = context.getConfiguration();
//			hdfs=FileSystem.get(conf);
//			
//			outInfo=new StringBuffer();
//			
//			//**** set InfoStrPath ************//
//			InfoStrPath=conf.get("mapred.InfoStrPath");
//			
//			reRankFlag_num=0;
//			dispInter=10;
//			// ***** setup finsihed ***//
//			System.out.println("only 1 reducer, combine analysized report from all reRankFlags, save String obj to InfoStrPath: "+InfoStrPath);
//			System.out.println("setup finsihed!");
//			super.setup(context);
//	 	}
//		
//		@Override
//		public void reduce(IntWritable ReRankFlagID, Iterable<Text> reports, Context context) throws IOException, InterruptedException {
//			//key: queryName, value: rank result
//			
//			//******** only one list in rank result! ************		
//			int reRankFlagID=ReRankFlagID.get(); Text oneReport = null; int loopNum=0; 
//			for(Iterator<Text> it=reports.iterator();it.hasNext();){// loop over all ranks				
//				oneReport= it.next();
//				loopNum++;
//			}
//			General.Assert(loopNum==1, "error! one ReRankFlagID, one Report, loopNum should == 1, here loopNum="+loopNum);
//			
//			outInfo.append(oneReport.toString()+"\n");
//			
//			reRankFlag_num++;	
//			
//			General.dispInfo_ifNeed(reRankFlag_num%dispInter==0, "", "current reRankFlagID:"+reRankFlagID+", Report:"+oneReport);
//
//		}
//		
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			//outPut
//			General_Hadoop.writeObject_HDFS(hdfs, InfoStrPath, outInfo.toString());
//			
//			// ***** setup finsihed ***//
//			System.out.println("\n Reducer finished! total reRankFlag_num:"+reRankFlag_num);
//			System.out.println("outInfo: \n"+ outInfo.toString());
//			super.setup(context);
//	 	}
//	}
//
//}
