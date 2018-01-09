package BuildRank;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;
import MyAPI.General.General_Hadoop;
import MyAPI.General.General_IR;
import MyAPI.Obj.Disp;
import MyAPI.Obj.SelectID;
import MyAPI.Obj.Statistics;
import MyAPI.SystemCommand.RenewKerberos;
import MyAPI.imagR.Conf_ImageR;
import MyAPI.imagR.HEParameters;
import MyAPI.imagR.ImageR_Evaluation;
import MyAPI.imagR.IndexTrans;
import MyAPI.imagR.ReRankTopRankInfo;
import MyAPI.imagR.TVector_Hadoop;
import MyAPI.imagR.VW_IniMatchedDocs;
import MyAPI.imagR.MakeRank;
import MyAPI.imagR.ScoreDoc;
import MyCustomedHaoop.Combiner.Combiner_combine_IntArr_HESig_ShortArr_Arr;
import MyCustomedHaoop.Combiner.Combiner_sumValues;
import MyCustomedHaoop.KeyClass.Comparator_groupKey_Collection.Comparator_groupKey_Key_RankFlagID_QID;
import MyCustomedHaoop.KeyClass.Key_QID_DID;
import MyCustomedHaoop.KeyClass.Key_RankFlagID_QID;
import MyCustomedHaoop.MapRedFunction.MapRed_SelectSample;
import MyCustomedHaoop.MapRedFunction.MapRed_countDataNum;
import MyCustomedHaoop.Partitioner.Partitioner_KeyisPartID_PartKey;
import MyCustomedHaoop.Partitioner.Partitioner_equalAssign;
import MyCustomedHaoop.Partitioner.Partitioner_forSearchTVector;
import MyCustomedHaoop.Partitioner.Partitioner_random_sameKey;
import MyCustomedHaoop.Partitioner.Partitioner_random_sameKey_PartKey;
import MyCustomedHaoop.Reducer.Reducer_InOut.Reducer_InOut_1key_1value;
import MyCustomedHaoop.Reducer.Reducer_InOut.Reducer_InOut_normal;
import MyCustomedHaoop.Reducer.Reducer_buildIniRank;
import MyCustomedHaoop.Reducer.Reducer_combineReport;
import MyCustomedHaoop.ValueClass.AbstractTwoWritable;
import MyCustomedHaoop.ValueClass.ArrWritableClassCollection.HESig_ShortArr_Arr;
import MyCustomedHaoop.ValueClass.ArrWritableClassCollection.MatchFeat_Arr;
import MyCustomedHaoop.ValueClass.ComposeWritableClassCollection.IntArr_HESig_ShortArr_Arr;
import MyCustomedHaoop.ValueClass.ComposeWritableClassCollection.PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr;
import MyCustomedHaoop.ValueClass.ComposeWritableClassCollection.QID_IntList_FloatList;
import MyCustomedHaoop.ValueClass.ComposeWritableClassCollection.QID_PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr;
import MyCustomedHaoop.ValueClass.DID_Score;
import MyCustomedHaoop.ValueClass.DID_Score_Arr;
import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr;
import MyCustomedHaoop.ValueClass.DID_Score_ImageRegionMatch_ShortArr_Arr;
import MyCustomedHaoop.ValueClass.DocAllMatchFeats;
import MyCustomedHaoop.ValueClass.HESig;
import MyCustomedHaoop.ValueClass.HESig_ShortArr_AggSig;
import MyCustomedHaoop.ValueClass.ImageRegionMatch;
import MyCustomedHaoop.ValueClass.IntArr;
import MyCustomedHaoop.ValueClass.IntArr_FloatArr;
import MyCustomedHaoop.ValueClass.IntList_FloatList;
import MyCustomedHaoop.ValueClass.Int_MatchFeatArr;
import MyCustomedHaoop.ValueClass.Int_SURFfeat_ShortArr;
import MyCustomedHaoop.ValueClass.PhotoAllFeats;
import MyCustomedHaoop.ValueClass.PhotoPointsLoc;
import MyCustomedHaoop.ValueClass.PhotoSize;
import MyCustomedHaoop.ValueClass.SURFpoint;
import MyCustomedHaoop.ValueClass.SURFpoint_ShortArr;
import MyCustomedHaoop.ValueClass.VW_DID_Score_Arr;

public class MapRed_buildRank extends Configured implements Tool{

	/**
	 * 1. In Reducer_SearchTVector_getHMScore, it generate one DID_ScoreList Per VW, so takes 4+ TB for 15k query against 8.8M ME13 dataset with HEThr=20
	 * 2. when work in paralise ranking flag mode, it needs to put allDocMatches into distributed cache, so when query number is large and rerankLength is high, this file will become too large to be put into distributed cache.
	 * 		then, the reducer may failed due to timeout (a long time for Localization of downloading the files in distributed cache)! one possible solution is set -Dmapreduce.task.timeout=600000 (a high value)
	 * 
	 * @throws Exception 
	 * @command_example: 
	 * 
	 * SURF(my own kmean VW):
	 * SanFran:	yarn jar BuildRank.jar BuildRank.MapRed_buildRank -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar -Dmapreduce.task.timeout=600000 -Dvw_num=20000 -DindexPath=ImageR/BenchMark/SanFrancisco/index/ -DASMK_Thr=0,0.1,0.2,0.3 	-DASMK_Alpa=1,2,3,4 -DHMDistThr=50 			-DHMWeight_deta=40 			-DiniR_scheme=_iniR-ASMK 			-DisOnlyUseHMDistFor1Vs1=true -Dis1vw1match=true -DisUpRightFeat=true -DHPM_ParaDim=4 -DHPM_level=6 -DhistRotation_binStep=0.26 -DhistScale_binStep=0.2 -DbinScaleRate=1 -DPointDisThr=0 -DbadPariWeight=0 -DweightThr=0 -DlineAngleStep=0 -DlineDistStep=0 -DsameLinePointNumThr=0 -DdocScoreThr=0 -DreRankLength=10 -DtopRank=1000 -DgroundTrueForReport=ImageR/BenchMark/SanFrancisco/SanFrancisco_groundTruth_onlyPCI_inSInd_corr2014.hashMap 	-Ds_to_lForReport= -DisConcateTwoList=false -DisDiffTopDocsByVW=false -DisParaQueries=false -DsaveRes=false -DmakeReport=true -DthresholdsForPRCurve="" 											50 50 50 ImageR/BenchMark/SanFrancisco/feats/SIFTUPRightINRIA2_QDPCIVW20k_MA_SanFran_Q 		_SanFran_DPCI_QDPCIVW_SIFTUPRightINRIA2 ImageR/BenchMark/SanFrancisco/ranks/AR10 _rankDocScore
	 * ME15:	yarn jar BuildRank.jar BuildRank.MapRed_buildRank -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar -Dmapreduce.task.timeout=600000 -Dvw_num=20000 -DindexPath=MediaEval15/index/ 					-DASMK_Thr=0 				-DASMK_Alpa=2 		-DHMDistThr=50 			-DHMWeight_deta=40 			-DiniR_scheme=_iniR-ASMK 			-DisOnlyUseHMDistFor1Vs1=true -Dis1vw1match=true -DisUpRightFeat=true -DHPM_ParaDim=4 -DHPM_level=6 -DhistRotation_binStep=0.26 -DhistScale_binStep=0.2 -DbinScaleRate=1 -DPointDisThr=0 -DbadPariWeight=0 -DweightThr=0 -DlineAngleStep=0 -DlineDistStep=0 -DsameLinePointNumThr=0 -DdocScoreThr=0 -DreRankLength=10 -DtopRank=1000 -DlatlonsForReport=MediaEval15/ME15_photos_latlons.floatArr -DlatlonEvaFlag=noDivideQ@0.001,0.01,0.1@1,3,5,10 	-Ds_to_lForReport= -DisConcateTwoList=false -DisDiffTopDocsByVW=false -DisParaQueries=false -DsaveRes=false -DmakeReport=true -DthresholdsForPRCurve="" -DSelQuerys=MediaEval15/Q_S_to_S_sub10K/	50 50 50 MediaEval15/feats/SURF_VW20k_MA_ME15_Q	_ME15_SURF MediaEval15/ranks/AR10 _rankDocScore
	 * 
	 * SIFT:
	 * SanFran:	yarn jar BuildRank.jar BuildRank.MapRed_buildRank -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar -Dmapreduce.task.timeout=600000 -Dvw_num=20000 -DindexPath=ImageR/BenchMark/SanFrancisco/index/ -DASMK_Thr=0,0.1,0.2,0.3 -DASMK_Alpa=1,2,3,4 -DHMDistThr=24,26,28 -DHMDistThr_rankDoc=24,26,28 -DHMWeight_deta=20,25,30 -DiniR_scheme=_iniR-BurstIntraInter -DisOnlyUseHMDistFor1Vs1=true -Dis1vw1match=true -DisUpRightFeat=true  -DHPM_ParaDim=4 -DHPM_level=6 -DhistRotation_binStep=0.26,0.52,0.78 -DhistScale_binStep=0.1,0.2,0.3 -DbinScaleRate=1 -DPointDisThr=0,0.005,0.01 -DbadPariWeight=0 -DweightThr=0,1,3,5 -DlineAngleStep=0 -DlineDistStep=0 -DsameLinePointNumThr=0 -DdocScoreThr=0 -DreRankLength=1000 -DtopRank=1000 -DgroundTrueForReport=ImageR/BenchMark/SanFrancisco/SanFrancisco_groundTruth_onlyPCI_inSInd_corr2014.hashMap -Ds_to_lForReport= -DisConcateTwoList=false -DisParaQueries=false -DsaveRes=false -DmakeReport=true -DthresholdsForPRCurve="" 50 50 50 ImageR/BenchMark/SanFrancisco/feats/SIFTUPRightINRIA2_QDPCIVW20k_MA_SanFran_Q _SanFran_DPCI_QDPCIVW_SIFTUPRightINRIA2 ImageR/BenchMark/SanFrancisco/ranks/R _rankDocScore
	 * 
	 * 
	 * Herve:	yarn jar BuildRank.jar BuildRank.MapRed_buildRank -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DSelQuerys=ImageR/BenchMark/Herve/Herve_querys_L_to_L.hashMap -DindexPath=ImageR/BenchMark/Herve/SURF/ -DASMK_Thr=0,0.1,0.2,0.3 -DASMK_Alpa=1,2,3,4 -DHMDistThr=20 -DHMWeight_deta=12 -DiniR_weight=_iniR-BurstIntraInter -DisOnlyUseHMDistFor1Vs1=true -DisUpRightFeat=false -DHPM_ParaDim=2,4 -DHPM_level=6 -DhistRotation_binStep=0.26,0.52,0.78 -DhistScale_binStep=0.1,0.2,0.3 -DbinScaleRate=1 -DPointDisThr=0.0001,0.001,0.01 -DbadPariWeight=0.1,0.2 -DweightThr=1,3,5 -DlineAngleStep=0.52 -DlineDistStep=0.01 -DsameLinePointNumThr=0,10,20,30 -DdocScoreThr=0,5,10,15,20 -DreRankLength=1000 -DtopRank=1000000 -DgroundTrueForReport=ImageR/BenchMark/Herve/Herve_groundTruth.hashMap -Ds_to_lForReport=ImageR/BenchMark/Herve/Herve_ori1.5K_SelPhos_S_to_L.intArr -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 -DisMultiAss=true -DvwSoftWeight=0.05 -DisConcateTwoList=true -DisParaQueries=false -DsaveRes=false -DmakeReport=true -DthresholdsForPRCurve=100,80,50,40,30,20,10,5,0 50 50 50 ImageR/BenchMark/Herve/HerverImage.seq _Herve_1.5K ImageR/BenchMark/Herve/SURF/ranks/SURF _rankDocScore
	 * Oxford:	yarn jar BuildRank.jar BuildRank.MapRed_buildRank -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DSelQuerys=ImageR/BenchMark/Oxford/Oxford_querys_L_to_L.hashMap -DindexPath=ImageR/BenchMark/Oxford/SURF/ -DASMK_Thr=0,0.1,0.2,0.3 -DASMK_Alpa=1,2,3,4 -DHMDistThr=20 -DHMWeight_deta=12 -DiniR_weight=_iniR-BurstIntraInter -DisOnlyUseHMDistFor1Vs1=true -DisUpRightFeat=false -DHPM_ParaDim=2,4 -DHPM_level=3,4,5,6,7 -DhistRotation_binStep=0.26,0.52,0.78 -DhistScale_binStep=0.1,0.2,0.3 -DbinScaleRate=1 -DPointDisThr=0.0001,0.001,0.01 -DbadPariWeight=0.1,0.2 -DweightThr=1,3,5 -DlineAngleStep=0.52 -DlineDistStep=0.01 -DdocScoreThr=0,5,10,15,20 -DreRankLength=1000 -DtopRank=1000 -DgroundTrueForReport=ImageR/BenchMark/Oxford/OxfordBuilding_groundTruth.hashMap -Ds_to_lForReport=ImageR/BenchMark/Oxford/Oxford_ori5K_SelPhos_S_to_L.intArr -DjunksForReport=ImageR/BenchMark/Oxford/OxfordBuilding_junks.hashMap -DbuildingInd_NameForReport=ImageR/BenchMark/Oxford/OxfordBuilding_buildingInd_Name.hashMap -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 -DisMultiAss=true -DvwSoftWeight=0.05 -DisConcateTwoList=true -DisParaQueries=false -DsaveRes=false -DmakeReport=true -DthresholdsForPRCurve=100,80,50,40,30,20,10,5,0 10 10 10 ImageR/BenchMark/Oxford/OxfordBuilding.seq _Oxford_5K ImageR/BenchMark/Oxford/SURF/ranks/OriQ _rankDocScore
	 * Oxford:	yarn jar BuildRank.jar BuildRank.MapRed_buildRank -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DSelQuerys=ImageR/BenchMark/Oxford/Oxford_querys_L_to_L.hashMap -DindexPath=ImageR/BenchMark/Oxford/SURF/ -DASMK_Thr=0,0.1,0.2,0.3 -DASMK_Alpa=1,2,3,4 -DHMDistThr=20 -DHMWeight_deta=12 -DiniR_weight=_iniR-BurstIntraInter -DisOnlyUseHMDistFor1Vs1=true -DisUpRightFeat=false -DHPM_ParaDim=2,4 -DHPM_level=3,4,5,6,7 -DhistRotation_binStep=0.26,0.52,0.78 -DhistScale_binStep=0.1,0.2,0.3 -DbinScaleRate=1 -DPointDisThr=0.0001,0.001,0.01 -DbadPariWeight=0.1,0.2 -DweightThr=1,3,5 -DlineAngleStep=0.52 -DlineDistStep=0.01 -DdocScoreThr=0,5,10,15,20 -DreRankLength=1000 -DtopRank=1000 -DgroundTrueForReport=ImageR/BenchMark/Oxford/OxfordBuilding_groundTruth.hashMap -Ds_to_lForReport=ImageR/BenchMark/Oxford/Oxford_ori5K_SelPhos_S_to_L.intArr -DjunksForReport=ImageR/BenchMark/Oxford/OxfordBuilding_junks.hashMap -DbuildingInd_NameForReport=ImageR/BenchMark/Oxford/OxfordBuilding_buildingInd_Name.hashMap -DQueryPos_HashMap=ImageR/BenchMark/Oxford/QueryID_Postions.hashMap -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 -DisMultiAss=true -DvwSoftWeight=0.05 -DisConcateTwoList=true -DisParaQueries=false -DsaveRes=false -DmakeReport=true -DthresholdsForPRCurve=100,80,50,40,30,20,10,5,0 10 10 10 ImageR/BenchMark/Oxford/OxfordBuilding.seq _Oxford_5K ImageR/BenchMark/Oxford/SURF/ranks/CutQ _rankDocScore
	 * Oxford:	yarn jar BuildRank.jar BuildRank.MapRed_buildRank -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DSelQuerys=ImageR/BenchMark/Oxford/Oxford_querys_L_to_L.hashMap -DindexPath=ImageR/BenchMark/Oxford/SURF_AllCutQ/ -DASMK_Thr=0,0.1,0.2,0.3 -DASMK_Alpa=1,2,3,4 -DHMDistThr=20 -DHMWeight_deta=12 -DiniR_weight=_iniR-BurstIntraInter -DisOnlyUseHMDistFor1Vs1=true -DisUpRightFeat=false -DHPM_ParaDim=2,4 -DHPM_level=3,4,5,6,7 -DhistRotation_binStep=0.26,0.52,0.78 -DhistScale_binStep=0.1,0.2,0.3 -DbinScaleRate=1 -DPointDisThr=0.0001,0.001,0.01 -DbadPariWeight=0.1,0.2 -DweightThr=1,3,5 -DlineAngleStep=0.52 -DlineDistStep=0.01 -DsameLinePointNumThr=0,10,20,30 -DdocScoreThr=0,5,10,15,20 -DreRankLength=1000 -DtopRank=1000 -DgroundTrueForReport=ImageR/BenchMark/Oxford/OxfordBuilding_groundTruth.hashMap -Ds_to_lForReport=ImageR/BenchMark/Oxford/Oxford_ori5K_SelPhos_S_to_L.intArr -DjunksForReport=ImageR/BenchMark/Oxford/OxfordBuilding_junks.hashMap -DbuildingInd_NameForReport=ImageR/BenchMark/Oxford/OxfordBuilding_buildingInd_Name.hashMap -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 -DisMultiAss=true -DvwSoftWeight=0.05 -DisConcateTwoList=true -DisParaQueries=false -DsaveRes=false -DmakeReport=true -DthresholdsForPRCurve=100,80,50,40,30,20,10,5,0 10 10 10 ImageR/BenchMark/Oxford/OxfordBuilding_cutQ.seq _Oxford_5K ImageR/BenchMark/Oxford/SURF_AllCutQ/ranks/AllCutQ _rankDocScore
	 * Barceln:	yarn jar BuildRank.jar BuildRank.MapRed_buildRank -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DSelQuerys=ImageR/BenchMark/Barcelona/Barcelona_querys_L_to_L.hashMap -DindexPath=ImageR/BenchMark/Barcelona/SURF/ -DASMK_Thr=0,0.1,0.2,0.3 -DASMK_Alpa=1,2,3,4 -DHMDistThr=20 -DHMWeight_deta=12 -DiniR_weight=_iniR-BurstIntraInter -DisOnlyUseHMDistFor1Vs1=true -DisUpRightFeat=false -DHPM_ParaDim=2,4 -DHPM_level=4,5,6 -DhistRotation_binStep=0.26,0.52,0.78 -DhistScale_binStep=0.1,0.2,0.3,0.4 -DbinScaleRate=1 -DPointDisThr=0.001,0.01 -DbadPariWeight=0.1,0.2 -DweightThr=1,2,3,5 -DlineAngleStep=0.28,0.52,0.84 -DlineDistStep=0.001,0.01 -DsameLinePointNumThr=0,10,20,30 -DdocScoreThr=0,5,10,15,20 -DreRankLength=1000 -DtopRank=1000 -DgroundTrueForReport=ImageR/BenchMark/Barcelona/Barcelona_groundTruthBuildingID.hashMap -Ds_to_lForReport=ImageR/BenchMark/Barcelona/Barcelona_ori1K_SelPhos_S_to_L.intArr -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 -DisMultiAss=true -DvwSoftWeight=0.05 -DisConcateTwoList=true -DisParaQueries=false -DsaveRes=false -DmakeReport=true -DthresholdsForPRCurve=10,5,4,3,2,1,0 50 50 50 ImageR/BenchMark/Barcelona/Barcelona1K.seq _Barcelona_1K ImageR/BenchMark/Barcelona/SURF/ranks/SURF _rankDocScore
	 * Barceln:	yarn jar BuildRank.jar BuildRank.MapRed_buildRank -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DSelQuerys=ImageR/BenchMark/Barcelona/Barcelona_allPhotoAsQuery_L_to_L.hashMap -DindexPath=ImageR/BenchMark/Barcelona/SURF/ -DASMK_Thr=0,0.1,0.2,0.3 -DASMK_Alpa=1,2,3,4 -DHMDistThr=20 -DHMWeight_deta=12 -DiniR_weight=_iniR-BurstIntraInter -DisOnlyUseHMDistFor1Vs1=true -DisUpRightFeat=false -DHPM_ParaDim=2,4 -DHPM_level=4,5,6 -DhistRotation_binStep=0.52,0.78 -DhistScale_binStep=0.1,0.2 -DbinScaleRate=1 -DPointDisThr=0 -DbadPariWeight=0 -DweightThr=0,1,2,3,5 -DlineAngleStep=0.52,0.78 -DlineDistStep=0.00001,0.01 -DsameLinePointNumThr=0,10,20,30 -DdocScoreThr=0,5,10,15 -DreRankLength=1000 -DtopRank=1000 -DgroundTrueForReport=ImageR/BenchMark/Barcelona/Barcelona_groundTruthBuildingID.hashMap -Ds_to_lForReport=ImageR/BenchMark/Barcelona/Barcelona_ori1K_SelPhos_S_to_L.intArr -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 -DisMultiAss=true -DvwSoftWeight=0.05 -DisConcateTwoList=true -DisParaQueries=false -DsaveRes=false -DmakeReport=true -DthresholdsForPRCurve=10,5,4,3,2,1,0 50 50 50 ImageR/BenchMark/Barcelona/Barcelona1K.seq _Barcelona_1K ImageR/BenchMark/Barcelona/SURF/ranks/SURF-PairWisePRCurve _rankDocScore
	 * MEva13:	yarn jar BuildRank.jar BuildRank.MapRed_buildRank -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DSelQuerys=MM15/ImageR/ME13Q_PS10k/Q0.hashMap -DindexPath=MM15/ImageR/ -DASMK_Thr=0,0.1,0.2,0.3 -DASMK_Alpa=1,2,3,4 -DHMDistThr=20 -DHMWeight_deta=12 -DiniR_weight=_iniR-BurstIntraInter -DisOnlyUseHMDistFor1Vs1=true -DisUpRightFeat=false -DHPM_ParaDim=4 -DHPM_level=6 -DhistRotation_binStep=0.52 -DhistScale_binStep=0.2 -DbinScaleRate=1 -DPointDisThr=0 -DbadPariWeight=0 -DweightThr=0 -DlineAngleStep=0 -DlineDistStep=0 -DsameLinePointNumThr=0 -DdocScoreThr=0 -DreRankLength=1000 -DtopRank=1000 -DgroundTrueForReport= -Ds_to_lForReport= -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 -DisMultiAss=true -DvwSoftWeight=0.05 -DisConcateTwoList=false -DisParaQueries=true -DsaveRes=true -DmakeReport=false -DthresholdsForPRCurve=10,5,4,3,2,1,0 1000 1000 1000 66M_Phos_Seqs _MEva13_9M MM15/ImageR/ranks/Q0 _rankDocMatches
	 * MEva14:	yarn jar BuildRank_ME14.jar BuildRank.MapRed_buildRank -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DSelQuerys=MediaEval14/Querys_perSubSet10k -DindexPath=MediaEval14/ -DASMK_Thr=0,0.1,0.2,0.3 -DASMK_Alpa=1,2,3,4 -DHMDistThr=20 -DHMWeight_deta=12 -DiniR_weight=_iniR-BurstIntraInter -DisOnlyUseHMDistFor1Vs1=true -DisUpRightFeat=false -DHPM_ParaDim=4 -DHPM_level=6 -DreRankLength=1000 -DtopRank=1000 -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 -DisMultiAss=true -DvwSoftWeight=0.05 -DisConcateTwoList=true -DisParaQueries=true -DsaveRes=true -DmakeReport=false 1000 1000 1000 Webscope100M/ME14_Crawl/Photos _MEva14_5MPho MediaEval14/ranks/R _rankDocScore
	 * MEva14:	yarn jar BuildRank_ME14.jar BuildRank.MapRed_buildRank -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jai_core.jar,jai_codec.jar -DvwCenters=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_KMean_VW20k/loop-99/part-r-00000 -DpMatrix=ImageR/HE_ProjectionMatrix64-64 -DHEThreshold=ImageR/forVW/SURF_MedEval3M_forVW-RFea1000-RPho50k_forHEThr-RFea3000-RPho100k_HEThr64_VW20k_KMloop99/part-r-00000 -DmiddleNode=ImageR/forVW/MiddleNode1000_onVW20k/loop-199/part-r-00000 -DnodeLink_learned=ImageR/forVW/D_MedEval3M-RFea3000-RPho100k_node_vw_links_M1000_VW20k_I200.ArrayList_HashSet -DSelQuerys=MediaEval14/VideoFrameTest/2012TestVideoFrames_L_to_L.hashMap -DindexPath=MediaEval14/ -DASMK_Thr=0,0.1,0.2,0.3 -DASMK_Alpa=1,2,3,4 -DHMDistThr=20 -DHMWeight_deta=12 -DiniR_weight=_iniR-BurstIntraInter -DisOnlyUseHMDistFor1Vs1=true -DisUpRightFeat=false -DHPM_ParaDim=4 -DHPM_level=6 -DreRankLength=100000 -DtopRank=100000 -DtargetFeature=SURF -DBinTool_SIFT= -DtargetImgSize=786432 -DisMultiAss=true -DvwSoftWeight=0.05 -DisConcateTwoList=true -DisParaQueries=true -DsaveRes=true -DmakeReport=false 1000 1000 1000 MediaEval14/VideoFrameTest/2012TestVideoFrames.seq _MEva14_5MPho MediaEval14/VideoFrameTest/ranks/R _rankDocScore
	 * 
	 * 	
	 */
	
	public static String[] oriArgs; //save all arguments
		
	public static void main(String[] args) throws Exception {
//		timeTest();
		
//		prepareData();
		
		runHadoop(args);
	}
	
	public static void runHadoop(String[] args) throws Exception {
		oriArgs=args;
		int ret = ToolRunner.run(new MapRed_buildRank(), args);
		System.exit(ret);
	}
	
	public static void prepareData() throws Exception {
//		//***** for 3M random sellected querys *********//
//		int totNum=3185258; int queryNum=100*1000;
//		int[] randInds=General.randIndex(totNum);
//		HashMap<Integer, Integer> query_transIndex_LtoS= new HashMap<Integer, Integer>(queryNum);
//		for (int i = 0; i < queryNum; i++) {
//			query_transIndex_LtoS.put(randInds[i], randInds[i]);
//		}
//		General.writeObject("D:/xinchaoli/Desktop/My research/My Code/DataSaved/ICMR2013/Querys_100K_LtoS_from_D3M.hashMap", query_transIndex_LtoS);
		
//		//***** for ICMR2013 3M random sellected querys *********//
//		String quryFilePath="D:/xinchaoli/Desktop/My research/My Code/DataSaved/ICMR2013/QDP/3M/";
//		String[] fileNames={"topLocGVSize_ori.oriRight","topLocGVSize_ori.oriWrong"};
//		int queryNum=100*1000; String line1;
//		HashMap<Integer, Integer> query_transIndex_LtoS= new HashMap<Integer, Integer>(queryNum);
//		for (int i = 0; i < fileNames.length; i++) {
//			BufferedReader inputStreamFeat = new BufferedReader(new InputStreamReader(new FileInputStream(quryFilePath+fileNames[i]), "UTF-8"));
//			while((line1=inputStreamFeat.readLine())!=null){//Q259_G4_R1:	0	0	0	0	0	0	0	0	0	0	
//				int queryName=Integer.valueOf(line1.split(":")[0].split("_")[0].substring(1));
//				query_transIndex_LtoS.put(queryName, queryName);
//			}
//			inputStreamFeat.close();
//		}
//		System.out.println("total query num:"+query_transIndex_LtoS.size());
//		General.writeObject("D:/xinchaoli/Desktop/My research/My Code/DataSaved/ICMR2013/Querys_100K_LtoS_from_D3M_ICMR2013.hashMap", query_transIndex_LtoS);
		
		//***** check photoFeatNum *********//
//		int[] photoFeatNum=(int[]) General.readObject("D:/xinchaoli/Desktop/My research/My Code/DataSaved/ImageRetrieval/photoFeatNum_3M");
//		int[] photoFeatNum=(int[]) General.readObject("O:/MediaEval13/photoFeatNum_MEva13_9M");
//		int noFeatPhoNum=0;
//		for (int pho_i : photoFeatNum) {
//			if (pho_i==0) {
//				noFeatPhoNum++;
//			}
//		}
//		System.out.println("totPhoNum:"+photoFeatNum.length+", some photo no feat, noFeatPhoNum:"+noFeatPhoNum);
		
//		//***** for making sub-query-set for 9M dataset, as when build rank, doc_scores take a lot space if dataset is large, so decrease the query number ************
//		int querySetSize=10*1000;//30*1000 for 9M
//		HashMap<Integer, Integer> totQ=(HashMap<Integer, Integer>) General.readObject("O:/MediaEval13/Querys/Q1.hashMap");
//		for (int i = 2; i < 9; i++) {
//			totQ.putAll((HashMap<Integer, Integer>) General.readObject("O:/MediaEval13/Querys/Q"+i+".hashMap"));
//		}
//		String subQSetFolder="O:/MediaEval13/Querys_test_perSubSet"+querySetSize/1000+"k/";
//		General.makeORdelectFolder(subQSetFolder);
//		Random rand=new Random();
//		ArrayList<HashMap<Integer, Integer>> Qsets =General.randSplitHashMap(rand, totQ, 0, querySetSize);
//		int totQnum=0;
//		for (int i = 0; i < Qsets.size(); i++) {
//			General.writeObject(subQSetFolder+"Q"+i+".hashMap", Qsets.get(i));
//			System.out.println(i+", "+Qsets.get(i).size());
//			totQnum+=Qsets.get(i).size();
//		}
//		General.Assert(totQnum==totQ.size(), "err, totQnum:"+totQnum+", should =="+totQ.size());
//		System.out.println("taget querySetSize:"+querySetSize+", totQnum:"+totQnum+", should =="+totQ.size());
	}
	
	public static void timeTest() throws Exception {
		
//		//******** time test ***********/
//		int TVectorLength=10*1000*1000; int HMDistThr=12; int matchNum=0; int HElength=64;
//		//make HESig_Bytes
//		BitSet HESig=new BitSet(HElength);
//		for(int i=0;i<HElength;i++)
//			HESig.set(i);// set i-th == true 
//		System.out.println(HESig);
//		byte[] HESig_Bytes=General.BitSettoByteArray(HESig); 
//		//make HESig_Bytes_q
//		BitSet HESig_q=new BitSet(HElength);
//		for(int i=0;i<HElength;i+=3)
//			HESig_q.set(i);// set i-th == true 
//		System.out.println(HESig_q);
//		byte[] HESig_Bytes_q=General.BitSettoByteArray(HESig_q);
//		//test HammingDist Time
//		long startTime=System.currentTimeMillis(); //startTime
//		int hammingDist=0;
//		for(int j=0;j<TVectorLength;j++){
//			hammingDist=General.get_DiffBitNum(HESig_Bytes_q, HESig_Bytes);// computing time: 15% of BigInteger!!
////			hammingDist=(new BigInteger(HESig_Bytes_q)).xor(new BigInteger(HESig_Bytes)).bitCount(); //slow!!
//			if(hammingDist<=HMDistThr){
//				matchNum++;
//			}
//		}
//		long endTime=System.currentTimeMillis(); //endTime
//		System.out.println("hammingDist:"+hammingDist+", make HammingDist times:"+TVectorLength+"...."+General.dispTime(endTime-startTime, "ms"));

	}

	public static ArrayList<String> makeIniRSchemes(Configuration conf){
		ArrayList<String> iniR_schemes=new ArrayList<>();
		String iniR_scheme=conf.get("iniR_scheme");
		if (iniR_scheme.equals("_iniR-ASMK")) {//ASMK
			for (String ASMKThr : conf.get("ASMK_Thr").split(",")) {
				for (String ASMKAlpa : conf.get("ASMK_Alpa").split(",")) {
					iniR_schemes.add(iniR_scheme+"@"+ASMKThr+"@"+ASMKAlpa);
				}
			}
		}else {//HE-based
			//set HMDistThr_selDoc
			String[] HMDistThr=conf.get("HMDistThr").split(",");
			//set HMWeight_deta
			String[] HMWeight_deta=conf.get("HMWeight_deta").split(",");
			for (String one_HMWeight_deta : HMWeight_deta) {//HMWeight_deta
				for (String one_HMDistThr : HMDistThr) {//HMDistThr_selDoc
					iniR_schemes.add(iniR_scheme+"@"+one_HMDistThr+"@"+one_HMWeight_deta);
				}
			}
		}
		return iniR_schemes;	
	}
	
	public static ArrayList<String> makeReRankHEParas(Configuration conf){
		ArrayList<String> reRankHEParas=new ArrayList<>();
		//set reRank HDr
		String[] HMDistThr=conf.get("HMDistThr").split(",");
		//set HMWeight_deta
		String[] HMWeight_deta=conf.get("HMWeight_deta").split(",");
		for (String one_HMWeight_deta : HMWeight_deta) {//HMWeight_deta
			for (String one_HMDistThr : HMDistThr) {//HMDistThr_selDoc
				reRankHEParas.add("_reRHE@"+one_HMDistThr+"@"+one_HMWeight_deta);
			}
		}
		return reRankHEParas;	
	}
	
	public static ArrayList<String> makeReRankFlags(Configuration conf){
		String isOnlyUseHMDistFor1Vs1=conf.get("isOnlyUseHMDistFor1Vs1");
		ArrayList<String> rerankFlags=new ArrayList<String>(); 
		for (String is1vw1match : conf.get("is1vw1match").split(",")) {
//			rerankFlags.add("_1vs1@"+isOnlyUseHMDistFor1Vs1+"@"+is1vw1match);
			for (String para0 : conf.get("HPM_ParaDim").split(",")) {//2,4
				for (String para1 : conf.get("HPM_level").split(",")) {//1,2,3,4,5
//					rerankFlags.add("_1vs1AndHPM@"+isOnlyUseHMDistFor1Vs1+"@"+is1vw1match+"@"+para0+"@"+para1);
				}
			}
			for (String para0 : conf.get("histRotation_binStep").split(",")) {
				for (String para1 : conf.get("histScale_binStep").split(",")) {
//					rerankFlags.add("_1vs1AndHist@"+isOnlyUseHMDistFor1Vs1+"@"+is1vw1match+"@"+para0+"@"+para1);
	//				rerankFlags.add("_HistAnd1vs1@"+isOnlyUseHMDistFor1Vs1+"@"+para0+"@"+para1);
				}
			}
			for (String isUpRightFeat : conf.get("isUpRightFeat").split(",")) {
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
				//											rerankFlags.add("_1vs1AndAngle@"+isOnlyUseHMDistFor1Vs1+"@"+para0+"@"+para1+"@"+para2+"@"+para3+"@"+para4+"@"+para5+"@"+para6+"@"+para7);
															rerankFlags.add("_1vs1AndHistAndAngle@"+isOnlyUseHMDistFor1Vs1+"@"+is1vw1match+"@"+isUpRightFeat+"@"+para0+"@"+para1+"@"+binScaleRate+"@"+para2+"@"+para3+"@"+para4+"@"+para5+"@"+para6+"@"+para7+"@"+para8);
		//													rerankFlags.add("_HistAnd1vs1AndAngle@"+isOnlyUseHMDistFor1Vs1+"@"+para0+"@"+para1+"@"+binScaleRate+"@"+para2+"@"+para3+"@"+para4+"@"+para5+"@"+para6+"@"+para7+"@"+para8);
														}
													}
												}
											}else {//no line detection
												String para5="0";
												String para7="0";
												for (String para8 : conf.get("docScoreThr").split(",")) {
			//										rerankFlags.add("_1vs1AndAngle@"+isOnlyUseHMDistFor1Vs1+"@"+para0+"@"+para1+"@"+para2+"@"+para3+"@"+para4+"@"+para5+"@"+para6+"@"+para7);
													rerankFlags.add("_1vs1AndHistAndAngle@"+isOnlyUseHMDistFor1Vs1+"@"+is1vw1match+"@"+isUpRightFeat+"@"+para0+"@"+para1+"@"+binScaleRate+"@"+para2+"@"+para3+"@"+para4+"@"+para5+"@"+para6+"@"+para7+"@"+para8);
		//											rerankFlags.add("_HistAnd1vs1AndAngle@"+isOnlyUseHMDistFor1Vs1+"@"+para0+"@"+para1+"@"+binScaleRate+"@"+para2+"@"+para3+"@"+para4+"@"+para5+"@"+para6+"@"+para7+"@"+para8);
												}
											}
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
//															rerankFlags.add("_1vs1AndHistAndAngleWithHPM@"+isOnlyUseHMDistFor1Vs1+"@"+para0+"@"+para1+"@"+binScaleRate+"@"+para2+"@"+para3+"@"+para4+"@"+para5+"@"+para6+"@"+para7+"@"+para8+"@"+para9+"@"+para10);
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
//													rerankFlags.add("_1vs1AndHistAndAngleWithHPM@"+isOnlyUseHMDistFor1Vs1+"@"+para0+"@"+para1+"@"+binScaleRate+"@"+para2+"@"+para3+"@"+para4+"@"+para5+"@"+para6+"@"+para7+"@"+para8+"@"+para9+"@"+para10);
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
		return rerankFlags;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int run(String[] args) throws Exception {//this args is already parsed by ToolRunner, so not all the oriArgs
		Configuration conf = getConf(); 
		FileSystem hdfs=FileSystem.get(conf);
		String[] otherArgs = args; //use this to parse args!
		ArrayList<String> cacheFilePaths=new ArrayList<String>();
		String dateFormate="yyyy.MM.dd G 'at' HH:mm:ss z";
		RenewKerberos renewTicket=new RenewKerberos();
		/*in job 5, paralise rerankFlags or queries, the fist one needs to put all query's top-k doc's matchFeats into distributed cache! so only for small query set,  
		 * attention: for 500 query, rerank top-1000, HMDistThr_rankDoc=18, the size of Out_job4 is 700mb!
		 * when the size is problem, try to divid query into small groups or use paralise queries!
		 */
		boolean isParaQueries=Boolean.valueOf(conf.get("isParaQueries"));
		//save rank result or make performance report
		boolean saveRes=Boolean.valueOf(conf.get("saveRes"));
		boolean makeReport=Boolean.valueOf(conf.get("makeReport"));
		PrintWriter outStr_report=null;
		//set rankLabels
		String rankLabels=conf.get("iniR_scheme")+"_ASMK"+conf.get("ASMK_Thr")+"-"+conf.get("ASMK_Alpa")+"_HD"+conf.get("HMDistThr")+"-HMW"+conf.get("HMWeight_deta")+
				"_ReR"+conf.get("reRankLength")+"_top"+conf.get("topRank");
		//get vw_num
	    int vw_num=Integer.valueOf(conf.get("vw_num"));
	    //set VWFileInter
	    int VWFileInter=conf.get("VWFileInter")==null?vw_num/1000:Integer.valueOf(conf.get("VWFileInter"));//by default VWFileInter=vw_num/1000
	    conf.set("VWFileInter", VWFileInter+"");
		//set reducer number
		int job1_1RedNum=Integer.valueOf(otherArgs[0]); //reducer number for extract query's SURF
		int job2_3RedNum=Integer.valueOf(otherArgs[1]); //reducer number for build ini rank
		int job5RedNum=Integer.valueOf(otherArgs[2]); //reducer number for combine query_MatchFeat from each vw, build final rank for query
		//set queryFeatPath
		String queryFeatPath=otherArgs[3]; //queryFeatPath
		//set Index label
		String indexLabel=otherArgs[4]; //_Oxford_1M
		indexLabel+="_VW"+vw_num/1000+"K";
		conf.set("indexLabel", indexLabel);
		conf.set("TVector", conf.get("indexPath")+"TVector"+indexLabel);
		conf.set("docInfo", conf.get("indexPath")+"docInfo"+indexLabel);
		conf.set("TVectorInfo", conf.get("indexPath")+"TVectorInfo"+indexLabel);
		//set output path
		String out=otherArgs[5]+indexLabel; //output path
		//set output rank format
		String saveRankFormat=otherArgs[6];//_rankDocScore, _rankDocMatches
		boolean onlyScore=saveRankFormat.equalsIgnoreCase("_rankDocScore");//only save rank with docScore or save rank with doc's all matches with the query
		if (!onlyScore) {//isParaQueries must be true
			General.Assert(isParaQueries, "err! need to save matches, this can only be done when isParaQueries==true, but now isParaQueries is "+isParaQueries);
			General.Assert((saveRes==true) && (makeReport==false), "err! need to save matches, in this case cannot make report!");
		}
		Class<? extends Reducer> buildRankReducer=onlyScore?Reducer_buildRank_final_ParaliseQuery_saveScore.class:Reducer_buildRank_final_ParaliseQuery_saveDocMatches.class;
		Class<? extends Writable> buildRankClass=onlyScore?QID_IntList_FloatList.class:QID_PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr.class;
		Class<? extends Writable> saveRankClass=onlyScore?IntList_FloatList.class:PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr.class;
		//set startloop
		int startloop_Q_iniR_reRL_reRHE=Integer.valueOf(otherArgs[7].split("@")[0]);//0@0
		int startloop_stage=Integer.valueOf(otherArgs[7].split("@")[1]);//for the startloop_stage in the loop of startloop_Q_iniR_reRL_reRHE
		//set iniR_schemes
		ArrayList<String> iniR_schemes=makeIniRSchemes(conf);
		//set reRankLength, output Top rank
		ReRankTopRankInfo reRTopRInfo=new ReRankTopRankInfo(conf.get("reRankLength"), conf.get("topRank"));
		//set isDiffTopDocsByVW
		boolean isDiffTopDocsByVW=Boolean.valueOf(conf.get("isDiffTopDocsByVW"));
		//set maxIniRankLength
		int maxIniRankLength=reRTopRInfo.getMaxIniRankLength();
		conf.set("maxIniRankLength", maxIniRankLength+"");
		//set ReRankHEParas
		ArrayList<String> reRankHEParas=makeReRankHEParas(conf);
		//set rerankFlag
		ArrayList<String> rerankFlags = makeReRankFlags(conf);
		boolean includeNoRerank=false;//this is oriRank, no rerank
		String rerankLabel=includeNoRerank?"_Ori":""+"_1vs1_1vs1AndHPM@paraDim"+conf.get("HPM_ParaDim")+"@level"+conf.get("HPM_level")
				+"_1vs1AndHist@histRotation_binStep"+conf.get("histRotation_binStep")+"@histScale_binStep"+conf.get("histScale_binStep")+"@binScaleRate"+conf.get("binScaleRate")
				+"@PointDisThr"+conf.get("PointDisThr")+"@badPariWeight"+conf.get("badPariWeight")+"@weightThr"+conf.get("weightThr")
				+"@lineAngleStep"+conf.get("lineAngleStep")+"@lineDistStep"+conf.get("lineDistStep")+"@sameLinePointNumThr"+conf.get("sameLinePointNumThr")
				+"@docScoreThr"+conf.get("docScoreThr");
		//set selected querys set
		ArrayList<String> selQuerys=new ArrayList<String>(); 
		String queryHashMapPath=conf.get("SelQuerys");
		boolean isSelQuerys=(queryHashMapPath!=null && !queryHashMapPath.isEmpty());
		if (isSelQuerys) {
			if (hdfs.isFile(new Path(queryHashMapPath))) {
				selQuerys.add(queryHashMapPath);
			}else {
				FileStatus[] files= hdfs.listStatus(new Path(queryHashMapPath));
				for (int i = 0; i < files.length; i++) {
					selQuerys.add(files[i].getPath().toString());
				}
			}
		}else {
			selQuerys.add("noSelQuerys_useThemAll");
		}
		//set Report
		if (makeReport) {
			General.Assert(onlyScore, "err! when make performance report, the save format should be _rankDocScore! here it is:"+saveRankFormat);
			outStr_report=new PrintWriter(new OutputStreamWriter(hdfs.create(new Path(out
					+rankLabels+"_Report"),false), "UTF-8"),true); 
			General.dispInfo(outStr_report,General.dispTimeDate(System.currentTimeMillis(), dateFormate)+", start processing!  ..................");
			General.dispInfo(outStr_report,"oriArgs:"+General.StrArrToStr(oriArgs, " "));
			General.dispInfo(outStr_report, "indexLabel: "+indexLabel+", vw_num:"+vw_num+", VWFileInter:"+VWFileInter+"\n"
					+"for Query, queryFeatPath:"+queryFeatPath+"\n"
					+selQuerys.size()+" selQuerys: "+selQuerys+"\n"
					+"TVectorPath:"+conf.get("TVector")+", TVectorInfoPath:"+conf.get("TVectorInfo")+", docInfoPath:"+conf.get("docInfo")+"\n"
					+"work dir:"+out+", saveRankFormat:"+saveRankFormat+"\n"
					+"job1_1RedNum for extract query's SURF:"+job1_1RedNum+", job2_3RedNum for build ini rank:"+job2_3RedNum+", job5RedNum for combine query_MatchFeat from each vw, build final rank for query:"+job5RedNum+"\n"
					+"reRankLengths:"+reRTopRInfo.getReRanksInStr(",")+" maxIniRankLength:"+maxIniRankLength+"\n"
					+"rankLabels:"+rerankLabel);
		}		
		General.dispInfo(outStr_report, "iniR_schemes: "+iniR_schemes+"\n iniR_schemes num:"+iniR_schemes.size());
		General.dispInfo(outStr_report, "reRankHEParas: "+reRankHEParas+"\n reRankHEParas num:"+reRankHEParas.size());
		General.dispInfo(outStr_report, "rerankFlags: "+rerankFlags+"\n rerankFlags num:"+rerankFlags.size());
		//rerank-flags
		String In_reRankFlags_job5_2=out+"_reRankFlags.seq";
		int job5_2_RedNum=rerankFlags.size()<2000?rerankFlags.size():Math.max(700, Math.min(rerankFlags.size()/10, 50000)); //if 1 rerankFlag need 2mins, one reducer process 20 rerankFlags
		int job5_2_reducerInter=(rerankFlags.size()-1)/job5_2_RedNum+1;
		if (!isParaQueries) {
			General_Hadoop.makeTextSeq_indIsKey(new Path(In_reRankFlags_job5_2), rerankFlags, conf);//ind in rerankFlags is the reducer ind 
		}
		//paths
		String[][][] Out_job5_2_all_toDelete = new String[iniR_schemes.size()][reRTopRInfo.getReRankNum()][reRankHEParas.size()];
		Path[][][][][] rankPaths_rerank=new Path[iniR_schemes.size()][reRTopRInfo.getReRankNum()][reRankHEParas.size()][isParaQueries?rerankFlags.size():job5_2_RedNum][selQuerys.size()];
		Path[][] rankPaths_oriRank=new Path[iniR_schemes.size()][selQuerys.size()];
		//******************************* loop-over selQuerys ******************************
		String currentLoopLabel; int tot_queryNum_feated=0; int loopInd_Q_iniR_reRL_reRHE=0; boolean isRunloop_Q=true; String[] QNumPaths=new String[selQuerys.size()];
		for (int qi = 0; qi < selQuerys.size(); qi++) {
			String queryloopLabel="_Q"+qi+"-"+(selQuerys.size()-1);	
			currentLoopLabel=queryloopLabel;
			General.dispInfo(outStr_report, "start process "+queryloopLabel+", "+General.dispTimeDate(System.currentTimeMillis(), dateFormate));
			long startTime=System.currentTimeMillis();
			conf.set("rerankFlag", "_OriHE");
			int topRank_iniRank=reRTopRInfo.getMaxIniRankLength();
			conf.set("topRank", topRank_iniRank+"");
			isRunloop_Q=(loopInd_Q_iniR_reRL_reRHE>startloop_Q_iniR_reRL_reRHE) || ((loopInd_Q_iniR_reRL_reRHE==startloop_Q_iniR_reRL_reRHE) && (0>=startloop_stage) );
			//******* job1_1: extract query's SURF feats and makeVWs ******
			String Out_job1_1=out+currentLoopLabel+"_querySURFRaw"; int queryNum_feated=0; QNumPaths[qi]=Out_job1_1+"_num";
			if (isRunloop_Q) {
				MapRed_SelectSample.runHadoop(conf, General_Hadoop.strArr_to_PathArr(queryFeatPath.split(",")), Out_job1_1, isSelQuerys?selQuerys.get(qi):null, IntWritable.class, PhotoAllFeats.class);
				queryNum_feated=MapRed_countDataNum.runHadoop(conf, new Path[]{new Path(Out_job1_1)}, Out_job1_1+"_tmp");
				General_Hadoop.writeObject_HDFS(FileSystem.get(conf), QNumPaths[qi], queryNum_feated);
			}
			queryNum_feated=(int) General_Hadoop.readObject_HDFS(FileSystem.get(conf), QNumPaths[qi]);
			tot_queryNum_feated+=queryNum_feated;
			General.dispInfo(outStr_report, "\t\t job1_1: select query's feats done! queryNum_feated:"+queryNum_feated+", "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			//******* job1_2: read query's SURF raw feats, save query_SURFpoint into MapFile ******
			String Out_job1_2=out+currentLoopLabel+"_querySURFpoint";
			General_Hadoop.Job(isRunloop_Q, conf, new Path[]{new Path(Out_job1_1)}, Out_job1_2, "Job1_2_getSURFpoint"+currentLoopLabel, 1, 8, 10, true,
					MapRed_buildRank.class, Mapper_GetQueryPoints.class, null,null,null,Reducer_InOut_1key_1value.class,
					IntWritable.class, SURFpoint_ShortArr.class, IntWritable.class,SURFpoint_ShortArr.class,
					SequenceFileInputFormat.class, MapFileOutputFormat.class, 1*1024*1024*1024L, 0,
					null,null);
			General.dispInfo(outStr_report, "\t\t job1_2: read query's SURF raw feats, save query_SURFpoint into MapFile done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			//******* job1_3: group HESig ******
			String Out_job1_3=out+currentLoopLabel+"_queryHESig";
			General_Hadoop.Job(isRunloop_Q, conf, new Path[]{new Path(Out_job1_1)}, Out_job1_3, "Job1_3_groupHESig"+currentLoopLabel, job1_1RedNum, 8, 10, true,
					MapRed_buildRank.class, Mapper_GetQueryVWHESig.class, null,null,null,null,
					IntWritable.class, IntArr_HESig_ShortArr_Arr.class, IntWritable.class,IntArr_HESig_ShortArr_Arr.class,
					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
					null,null);
			General.dispInfo(outStr_report, "\t\t job1_3: read query's SURF feats, and group HESig done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			
			
			//******* job1_4: extract query's sizeInfo ******
			String QuerySize_HashMap_Path=out+currentLoopLabel+"_querySize.hashMap";
			conf.set("QuerySize_HashMap", QuerySize_HashMap_Path); 
			if (isRunloop_Q)
				FileSystem.get(conf).delete(new Path(conf.get("QuerySize_HashMap")), true);
			General_Hadoop.Job(isRunloop_Q, conf, new Path[]{new Path(Out_job1_1)}, null, "Job1_4_getQuerySizes"+currentLoopLabel, 1, 8, 10, true,
					MapRed_buildRank.class, Mapper_GetQuerySize.class, null,null,null,Reducer_ExtractQuerySize.class,
					IntWritable.class, PhotoSize.class, IntWritable.class,PhotoSize.class,
					SequenceFileInputFormat.class, NullOutputFormat.class, 1*1024*1024*1024L, 0,
					null,null);
			General.dispInfo(outStr_report, "\t\t job1_4: extract query's size info done! no outPut, save QuerySize_HashMap to "+QuerySize_HashMap_Path+", "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			
			
			
			
			//******* job2_1: make VW_PartitionerIDs for partition reducers in Search TVector, no outPut, save VW_PartitionerIDs to VW_PartitionerIDs_Path ******
			conf.set("VW_PartitionerIDs", out+"_VW_PartitionerIDs"+currentLoopLabel); 
			if (isRunloop_Q)
				FileSystem.get(conf).delete(new Path(conf.get("VW_PartitionerIDs")), true);
			//add Distributed cache
			cacheFilePaths.clear();
			Conf_ImageR.addDistriCache_TVectorInfo(conf, cacheFilePaths); //TVectorInfo with symLink
			General_Hadoop.Job(isRunloop_Q, conf, new Path[]{new Path(Out_job1_3)}, null, "Job2_1_makeVW_PartitionerIDs"+currentLoopLabel, 1, 8, 10, true,
					MapRed_buildRank.class, Mapper_countVW_FeatNum.class, null, Combiner_sumValues.class, null, Reducer_makeVW_PartitionerIDs.class,
					IntWritable.class, IntWritable.class, IntWritable.class,IntWritable.class,
					SequenceFileInputFormat.class, NullOutputFormat.class, 0, 0,
					cacheFilePaths.toArray(new String[0]),null);
			General.dispInfo(outStr_report, "\t\t job2_1: make VW_PartitionerIDs for partition reducers in Search TVector, no outPut, save VW_PartitionerIDs to "+conf.get("VW_PartitionerIDs")+" done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			//****************** loop-over iniR_schemes ***************
			General.dispInfo(outStr_report, "\t\t\t loop over different iniR_schemes:"+iniR_schemes.size());
			for (int iniR_i = 0; iniR_i < iniR_schemes.size(); iniR_i++) {//iniR_schemes
				renewTicket.renewTicket(true);
				String iniR_scheme= iniR_schemes.get(iniR_i);
				conf.set("iniR_scheme", iniR_scheme);
				boolean isIniRcontainsHEPara=!iniR_scheme.split("@")[0].equalsIgnoreCase("_iniR-ASMK");//iniR_scheme: _iniR-ASMK@0@3, _iniR-BurstIntraInter@18@12
				//commons
				String iniRSchemesloopLabel="_iniR"+iniR_i+"-"+(iniR_schemes.size()-1);	
				currentLoopLabel=queryloopLabel+iniRSchemesloopLabel;
				isRunloop_Q=(loopInd_Q_iniR_reRL_reRHE>startloop_Q_iniR_reRL_reRHE) || ((loopInd_Q_iniR_reRL_reRHE==startloop_Q_iniR_reRL_reRHE) && (1>=startloop_stage) );
				//******* job2_2: Search TVector, get query_doc hmScore for each vw ******
				String Out_job2_2=out+currentLoopLabel+"_DocScores";
				//set job2_2RedNum based on VW_PartitionerIDs
				if (isRunloop_Q) {
					int[] PaIDs=(int[]) General_Hadoop.readObject_HDFS(hdfs, new Path(conf.get("VW_PartitionerIDs")).toString());
					int job2_2RedNum=General_Hadoop.getTotReducerNum_from_vwPartitionIDs(PaIDs); //reducer number for seachTVector, PaIDs: values from 0!
					//add Distributed cache
					cacheFilePaths.clear();
					Conf_ImageR.addDistriCache_docInfo(conf, cacheFilePaths); //docInfo with symLink
					Conf_ImageR.addDistriCache_TVectorInfo(conf, cacheFilePaths); //TVectorInfo with symLink
					Conf_ImageR.addDistriCache_VWPaIDs(conf, cacheFilePaths); //PaIDs with symLink
					General_Hadoop.Job(isRunloop_Q,conf, new Path[]{new Path(Out_job1_3)}, Out_job2_2, "Job2_2_getDocScore"+currentLoopLabel, job2_2RedNum, 8, 10, true,
							MapRed_buildRank.class, null, Partitioner_forSearchTVector.class, Combiner_combine_IntArr_HESig_ShortArr_Arr.class, null, Reducer_SearchTVector_getHMScore.class,
							IntWritable.class, IntArr_HESig_ShortArr_Arr.class, IntWritable.class, VW_DID_Score_Arr.class,
							SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0, 0,
							cacheFilePaths.toArray(new String[0]),null);
				}
				General.dispInfo(outStr_report, "\t\t job2_2: Search TVector, get query_doc hmScore for each vw done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
				//******* job2_3: combine query_MatchScore from each vw, build initial top ranked docs for query ******
				String Out_job2_3=out+currentLoopLabel+"_allIniDocs"+maxIniRankLength+"_temp";//vw_QID-DID-Rank-ScoreInInt
				//add Distributed cache
				cacheFilePaths.clear();			
				Conf_ImageR.addDistriCache_docInfo(conf, cacheFilePaths); //docInfo with symLink
				Conf_ImageR.addDistriCache_VWPaIDs(conf, cacheFilePaths); //PaIDs with symLink for check duplicated VW
				General_Hadoop.Job(isRunloop_Q, conf, new Path[]{new Path(Out_job2_2)}, Out_job2_3, "job2_3_buildInitialRank"+currentLoopLabel, job2_3RedNum, 8, 10, true,
						MapRed_buildRank.class, null, Partitioner_random_sameKey.class, null, null, Reducer_buildIniRank.class,
						IntWritable.class, VW_DID_Score_Arr.class, IntWritable.class,IntArr_FloatArr.class,
						SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024L, 0,
						cacheFilePaths.toArray(new String[0]),null);
				General.dispInfo(outStr_report, "\t\t job2_3: combine query_MatchScore from each vw, build initial top ranked docs for query done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
				//******* job2_4: combine result from job2_3, save initial top ranked docs for query ******
				String Out_job2_4=out+currentLoopLabel+"_IniRank";
				int job2_4RedNum=Math.min(100, job2_3RedNum/2);
				conf.set("selTopRankLength", topRank_iniRank+"");//for Mapper_selectTopRankDocs
				if (isDiffTopDocsByVW) {
					General_Hadoop.Job(isRunloop_Q,conf, new Path[]{new Path(Out_job2_3)}, Out_job2_4, "job2_4_saveInitialRank"+currentLoopLabel, job2_4RedNum, 8, 10, true,
							MapRed_buildRank.class, Mapper_selectTopRankDocs_forIniRank.class, Partitioner_random_sameKey.class, null, null, Reducer_buildInitialRankAsFinal_HE.class,
							IntWritable.class, IntArr_FloatArr.class, IntWritable.class,IntList_FloatList.class,
							SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024L, 0,
							null,null);
				}else {
					General_Hadoop.Job(isRunloop_Q,conf, new Path[]{new Path(Out_job2_3)}, Out_job2_4, "job2_4_saveInitialRank"+currentLoopLabel, job2_4RedNum, 8, 10, true,
							MapRed_buildRank.class, Mapper_getTopRankDocs_forIniRank.class, null, null, null, null,
							IntWritable.class, IntList_FloatList.class, IntWritable.class,IntList_FloatList.class,
							SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024L, 0,
							null,null);
				}
				rankPaths_oriRank[iniR_i][qi]=new Path(Out_job2_4);
				General.dispInfo(outStr_report, "\t\t job2_4: save initial rank list for query done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
				//************** loop over different rerank length **************
				General.dispInfo(outStr_report, "\t\t start rerank! loop over different rerank length:"+reRTopRInfo.getReRankNum());
				for (int rer_i = 0; rer_i < reRTopRInfo.getReRankNum(); rer_i++) {
					conf.set("reRankLength",reRTopRInfo.getReRank(rer_i)+"");
					conf.set("selTopRankLength",reRTopRInfo.getReRank(rer_i)+"");//for Mapper_selectTopRankDocs or Mapper_getReRankDocs
					conf.set("topRank", reRTopRInfo.getTopRank(rer_i)+"");//for final build rank
					String rerankLoopLabel="_R"+rer_i+"-"+(reRTopRInfo.getReRankNum()-1);
					currentLoopLabel=queryloopLabel+iniRSchemesloopLabel+rerankLoopLabel;
					isRunloop_Q=(loopInd_Q_iniR_reRL_reRHE>startloop_Q_iniR_reRL_reRHE) || ((loopInd_Q_iniR_reRL_reRHE==startloop_Q_iniR_reRL_reRHE) && (2>=startloop_stage) );
					//******* job3_1: combine result from job2_3, 1 reducer process mutiple reduce(vws), make vw_matchedDocs ******
					String iniDocPath=out+currentLoopLabel+"_iniDocs";
					conf.set("vw_iniDocs", iniDocPath);//job3's result
					if (isRunloop_Q) {
						FileSystem.get(conf).delete(new Path(iniDocPath), true);
						if (isDiffTopDocsByVW) {
							conf.set("reducerInter", VWFileInter+"");
							Partitioner_equalAssign partitioner_equalAssign=new Partitioner_equalAssign(conf,false);
							int[] PaIDs=(int[]) General_Hadoop.readObject_HDFS(hdfs, new Path(conf.get("VW_PartitionerIDs")).toString());
							int job3_1RedNum=partitioner_equalAssign.getReducerNum(PaIDs.length); //each reducer process multiple VWs
							General_Hadoop.Job(conf, new Path[]{new Path(Out_job2_3)}, null, "job3_1_groupVWQIDDocIDs"+currentLoopLabel, job3_1RedNum, 8, 10, false,
									MapRed_buildRank.class, Mapper_selectTopRankDocs.class, partitioner_equalAssign.getPartitioner(), null, null, Reducer_groupVW_QID_DocIDs.class,
									IntWritable.class,IntArr.class,IntWritable.class,IntArr.class,
									SequenceFileInputFormat.class, NullOutputFormat.class, 1*1024*1024*1024L, 0,
									null,null);
						}else {
							General_Hadoop.Job(conf, new Path[]{new Path(Out_job2_3)}, iniDocPath, "job3_1_groupQIDDocIDs"+currentLoopLabel, 1, 8, 10, true,
									MapRed_buildRank.class, Mapper_getReRankDocs.class, null, null, null, Reducer_InOut_1key_1value.class,
									IntWritable.class, IntArr.class, IntWritable.class, IntArr.class,
									SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024L, 0,
									null,null);
						}
					}
					General.dispInfo(outStr_report, "\t\t\t job3_1: combine result from job2_3, 1 reducer, mutiple reduce(vws), make vw_matchedDocs done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
					//******* job3_2: combine result from job2_4, only 1 reducer, save the last part of the iniRank: [rerankLength ~ topRank) ******
					int lastPartLength_iniRank=reRTopRInfo.getTopRank(rer_i)-reRTopRInfo.getReRank(rer_i);
					String Out_job3_2=out+"LastPartOfIniRank";
					if (lastPartLength_iniRank>0) {//need put last part of iniRank into the tail of the reranked list
						General_Hadoop.Job(isRunloop_Q, conf, new Path[]{new Path(Out_job2_4)}, Out_job3_2, "job3_2_LastPartOfIniRank"+currentLoopLabel, 1, 8, 10, true,
								MapRed_buildRank.class, null, null, null, null, Reducer_selectTailFromIniRank.class,
								IntWritable.class,IntList_FloatList.class,IntWritable.class,IntList_FloatList.class,
								SequenceFileInputFormat.class, MapFileOutputFormat.class, 1*1024*1024*1024L, 0,
								null,null);
						General.dispInfo(outStr_report, "\t\t\t job3_1: combine result from job2_4, only 1 reducer, save the last part of the iniRank: (topRank-rerankLength)="+lastPartLength_iniRank+" done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
					}
					//************** loop over different reRankHEParas **************
					General.dispInfo(outStr_report, "\t\t\t loop over different reRankHEParas:"+reRankHEParas.size());
					for (int rerHEPara_i = 0; rerHEPara_i < reRankHEParas.size(); rerHEPara_i++) {
						boolean isRunThisLoop=true;
						if (isDiffTopDocsByVW && isIniRcontainsHEPara) {//when iniR has HEpara, then rerank should use the same HEpara
							String[] para_ini=iniR_scheme.split("@");
							String[] para_reR=reRankHEParas.get(rerHEPara_i).split("@");
							isRunThisLoop=(para_ini[1].equalsIgnoreCase(para_reR[1])) && (para_ini[2].equalsIgnoreCase(para_reR[2]));
						}
						isRunloop_Q=(loopInd_Q_iniR_reRL_reRHE>startloop_Q_iniR_reRL_reRHE) || ((loopInd_Q_iniR_reRL_reRHE==startloop_Q_iniR_reRL_reRHE) && (3>=startloop_stage) );
						if(isRunThisLoop){//guaratee HMW-delta used in iniR_scheme is the same with reRankHEPara
							conf.set("rerankHEPara", reRankHEParas.get(rerHEPara_i));
							String rerHDrLoopLabel="_H"+rerHEPara_i+"-"+(reRankHEParas.size()-1);
							currentLoopLabel=queryloopLabel+iniRSchemesloopLabel+rerankLoopLabel+rerHDrLoopLabel;
							//******* job4_1: Search TVector, get query_selectedDoc MatchFeat for each vw ******
							String Out_job4_1=out+currentLoopLabel+"_MatchFeat";
							int	job4RedNum=0;
							if (isRunloop_Q) {
								int[] PaIDs=(int[]) General_Hadoop.readObject_HDFS(hdfs, new Path(conf.get("VW_PartitionerIDs")).toString());
								job4RedNum=General_Hadoop.getTotReducerNum_from_vwPartitionIDs(PaIDs); //reducer number for seachTVector, PaIDs: values from 0!
							}
							//add Distributed cache
							cacheFilePaths.clear();
							Conf_ImageR.addDistriCache_TVectorInfo(conf, cacheFilePaths); //TVectorInfo with symLink
							Conf_ImageR.addDistriCache_VWPaIDs(conf, cacheFilePaths); //PaIDs with symLink
							General_Hadoop.Job(isRunloop_Q, conf, new Path[]{new Path(Out_job1_3)}, Out_job4_1, "Job4_1_getMatchFeat"+currentLoopLabel, job4RedNum, 8, 10, true,
									MapRed_buildRank.class, null, Partitioner_forSearchTVector.class, Combiner_combine_IntArr_HESig_ShortArr_Arr.class, null, Reducer_SearchTVector_getDocMatches.class,
									IntWritable.class,IntArr_HESig_ShortArr_Arr.class, Key_QID_DID.class, Int_MatchFeatArr.class,
									SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0, 0,
									cacheFilePaths.toArray(new String[0]),null);
							//******* job4_2: organise Out_job4 into QID, DID_[vw_matchFeats]******
							String Out_job4_2=Out_job4_1+"_organised";
							General_Hadoop.Job(isRunloop_Q, conf, new Path[]{new Path(Out_job4_1)}, Out_job4_2, "job4_2_organise_job4_1"+currentLoopLabel, job5RedNum, 8, 10, true,
									MapRed_buildRank.class, null, Partitioner_random_sameKey_PartKey.class, null, null, Reducer_group_QDMatches.class,
									Key_QID_DID.class, Int_MatchFeatArr.class, IntWritable.class, DocAllMatchFeats.class,
									SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
									null,null);
							General.dispInfo(outStr_report, "\t\t\t\t job4: Search TVector, get query_selectedDoc MatchFeat for each vw done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
							//******* job5: combine query_MatchFeat from each vw, build final rank for query ******
							General.dispInfo(outStr_report, "\t\t\t\t loop over different rerank strategies:"+rerankFlags.size());
							if (isParaQueries) {//paralise queries, each rerankFlags lunch a job
								for (int j = 0; j < rerankFlags.size(); j++) {//run for different rerank strategy
									isRunloop_Q=(loopInd_Q_iniR_reRL_reRHE>startloop_Q_iniR_reRL_reRHE) || ((loopInd_Q_iniR_reRL_reRHE==startloop_Q_iniR_reRL_reRHE) && ((4+j)>=startloop_stage) );
									currentLoopLabel=queryloopLabel+iniRSchemesloopLabel+rerankLoopLabel+rerHDrLoopLabel+"_F"+j+"-"+(rerankFlags.size()-1);
									String Out_job5=out+currentLoopLabel+saveRankFormat;
									conf.set("rerankFlag", rerankFlags.get(j));
									conf.set("rerankFlagInd", j+"");
									//add Distributed cache
									cacheFilePaths.clear();
									cacheFilePaths.add(Out_job1_2+"/part-r-00000#queryFeat.mapFile"); //queryFeat_MapFile data path with symLink
									Conf_ImageR.addDistriCache_docInfo(conf, cacheFilePaths); //docInfo with symLink
									Conf_ImageR.addDistriCache_TVectorInfo(conf, cacheFilePaths); //TVectorInfo with symLink
									Conf_ImageR.addDistriCache_QuerySize(conf, cacheFilePaths); //QuerySize_HashMap with symLink
									if (lastPartLength_iniRank>0) {
										cacheFilePaths.add(Out_job3_2+"/part-r-00000#TailFromIniRank.mapFile"); //TailFromIniRank with symLink, this is a mapFile folder
									}
									General_Hadoop.Job(isRunloop_Q, conf, new Path[]{new Path(Out_job4_2)}, Out_job5, "buildRank"+currentLoopLabel, job5RedNum, 8, 10, true,
											MapRed_buildRank.class, null, Partitioner_random_sameKey.class,null, null,buildRankReducer,
											IntWritable.class, DocAllMatchFeats.class, Key_RankFlagID_QID.class,buildRankClass,
											SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
											cacheFilePaths.toArray(new String[0]),null);	
									rankPaths_rerank[iniR_i][rer_i][rerHEPara_i][j][qi]=new Path(Out_job5);//j:rankFlagInd, i:queryInd
									General.dispInfo(outStr_report, "\t\t\t\t\t job5: combine query_MatchFeat from each vw, build final rank for query done! paralise queries, current finished rerankFlag: F"+j+"-"+(rerankFlags.size()-1)+" ... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
								}
							}else {//paralise rerankFlags, only lunch one job, (one rerankFlag one reducer)
								/*
								 * group Out_job4 into one seqFile, and put it into distributed cache,
								 * attention: for 500 query, rerank top-1000, HMDistThr_rankDoc=18, the size of Out_job4 is 700mb!
								 * when the size is problem, try to divid query into small groups!
								 */
								isRunloop_Q=(loopInd_Q_iniR_reRL_reRHE>startloop_Q_iniR_reRL_reRHE) || ((loopInd_Q_iniR_reRL_reRHE==startloop_Q_iniR_reRL_reRHE) && (4>=startloop_stage) );
								//******* job5_1: group Out_job4 into one seqFile ******
								String Out_job5_1=Out_job4_2+".seq";		
								General_Hadoop.Job(isRunloop_Q, conf, new Path[]{new Path(Out_job4_2)}, Out_job5_1, "job5_1_groupOut_job4_2"+currentLoopLabel, 1, 8, 10, true,
										MapRed_buildRank.class, null, null, null, null, Reducer_InOut_normal.class,
										IntWritable.class, DocAllMatchFeats.class, IntWritable.class, DocAllMatchFeats.class,
										SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
										null,null);
								General.dispInfo(outStr_report, "\t\t job5_1: group Out_job4 into one seqFile done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
								//******* job5_2: paralise rerankFlags, do final ranking ******
								String Out_job5_2=out+currentLoopLabel+saveRankFormat;
								Out_job5_2_all_toDelete[iniR_i][rer_i][rerHEPara_i]=Out_job5_2;
								conf.set("reducerInter", job5_2_reducerInter+"");
								Partitioner_equalAssign partitioner_ParaFlag=new Partitioner_equalAssign(conf,false);
								//add Distributed cache
								cacheFilePaths.clear();
								cacheFilePaths.add(Out_job1_2+"/part-r-00000#queryFeat.mapFile"); //queryFeat_MapFile path with symLink
								Conf_ImageR.addDistriCache_docInfo(conf, cacheFilePaths); //docInfo with symLink
								Conf_ImageR.addDistriCache_TVectorInfo(conf, cacheFilePaths); //TVectorInfo with symLink
								Conf_ImageR.addDistriCache_QuerySize(conf, cacheFilePaths); //QuerySize_HashMap with symLink
								cacheFilePaths.add(Out_job5_1+"/part-r-00000"+"#AllDocMatchs.file"); //Out_job5_1 with symLink
								General_Hadoop.Job(isRunloop_Q, conf, new Path[]{new Path(In_reRankFlags_job5_2)}, Out_job5_2, "buildRank"+currentLoopLabel+"_F-"+rerankFlags.size(), job5_2_RedNum, 8, 10, true,
										MapRed_buildRank.class, null, partitioner_ParaFlag.getPartitioner(), null, null, Reducer_buildRank_final_ParaliseFlag_saveScore.class,
										IntWritable.class, Text.class, Key_RankFlagID_QID.class, QID_IntList_FloatList.class,
										SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
										cacheFilePaths.toArray(new String[0]),null);	
								//add ranks paths
								for (int j = 0; j < job5_2_RedNum; j++) {
									rankPaths_rerank[iniR_i][rer_i][rerHEPara_i][j][qi]=new Path(Out_job5_2+"/part-r-"+General.StrleftPad(j+"", 0, 5, "0"));//j:rankFlagInd, qi:queryInd
								}
								General.dispInfo(outStr_report, "\t\t\t\t\t job5: combine query_MatchFeat from each vw, build final rank for query done! paralise rerankFlags: "+rerankFlags.size()+" ... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
								//clean-up
								hdfs.delete(new Path(Out_job5_1), true);
							}
							//clean-up reRankHEParas
							hdfs.delete(new Path(Out_job4_1), true);
							hdfs.delete(new Path(Out_job4_2), true);
						}
						loopInd_Q_iniR_reRL_reRHE++;
					}
					//clean-up reRTopRInfo
					hdfs.delete(new Path(conf.get("vw_iniDocs")), true);//job 3
					hdfs.delete(new Path(Out_job3_2), true);//job 3
				}
				//clean-up iniR_schemes
				hdfs.delete(new Path(Out_job2_2), true);//docScores
				hdfs.delete(new Path(Out_job2_3), true);
			}
			//clean-up selQuerys
			hdfs.delete(new Path(Out_job1_1), true);
			hdfs.delete(new Path(Out_job1_2), true);
			hdfs.delete(new Path(Out_job1_3), true);
			hdfs.delete(new Path(conf.get("QuerySize_HashMap")), true);
			hdfs.delete(new Path(conf.get("VW_PartitionerIDs")), true);	
		}
		General.dispInfo(outStr_report, "All querys are done! tot_queryNum_feated: "+tot_queryNum_feated+", "+General.dispTimeDate(System.currentTimeMillis(), dateFormate));
		conf.set("ev_queryNum", tot_queryNum_feated+"");
		//clean
		General_Hadoop.deleteIfExist(In_reRankFlags_job5_2, hdfs);//clean reRankFlags seqFile
		//******* job6: save all querys' rank into one MapFile ******
		String rankFlagsData=out+"_rankFlags.arrList";
		if (makeReport) {
			General_Hadoop.writeObject_HDFS(hdfs, rankFlagsData, rerankFlags);
		}
		String[] rankLabel_common=new String[4];
		for (int iniR_i = 0; iniR_i < iniR_schemes.size(); iniR_i++) {//iniR_schemes
			renewTicket.renewTicket(true);
			String iniR_scheme= iniR_schemes.get(iniR_i);
			rankLabel_common[0]=iniR_scheme;
			//1. for oriRank
			if (includeNoRerank) {
				rankLabel_common[3]=reRTopRInfo.getTopRankLabel_IniRank();
				if (saveRes) {
					String finalRankPath=out+rankLabel_common[0]+rankLabel_common[3]+saveRankFormat;
					General_Hadoop.Job(conf, rankPaths_oriRank[iniR_i], finalRankPath, "combine&save_oriRank", 1, 8, 10, true,
							MapRed_buildRank.class, null, null, null, null, Reducer_InOut_1key_1value.class,
							IntWritable.class, IntList_FloatList.class, IntWritable.class, IntList_FloatList.class,
							SequenceFileInputFormat.class, MapFileOutputFormat.class, 0, 10,
							null,null);	
				}
				if (makeReport) {
					// ********* make report ************
					String finalRankLabel=rankLabel_common[0]+rankLabel_common[3];
					conf.set("rankLabel", finalRankLabel+"_OriRank");
					//set info
					String InfoStrPath=out+".ResAnaInfoStr";
					conf.set("InfoStrPath",InfoStrPath); //Job3 save MAPInfo as String object to InfoStrPath
					//add Distributed cache
					cacheFilePaths.clear();
					Conf_ImageR.addDistriCache_evaluation(conf, cacheFilePaths);
					//run
					General_Hadoop.Job(conf, rankPaths_oriRank[iniR_i], null, "combineReport_OriRank", 1, 8, 10, false,
							MapRed_buildRank.class, null, null, null, null, Reducer_makeReport_forOriRank.class,
							IntWritable.class, IntList_FloatList.class, IntWritable.class,Text.class,
							SequenceFileInputFormat.class, NullOutputFormat.class, 0, 10,
							cacheFilePaths.toArray(new String[0]),null);
					//print Info
					String Info=(String) General_Hadoop.readObject_HDFS(hdfs, InfoStrPath);
					General.dispInfo(outStr_report,Info);
					outStr_report.flush();	
					//********* clean-up ***********//
					hdfs.delete(new Path(InfoStrPath), true);
				}
			}
			//2. for reranks
			for (int rer_i = 0; rer_i < reRTopRInfo.getReRankNum(); rer_i++) {
				rankLabel_common[1]=reRTopRInfo.getReRankLabel(rer_i);
				rankLabel_common[3]=reRTopRInfo.getTopRankLabel(rer_i);
				for (int rerHEPara_i = 0; rerHEPara_i < reRankHEParas.size(); rerHEPara_i++) {
					rankLabel_common[2]=reRankHEParas.get(rerHEPara_i);
					String loopLabel="_R"+rer_i+"-"+(reRTopRInfo.getReRankNum()-1)+"_H"+rerHEPara_i+"-"+(reRankHEParas.size()-1);
					Path[] rankPaths_allFlags=General.arrArrToArrList(rankPaths_rerank[iniR_i][rer_i][rerHEPara_i], "rowFirst").toArray(new Path[0]);
					if (rankPaths_allFlags[0]!=null) {//to guaratee HMW-delta used in iniR_scheme is the same with reRankHEPara, some element in rerHEPara_i is not run, so their rankPaths_rerank[iniR_i][rer_i][rerHEPara_i] is empty (all entries in rankPaths_rerank[iniR_i][rer_i][rerHEPara_i] is null, but rankPaths_rerank[iniR_i][rer_i][rerHEPara_i]!=null)
						String currentBasePath=out+rankLabel_common[0]+rankLabel_common[1]+rankLabel_common[2]+rankLabel_common[3];
						if (saveRes) {//parilise saving for all rankFlags
							String combinedRanks=currentBasePath+saveRankFormat; //this is a dirtory, each part-r-000j is for one RankFlag j
							General_Hadoop.Job(conf, rankPaths_allFlags, combinedRanks, "combine&save_"+loopLabel, rerankFlags.size(), 8, 10, true,
										MapRed_buildRank.class, null, Partitioner_KeyisPartID_PartKey.class, null, null, Reducer_InOut_SaveRank.class,
										Key_RankFlagID_QID.class, buildRankClass, IntWritable.class, saveRankClass,
										SequenceFileInputFormat.class, MapFileOutputFormat.class, 0, 10,
										null,null);	
							for (int j = 0; j < rerankFlags.size(); j++) {//run for different rerank strategy, each rankFlag lunch one job and save into mapFile
								String finalRankPath=currentBasePath+rerankFlags.get(j)+saveRankFormat;
								General.runSysCommand(Arrays.asList("hadoop", "fs", "-mv",
										combinedRanks+"/part-r-"+General.StrleftPad(j+"", 0, 5, "0"), finalRankPath), null, false);
							}
							General_Hadoop.deleteIfExist(combinedRanks, hdfs);
						}
						if (makeReport) {//parilise evaluation for all rankFlags
							// ********* make report ************
							String finalRankLabel=rankLabel_common[0]+rankLabel_common[1]+rankLabel_common[2]+rankLabel_common[3];
							conf.set("rankLabel", finalRankLabel);
							conf.set("reducerInter", job5_2_reducerInter+"");
							Partitioner_equalAssign partitioner_ParaFlag=new Partitioner_equalAssign(conf,true);
							//add Distributed cache
							cacheFilePaths.clear();
							cacheFilePaths.add(rankFlagsData+"#rankFlagsData.file"); //rankFlagsData path with symLink
							Conf_ImageR.addDistriCache_evaluation(conf, cacheFilePaths);
							//run, parilise making report for all rankFlags
							String reportPath=currentBasePath+"_reports";
							System.out.println(reportPath);
							General_Hadoop.Job(conf, rankPaths_allFlags, reportPath, "analysis_"+loopLabel, job5_2_RedNum, 8, 10, true,
									MapRed_buildRank.class, null, partitioner_ParaFlag.getPartitioner(), null, Comparator_groupKey_Key_RankFlagID_QID.class, Reducer_makeReport_forRerank.class,
									Key_RankFlagID_QID.class, QID_IntList_FloatList.class, IntWritable.class,Text.class,
									SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0, 10,
									cacheFilePaths.toArray(new String[0]),null);
							// ********* combine report ************
							String InfoStrPath=out+".ResAnaInfoStr";
							String Info=Reducer_combineReport.Job_combineReports(conf, hdfs, reportPath, InfoStrPath, MapRed_buildRank.class, "combineReport_"+loopLabel, true);
							//print Info
							General.dispInfo(outStr_report,Info);
							outStr_report.flush();	
							//********* clean-up ***********//
							hdfs.delete(new Path(reportPath), true);
						}
					}
				}
			}
		}
		General.dispInfo(outStr_report, "\n combine all query results are done! "+General.dispTimeDate(System.currentTimeMillis(), dateFormate));
		General_Hadoop.deleteIfExist(rankFlagsData, hdfs);//clean reRankFlags ArrayList
		//clean-up rankPaths
		for (Path[] paths : rankPaths_oriRank) {
			for (Path path : paths) {
				hdfs.delete(path, true);
			}
		}
		if (isParaQueries) {//clean Out_job5_2
			for (Path[][][][] pathssss : rankPaths_rerank) {//rankPaths_rerank[iniR_i][rer_i][rerHEPara_i][j][qi]
				for (Path[][][] pathsss : pathssss) {
					for (Path[][] pathss: pathsss) {
						for (Path[] paths: pathss) {
							for (Path path: paths) {
								if (path!=null) {//to guaratee HMW-delta used in iniR_scheme is the same with reRankHEPara, some path are null
									hdfs.delete(path, true);
								}
							}
						}
					}
				}
			}
		}else {
			for (String[][] pathss: Out_job5_2_all_toDelete) {//Out_job5_2_all_toDelete[iniR_i][rer_i][rerHEPara_i]
				for (String[] paths: pathss) {
					for (String path: paths) {
						if (path!=null) {//to guaratee HMW-delta used in iniR_scheme is the same with reRankHEPara, some path are null
							hdfs.delete(new Path(path), true);
						}
					}
				}
			}
		}
		for (String path: QNumPaths) {
			hdfs.delete(new Path(path), true);
		}
		if (outStr_report!=null) {
			outStr_report.close();
		}
		return 0;
	}
	
	//******** job1_2 **************	
	public static class Mapper_GetQueryPoints extends Mapper<IntWritable, PhotoAllFeats, IntWritable, SURFpoint_ShortArr>{

		private int procSamples;
		private int progInter;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			procSamples=0;
			progInter=1000;
			// ***** setup finished ***//
			System.out.println("Mapper_GetQueryPoints setup finsihed!");
			
	 	}
		
		@Override
		protected void map(IntWritable key, PhotoAllFeats value, Context context) throws IOException, InterruptedException {
			//key: queryID, value: Feats

			ArrayList<SURFpoint> points=value.group_InterestPoints();
			context.write(key, new SURFpoint_ShortArr(points));
			
			procSamples++;
			if(procSamples%progInter==0){ //debug disp info
				System.out.println(procSamples+" queryID_feats samples finished: ");
				System.out.println("--current finished query num:"+procSamples+", queryID:"+key+", queryFeatNum:"+points.size());
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one mapper finished! total Samples in this Mapper: "+procSamples);
			
	 	}
	}
		
	//******** job1_3 **************	
	public static class Mapper_GetQueryVWHESig extends Mapper<IntWritable, PhotoAllFeats, IntWritable, IntArr_HESig_ShortArr_Arr>{

		private int procSamples;
		private int progInter;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			procSamples=0;
			progInter=1000;
			// ***** setup finished ***//
			System.out.println("Mapper_GetQueryPoints setup finsihed!");
			
	 	}
		
		@Override
		protected void map(IntWritable key, PhotoAllFeats value, Context context) throws IOException, InterruptedException {
			//key: queryID, value: Feats
			int queryID=key.get();
			HashMap<Integer, HESig_ShortArr_AggSig> VW_Sigs=value.group_VW_HESigAggSig();
			for (Entry<Integer, HESig_ShortArr_AggSig> oneVW : VW_Sigs.entrySet()) {
				context.write(new IntWritable(oneVW.getKey()), new IntArr_HESig_ShortArr_Arr(new int[]{queryID}, new HESig_ShortArr_Arr(new HESig_ShortArr_AggSig[]{oneVW.getValue()})) );
			}
			
			procSamples++;
			if(procSamples%progInter==0){ //debug disp info
				System.out.println(procSamples+" queryID_feats samples finished: ");
				System.out.println("--current finished queryID:"+key+", "+value.toString());
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one mapper finished! total Samples in this Mapper: "+procSamples);
			
	 	}
	}
	
	//******** job1_4 **************
	public static class Mapper_GetQuerySize extends Mapper<IntWritable, PhotoAllFeats, IntWritable, PhotoSize>{

		private int procSamples;
		private int progInter;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			procSamples=0;
			progInter=1000;
			// ***** setup finished ***//
			System.out.println("Mapper_GetQueryPoints setup finsihed!");
			
	 	}
		
		@Override
		protected void map(IntWritable key, PhotoAllFeats value, Context context) throws IOException, InterruptedException {
			//key: queryID, value: Feats
			context.write(key, value.getPhotoSize());
			
			procSamples++;
			if(procSamples%progInter==0){ //debug disp info
				System.out.println(procSamples+" queryID_feats samples finished: ");
				System.out.println("--current finished queryID:"+key+", photoSize:"+value.getPhotoSize());
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one mapper finished! total Samples in this Mapper: "+procSamples);
			
	 	}
	}
	
	public static class Reducer_ExtractQuerySize extends Reducer<IntWritable,PhotoSize,IntWritable,PhotoSize>{
		//Reducer_ExtractQuerySize: extract query image size
		private HashMap<Integer, int[]> querySizes;
		private String QuerySize_HashMap_Path;
		private int totImagePhotos;
		private int dispInter;
		private long startTime;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			Conf_ImageR conf_ImageR=new Conf_ImageR(conf);
			querySizes=new HashMap<Integer, int[]>();
			//** set QuerySize_HashMap_Path **//
			QuerySize_HashMap_Path=(Conf_ImageR.hdfs_address+conf_ImageR.ef_QuerySize_HashMap_Path);
			System.out.println("QuerySize_HashMap path setted: "+QuerySize_HashMap_Path);
			//set procPhotos
			totImagePhotos=0;
			//set dispInter
			dispInter=10000;
			startTime=System.currentTimeMillis(); //startTime
			System.out.println("reducer setup finsihed!");
	 	}
		
		protected void reduce(IntWritable key, Iterable<PhotoSize> value, Context context) throws IOException, InterruptedException {
			//key: photoName
			//value: file content
			int photoName=key.get();// photoName
			//******** only one in value! ************	
			PhotoSize photoSize=General_Hadoop.readOnlyOneElement(value, photoName+"");
			int[] size_w_h=new int[]{photoSize.w, photoSize.h};
    		int[] previous=querySizes.put(photoName, size_w_h);
			General.Assert(previous==null, "err, duplicate for pho-"+photoName+" in hashMap: querySizes!");
			
			totImagePhotos++; 
			//disp
			General.dispInfo_ifNeed(totImagePhotos%dispInter==0, "", "extractSURF photo size, "+totImagePhotos+" photos finished!! current photo:"+photoName
					+", its width_height:"+General.IntArrToString(size_w_h, "_")+"......"+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			Configuration conf = context.getConfiguration();
			FileSystem HDFS=FileSystem.get(conf);
			General_Hadoop.writeObject_HDFS(HDFS, QuerySize_HashMap_Path, querySizes);
			System.out.println("\n one reducer finished! in this reducer, photos: "+totImagePhotos+" ....."+ General.dispTime(System.currentTimeMillis()-startTime, "min"));
			
	 	}
	}

	//******** job2_1 **************	
	public static class Mapper_countVW_FeatNum extends Mapper<IntWritable, IntArr_HESig_ShortArr_Arr, IntWritable, IntWritable>{

		private int[] vw_featNum;
		private int procSamples;
		private int progInter;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			//check file in distributted cache
			General.checkDir(new Disp(true, "", null),".");
			//***** TVectorInfo ***//
			int[][] TVectorInfo=(int[][]) General.readObject(Conf_ImageR.sd_TVectorInfo); //photoNum,featNum
			System.out.println("read int[][] TVectorInfo finished, total vw number:"+TVectorInfo.length);
			//** set vw_featNum **//
			vw_featNum=new int[TVectorInfo.length];
			
			procSamples=0;
			progInter=1000;
			// ***** setup finished ***//
			System.out.println("Mapper_transfer setup finsihed!");
			
	 	}
		
		@Override
		protected void map(IntWritable key, IntArr_HESig_ShortArr_Arr value, Context context) throws IOException, InterruptedException {
			//key: VW, value: queryIDs, Feats
			int vw =key.get();
			int queryFeatNum=0;
			
			for (HESig_ShortArr_AggSig oneQueryFeats : value.obj_2.getArr()) {
				queryFeatNum+=oneQueryFeats.HESigs.length;
			}
			vw_featNum[vw]+=queryFeatNum;
			
			procSamples++;
			if(procSamples%progInter==0){ //debug disp info
				System.out.println(procSamples+" vw_queryIDs_feats-samples finished: ");
				System.out.println("--current finished vw: "+vw+", query num:"+value.obj_1.getIntArr().length+", queryFeatNum:"+queryFeatNum
						+", current totalFeatNum for this vw:"+vw_featNum[vw]);
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			int zeroFeatVWnum=0;
			for (int i = 0; i < vw_featNum.length; i++) {
				if (vw_featNum[i]!=0) {
					context.write(new IntWritable(i), new IntWritable(vw_featNum[i]));
				}else {
					zeroFeatVWnum++;
				}
			}
			
			// ***** setup finsihed ***//
			System.out.println("one mapper finished! total Samples in this Mapper: "+procSamples+", in this mapper, in total "+vw_featNum.length+" vws, zeroFeatVWnum:"+zeroFeatVWnum);
			
	 	}
	}
	
	public static class Reducer_makeVW_PartitionerIDs extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>  {
		private int[][] TVectorInfo;
		private int[] vw_featNum;
		private String VW_PartitionerIDs_Path;
		private int procSamples;
		private int progInter;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			//check file in distributted cache
			General.checkDir(new Disp(true, "", null),".");
			Conf_ImageR conf_ImageR=new Conf_ImageR(context.getConfiguration());
			//***** TVectorInfo ***//
			TVectorInfo=(int[][]) General.readObject(Conf_ImageR.sd_TVectorInfo); //photoNum,featNum
			System.out.println("read int[][] TVectorInfo finished, total vw number:"+TVectorInfo.length);
			//** set vw_featNum **//
			vw_featNum=new int[TVectorInfo.length];
			//** set VW_PartitionerIDs_Path **//
			VW_PartitionerIDs_Path=conf_ImageR.sd_VWPaIDs_HDFSPath;
			System.out.println("VW_PartitionerIDs_Path setted: "+VW_PartitionerIDs_Path);
			// ***** setup finsihed ***//
			procSamples=0;
			progInter=1000;
			System.out.println("setup finsihed!");
			
	 	}
		
		@Override
		public void reduce(IntWritable VW, Iterable<IntWritable> queryFeatNums, Context context) throws IOException, InterruptedException {
			//QueryNameSigs: QueryName-Integer, Sigs:-ByteArrList

			int vw=VW.get();
			
			for (IntWritable one_queryFeatNum : queryFeatNums) {
				vw_featNum[vw]+=one_queryFeatNum.get();
			}
			
			procSamples++;
			if(procSamples%progInter==0){ //debug disp info
				System.out.println(procSamples+" vw_queryFeatNum-samples finished: ");
				System.out.println("--current finished vw: "+vw+", queryFeatNum:"+vw_featNum[vw]);
			}
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** make  PaIDs ***//
			int[] PaIDs=General_Hadoop.make_vwPartitionIDs_HESig(TVectorInfo, vw_featNum,1000);
			System.out.println("\n Reducer for make vwPartitionIDs finished, save it as int[] to VW_PartitionerIDs_Path! total partioned reducer number : "+General_Hadoop.getTotReducerNum_from_vwPartitionIDs(PaIDs)+", job.setNumReduceTasks(jobRedNum) should == this value!!");
			int maxReducerNum=0; int maxReducerNum_vw=0;
			for (int i = 0; i < PaIDs.length; i++) {
				int reducerNum=General_Hadoop.getVWReducerNum_from_vwPartitionIDs(PaIDs, i);
				if(maxReducerNum<reducerNum){
					maxReducerNum=reducerNum;
					maxReducerNum_vw=i;
				}
			}
			System.out.println("vw:"+maxReducerNum_vw+" has max reducerNum:"+maxReducerNum+", its TVector-featNum:"+TVectorInfo[maxReducerNum_vw][1]+", querys-featNum:"+vw_featNum[maxReducerNum_vw]);
			// ***** write-out ***//
			Configuration conf = context.getConfiguration();
			FileSystem HDFS=FileSystem.get(conf);
			General_Hadoop.writeObject_HDFS(HDFS, VW_PartitionerIDs_Path, PaIDs);
			
	 	}
	}

	//******** job2_2 **************	
	public static class Reducer_SearchTVector_getHMScore extends Reducer<IntWritable,IntArr_HESig_ShortArr_Arr,IntWritable,VW_DID_Score_Arr>  {

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
			TVectorPath = conf_ImageR.sd_TVector_HDFSPath;
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
			dispInter_reduce=2;
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
			int queryNum_thisVW=0; int queryFeatNum_thisVW=0; HESig_ShortArr_AggSig queryFeats; int queryNum_existMatch=0; Statistics<Integer> stat=new Statistics<>(3);
			
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
	        		int queryNum_thisInterator=Querys.obj_1.getIntArr().length;
	        		for(int query_i=0;query_i<queryNum_thisInterator;query_i++){
		        		//********* process one query *********
						int queryName=Querys.obj_1.getIntArr()[query_i]; queryFeats=Querys.obj_2.getArr()[query_i];
						General.Assert(checkDupliQuerys.add(queryName), "err! duplicate querys for VW:"+vw+", duplicate query:"+queryName);
						//compare docs in TVector for this query
						ArrayList<DID_Score> docs_scores=scoreDoc.scoreDocs_inOneTVector(queryFeats.HESigs, queryFeats.aggSig, vw, tVector.tVector);
						//outputfile: SequcenceFile; outputFormat: key->queryID  value->IntList_FloatList
						if (docs_scores.size()>0) {
							context.write(new IntWritable(queryName), new VW_DID_Score_Arr(vw, new DID_Score_Arr(docs_scores))); 
							queryNum_existMatch++;
							stat.addSample(docs_scores.size(), queryFeats.HESigs.length);
						}
						//************ report progress ******************
	        			queryNum_thisVW++; queryFeatNum_thisVW+=queryFeats.HESigs.length;
						if(disp==true && queryNum_thisVW%progInter==0){
							long time=System.currentTimeMillis()-startTime;
							System.out.println("\t --curent total finished querys:"+queryNum_thisVW+", queryFeatNum_thisVW:"+queryFeatNum_thisVW+", queryNum_existMatch:"+queryNum_existMatch
									+", time:"+General.dispTime(time, "min")+", average compare time per sig pair:"+(double)time/TVectorFeatNum/queryFeatNum_thisVW);
							System.out.println("\t --current queryName: "+queryName+", its matched docs for this vw: "+docs_scores.size()+", 10 examples: "+docs_scores.subList(0, Math.min(docs_scores.size(), 10)));
							System.out.println("\t ----current memory info: "+General.memoryInfo());
						}
	        		}
		        }
			}
			
        	
        	//*** some statistic ********
        	General.dispInfo_ifNeed(disp, "", "one reduce finished! total query number for this vw: "+queryNum_thisVW+", queryFeatNum_thisVW:"+queryFeatNum_thisVW+", queryNum_existMatch:"+queryNum_existMatch
        			+", stat QMachDocNum_QFeatNum:"+stat.getFullStatistics("0", false));
        	disp=false;
        	
        	vws_queryNums.append(vw+"_"+queryNum_thisVW+"_"+queryFeatNum_thisVW+"_"+queryNum_existMatch+", "+stat.getFullStatistics("0", false)+"\n");
        	
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("\n Reducer finished! reduceNum(vws num): "+reduceNum+"\n vws_queryNums_queryFeatNums_queryNumExistMatch, stat QMachDocNum_QFeatNum: \n"+vws_queryNums.toString());
			
	 	}
	}

	//******** job2_3 **************	
	public static class Mapper_selectTopRankDocs_forIniRank extends Mapper<IntWritable,IntArr_FloatArr,IntWritable,IntArr_FloatArr>{
		
		private int selTopRankLength;
		private int procSamples;
		private int procSelSamples;
		private int dispInter;
		private long startTime, endTime;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//***** set reRankLength for 1vs1&HPM rerank ***//
			selTopRankLength=Integer.valueOf(conf.get("selTopRankLength")); //select top rank to do 1vs1 and HPM check
			System.out.println("select top-"+selTopRankLength+" in the initial rankList to save as ini rank");
			//set procSamples
			procSamples=0;
			procSelSamples=0;
			//set dispInter
			dispInter=5000;
			startTime=System.currentTimeMillis(); //startTime
			
			System.out.println("mapper setup finsihed!");
			
	 	}
		
		@Override
		protected void map(IntWritable key, IntArr_FloatArr value, Context context) throws IOException, InterruptedException {
			//key: vw
			//value: QID_DID_Rank_ScoreInInt
			procSamples++;
			if(value.getIntArr()[2]<selTopRankLength){//doc's rank is within the rerank scale
				procSelSamples++;
				context.write(new IntWritable(value.getIntArr()[0]), new IntArr_FloatArr(General.selectArrInt(value.getIntArr(), new int[]{1,2}, 0), value.getFloatArr()));//QID_DID-Rank-Score
			}
			//disp
			if((procSamples)%dispInter==0){ 							
				endTime=System.currentTimeMillis(); //end time 
				System.out.println( "select Samples, "+procSamples+" Samples finished!! selected: "+procSelSamples+" ......"+ General.dispTime (endTime-startTime, "min"));
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
		    endTime=System.currentTimeMillis(); //end time 
			System.out.println("one mapper finished! total Samples in this Mapper: "+procSamples+", selected: "+procSelSamples+" ....."+ General.dispTime ( endTime-startTime, "min"));
			
	 	}
	}

	public static class Reducer_buildInitialRankAsFinal_HE extends Reducer<IntWritable,IntArr_FloatArr,IntWritable,IntList_FloatList>  {
		Configuration conf;
		
		private int topRank;
				
		private int processedQueryNum;
		private long startTime;
		private int dispInter;
		private boolean disp;
	
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();	
			Conf_ImageR conf_ImageR=new Conf_ImageR(conf);
			//***** select top rank for output ***//
			topRank=conf_ImageR.mr_topRank; //select top rank as output
			System.out.println("select top-"+topRank+" in rankList as output");
			
			// ***** setup finsihed ***//
			processedQueryNum=0;
			dispInter=5;
			disp=true;
			startTime=System.currentTimeMillis();
			System.out.println("setup finsihed!");
			
	 	}
			
		@Override
		public void reduce(IntWritable QID, Iterable<IntArr_FloatArr> doc_rank_score_I, Context context) throws IOException, InterruptedException {
			/**
			 * 1 reduce: for 1 query, merge mutiple vw-list into one list, each list should be ordered in ASC by docID! 
			 */
			
			//key: query, value: vw and this vw-mathed docs and scores for this query

			int thisQueryID=QID.get();
			
			//disp progress
			processedQueryNum++;
			if (processedQueryNum%dispInter==0){ 
				disp=true;
				System.out.println("\n this reduce for "+processedQueryNum+" -th queries");
			}
			//********combine all doc_rank_score for one query************
			int[] topDocs=new int[topRank]; float[] topScores=new float[topRank]; boolean[] isExist=new boolean[topRank];
			int docNum=0;//for some query, docNum<topRank
			for(Iterator<IntArr_FloatArr> it=doc_rank_score_I.iterator();it.hasNext();){
				IntArr_FloatArr one=it.next(); //can not: it.next().getInteger(), it.next().getByteArr()
				int[] docID_rank=one.getIntArr();
				int rank=docID_rank[1];
				topDocs[rank]=docID_rank[0];//DID
				topScores[rank]=one.getFloatArr()[0];//Score
				isExist[rank]=true;
				docNum++;
			}
			//**************** find top ranked docs, and output them  ****************
			if (docNum>0) {
				ArrayList<Integer> docs=new ArrayList<Integer>(docNum); ArrayList<Float> scores=new ArrayList<Float>(docNum); 
				for (int i = 0; i < isExist.length; i++) {
					if (isExist[i]) {
						docs.add(topDocs[i]);
						scores.add(topScores[i]);
					}
				}
				//out put top ranked docs
				context.write(QID, new IntList_FloatList(docs, scores));
				//disp
				if (disp){//show example for one Query
					int topToShow=Math.min(docNum,10);
					System.out.println(processedQueryNum+" querys finished! time:"+General.dispTime(System.currentTimeMillis()-startTime, "min")
							+", current finished queryName: "+thisQueryID
							+", total vw_doc num for this query:"+docNum+", saved findal top rank:"+topRank
							+", top ranked Docs:"+docs.subList(0, topToShow)+", Scores: "+scores.subList(0, topToShow));
			    	disp=false;
				}
			}else{
				System.out.println(processedQueryNum+" querys finished! time:"+General.dispTime(System.currentTimeMillis()-startTime, "min")
						+", current finished queryName: "+thisQueryID
						+", total vw_doc num for this query:"+docNum+", so not match exist for this Q, no output");
			}
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("\n one Reducer finished! total querys in this reducer:"+processedQueryNum+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );
			// ***** finsihed ***//			
			
	 	}
	}

	public static class Mapper_getTopRankDocs_forIniRank extends Mapper<IntWritable,IntArr_FloatArr,IntWritable,IntList_FloatList>{
		
		private int selTopRankLength;
		private int procSamples;
		private int dispInter;
		private long startTime;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//***** set reRankLength for 1vs1&HPM rerank ***//
			selTopRankLength=Integer.valueOf(conf.get("selTopRankLength")); //select top rank to do 1vs1 and HPM check
			System.out.println("select top-"+selTopRankLength+" in the initial rankList to save as ini rank");
			//set procSamples
			procSamples=0;
			//set dispInter
			dispInter=200;
			startTime=System.currentTimeMillis(); //startTime
			
			System.out.println("mapper setup finsihed!");
			
	 	}
		
		@Override
		protected void map(IntWritable key, IntArr_FloatArr value, Context context) throws IOException, InterruptedException {
			//key: vw
			//value: QID_DID_Rank_ScoreInInt
			procSamples++;
			LinkedList<Integer> docs=new LinkedList<>(); LinkedList<Float> scores=new LinkedList<>();
			for (int i = 0; i < Math.min(value.getIntArr().length,selTopRankLength); i++) {
				docs.add(value.getIntArr()[i]);
				scores.add(value.getFloatArr()[i]);
			}
			context.write(key, new IntList_FloatList(docs, scores));
			
			//disp
			if((procSamples)%dispInter==0){ 							
				System.out.println( "select Samples, "+procSamples+" querys finished!! ......"+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one mapper finished! total querys in this Mapper: "+procSamples+" ....."+ General.dispTime ( System.currentTimeMillis()-startTime, "min"));
			
	 	}
	}

	//******** job3 **************
	public static class Mapper_selectTopRankDocs extends Mapper<IntWritable,IntArr_FloatArr,IntWritable,IntArr>{
		
		private int selTopRankLength;
		private int procSamples;
		private int procSelSamples;
		private int dispInter;
		private long startTime, endTime;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//***** set reRankLength for 1vs1&HPM rerank ***//
			selTopRankLength=Integer.valueOf(conf.get("selTopRankLength")); //select top rank to do 1vs1 and HPM check
			System.out.println("select top-"+selTopRankLength+" in the initial rankList to do 1vs1 and HPM check!");
			//set procSamples
			procSamples=0;
			procSelSamples=0;
			//set dispInter
			dispInter=5000;
			startTime=System.currentTimeMillis(); //startTime
			
			System.out.println("mapper setup finsihed!");
			
	 	}
		
		@Override
		protected void map(IntWritable key, IntArr_FloatArr value, Context context) throws IOException, InterruptedException {
			//key: vw
			//value: QID_DID_Rank_Score
			procSamples++;
			if(value.getIntArr()[2]<selTopRankLength){//doc's rank is within the rerank scale
				procSelSamples++;
				context.write(key, new IntArr(value.getIntArr())); //vw, QID_DID_Rank
			}
			//disp
			if((procSamples)%dispInter==0){ 							
				endTime=System.currentTimeMillis(); //end time 
				System.out.println( "select Samples, "+procSamples+" Samples finished!! selected: "+procSelSamples+" ......"+ General.dispTime (endTime-startTime, "min"));
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
		    endTime=System.currentTimeMillis(); //end time 
			System.out.println("one mapper finished! total Samples in this Mapper: "+procSamples+", selected: "+procSelSamples+" ....."+ General.dispTime ( endTime-startTime, "min"));
			
	 	}
	}

	public static class Reducer_groupVW_QID_DocIDs extends Reducer<IntWritable,IntArr,IntWritable,IntArr>  {
		private Configuration conf;
		private VW_IniMatchedDocs vw_IniMatchedDocs;
		private int reduceNum;
		private StringBuffer vws;
		private boolean disp;
		private int dispInter;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			conf=context.getConfiguration();
			Conf_ImageR conf_ImageR=new Conf_ImageR(conf);
			vw_IniMatchedDocs=new VW_IniMatchedDocs(conf, conf_ImageR.sd_vw_iniDocs_HDFSPath, conf_ImageR.sd_VWFileInter, true);
			// ***** setup finsihed ***//
			reduceNum=0;
			vws=new StringBuffer();
			disp=false;
			dispInter=50;
			System.out.println("setup finsihed!");
			
	 	}
		
		@Override
		public void reduce(IntWritable VW, Iterable<IntArr> QID_DIDs, Context context) throws IOException, InterruptedException {
			//key: VW, value: QID_DID_Rank

			int vw=VW.get();
			//******** matched  ************	
			HashMap<Integer, ArrayList<Integer>> Q_Docs=new HashMap<Integer, ArrayList<Integer>>(); 
			for(Iterator<IntArr> it=QID_DIDs.iterator();it.hasNext();){// loop over all HashMaps				
				IntArr oneQ_D_R=it.next();
				int thisQ=oneQ_D_R.getIntArr()[0]; int thisD=oneQ_D_R.getIntArr()[1]; 
				ArrayList<Integer> thisQueryDocs=Q_Docs.get(thisQ);
				if (thisQueryDocs==null) {
					thisQueryDocs=new ArrayList<Integer>();
					thisQueryDocs.add(thisD);
					Q_Docs.put(thisQ, thisQueryDocs);
				}else {
					thisQueryDocs.add(thisD);
				}
			}		
			//run
			int docNum=vw_IniMatchedDocs.makeOne_VW_IniMatchedDocs(vw, Q_Docs, disp);

			//disp progress
			reduceNum++;
			if (reduceNum%dispInter==0){ 
				System.out.println(reduceNum+" reduce(vw) finished! current finished vw: "+vw+", total queryNum: "+Q_Docs.size());
				disp=true;
			}
			vws.append(vw+"_"+Q_Docs.size()+"_"+docNum+", ");
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("this reducer is for vw_matchedDocs, one reducer finished! total "+reduceNum+" vws in this Reducer, vw_queryNum_docNum: "+vws.toString());
			
	 	}
	}
	
	public static class Mapper_getReRankDocs extends Mapper<IntWritable,IntArr_FloatArr,IntWritable,IntArr>{
		
		private int selTopRankLength;
		private int procSamples;
		private int dispInter;
		private long startTime;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//***** set reRankLength for 1vs1&HPM rerank ***//
			selTopRankLength=Integer.valueOf(conf.get("selTopRankLength")); //select top rank to do 1vs1 and HPM check
			System.out.println("select top-"+selTopRankLength+" in the initial rankList to save as reRank docs");
			//set procSamples
			procSamples=0;
			//set dispInter
			dispInter=200;
			startTime=System.currentTimeMillis(); //startTime
			
			System.out.println("mapper setup finsihed!");
			
	 	}
		
		@Override
		protected void map(IntWritable key, IntArr_FloatArr value, Context context) throws IOException, InterruptedException {
			//key: vw
			//value: QID_DID_Rank_ScoreInInt
			procSamples++;
			int[] docs=new int[ Math.min(value.getIntArr().length, selTopRankLength)];
			for (int i = 0; i <docs.length; i++) {
				docs[i]=value.getIntArr()[i];
			}
			Arrays.sort(docs);//sort topDocs by docID in ASC
			context.write(key, new IntArr(docs));
			
			//disp
			if((procSamples)%dispInter==0){ 							
				System.out.println( "select Samples, "+procSamples+" querys finished!! ......"+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one mapper finished! total querys in this Mapper: "+procSamples+" ....."+ General.dispTime ( System.currentTimeMillis()-startTime, "min"));
			
	 	}
	}

	public static class Reducer_selectTailFromIniRank extends Reducer<IntWritable,IntList_FloatList,IntWritable,IntList_FloatList>  {
		
		private int topRank;
		private int reRankLength;
		private int QueryNums;
		private int noTailQueryNum;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			Conf_ImageR conf_ImageR=new Conf_ImageR(conf);
			//***** select top rank for output ***//
			topRank=conf_ImageR.mr_topRank; //select top rank as output
			System.out.println("select top-"+topRank+" in rankList as output");
			//***** set reRankLength for 1vs1&HPM rerank ***//
			reRankLength=conf_ImageR.getRerankLen(); //select top rank to do 1vs1 and HPM check
			System.out.println("select top-"+reRankLength+" in the initial rankList to do 1vs1 and HPM check!");
			General.Assert(topRank>reRankLength, "err! topRank should > reRankLength if lunch Reducer_selectTailFromIniRank! ");
			QueryNums=0;
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			
	 	}
		
		@Override
		public void reduce(IntWritable QID, Iterable<IntList_FloatList> values, Context context) throws IOException, InterruptedException {
			//key: sampleName, value: content
			QueryNums++;
			//******** only one list in rank result! ************		
			IntList_FloatList rank = null; int loopNum=0; 
			for(Iterator<IntList_FloatList> it=values.iterator();it.hasNext();){// loop over all ranks				
				rank= it.next();
				loopNum++;
			}
			General.Assert(loopNum==1, "error! one queryID, one rank, loopNum should == 1, here loopNum="+loopNum);
			if (rank.getIntegers().size()>reRankLength) {
				int actLength=Math.min(topRank, rank.getIntegers().size());
				context.write(QID, new IntList_FloatList(rank.getIntegers().subList(reRankLength, actLength), rank.getFloats().subList(reRankLength,actLength)));
			}else {
				noTailQueryNum++;
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("read and out finished! total QueryNums:"+QueryNums+", noTailQueryNum:"+noTailQueryNum);
			
	 	}
	}
	
	//******** job4 **************	
	public static class Reducer_SearchTVector_getDocMatches extends Reducer<IntWritable,IntArr_HESig_ShortArr_Arr,Key_QID_DID,Int_MatchFeatArr>  {

		private Configuration conf;
		private int VWFileInter;
		private String TVectorPath;
		private boolean isDiffTopDocsByVW;
		private String vw_iniDocsPath;
		private int HMDistThr_rankDoc;
		private int[] PaIDs;
		private StringBuffer vws_queryNums;
		private int reduceNum;
		private int dispInter_reduce;
		private boolean disp;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			Conf_ImageR conf_ImageR=new Conf_ImageR(conf);
			//check file in distributted cache
			General.checkDir(new Disp(true,"",null), ".");
			//***** set TVector SeqFiles Path***//
			VWFileInter=conf_ImageR.sd_VWFileInter;
			TVectorPath = conf_ImageR.sd_TVector_HDFSPath;
			System.out.println("TVectorPath:"+TVectorPath+", VWFileInter:"+VWFileInter);
			//***** set vw_matchedDocs SeqFile Path***//
			isDiffTopDocsByVW=conf_ImageR.mr_isDiffTopDocsByVW;
			vw_iniDocsPath = conf_ImageR.sd_vw_iniDocs_HDFSPath;
			System.out.println("vw_iniDocsPath: "+vw_iniDocsPath);
			//********* set Hamming distance threshold***//
			HEParameters HEPara_reR=new HEParameters(conf_ImageR.sd_rerankHEPara); //rerankHEPara: reRHE@18@12		
			HMDistThr_rankDoc=HEPara_reR.HMDistThr;//reRHE@18@12
			System.out.println("HEPara_reR: "+HEPara_reR);
			//********** set PaIDs **************//
			PaIDs= (int[]) General.readObject(Conf_ImageR.sd_VWPaIDs);
			System.out.println("PaIDs load finished, total partioned reducer number : "+(PaIDs[PaIDs.length-1]+1)+", job.setNumReduceTasks(jobRedNum) should == this value!!");
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			vws_queryNums=new StringBuffer();
			reduceNum=0;
			dispInter_reduce=2;
			disp=false;
			
	 	}
		
		@Override
		public void reduce(IntWritable VW, Iterable<IntArr_HESig_ShortArr_Arr> QueryNames_feats, Context context) throws IOException, InterruptedException {
			//QueryNameSigs: QueryName-Integer, Sigs:-ByteArrList

			int progInter=500;  int vw=VW.get();
			
			reduceNum++;
			if (reduceNum%dispInter_reduce==0) {
				disp=true;
			}
			
			General.dispInfo_ifNeed(disp,"\n ", "this reduce is for VW: "+VW+", total allocated reducers for this vw: "+General_Hadoop.getVWReducerNum_from_vwPartitionIDs(PaIDs, vw));	
			
			long startTime=System.currentTimeMillis();
			int queryNum_thisVW=0; int queryFeatNum_thisVW=0; int queryNum_existMatch=0; 

			//********read vw_matchedDocs(SeqFile) into memory************
			HashMap<Integer, int[]> QID_DIDs = new HashMap<Integer, int[]>(); int allQueryMatchedDocNum=0;
			if (isDiffTopDocsByVW) {
				VW_IniMatchedDocs vw_IniMatchedDocs=new VW_IniMatchedDocs(context.getConfiguration(), vw_iniDocsPath, VWFileInter,disp);
				allQueryMatchedDocNum=vw_IniMatchedDocs.readVW_IniMatchedDocsIntoMemory(vw, QID_DIDs);
			}else {//do not DiffTopDocsByVW, seach for all topDocs
				SequenceFile.Reader Reader=new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(vw_iniDocsPath+"/part-r-00000")));
				IntWritable TVector_key = new IntWritable(vw) ; //queryID
				IntArr TVector_value = new IntArr();//docID[]
				//******* read TVector into memory **************
				while (Reader.next(TVector_key, TVector_value)) {
					QID_DIDs.put(TVector_key.get(), TVector_value.getIntArr());
					allQueryMatchedDocNum+=TVector_value.getIntArr().length;
				}
				Reader.close();
			}
						
			if (allQueryMatchedDocNum>0) {
				General.dispInfo_ifNeed(disp,"\t ", "read this vw_matchedDocs into memory finished! queryNum: "+ QID_DIDs.size()+", allQueryMatchedDocNum: "+allQueryMatchedDocNum
						+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "s")+", current memory info: "+General.memoryInfo());
				//********read TVector(SeqFile) into memory************
				TVector_Hadoop tVector=new TVector_Hadoop(context.getConfiguration(),TVectorPath, VWFileInter,disp);
				int TVectorFeatNum=tVector.readTVectorIntoMemory(vw);	
				General.dispInfo_ifNeed(disp, "\t ", "read this TVector into memory finished! docNum: "+ tVector.tVector.docID_feats.size()+", featNum: "+TVectorFeatNum
							+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "s")+", current memory info: "+General.memoryInfo());
				//******* search TVector ***********
				HESig[] queryFeats; int[] matchedDocs=null; startTime=System.currentTimeMillis();
				//process querys have this vw
	        	for(Iterator<IntArr_HESig_ShortArr_Arr> it=QueryNames_feats.iterator();it.hasNext();){
	        		IntArr_HESig_ShortArr_Arr Querys=it.next(); //can not: it.next().getInteger(), it.next().getByteArr()
	        		int queryNum_thisInterator=Querys.obj_1.getIntArr().length; boolean showThisQuery=false;
	        		for(int query_i=0;query_i<queryNum_thisInterator;query_i++){
		        		//********* process one query *********
						int queryName=Querys.obj_1.getIntArr()[query_i]; queryFeats=Querys.obj_2.getArr()[query_i].HESigs; 	 
						queryNum_thisVW++; queryFeatNum_thisVW+=queryFeats.length;
						if(disp && queryNum_thisVW%progInter==0){
							showThisQuery=true;
						}
						int matchDocNum_act=0; int matchFeatNum=0; 
						if((matchedDocs=QID_DIDs.get(queryName))!=null){//this vw exist top-selected docs for this query
							//get selected docs' match feat in TVector for this query
							boolean showOneExample=showThisQuery;
							LinkedList<Int_SURFfeat_ShortArr> commonDocs=SelectID.select_in_twoSorted_ASC(matchedDocs, tVector.tVector.docID_feats);
							boolean isExistMatch=false;
							for (Int_SURFfeat_ShortArr oneDoc : commonDocs) {
								//get match link and score
								MatchFeat_Arr oneDocMatches= General_BoofCV.compare_HESigs(queryFeats, oneDoc.feats.feats, HMDistThr_rankDoc);
								if (oneDocMatches!=null) {//when HDr < HDs, some doc that match in iniRank, may do not match when rerank!
									matchDocNum_act++;
									matchFeatNum+=oneDocMatches.getArr().length;
									isExistMatch=true;
									//outputfile: SequcenceFile; outputFormat: key->Key_QID_VW  value->Int_MatchFeat_ShortArr
									context.write(new Key_QID_DID(queryName, oneDoc.integer), new Int_MatchFeatArr(vw, oneDocMatches)); 	       
									if(showOneExample){//for debug
										System.out.println("\t ---show one example: 1 doc's matches of 1 query --- queryName:"+queryName+", docID:"+oneDoc.integer+", vw:"+vw+", its Matches:"+oneDocMatches.getArr().length);
										for (int m_i = 0;m_i < oneDocMatches.getArr().length; m_i++) {
											System.out.println("\t ------ match-"+m_i+": "+oneDocMatches.getArr()[m_i].toString());
										}
										showOneExample=false;
									}
								}
							}
							if (isExistMatch) {
								queryNum_existMatch++;
							}
						}
						//************ report progress ******************
						if(showThisQuery){
							System.out.println("\t -curent total finished querys:"+queryNum_thisVW+", queryFeatNum_thisVW:"+queryFeatNum_thisVW+", queryNum_existMatch:"+queryNum_existMatch
									+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min"));
							System.out.println("\t ---show info for current query: "+queryName+", sig number:"+queryFeats.length);
							System.out.println("\t ---selected top docs number in iniRank: "+(matchedDocs==null?0:matchedDocs.length)+", actual match docNum in this vw under HE: "+matchDocNum_act+", tot matchFeatNum:"+matchFeatNum);
							System.out.println("\t ---current memory info: "+General.memoryInfo());
							showThisQuery=false;
						}
	        		}
		        }
			}else {
				General.dispInfo_ifNeed(disp,"", "this vw do not have matched query-doc! just ignor!");
			}
        	//*** some statistic ********
        	General.dispInfo_ifNeed(disp,"\t ", "one reduce finished! total query number for this vw: "+queryNum_thisVW+", queryFeatNum_thisVW:"+queryFeatNum_thisVW+", queryNum_existMatch:"+queryNum_existMatch);
        	vws_queryNums.append(vw+"_"+queryNum_thisVW+"_"+queryFeatNum_thisVW+"_"+queryNum_existMatch+",");
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("Reducer finished! reduceNum: "+reduceNum+", vws_queryNums_queryFeatNums_queryNumExistMatch: "+vws_queryNums.toString());
			
	 	}
	}

	//******** job5 **************	
	public static class Reducer_group_QDMatches extends Reducer<Key_QID_DID,Int_MatchFeatArr,IntWritable,DocAllMatchFeats>  {
		
		private int reduceNum;
		private long startTime;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			reduceNum=0;
			startTime=System.currentTimeMillis();
			System.out.println("setup finsihed!");
			
	 	}
			
		@Override
		public void reduce(Key_QID_DID QID_DID, Iterable<Int_MatchFeatArr> vw_MatchFeats, Context context) throws IOException, InterruptedException {
			/**
			 * 1 reduce: process 1 doc for 1 query, 
			 */
			
			//********combine all vw_MatchFeats for one doc************
			LinkedList<Int_MatchFeatArr>  matches = new LinkedList<Int_MatchFeatArr>(); 
			for(Iterator<Int_MatchFeatArr> it=vw_MatchFeats.iterator();it.hasNext();){
				Int_MatchFeatArr oneVW_matches=it.next(); //can not: it.next().getInteger(), it.next().getByteArr()
				matches.add(new Int_MatchFeatArr(oneVW_matches.Integer,oneVW_matches.feats));
			}
			//**************** save this doc into QID, DID_[vw_MatchFeats] ****************
			context.write(new IntWritable(QID_DID.queryID), new DocAllMatchFeats(QID_DID.docID, matches.toArray(new Int_MatchFeatArr[0])));
			reduceNum++;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			// ***** finsihed ***//			
			System.out.println("one Reducer finished! reduceNum:"+reduceNum+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );
			
	 	}
	}

	public static class Reducer_buildRank_final_ParaliseQuery_saveScore extends Reducer<IntWritable, DocAllMatchFeats, Key_RankFlagID_QID, QID_IntList_FloatList>  {
		
		private Conf_ImageR conf_ImageR;
		private MapFile.Reader TailFromIniRank;
		private int queryNum;
		private long startTime;
		private int dispInter_D;
		private int dispInter_Q;
		
		private MapFile.Reader queryFeatReader;
		private HashMap<Integer, int[]> QuerySize_HashMap;
		
		private ScoreDoc scoreDoc;
		private MakeRank<Integer> makeRank;
		
		private HashSet<Integer> processedQuery;
//		private boolean debugFail;
		
		@SuppressWarnings({ "unchecked" })
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			conf_ImageR=new Conf_ImageR(conf);
			//check file in distributted cache
			General.checkDir(new Disp(true, "", null), ".");
			//***** select TailFromIniRank ***//
			TailFromIniRank=General_Hadoop.openMapFileInNode("TailFromIniRank.mapFile", conf, true);
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
			//-setup_makeRank
			makeRank= new MakeRank<Integer>(new Disp(true, "\t", null), conf_ImageR.mr_topRank, conf_ImageR.mr_isConcateTwoList, conf_ImageR.getRerankLen());
			System.out.println("current memory:"+General.memoryInfo());
			//***** set queryID_previous ***//
			processedQuery=new HashSet<Integer>();
			// ***** setup finsihed ***//
			queryNum=0;
			startTime=System.currentTimeMillis();
			dispInter_D=(conf_ImageR.getRerankLen()-1)/10+1;
			dispInter_Q=100;
			System.out.println("setup finsihed!\n");
//			debugFail=false;
			
	 	}
			
		@Override
		public void reduce(IntWritable QID, Iterable<DocAllMatchFeats> values, Context context) throws IOException, InterruptedException {
			/**
			 * 1 reduce: process docs for 1 query, key: queryID, value: docID and this doc's MatchFeats for this query
			 * 
			 */
			queryNum++;
			boolean disp=(queryNum%dispInter_Q==0); 
			//get query feat
			SURFpoint_ShortArr queryFeats=new SURFpoint_ShortArr();
			if(queryFeatReader.get(QID, queryFeats)==null){
				throw new InterruptedException("err! no feat for query:"+QID);
			}
			//get query size
			int[] querySize=QuerySize_HashMap.get(QID.get());
			int queryMaxSize=Math.max(querySize[0], querySize[1]);
			//run
			int docNum=0;
			for (DocAllMatchFeats docAllMatchFeats : values) {
				float[] docScores=scoreDoc.scoreOneDoc(docAllMatchFeats, queryFeats.getArr(), QID.get(), queryMaxSize, null, null, null, disp&&(docNum%dispInter_D==0));
				makeRank.addOneDoc(docAllMatchFeats.DocID, docScores);
				docNum++;
			}
			//get rank
			ArrayList<Integer> topDocs=new ArrayList<Integer>(); ArrayList<Float> topScores=new ArrayList<Float>();
			makeRank.getRes(topDocs, topScores);
			makeRank.clearDocScores();
			IntList_FloatList returnRank =new IntList_FloatList(topDocs, topScores);
			addTailFromIniRank(QID, returnRank, TailFromIniRank);
			//output
			context.write(new Key_RankFlagID_QID(conf_ImageR.sd_rankFlagInd,QID.get()), new QID_IntList_FloatList(QID.get(),returnRank));
			General.Assert(processedQuery.add(QID.get()), "err! duplicated queryID:"+QID.get());
			//done
			General.dispInfo_ifNeed(disp, "", queryNum+"queries are done! current finished queryID: "+QID+", "+General_IR.rankToString(10, returnRank.getIntegers(), returnRank.getFloats())
					+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );						
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** finsihed ***//			
			System.out.println("one Reducer finished! total querys in this reducer:"+queryNum+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );
//			if (debugFail) {
//				throw new InterruptedException("this is the debug target! fail this reducer!");
//			}
			
	 	}
	
	}

	public static class Reducer_buildRank_final_ParaliseQuery_saveDocMatches extends Reducer<IntWritable, DocAllMatchFeats, Key_RankFlagID_QID, QID_PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr>  {
		
		private Conf_ImageR conf_ImageR;
		private int queryNum;
		private long startTime;
		private int dispInter_D;
		private int dispInter_Q;
		
		private MapFile.Reader queryFeatReader;
		private HashMap<Integer, int[]> QuerySize_HashMap;
		
		private ScoreDoc scoreDoc;
		private MakeRank<DID_Score_ImageRegionMatch_ShortArr> makeRank;
		
		private HashSet<Integer> processedQuery;
//		private boolean debugFail;
		
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
			//-setup_makeRank
			makeRank= new MakeRank<DID_Score_ImageRegionMatch_ShortArr>(new Disp(true, "\t", null), conf_ImageR.mr_topRank, false, conf_ImageR.getRerankLen());
			System.out.println("current memory:"+General.memoryInfo());
			//***** set queryID_previous ***//
			processedQuery=new HashSet<Integer>();
			// ***** setup finsihed ***//
			queryNum=0;
			startTime=System.currentTimeMillis();
			dispInter_D=(conf_ImageR.getRerankLen()-1)/10+1;
			dispInter_Q=100;
			System.out.println("setup finsihed!\n");
//			debugFail=false;
			
	 	}
			
		@Override
		public void reduce(IntWritable QID, Iterable<DocAllMatchFeats> values, Context context) throws IOException, InterruptedException {
			/**
			 * 1 reduce: process docs for 1 query, key: queryID, value: docID and this doc's MatchFeats for this query
			 * 
			 */
			queryNum++;
			boolean disp=(queryNum%dispInter_Q==0); 
			//get query feat
			SURFpoint_ShortArr queryFeats=new SURFpoint_ShortArr();
			if(queryFeatReader.get(QID, queryFeats)==null){
				throw new InterruptedException("err! no feat for query:"+QID);
			}
			//get query size
			int[] querySize=QuerySize_HashMap.get(QID.get());
			int queryMaxSize=Math.max(querySize[0], querySize[1]);
			//run
			int docNum=0;
			for (DocAllMatchFeats docAllMatchFeats : values) {
				LinkedList<ImageRegionMatch> finalMatches=new LinkedList<ImageRegionMatch>();
				float[] docScores=scoreDoc.scoreOneDoc(docAllMatchFeats, queryFeats.getArr(), QID.get(), queryMaxSize, finalMatches, null, null, disp&&(docNum%dispInter_D==0));
				makeRank.addOneDoc(new DID_Score_ImageRegionMatch_ShortArr(docAllMatchFeats.DocID, docScores[0], finalMatches), docScores);
			}
			//get rank
			ArrayList<DID_Score_ImageRegionMatch_ShortArr> topDocs=new ArrayList<DID_Score_ImageRegionMatch_ShortArr>(); ArrayList<Float> topScores=new ArrayList<Float>();
			makeRank.getRes(topDocs, topScores);
			makeRank.clearDocScores();
			//output
			context.write(new Key_RankFlagID_QID(conf_ImageR.sd_rankFlagInd,QID.get()), new QID_PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr(
					QID.get(),new PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr(
							new PhotoPointsLoc(querySize[0], querySize[1], queryFeats.getSURFPointOnlyLoc()), new DID_Score_ImageRegionMatch_ShortArr_Arr(topDocs))));
			General.Assert(processedQuery.add(QID.get()), "err! duplicated queryID:"+QID.get());
			//done
			General.dispInfo_ifNeed(disp, "", queryNum+"queries are done! current finished queryID: "+QID+", "+General_IR.rankToString(10, DID_Score_ImageRegionMatch_ShortArr_Arr.extractDIDs(topDocs), topScores)
					+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );						
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** finsihed ***//			
			System.out.println("one Reducer finished! total querys in this reducer:"+queryNum+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );
			queryFeatReader.close();
//			if (debugFail) {
//				throw new InterruptedException("this is the debug target! fail this reducer!");
//			}
			
	 	}
	
	}

	public static class Reducer_buildRank_final_ParaliseFlag_saveScore extends Reducer<IntWritable,Text,Key_RankFlagID_QID,QID_IntList_FloatList>  {
		
		private Conf_ImageR conf_ImageR;
		private MapFile.Reader TailFromIniRank;
		private int reduceNum;
		private long startTime;
		private int dispInter;
		
		private MapFile.Reader queryFeatReader;
		private HashMap<Integer, int[]> QuerySize_HashMap;
						
//		private boolean debugFail;
		
		@SuppressWarnings({ "unchecked" })
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			conf_ImageR=new Conf_ImageR(conf);
			//check file in distributted cache
			General.checkDir(new Disp(true,"",null), ".");
			//***** select top rank for output ***//
			TailFromIniRank=General_Hadoop.openMapFileInNode("TailFromIniRank.mapFile", conf, true);
			//***** read query feats ***//
			queryFeatReader=General_Hadoop.openMapFileInNode("queryFeat.mapFile", conf, true);
			System.out.println("open query-Feat-MapFile finished");
			//***** read query Size_HashMap ***//
			QuerySize_HashMap=(HashMap<Integer, int[]>) General.readObject(Conf_ImageR.ef_QuerySize_HashMap);
			System.out.println("read QuerySize_HashMap finished! tot-query num:"+QuerySize_HashMap.size());
			// ***** setup finsihed ***//
			reduceNum=0;
			startTime=System.currentTimeMillis();
			dispInter=QuerySize_HashMap.size()/2;
			System.out.println("setup finsihed! ");
			
	 	}
			
		@Override
		public void reduce(IntWritable rankFlagInd, Iterable<Text> rankFlags, Context context) throws IOException, InterruptedException {
			/**
			 * 1 reduce: process 1 reRankFlag for all query, 
			 */
			//******** only one value in list! ************		
			Text oneRankFlag=General_Hadoop.readOnlyOneElement(rankFlags, rankFlagInd+"");
			//******** start rank ***********
			Configuration conf = context.getConfiguration();
			System.out.println("start process rank for "+oneRankFlag.toString()+", current memory:"+General.memoryInfo());
			//-setup_scoreDoc for "_OriHE", "_1vs1", "_1vs1AndHPM", "_1vs1AndAngle"
			conf_ImageR.sd_rerankFlag=oneRankFlag.toString();
			ScoreDoc scoreDoc= new ScoreDoc(new Disp(true, "\t", null), conf_ImageR);
			//-setup_makeRank
			MakeRank<Integer> makeRank= new MakeRank<Integer>(new Disp(true, "\t", null), conf_ImageR.mr_topRank, conf_ImageR.mr_isConcateTwoList, conf_ImageR.getRerankLen());
			System.out.println("current memory:"+General.memoryInfo());
			//***** read docMatches ***//
			SequenceFile.Reader DocMatchReader= General_Hadoop.openSeqFileInNode("AllDocMatchs.file", conf, true);
			System.out.println("open AllDocMatchs.file finished");
			//set queryID_previous ***//
			UpdateInReRankFunction upDates=new UpdateInReRankFunction(0, -1, 0, new SURFpoint_ShortArr(), true);
			int queryNum=0; int queryNum_existMatch=0;
			HashSet<Integer> processedQuery=new HashSet<Integer>();
			//key: query doc, value: vw and this doc's MatchFeats for this query
			IntWritable QID=new IntWritable(); DocAllMatchFeats docAllMatchFeats=new DocAllMatchFeats();
			int pairNum=0;
			IntWritable returnQID=new IntWritable();
			IntList_FloatList returnRank=new IntList_FloatList();
			while (DocMatchReader.next(QID, docAllMatchFeats)) {//each is for one Query-Doc
				boolean isNewQ=reRankFunction(pairNum==0, QID, docAllMatchFeats, 
						queryFeatReader, QuerySize_HashMap,
						scoreDoc, makeRank, dispInter, startTime,
						returnQID, returnRank, upDates);
				if (isNewQ) {//one query's rank is finished
					if (returnRank.getIntegers().size()!=0) {//this query have matched docs
						queryNum_existMatch++;
						saveThisRank(rankFlagInd, context, processedQuery, returnQID, returnRank);
						returnRank.getFloats().clear();
						returnRank.getIntegers().clear();
					}
					queryNum++;
				}
				pairNum++;
				context.progress();
			}
			DocMatchReader.close();
			//last query
			ArrayList<Integer> topDocs=new ArrayList<Integer>(); ArrayList<Float> topScores=new ArrayList<Float>();
			makeRank.getRes(topDocs, topScores);
			returnQID=QID; returnRank.set(topDocs, topScores);
			upDates.processedQueryNum++;
			if (returnRank.getIntegers().size()!=0){//this query have matched docs
				queryNum_existMatch++;
				saveThisRank(rankFlagInd, context, processedQuery, returnQID, returnRank);
			}
			queryNum++;
			//done
			General.Assert(upDates.processedQueryNum==queryNum, "err! processedQueryNum != saveQueryNum, "+upDates.processedQueryNum+":"+queryNum);
			System.out.println("one reRankFlag done! "+oneRankFlag.toString()+", queryNum:"+upDates.processedQueryNum+", Q_D pairNum:"+pairNum
					+", queryNum_existMatch:"+queryNum_existMatch+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );
			reduceNum++;
		}

		private void saveThisRank(IntWritable rankFlagInd, Context context, HashSet<Integer> processedQuery, IntWritable returnQID, IntList_FloatList returnRank) throws IOException, InterruptedException {
			addTailFromIniRank(returnQID, returnRank, TailFromIniRank);
			//output
			context.write(new Key_RankFlagID_QID(rankFlagInd.get(),returnQID.get()), new QID_IntList_FloatList(returnQID.get(), returnRank));
			General.Assert(processedQuery.add(returnQID.get()), "err! duplicated queryID:"+returnQID.get());
			queryFeatReader.close();
		}
		
		private class UpdateInReRankFunction{
			public int queryMaxSize, queryID_previous, processedQueryNum;
			public SURFpoint_ShortArr queryFeats;
			public boolean disp;
			
			public UpdateInReRankFunction(int queryMaxSize, int queryID_previous, int processedQueryNum, SURFpoint_ShortArr queryFeats, boolean disp){
				this.queryMaxSize=queryMaxSize;
				this.queryID_previous=queryID_previous;
				this.processedQueryNum=processedQueryNum;
				this.queryFeats=queryFeats;
				this.disp=disp;
			}
		}
		
		private boolean reRankFunction(boolean isFirstQ_D, IntWritable QID, DocAllMatchFeats docAllMatchFeats,
				MapFile.Reader queryFeatReader, HashMap<Integer, int[]> QuerySize_HashMap, 
				ScoreDoc scoreDoc, MakeRank<Integer> makeRank, int dispInter, long startTime,
				IntWritable returnQID, IntList_FloatList returnRank, UpdateInReRankFunction upDates) throws IOException, InterruptedException {
			
			boolean isNewQ=false;
			int thisQueryID=QID.get();
			if (isFirstQ_D) {//first query_doc
				//get query feat
				if(queryFeatReader.get(new IntWritable(thisQueryID), upDates.queryFeats)==null){
					throw new InterruptedException("err! no feat for query:"+thisQueryID);
				}
				//get query size
				int[] querySize=QuerySize_HashMap.get(thisQueryID);
				upDates.queryMaxSize=Math.max(querySize[0], querySize[1]);
				//change queryID_previous
				upDates.queryID_previous=thisQueryID;
			}
			//**************** check if new query ****************
			if (thisQueryID!=upDates.queryID_previous) {//new query
				isNewQ=true;
				//do 1vs1 and HPM check on previous query's initial rank
				ArrayList<Integer> topDocs=new ArrayList<Integer>(); ArrayList<Float> topScores=new ArrayList<Float>();
				makeRank.getRes(topDocs, topScores);
				returnQID.set(upDates.queryID_previous); returnRank.set(topDocs, topScores);
				//disp progress
				upDates.processedQueryNum++;
				if (upDates.processedQueryNum%dispInter==0){ 
					System.out.println("\n"+upDates.processedQueryNum+" querys finished! time:"+General.dispTime(System.currentTimeMillis()-startTime, "min")
							+", current finished queryName: "+upDates.queryID_previous+", total selected photos in its initial rank-0: "+makeRank.getScoredDocNum(0)+", rank-1: "+makeRank.getScoredDocNum(1)+", saved top doc numbers:"+topDocs.size()
							+", top10Docs:"+topDocs.subList(0, Math.min(10, topDocs.size()))+", top10Scores:"+topScores.subList(0, Math.min(10, topDocs.size())));
					upDates.disp=true;
				}
				//prepareForNext
				makeRank.clearDocScores();
				//get query feat
				if(queryFeatReader.get(new IntWritable(thisQueryID), upDates.queryFeats)==null){
					throw new InterruptedException("err! no feat for query:"+thisQueryID);
				}
				//get query size
				int[] querySize=QuerySize_HashMap.get(thisQueryID);
				upDates.queryMaxSize=Math.max(querySize[0], querySize[1]);
				//change queryID_previous
				upDates.queryID_previous=thisQueryID;
			}
			//**************** save this doc into doc_scores_order ****************
			float[] docScores=scoreDoc.scoreOneDoc(docAllMatchFeats, upDates.queryFeats.getArr(), thisQueryID, upDates.queryMaxSize, null, null, null, upDates.disp);
			makeRank.addOneDoc(docAllMatchFeats.DocID, docScores);
			upDates.disp=false;
			return isNewQ;
		}
	
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** finsihed ***//			
			queryFeatReader.close();
			System.out.println("one Reducer finished! total reduceNum:"+reduceNum+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "min") );
//			if (debugFail) {
//				throw new InterruptedException("this is the debug target! fail this reducer!");
//			}
			
	 	}
		
	}
	
	public static void addTailFromIniRank(IntWritable returnQID, IntList_FloatList returnRank, MapFile.Reader TailFromIniRank) throws IOException{
		if (TailFromIniRank!=null) {//need TailFromIniRank
			IntList_FloatList rank_iniRank=new IntList_FloatList();
			if(TailFromIniRank.get(returnQID, rank_iniRank)!=null){//this Query has tail
				for (int i = 0; i < rank_iniRank.getIntegers().size(); i++) {
					returnRank.getIntegers().add(rank_iniRank.getIntegers().get(i));
					returnRank.getFloats().add(rank_iniRank.getFloats().get(i));
				}
			}
		}
	}
			
	//******** job6: analysis result ******
	public static class Reducer_InOut_SaveRank <T extends Writable, V extends Writable, K extends AbstractTwoWritable<T, V>> extends Reducer<Key_RankFlagID_QID, K, T, V>  {
		//1 reducer per rankFlag
		private int totQueryNum;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			totQueryNum=0;
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			
	 	}
		
		@Override
		public void reduce(Key_RankFlagID_QID Key_RankFlagID_QID, Iterable<K> values, Context context) throws IOException, InterruptedException {
			//1 Q, 1 rank, 1 reduce
			K oneQ_rank=General_Hadoop.readOnlyOneElement(values, Key_RankFlagID_QID+"");
			context.write(oneQ_rank.obj_1, oneQ_rank.obj_2);
			totQueryNum++;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("read and out finished! totQueryNum:"+totQueryNum);
			
	 	}
	}

	public static class Reducer_makeReport_forOriRank extends Reducer<IntWritable,IntList_FloatList,IntWritable,Text>  {
		private Configuration conf;
		private ImageR_Evaluation imageR_eval;
		private IndexTrans indexTrans;
		private int queryNum;	
		private int dispInter;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			Conf_ImageR conf_ImageR=new Conf_ImageR(conf);
			Disp disp=new Disp(true, "", null);
			//setup ImageR_Evaluation
			imageR_eval=new ImageR_Evaluation(disp, conf_ImageR);
			indexTrans=new IndexTrans(disp,conf_ImageR);
			// ***** setup finsihed ***//
			queryNum=0;
			dispInter=10;
			System.out.println("combine result and analysize performance");
			System.out.println("setup finsihed! \n");
			
	 	}
		
		@Override
		public void reduce(IntWritable queryID, Iterable<IntList_FloatList> ranks, Context context) throws IOException, InterruptedException {
			//key: queryName, value: rank result
			//******** only one list in rank result! ************	
			IntList_FloatList oneRank=General_Hadoop.readOnlyOneElement(ranks, queryID+"");
			//******* analysis this query's result *********************
			int queryName=queryID.get(); 
			imageR_eval.add_oneRank(queryName, indexTrans.translateOneList(oneRank), new Disp(queryNum%dispInter==0, "---",null));
			
			queryNum++;		
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			FileSystem hdfs=FileSystem.get(conf);
			//outPut as String
			System.out.println("outInfo: \n"+ imageR_eval.getRes()+"\n");
			General_Hadoop.writeObject_HDFS(hdfs, conf.get("InfoStrPath"), imageR_eval.getRes());
			
	 	}
	}
	
	public static class Reducer_makeReport_forRerank extends Reducer<Key_RankFlagID_QID,QID_IntList_FloatList,IntWritable,Text>  {
		private Configuration conf;
		private Conf_ImageR conf_ImageR;
		private ImageR_Evaluation imageR_eval;
		private IndexTrans indexTrans;
		private ArrayList<String> rerankFlags;
		private int reduceNum;
		private int dispInter;
		
		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			conf_ImageR=new Conf_ImageR(conf);
			Disp disp=new Disp(true, "", null);
			//setup ImageR_Evaluation
			imageR_eval=new ImageR_Evaluation(disp, conf_ImageR);
			indexTrans=new IndexTrans(disp,conf_ImageR);
			//load rankFlagsData.file
			rerankFlags=(ArrayList<String>) General.readObject(Conf_ImageR.sd_rerankFlagsData);
			// ***** setup finsihed ***//
			reduceNum=0;
			dispInter=10;
			System.out.println("combine result and analysize performance");
			System.out.println("setup finsihed! \n");
			
	 	}
		
		@Override
		public void reduce(Key_RankFlagID_QID key, Iterable<QID_IntList_FloatList> ranks, Context context) throws IOException, InterruptedException {
			//key: RankFlagID_QID, value: rank result
			//note: when use groupCamparator for secondarySorting to group key-values with different key into the same reduece, the key is no correct anymore, it is the fist key of this group!

			int rankFlagID=key.obj_1.get();
			imageR_eval.ini_ImageR_Evaluation(conf_ImageR.mr_rankLabel+rerankFlags.get(rankFlagID));
			

			int queryNum=0;
			//run
			for(Iterator<QID_IntList_FloatList> it=ranks.iterator();it.hasNext();){// loop over all ranks				
				QID_IntList_FloatList oneQ_Rank= it.next();
				int queryName=oneQ_Rank.obj_1.get(); //add queryID to its ranks as the last one! so interger num = score num + 1
				//******* analysis this query's result *********************
				imageR_eval.add_oneRank(queryName, indexTrans.translateOneList(oneQ_Rank.obj_2), new Disp(queryNum%dispInter==0, "---",null));
				queryNum++;	
			}
			
			//outPut
			context.write(new IntWritable(rankFlagID), new Text(imageR_eval.getRes()+"\n"));
			
			// ***** setup finsihed ***//
			System.out.println("one reduce finished! rankFlagID:"+rankFlagID+", total querys:"+queryNum);
			System.out.println("outInfo: \n"+ imageR_eval.getRes()+"\n");
			reduceNum++;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			
			// ***** setup finsihed ***//
			System.out.println("\n Reducer finished! total reduce num:"+reduceNum);
			
	 	}
	}
	
}
