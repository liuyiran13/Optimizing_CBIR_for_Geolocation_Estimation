package MyAPI.imagR;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;

import MyAPI.General.General_Hadoop;
import MyAPI.Obj.Conf_General;

public class Conf_ImageR extends Conf_General{
	
	//********** dataset *****************//
	public String ds_indexLabel;
	//********** extract feat *****************//
	//setup preproc image
	public int ef_targetImgSize; 
	public boolean ef_isIntensityNorm; 
	public static final String ef_PhotoPos_HashMap="PhotoPos_HashMap.file";
	public static final String ef_QuerySize_HashMap="QuerySize_HashMap.file";
	public String ef_PhotoPos_HashMap_Path;
	public String ef_QuerySize_HashMap_Path;
	//setup targetFeat
	public String ef_targetFeat; //SURF, SIFT_binTool 
	public static final String ef_tempFilesPath="./";
	public static final String ef_detector_localPoint="BinTool_SIFT.exe";
	public String ef_detector_localPoint_Path;
	public String ef_detector_localPoint_LibsPath;
	//setup vw
	public static final String ef_vwCenters="centers.file";
	public static final String ef_PMatrix="pMatrix.file"; 
	public static final String ef_HE_Thresholds="HEThreshold.file";
	public static final String ef_middleNodes="middleNodes.file";
	public static final String ef_node_vw_links="nodeLink_learned.file";
	public String ef_vwCenters_Path;
	public String ef_PMatrix_Path; 
	public String ef_HE_Thresholds_Path;
	public String ef_middleNodes_Path;
	public String ef_node_vw_links_Path;
	public MutiAssVW ef_mutiAssVW;
	//********** score doc *****************//
	public String sd_iniR_scheme;
	public String sd_rerankHEPara; //sd_rerankHEPara and sd_rerankFlag should not be combined together, as they are different stage in Hadoop-based system
	public String sd_rerankFlag;
	public int sd_rankFlagInd;
	public int sd_rerankFlagGroupNum;
	public static final String sd_rerankFlagsData="rankFlagsData.file";
	public double sd_ASMK_thr; 
	public int sd_ASMK_alpa;
	public static final String sd_docInfo="docInfo.file"; 
	public static final String sd_TVectorInfo="TVectorInfo.file";
	public static final String sd_VWPaIDs="PaIDs.file";
	public String sd_docInfo_HDFSPath;
	public String sd_TVectorInfo_HDFSPath;
	public String sd_VWPaIDs_HDFSPath;
	public int sd_VWFileInter;
	public String sd_TVector_HDFSPath;
	public String sd_vw_iniDocs_HDFSPath;
	//********** make rank *****************//
	public boolean mr_isDiffTopDocsByVW;
	public int mr_maxIniRankLength;
	public int mr_topRank;
	public String mr_rankLabel;
	public boolean mr_isConcateTwoList;
	//********** evaluation *****************//
	public int ev_queryNum;
	public static final String ev_groundTrue="groundTrue.file";
	public static final String ev_s_to_l="s_to_l.file";
	public static final String ev_junks="junks.file";
	public static final String ev_buildingInd_Name="buildingInd_Name.file";
	public static final String ev_latlons="latlons.file";
	public String ev_groundTruePath;
	public String ev_s_to_lPath;
	public String ev_junksPath;
	public String ev_buildingInd_NamePath;
	public String ev_thresholdsForPRCurve;
	public String ev_latlonEvaFlag;
	

					
	public Conf_ImageR(Configuration conf) {
		super(conf);
		ds_indexLabel=conf.get("indexLabel");
		//********** extract feat *****************//
		//setup preproc image
		if (conf.get("targetImgSize")!=null) {
			ef_targetImgSize=Integer.valueOf(conf.get("targetImgSize")); //1024*768=786432 pixels
		}
		if (conf.get("isIntensityNorm")!=null) {
			ef_isIntensityNorm=Boolean.valueOf(conf.get("isIntensityNorm"));
		}
		ef_PhotoPos_HashMap_Path=conf.get("QueryPos_HashMap");
		ef_QuerySize_HashMap_Path=conf.get("QuerySize_HashMap");
		//setup targetFeat
		ef_targetFeat=conf.get("targetFeature"); //SURF, SIFT_binTool 
		ef_detector_localPoint_Path=conf.get("BinTool_SIFT");
		ef_detector_localPoint_LibsPath=conf.get("BinTool_libs");
		//setup vw
		ef_vwCenters_Path=conf.get("vwCenters");
		ef_PMatrix_Path=conf.get("pMatrix");
		ef_HE_Thresholds_Path=conf.get("HEThreshold");
		ef_middleNodes_Path=conf.get("middleNode");
		ef_node_vw_links_Path=conf.get("nodeLink_learned");
		//setup vw_mutiAss
		if (conf.get("multiAssFlag")!=null) {//isMultiAss@vws_NN@alph_NNDist@deta_vwSoftWeight
			ef_mutiAssVW=new MutiAssVW(conf.get("multiAssFlag"));
		}		
		//********** score doc *****************//
		sd_iniR_scheme=conf.get("iniR_scheme");
		sd_rerankFlag=conf.get("rerankFlag");
		if (conf.get("rerankFlagInd")!=null) {
			sd_rankFlagInd=Integer.valueOf(conf.get("rerankFlagInd"));
		}
		if (conf.get("rerankFlagGroupNum")!=null) {
			sd_rerankFlagGroupNum=Integer.valueOf(conf.get("rerankFlagGroupNum"));
		}
		if (conf.get("rerankHEPara")!=null) {
			sd_rerankHEPara=conf.get("rerankHEPara");
		}
		if (conf.get("ASMK_thr")!=null) {
			sd_ASMK_thr=Double.valueOf(conf.get("ASMK_thr"));
		}
		if (conf.get("ASMK_alpa")!=null) {
			sd_ASMK_alpa=Integer.valueOf(conf.get("ASMK_alpa")); 
		}
		sd_docInfo_HDFSPath=hdfs_address+conf.get("docInfo");
		sd_TVectorInfo_HDFSPath=hdfs_address+conf.get("TVectorInfo");
		sd_VWPaIDs_HDFSPath=hdfs_address+conf.get("VW_PartitionerIDs");
		if (conf.get("VWFileInter")!=null) {
			sd_VWFileInter=Integer.valueOf(conf.get("VWFileInter"));
		}
		sd_TVector_HDFSPath=hdfs_address+conf.get("TVector");
		sd_vw_iniDocs_HDFSPath=hdfs_address+conf.get("vw_iniDocs");
		//********** make rank *****************//
		if (conf.get("isDiffTopDocsByVW")!=null) {
			mr_isDiffTopDocsByVW=Boolean.valueOf(conf.get("isDiffTopDocsByVW"));
		}
		if (conf.get("maxIniRankLength")!=null) {
			mr_maxIniRankLength=Integer.valueOf(conf.get("maxIniRankLength")); //select top rank to do 1vs1 and HPM check
		}
		if (conf.get("topRank")!=null) {
			mr_topRank=Integer.valueOf(conf.get("topRank"));
		}
		mr_rankLabel=conf.get("rankLabel");
		if (conf.get("isConcateTwoList")!=null) {
			mr_isConcateTwoList=Boolean.valueOf(conf.get("isConcateTwoList"));
		}
		//********** evaluation *****************//
		if (conf.get("ev_queryNum")!=null) {
			ev_queryNum=Integer.valueOf(conf.get("ev_queryNum")); //ev_queryNum for evaluation, this is to prevent some query do not have any matches as a high HE thre, so no rank, missed num in evaluation 
		}
		ev_groundTruePath=conf.get("groundTrueForReport");
		ev_s_to_lPath=conf.get("s_to_lForReport");
		ev_junksPath=conf.get("junksForReport");
		ev_buildingInd_NamePath=conf.get("buildingInd_NameForReport");
		ev_thresholdsForPRCurve=conf.get("thresholdsForPRCurve");
		ev_latlonEvaFlag=conf.get("latlonEvaFlag");
	}
	
	public int getRerankLen(){
		return Integer.valueOf(conf.get("reRankLength"));
	}
	
	public static void addDistriCache_LocFeatDetector(Configuration conf, ArrayList<String> cacheFilePaths){
		if(conf.get("targetFeature").startsWith("SIFT-binTool")){
			cacheFilePaths.add(conf.get("BinTool_SIFT")+"#"+ef_detector_localPoint); //BinTool_SIFT path with symLink
			General_Hadoop.addToCacheListWithOriNameAsSymLink(cacheFilePaths, conf.get("BinTool_libs"), ",", "");//libs without symLink, needs keep original name
		}
	}
	
	public static void addDistriCache_ImageCodec(Configuration conf, ArrayList<String> cacheFilePaths){
		if(conf.get("imageCodec")!=null){
			General_Hadoop.addToCacheListWithOriNameAsSymLink(cacheFilePaths, conf.get("imageCodec"), ",", "");//libs without symLink, needs keep original name
		}
	}
	
	public static void addDistriCache_QueryPos(Configuration conf, ArrayList<String> cacheFilePaths){
		if (conf.get("QueryPos_HashMap")!=null) {//queries from Oxford has bounding box
			cacheFilePaths.add(conf.get("QueryPos_HashMap")+"#"+ef_PhotoPos_HashMap); //QueryPos_HashMap with symLink
		}
	}
	
	public static void addDistriCache_assVW_makeHE(Configuration conf, ArrayList<String> cacheFilePaths){
		cacheFilePaths.add(conf.get("vwCenters")+"#"+ef_vwCenters); //VWs path with symLink
		cacheFilePaths.add(conf.get("pMatrix")+"#"+ef_PMatrix); //VWs path with symLink
		cacheFilePaths.add(conf.get("HEThreshold")+"#"+ef_HE_Thresholds); //VWs path with symLink
		cacheFilePaths.add(conf.get("middleNode")+"#"+ef_middleNodes); //VWs path with symLink
		cacheFilePaths.add(conf.get("nodeLink_learned")+"#"+ef_node_vw_links); //VWs path with symLink
	}
	
	public static void addDistriCache_docInfo(Configuration conf, ArrayList<String> cacheFilePaths){
		cacheFilePaths.add(conf.get("docInfo")+"/part-r-00000#"+sd_docInfo); //docInfo with symLink
	}

	public static void addDistriCache_TVectorInfo(Configuration conf, ArrayList<String> cacheFilePaths){
		cacheFilePaths.add(conf.get("TVectorInfo")+"#"+sd_TVectorInfo); //TVectorInfo with symLink
	}
	
	public static void addDistriCache_VWPaIDs(Configuration conf, ArrayList<String> cacheFilePaths){
		cacheFilePaths.add(conf.get("VW_PartitionerIDs")+"#"+sd_VWPaIDs); //VWPaIDs with symLink
	}
	
	public static void addDistriCache_QuerySize(Configuration conf, ArrayList<String> cacheFilePaths){
		cacheFilePaths.add(conf.get("QuerySize_HashMap")+"#"+ef_QuerySize_HashMap); //QuerySize_HashMap with symLink
	}
	
	public static void addDistriCache_rerankFlagsData(String path, Configuration conf, ArrayList<String> cacheFilePaths){
		cacheFilePaths.add(path+"#"+sd_rerankFlagsData); //rerankFlagsData with symLink
	}
	
	public static void addDistriCache_evaluation(Configuration conf, ArrayList<String> cacheFilePaths){
		if (conf.get("groundTrueForReport")!=null && !conf.get("groundTrueForReport").isEmpty()) {
			cacheFilePaths.add(conf.get("groundTrueForReport")+"#"+ev_groundTrue); //groundTrue path with symLink
		}
		if (conf.get("s_to_lForReport")!=null && !conf.get("s_to_lForReport").isEmpty()) {
			cacheFilePaths.add(conf.get("s_to_lForReport")+"#"+ev_s_to_l); //s_to_l path with symLink
		}
		if (conf.get("junksForReport")!=null && !conf.get("junksForReport").isEmpty()) {
			cacheFilePaths.add(conf.get("junksForReport")+"#"+ev_junks); //junks path with symLink
		}
		if (conf.get("buildingInd_NameForReport")!=null && !conf.get("buildingInd_NameForReport").isEmpty()) {
			cacheFilePaths.add(conf.get("buildingInd_NameForReport")+"#"+ev_buildingInd_Name); //buildingInd_Name path with symLink
		}
		if (conf.get("latlonsForReport")!=null && !conf.get("latlonsForReport").isEmpty()) {
			cacheFilePaths.add(conf.get("latlonsForReport")+"#"+ev_latlons); //groundTrue path with symLink
		}
	}
	
	
}
