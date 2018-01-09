package BenchMarkTest;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.General.General_IR;
import MyAPI.Obj.Disp;
import MyAPI.imagR.ImageR_Evaluation;
import MyAPI.imagR.RankModel.ReRankTopRank;
import MyCustomedHaoop.ValueClass.IntList_FloatList;
import MyCustomedHaoop.ValueClass.PhotoAllFeats;
import MyCustomedHaoop.ValueClass.SURFpointVWs;

public class mimicHPM {
	/*
	 * run hpm in linux
	 * 
	 * Oxford: 		krenew -s -- java -Xms4g -Xmx10g -cp mimicHPM.jar:$CLASSPATH BenchMarkTest.mimicHPM OxfordBuilding_groundTruth.hashMap OxfordBuilding_junks.hashMap OxfordBuilding_buildingInd_Name.hashMap Oxford_ori5K_SelPhos_L_to_S.hashMap,Oxford_querys_L_to_L.hashMap SURFFeat_VW100k_SA_Oxford_5K_CutQ,NoUniDistractors _Oxford_5K_CutQ 100000 1000 100000
	 * Barcelona: 	krenew -s -- java -Xms4g -Xmx10g -cp mimicHPM.jar:$CLASSPATH BenchMarkTest.mimicHPM Barcelona_groundTruthBuildingID.hashMap NoJunks NoBuildingInds Barcelona_ori1K_SelPhos_L_to_S.hashMap,Barcelona_querys_L_to_L.hashMap SURFFeat_VW100k_SA_Barcelona_1K,NoUniDistractors _Barcelona_1K 100000 1000 100000
	 * Herve: 		krenew -s -- java -Xms4g -Xmx10g -cp mimicHPM.jar:$CLASSPATH BenchMarkTest.mimicHPM Herve_groundTruth.hashMap NoJunks NoBuildingInds Herve_ori1.5K_SelPhos_L_to_S.hashMap,Herve_querys_L_to_L.hashMap SURFFeat_VW100k_SA_Herve_1.5K,NoUniDistractors _Herve_1.5K 100000 1000 100000
	 *
	 */
	
	public static void main(String[] args) throws Exception {
//		prepareQuery();
		
		//run in linux, 
		/*attention: if build index with different java-version than search index, then there is a bug: 
		 * the innerPhoInds in prepareDataForHPMbin depends on the order of the hashMap entries, different java-version, the order is different!!
		 */
		buildRank_inLinux(args);
		
//		//transfer rank formate to mapfile
//		String basePath="O:/ImageRetrieval/Barcelona1K/CVPR15/MimicHPM/";
//		String rankFile=basePath+"Rerank1000_Barcelona_1K.rank";
//		int topRank=10000;
//		String rankMapFile=rankFile+"_top"+topRank+".mapFile";
//		String innerPhoInds_DB=basePath+"HPM_Barcelona_1K_DB.innerPhoInds";
//		String innerPhoInds_Q=basePath+"HPM_Barcelona_1K_Q.innerPhoInds";
//		transferRankFromHPMbin_to_MapFileRank(topRank, rankFile, rankMapFile, "O:/ImageRetrieval/Barcelona1K/Barcelona_ori1K_SelPhos_L_to_S.hashMap", innerPhoInds_DB, innerPhoInds_Q);
	
	}
	
	@SuppressWarnings("unchecked")
	public static void prepareQuery() throws InterruptedException, FileNotFoundException, IOException{
		//***** for making sub-query-set for Hever Holiday, as when rerank 1M in 1M scale, one query needs 20mins, so distribute to multi machine ************
		int querySetSize=2;
		HashMap<Integer, Integer> totQ=(HashMap<Integer, Integer>) General.readObject("O:/ImageRetrieval/Herve1.5K/Herve_querys_L_to_L.hashMap");
		
		String subQSetFolder="O:/ImageRetrieval/Herve1.5K/HerveQuerys_LtoL_perSubSet"+querySetSize+"/";
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
		
	public static void buildRank_inLinux(String[] args) throws Exception{
		//dataset info
		String dataSetInfoPath="/tudelft.net/staff-bulk/ewi/insy/mmc/XinchaoLi/CVPR15/"; ///tudelft.net/staff-bulk/ewi/insy/mmc/XinchaoLi/CVPR15/
		String grondTruthPath=dataSetInfoPath+args[0];//"OxfordBuilding_groundTruth.hashMap"
		String junkPath=args[1].equalsIgnoreCase("NoJunks")?null:dataSetInfoPath+args[1];//"OxfordBuilding_junks.hashMap"
		String buildingInd_NamePath=args[2].equalsIgnoreCase("NoBuildingInds")?null:dataSetInfoPath+args[2]; //"OxfordBuilding_buildingInd_Name.hashMap";
		String[] L_to_S_dbAndQ=args[3].split(",");// "Oxford_ori5K_SelPhos_L_to_S.hashMap,Oxford_querys_L_to_L.hashMap"
		String S_to_L_db=dataSetInfoPath+args[4];
		General.addStrArr_prefx(dataSetInfoPath, L_to_S_dbAndQ);
		//feat files
		String basePath="/tudelft.net/staff-bulk/ewi/insy/mmc/XinchaoLi/CVPR15/mimicHPM/";///tudelft.net/staff-bulk/ewi/insy/mmc/XinchaoLi/CVPR15/mimicHPM/
		String[] featMapFile_dbAndUniDist=args[5].split(","); //"SURFFeat_VW100k_SA_Oxford_5K_CutQ,SURFFeat_VW100k_SA_CVPR15UniDistra_10M_Inter100K", "SURFFeat_VW100k_SA_Oxford_5K_CutQ,NoUniDistractors"
		General.addStrArr_prefx(basePath, featMapFile_dbAndUniDist);
		String[] phoMaxSizePath=new String[2];
		String[] featFileDir=new String[2];
		String[] minMaxScalePath=new String[2];
		//retrieval config
		String indexLabel=args[6]; //"_Oxford_5K_CutQ"
		int saveInter=Integer.valueOf(args[7]);//100000
		ReRankTopRank reRankTopRank=new ReRankTopRank(args[8]); //reR@1000@1000
		int vwNum=Integer.valueOf(args[9]);//100000
		String threthods=args[10];//for pairwise PRCurve,
		String Qlabel=args[11];//for the case that divide query into small sets and test 
		//workDirs
		String workDir=basePath+"VW"+vwNum+indexLabel+"/";
		General.makeFolder(workDir);
		//bin
		String hpmBinPath="/home/nfs/xinchaoli/Code/HPM/hpm_retrieval_fromGiorgos";///home/nfs/xinchaoli/Code/HPM/hpm
		//retrieval files
		String[] db_query={"DB",Qlabel};
		String paramPath=workDir+"Rerank"+reRankTopRank.rerankLen+indexLabel+"_"+db_query[1]+".param";
		String indexPath=workDir+"SURF"+indexLabel+"_Java"+System.getProperty("java.version")+".index"; //the innerPhoInds in prepareDataForHPMbin depends on the order of the hashMap entries, different java-version, the order is different!!
		String rankPath=workDir+"Rerank"+reRankTopRank.rerankLen+indexLabel+"_"+db_query[1]+".rank";
		String scorePath=workDir+"Rerank"+reRankTopRank.rerankLen+indexLabel+"_"+db_query[1]+".score";
		String rankMapFilePath=workDir+reRankTopRank+"_"+db_query[1]+".mapFile";
		//report
		PrintWriter outputStream_report=new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"mimicHPM_SURF"+"VW"+vwNum+indexLabel+"_"+reRankTopRank+"_"+db_query[1]+".report", false), "UTF-8"),true); 
		long startTime=System.currentTimeMillis();
		//make binary feat files
		for (int i=0; i<featMapFile_dbAndUniDist.length;i++) {
			if (!featMapFile_dbAndUniDist[i].endsWith("NoUniDistractors")) {//NoUniDistractors means only use dataset photos
				featFileDir[i]=featMapFile_dbAndUniDist[i]+"_binFeats/";
				phoMaxSizePath[i]=featMapFile_dbAndUniDist[i]+".phoMaxSize";
				minMaxScalePath[i]=featMapFile_dbAndUniDist[i]+".minMaxScale";
				makeFeatData(featMapFile_dbAndUniDist[i], featFileDir[i], phoMaxSizePath[i], minMaxScalePath[i], outputStream_report, startTime);
			}
		}
		//prepare data for hpm
		String[][] phoInfoPaths=new String[2][];//phoVWPath, phoGeoPath, phoMaxDimPath
		for (int i=0; i<db_query.length;i++) {
			String one = "HPM"+indexLabel+"_"+db_query[i];
			String phoVWPath=workDir+one+"_vwFiles.txt";
			String phoGeoPath=workDir+one+"_geoFiles.txt";
			String phoMaxDimPath=workDir+one+".phoMaxSize";
			String phoInnerInds=workDir+one+".innerPhoInds";
			prepareDataForHPMbin(featFileDir[0], phoMaxSizePath[0], 
					featFileDir[1], phoMaxSizePath[1], saveInter,
					L_to_S_dbAndQ[i], phoVWPath, phoGeoPath, phoMaxDimPath, phoInnerInds, outputStream_report, startTime);
			phoInfoPaths[i]=new String[]{phoVWPath, phoGeoPath, phoMaxDimPath, phoInnerInds};
		}
		//make params
		double[] minMaxScales=getMinMaxScale(minMaxScalePath[0], minMaxScalePath[1]); 
		makeParams(reRankTopRank.rerankLen, minMaxScales[0], minMaxScales[1], paramPath, phoInfoPaths[0][2], phoInfoPaths[1][2], outputStream_report);
		//run HPM bin
		String[] resulRank_Score= runHPM(vwNum, indexPath, paramPath, hpmBinPath, phoInfoPaths[0][0], phoInfoPaths[0][1], phoInfoPaths[1][0], phoInfoPaths[1][1], outputStream_report, startTime);
		//rename rank name and score name
		General.runSysCommand(Arrays.asList("mv",resulRank_Score[0],rankPath), null, true);
		General.runSysCommand(Arrays.asList("mv",resulRank_Score[1],scorePath), null, true);
		//transfer rank to MapFile
		transferRankFromHPMbin_to_MapFileRank(reRankTopRank.topRankLen, rankPath, scorePath, rankMapFilePath, L_to_S_dbAndQ[0], phoInfoPaths[0][3], phoInfoPaths[1][3], outputStream_report, startTime);;
		//analysis results
		ImageR_Evaluation imagR_eval=new ImageR_Evaluation(new Disp(true,"",null), indexLabel, "_"+reRankTopRank, grondTruthPath, junkPath, buildingInd_NamePath, S_to_L_db, threthods);
		General.dispInfo(outputStream_report, imagR_eval.analysisOneMapFile(rankMapFilePath, startTime));			
		//done
		outputStream_report.close();
	}

	public static void makeFeatData(String MapFilesPath, String resDir, String phoMaxSizePath, String minMaxScalePath, PrintWriter report, long startTime) throws IOException{
		General.dispInfo(report, "start makeFeatData for "+MapFilesPath+", resDir:"+resDir+", phoMaxSizePath:"+phoMaxSizePath+", minMaxScalePath:"+minMaxScalePath);
		MapFile.Reader[] mapFiles=General_Hadoop.openAllMapFiles(new String[]{MapFilesPath+"/"});
		//judge whether feat files need to be updated
		boolean needUpdate=false;
		for (int i = 0; i < Math.min(mapFiles.length, 51); i++) {//one mapFile, one feat dir, each then has one vw dir and one geo dir
			String vwPath=resDir+i+"/VW/";
			if (!new File(vwPath).exists()) {//new mapfiles are added, so need update feat files
				needUpdate=true;
				break;
			}
		}
		if (needUpdate) {
			IntWritable Key= new IntWritable();
			PhotoAllFeats Value= new PhotoAllFeats();
			float strenth_notUsed=(float) 1; int geoMetricNum=5; 
			double minScale=Integer.MAX_VALUE; double maxScale=-1; 
			//get max absolute phoID for making phoMaxSize arr
			int phoNum=0; int maxAbsPhoID=Integer.MIN_VALUE;
			for (MapFile.Reader oneMapFile : mapFiles) {
				while(oneMapFile.next(Key, Value)){ //loop over all photos
					maxAbsPhoID=Math.max(maxAbsPhoID, Math.abs(Key.get()));
					phoNum++;
				}
				oneMapFile.close();
			}
			General.dispInfo(report, "in total phoNum:"+phoNum+", maxAbsPhoID: "+maxAbsPhoID+" .... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			//make feat files
			mapFiles=General_Hadoop.openAllMapFiles(new String[]{MapFilesPath+"/"});
			int[] maxPhoSizes=new int[maxAbsPhoID+1];
			for (int i = 0; i < mapFiles.length; i++) {//one mapFile, one feat dir, each then has one vw dir and one geo dir
				MapFile.Reader oneMapFile=mapFiles[i];
				String vwPath=resDir+i+"/VW/";
				String geoPath=resDir+i+"/Geo/";
				boolean isVWExist=new File(vwPath).exists();
				if (isVWExist) {
					General.dispInfo(report, i+" th mapFile's feat files: VW.txt and Geo.txt exist, only need save maxPhoSizes.");
				}else {
					General.makeFolder(vwPath);
					General.makeFolder(geoPath);
				}	
				phoNum=0; 
				while(oneMapFile.next(Key, Value)){ //loop over all photos
					phoNum++;
					int phoName=Key.get();//photoName is in L
					//get maxSize
					maxPhoSizes[Math.abs(phoName)]=Math.max(Value.width, Value.height);
					//get vws and geos
					if (!isVWExist) {
						ByteBuffer buffer_vws = ByteBuffer.allocate(Value.feats.getArr().length*4+8);  ByteBuffer buffer_geo = ByteBuffer.allocate(Value.feats.getArr().length*28+8); 
						buffer_vws.order(ByteOrder.LITTLE_ENDIAN); buffer_geo.order(ByteOrder.LITTLE_ENDIAN);  
						buffer_vws.putDouble(Value.feats.getArr().length); buffer_geo.putDouble(Value.feats.getArr().length);
						for (SURFpointVWs onePoint : Value.feats.getArr()) {
							//write vw
							buffer_vws.putInt(onePoint.vws.getArr()[0].vw);
							//write geo
							buffer_geo.putDouble(geoMetricNum);
							buffer_geo.putFloat(onePoint.point.x);
							buffer_geo.putFloat(onePoint.point.y);
							buffer_geo.putFloat(strenth_notUsed);
							buffer_geo.putFloat(onePoint.point.scale);
							buffer_geo.putFloat(onePoint.point.angle);
							//updata minScale and maxScale
							minScale=Math.min(minScale, onePoint.point.scale);
							maxScale=Math.max(maxScale, onePoint.point.scale);
						}
						//save bins
						DataOutputStream bin_vws = new DataOutputStream(new FileOutputStream(vwPath+phoName+".vw"));
						DataOutputStream bin_geometric = new DataOutputStream(new FileOutputStream(geoPath+phoName+".geo"));
						bin_vws.write(buffer_vws.array()); bin_geometric.write(buffer_geo.array());
						bin_vws.close(); bin_geometric.close();
					}
				}
				General.dispInfo(report, i+" th mapFile finished, phoNum:"+phoNum+" .... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			}
			General.dispInfo(report, "done! all "+mapFiles.length+" mapFiles finished, minScale:"+minScale+", maxScale:"+maxScale+" ..... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			//save phoMaxSizePath, minMaxScalePath
			General.writeObject(phoMaxSizePath, maxPhoSizes); 
			if (!(new File(minMaxScalePath).exists())) {//to save time, do not update minScale, maxScale. only use the first-time-one
				General.writeObject(minMaxScalePath, new double[]{minScale, maxScale}); 
			}
			//clean up
			General_Hadoop.closeAllMapFiles(mapFiles);
		}else {
			General_Hadoop.closeAllMapFiles(mapFiles);
			General.dispInfo(report, "done! mapfiles are not changed, so no need to update feat files");
		}
	}
	
	@SuppressWarnings("unchecked")
	public static void prepareDataForHPMbin(String binFeatPath_dataSet, String phoMaxSizePath_dataSet, 
			String binFeatPath_UniDist, String phoMaxSizePath_dataSet_UniDist, int saveInter,
			String L_to_S_path, String phoVWPath, String phoGeoPath, String phoMaxDimPath, String phoInnerInds, PrintWriter outputStream_report, long startTime) throws Exception {
		
		General.dispInfo(outputStream_report, "start prepareDataForHPMbin for "+L_to_S_path+" from "+binFeatPath_dataSet+" and "+binFeatPath_UniDist);
		General.Assert(new File(binFeatPath_dataSet).exists(), "binFeatPath_dataSet does not exist! "+binFeatPath_dataSet);
		General.Assert(binFeatPath_UniDist==null||new File(binFeatPath_UniDist).exists(), "binFeatPath_UniDist does not exist! "+binFeatPath_UniDist);
		//prepare commons
		int[] phoMaxSizes_dataSet=(int[]) General.readObject(phoMaxSizePath_dataSet);
		int[] phoMaxSizes_UniDist=(int[]) General.readObject(phoMaxSizePath_dataSet_UniDist);
		HashMap<Integer, Integer> L_to_S=(HashMap<Integer, Integer>) General.readObject(L_to_S_path);
		PrintWriter outputStream_vw=new PrintWriter(new OutputStreamWriter(new FileOutputStream(phoVWPath,false), "UTF-8"),true); 
		PrintWriter outputStream_geo=new PrintWriter(new OutputStreamWriter(new FileOutputStream(phoGeoPath,false), "UTF-8"),true); 
		LinkedList<Integer> pho_maxSize=new LinkedList<Integer>(); 
		LinkedList<Integer> innerPhoInds=new LinkedList<Integer>(); 
		for (Integer phoID_L : L_to_S.keySet()) {
			innerPhoInds.add(phoID_L);
			boolean isDataSetPho=phoID_L<0;
			pho_maxSize.add(isDataSetPho?phoMaxSizes_dataSet[-phoID_L]:phoMaxSizes_UniDist[phoID_L]);
			//record bin file name
			outputStream_vw.println((isDataSetPho?binFeatPath_dataSet+"0":binFeatPath_UniDist+phoID_L/saveInter)+"/VW/"+phoID_L+".vw");
			outputStream_geo.println((isDataSetPho?binFeatPath_dataSet+"0":binFeatPath_UniDist+phoID_L/saveInter)+"/Geo/"+phoID_L+".geo");
		}
		outputStream_vw.close();
		outputStream_geo.close();
		//save photo max size
		int phoNum=L_to_S.size();
		ByteBuffer buffer_phoMaxSize = ByteBuffer.allocate(phoNum*4+8);  //save photo max-size
		buffer_phoMaxSize.order(ByteOrder.LITTLE_ENDIAN); 
		buffer_phoMaxSize.putDouble(phoNum);
		for (Integer one : pho_maxSize) {
			buffer_phoMaxSize.putInt(one);
		}
		DataOutputStream bin_phoMaxSize = new DataOutputStream(new FileOutputStream(phoMaxDimPath));
		bin_phoMaxSize.write(buffer_phoMaxSize.array());
		bin_phoMaxSize.close();
		//save innerPhoInds
		General.writeObject(phoInnerInds, innerPhoInds.toArray(new Integer[0]));
		//done
		General.dispInfo(outputStream_report, "prepareDataForHPMbin done! phoNum:"+phoNum+"  ..... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
	}
	
	public static double[] getMinMaxScale(String phoMaxSizePath_dataSet, String phoMaxSizePath_UniDist) throws Exception {
		double[] minMaxScale_dataSet=(double[]) General.readObject(phoMaxSizePath_dataSet);
		double[] minMaxScale_UniDist=(double[]) General.readObject(phoMaxSizePath_UniDist);
		if (minMaxScale_UniDist==null) {
			return minMaxScale_dataSet;
		}else {
			return new double[]{Math.min(minMaxScale_dataSet[0], minMaxScale_UniDist[0]), Math.max(minMaxScale_dataSet[1], minMaxScale_UniDist[1])};
		}
	}
	
	public static void makeParams(int rerank, double minScale, double maxScale, String paramPath, String phoMaxDimPath_db, String phoMaxDimPath_query, PrintWriter outputStream_report) throws UnsupportedEncodingException, FileNotFoundException {
		//make parameters
		PrintWriter outputStream_para=new PrintWriter(new OutputStreamWriter(new FileOutputStream(paramPath,false), "UTF-8"),true); 
		outputStream_para.println("SET QUANT_MIN_SCALE "+minScale);
		outputStream_para.println("SET QUANT_MAX_SCALE "+maxScale);
		outputStream_para.println("SET QSZ "+phoMaxDimPath_query);
		outputStream_para.println("SET DBSZ "+phoMaxDimPath_db);
		outputStream_para.println("SET NR "+rerank);
		outputStream_para.close();
		//save a copy to report
		General.dispInfo(outputStream_report, "params: ");
		General.dispInfo(outputStream_report, "SET QUANT_MIN_SCALE "+minScale);
		General.dispInfo(outputStream_report, "SET QUANT_MAX_SCALE "+maxScale);
		General.dispInfo(outputStream_report, "SET QSZ "+phoMaxDimPath_query);
		General.dispInfo(outputStream_report, "SET DBSZ "+phoMaxDimPath_db);
		General.dispInfo(outputStream_report, "SET NR "+rerank);
	}
	
	public static String[] runHPM(int vwNum, String indexPath, String paramPath, String hpmBinPath, String phoVWPath_db, String phoGeoPath_db, String phoVWPath_query, String phoGeoPath_query, PrintWriter outputStream_report, long startTime) throws Exception {
		//******* run HPM bin  ***********
		//1.index
		//./hpm -nvw 100000 -ofile SURF_VW100k_5KCutQ.index -p params -vw SURFFeat_VW100k_SA_Oxford5KCutQ_DataBase_vwFiles.txt -feat SURFFeat_VW100k_SA_Oxford5KCutQ_DataBase_geoFiles.txt
		if (new File(indexPath).exists()) {
			General.dispInfo(outputStream_report, "indexPath:"+indexPath+" is already exist, no need to build .... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
		}else {
			String info=General.runSysCommand(Arrays.asList(
					hpmBinPath, "-nvw", ""+vwNum, "-ofile", indexPath, "-p", paramPath, "-vw", phoVWPath_db, "-feat", phoGeoPath_db
					), null, true);
			General.dispInfo(outputStream_report, "build index done! "+indexPath+" .... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			General.dispInfo(outputStream_report, "info from HPM bin: "+info);
		}
		
		//2.retrieval
		//./hpm -nvw 100000 -ofile SURF_VW100k_5KCutQ.index -p params -qvw SURFFeat_VW100k_SA_OxfordCutQ_Query_vwFiles.txt -qfeat SURFFeat_VW100k_SA_OxfordCutQ_Query_geoFiles.txt
//		String info=General.runSysCommand(Arrays.asList(
//				hpmBinPath, "-nvw", ""+vwNum, "-ofile", indexPath, "-p", paramPath, "-qvw", phoVWPath_query, "-qfeat", phoGeoPath_query
//				), null, true);
//		General.dispInfo(outputStream_report, "retrieval done! .... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
//		General.dispInfo(outputStream_report, "info from HPM bin: "+info);
		
		return new String[]{phoVWPath_query+".ranks.bin", phoVWPath_query+".scores.bin"}; //result rank file
	}

	public static void readRankFromHPMbin(String indexLabel, String rankLabel, String resulRank, String resulScore, String innerPhoInds_dbPath, String innerPhoInds_queryPath, 
			String grondTruthPath, String junkPath, String buildingInd_NamePath, String threthods,
			PrintWriter outputStream_report, long startTime) throws Exception {
		ImageR_Evaluation imagR_eval=new ImageR_Evaluation(new Disp(true,"",null), indexLabel, rankLabel, grondTruthPath, junkPath, buildingInd_NamePath, null, threthods);
		int dispInter=20;
		DataInputStream rankStream = new DataInputStream( new FileInputStream(resulRank));
		DataInputStream scoreStream = new DataInputStream( new FileInputStream(resulScore));
		Integer[] innerPhoInds_db=(Integer[]) General.readObject(innerPhoInds_dbPath);
		Integer[] innerPhoInds_query=(Integer[]) General.readObject(innerPhoInds_queryPath);
		byte[] buffer_double = new byte[8]; 
		int queryNum=(int) General.byteArr_to_double(General.readByteArr(buffer_double, rankStream));
		int queryNum_inScore=(int) General.byteArr_to_double(General.readByteArr(buffer_double, scoreStream));
		General.Assert(queryNum==queryNum_inScore, "queryNum should == queryNum_inScore, here queryNum:"+queryNum+", queryNum_inScore:"+queryNum_inScore);
		for (int i = 0; i < queryNum; i++) {
			int queryName=innerPhoInds_query[i];
			//get rank
			int dataNum_inRank=(int) General.byteArr_to_double(General.readByteArr(buffer_double, rankStream));
			int dataNum_inScore=(int) General.byteArr_to_double(General.readByteArr(buffer_double, scoreStream));//attention: when rerank length < total photoNum in db, then the tailer part do not have scores! 
			int[] rankedDoc=General.read_intArr_littleEndian(dataNum_inRank, rankStream);
			double[] rankedScores=General.read_doubleArr_littleEndian(dataNum_inScore, scoreStream);
			ArrayList<Integer> oriIDs=new ArrayList<Integer>(rankedDoc.length); ArrayList<Float> scores=new ArrayList<Float>(rankedDoc.length); 
			for (int j = 0; j < rankedDoc.length; j++) {
				float score=(j<dataNum_inScore)?(float) rankedScores[j]:0;
				oriIDs.add(innerPhoInds_db[rankedDoc[j]]);
				scores.add(score);
			}
			//eval
			imagR_eval.add_oneRank(queryName, new IntList_FloatList(oriIDs,scores), new Disp(i%dispInter==0, "---", null));			
		}
		General.dispInfo(outputStream_report, imagR_eval.getDataSetInfo());	
		General.dispInfo(outputStream_report, "\t "+imagR_eval.getRes()+"  ......  "+General.dispTime(System.currentTimeMillis()-startTime, "min"));	
	}

	@SuppressWarnings("unchecked")
	public static void transferRankFromHPMbin_to_MapFileRank(int topRank, String resulRank, String resulScore, String rankMapFilePath, String L_to_S_forDoc_path, String innerPhoInds_dbPath, String innerPhoInds_queryPath, 
			PrintWriter outputStream_report, long startTime) throws Exception {
		HashMap<Integer, Integer> L_to_S_forDoc=(HashMap<Integer, Integer>) General.readObject(L_to_S_forDoc_path);
		Integer[] innerPhoInds_db=(Integer[]) General.readObject(innerPhoInds_dbPath);
		Integer[] innerPhoInds_query=(Integer[]) General.readObject(innerPhoInds_queryPath);
		//read rank
		DataInputStream rankStream = new DataInputStream( new FileInputStream(resulRank));
		DataInputStream scoreStream = new DataInputStream( new FileInputStream(resulScore));
		byte[] buffer_double = new byte[8]; 
		int queryNum=(int) General.byteArr_to_double(General.readByteArr(buffer_double, rankStream));
		int queryNum_inScore=(int) General.byteArr_to_double(General.readByteArr(buffer_double, scoreStream));
		General.Assert(queryNum==queryNum_inScore, "queryNum should == queryNum_inScore, here queryNum:"+queryNum+", queryNum_inScore:"+queryNum_inScore);
		ArrayList<Integer> queryIDs=new ArrayList<Integer>(); ArrayList<IntList_FloatList> ranks=new ArrayList<IntList_FloatList>(queryNum*2); 
		for (int i = 0; i < queryNum; i++) {
			int queryName=innerPhoInds_query[i];
			//get rank
			int dataNum_inRank=(int) General.byteArr_to_double(General.readByteArr(buffer_double, rankStream));
			int dataNum_inScore=(int) General.byteArr_to_double(General.readByteArr(buffer_double, scoreStream));//attention: when rerank length < total photoNum in db, then the tailer part do not have scores! 
			int[] rankedDoc=General.read_intArr_littleEndian(dataNum_inRank, rankStream);
			double[] rankedScores=General.read_doubleArr_littleEndian(dataNum_inScore, scoreStream);
			ArrayList<Integer> oriIDs=new ArrayList<Integer>(rankedDoc.length); ArrayList<Float> scores=new ArrayList<Float>(rankedDoc.length); 
			int actRankLength=Math.min(topRank, rankedDoc.length);
			for (int j = 0; j < actRankLength; j++) {
				float score=(j<dataNum_inScore)?(float) rankedScores[j]:0;
				oriIDs.add(L_to_S_forDoc.get(innerPhoInds_db[rankedDoc[j]]));
				scores.add(score);
			}
			queryIDs.add(queryName);
			ranks.add(new IntList_FloatList(oriIDs, scores));
		}
		rankStream.close(); scoreStream.close();
		//sort queryID
		ArrayList<Integer> queryIDs_sorted=new ArrayList<Integer>(); ArrayList<IntList_FloatList> ranks_sorted=new ArrayList<IntList_FloatList>(queryNum*2); 
		General_IR.rank_get_AllSortedDocIDs_treeSet(ranks, queryIDs, ranks_sorted, queryIDs_sorted, "ASC");
		//save to mapFile
		Configuration conf=new Configuration();
		MapFile.Writer rank_mapFile=new MapFile.Writer(conf, new Path(rankMapFilePath), MapFile.Writer.keyClass(IntWritable.class), MapFile.Writer.valueClass(IntList_FloatList.class));
		for (int i = 0; i < queryIDs_sorted.size(); i++) {
			rank_mapFile.append(new IntWritable(queryIDs_sorted.get(i)), ranks_sorted.get(i));
		}
		rank_mapFile.close(); 
		General.dispInfo(outputStream_report, "transfer Rank From HPMbin to MapFileRank finished, tot "+queryNum+" querys  ......  "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
	}
}
