package BenchMarkTest;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;

import MyAPI.General.General;
import MyAPI.General.General_IR;
import MyCustomedHaoop.ValueClass.PhotoAllFeats;
import MyCustomedHaoop.ValueClass.SURFpointVWs;

public class mimicHPM_notForLargeScale {
	/*
	 * run hpm in linux
	 * 
	 * Oxford: 		krenew -s -- java -Xms4g -Xmx10g -cp mimicHPM.jar:$JAPI/MyAPI.jar:$JAPI/hadoop-core-0.20.2-cdh3u4.jar:$JAPI/EJML_boof0.9.jar:$JAPI/commons-math3-3.2.jar:$JAPI/mahout-core-0.8-SNAPSHOT-job.jar:$JAPI/MyCustomedHadoop.jar:$JAPI/sqlite4java.jar:$JAPI/BoofCV0.9.jar:$JAPI/GeoRegression_boof0.9.jar:$JAPI/libpja_boof0.9.jar:$JAPI/commons-compress-1.7.jar BenchMarkTest.mimicHPM OxfordBuilding_groundTruth.hashMap OxfordBuilding_junks.hashMap OxfordBuilding_buildingInd_Name.hashMap Oxford_ori5K_SelPhos_S_to_L.intArr _Oxford_5K_CutQ _Oxford_CutQ 1000
	 * Bacelona: 	krenew -s -- java -Xms4g -Xmx10g -cp mimicHPM.jar:$JAPI/MyAPI.jar:$JAPI/hadoop-core-0.20.2-cdh3u4.jar:$JAPI/EJML_boof0.9.jar:$JAPI/commons-math3-3.2.jar:$JAPI/mahout-core-0.8-SNAPSHOT-job.jar:$JAPI/MyCustomedHadoop.jar:$JAPI/sqlite4java.jar:$JAPI/BoofCV0.9.jar:$JAPI/GeoRegression_boof0.9.jar:$JAPI/libpja_boof0.9.jar:$JAPI/commons-compress-1.7.jar BenchMarkTest.mimicHPM Barcelona_groundTruthBuildingID.hashMap NoJunks NoBuildingInds Barcelona_ori1K_SelPhos_S_to_L.intArr _Barcelona_1K _Barcelona 1000
	 * Herve:		krenew -s -- java -Xms4g -Xmx10g -cp mimicHPM.jar:$JAPI/MyAPI.jar:$JAPI/hadoop-core-0.20.2-cdh3u4.jar:$JAPI/EJML_boof0.9.jar:$JAPI/commons-math3-3.2.jar:$JAPI/mahout-core-0.8-SNAPSHOT-job.jar:$JAPI/MyCustomedHadoop.jar:$JAPI/sqlite4java.jar:$JAPI/BoofCV0.9.jar:$JAPI/GeoRegression_boof0.9.jar:$JAPI/libpja_boof0.9.jar:$JAPI/commons-compress-1.7.jar BenchMarkTest.mimicHPM Herve_groundTruth.hashMap NoJunks NoBuildingInds Herve_ori1.5K_SelPhos_S_to_L.intArr _Herve_1.5K _Herve 1000
	 * 
	 */
	
	public static void main(String[] args) throws Exception {
		//dataset info
		String dataSetInfoPath="/tudelft.net/staff-bulk/ewi/insy/mmc/XinchaoLi/CVPR15/";
		String grondTruthPath=dataSetInfoPath+args[0];//"OxfordBuilding_groundTruth.hashMap"
		String junkPath=args[1].equalsIgnoreCase("NoJunks")?null:dataSetInfoPath+args[1];//"OxfordBuilding_junks.hashMap"
		String buildingInd_NamePath=args[2].equalsIgnoreCase("NoBuildingInds")?null:dataSetInfoPath+args[2]; //"OxfordBuilding_buildingInd_Name.hashMap";
		String s_to_L=dataSetInfoPath+args[3];//"Oxford_ori5K_SelPhos_S_to_L.intArr"
		//retrieval workDir
		String basePath="/tudelft.net/staff-bulk/ewi/insy/mmc/XinchaoLi/CVPR15/mimicHPM/";
		String dataLabel=args[4]; //"_Oxford_5K_CutQ"
		String dataLabel_query=args[5]; //"_Oxford_CutQ"
		int rerank=Integer.valueOf(args[6]); //1000
		int vwNum=100000;
		String workDir=basePath+"VW"+vwNum+dataLabel+"/";
		General.makeFolder(workDir);
		String hpmBinPath="/home/nfs/xinchaoli/Code/HPM/hpm";
		String[] MapFile_label={"SURFFeat_VW100k_SA_DataBase"+dataLabel, "SURFFeat_VW100k_SA_Query"+dataLabel_query};
		String paramPath=workDir+"Rerank"+rerank+dataLabel+".param";
		String indexPath=workDir+"SURF"+dataLabel+".index";
		String rankPath=workDir+"Rerank"+rerank+dataLabel+".rank";
		PrintWriter outputStream_report=new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"mimicHPM_SURF"+"VW"+vwNum+dataLabel+"_Re"+rerank+".report", false), "UTF-8"),true); 
		long startTime=System.currentTimeMillis();
		//prepare feat files
		double[][] minMaxScales=new double[2][]; 
		String[][] phoInfoPaths=new String[2][];//phoVWPath, phoGeoPath, phoMaxDimPath
		for (int i=0; i<MapFile_label.length;i++) {
			String one = MapFile_label[i];
			String mapFile=basePath+one+"/part-r-00000";
			String binFeatPath=workDir+one+"_BinFeats"+"/";
			String phoVWPath=workDir+one+"_vwFiles.txt";
			String phoGeoPath=workDir+one+"_geoFiles.txt";
			String phoMaxDimPath=workDir+one+".phoMaxSize";
			String phoInnerInds=workDir+one+".innerPhoInds";
			minMaxScales[i] = prepareDataForHPMbin( mapFile,  binFeatPath, phoVWPath, phoGeoPath, phoMaxDimPath, phoInnerInds, outputStream_report, startTime);
			phoInfoPaths[i]=new String[]{phoVWPath, phoGeoPath, phoMaxDimPath, phoInnerInds};
		}
		//make params
		makeParams(rerank, minMaxScales[0][0], minMaxScales[0][1], paramPath, phoInfoPaths[0][2], phoInfoPaths[1][2], outputStream_report);
		//run HPM bin
		String resulRank= runHPM(vwNum, indexPath, paramPath, hpmBinPath, phoInfoPaths[0][0], phoInfoPaths[0][1], phoInfoPaths[1][0], phoInfoPaths[1][1], outputStream_report, startTime);
		//rename rank name
		General.runSysCommand(Arrays.asList("mv",resulRank,rankPath), null, true);
		//analysis results
		readRankFromHPMbin(dataLabel, rankPath, phoInfoPaths[0][3], phoInfoPaths[1][3], s_to_L, grondTruthPath, junkPath, buildingInd_NamePath, outputStream_report);
		//done
		outputStream_report.close();
	}
	
	public static double[] prepareDataForHPMbin(String MapFile, String binFeatPath, String phoVWPath, String phoGeoPath, String phoMaxDimPath, String phoInnerInds, PrintWriter outputStream_report, long startTime) throws Exception {
		General.dispInfo(outputStream_report, "start prepareDataForHPMbin from "+MapFile);
		Configuration conf = new Configuration();
	    FileSystem hdfs  = FileSystem.get(conf);
		MapFile.Reader MapFileReader=new MapFile.Reader(hdfs, MapFile, conf); //MapFile_path+"/part-r-00000"
		IntWritable Key= new IntWritable();
		PhotoAllFeats Value= new PhotoAllFeats();
		double minScale=Integer.MAX_VALUE; double maxScale=-1; 
		int phoNum=0;
		if (!new File(binFeatPath).exists()) {//feat alreay exist, only need minScale and maxScale for params
			General.makeFolder(binFeatPath);
			float strenth_notUsed=(float) 1; int geoMetricNum=5; 
			PrintWriter outputStream_vw=new PrintWriter(new OutputStreamWriter(new FileOutputStream(phoVWPath,false), "UTF-8"),true); 
			PrintWriter outputStream_geo=new PrintWriter(new OutputStreamWriter(new FileOutputStream(phoGeoPath,false), "UTF-8"),true); 
			LinkedList<Integer> pho_maxSize=new LinkedList<Integer>(); 
			LinkedList<Integer> innerPhoInds=new LinkedList<Integer>(); 
			while(MapFileReader.next(Key, Value)){ //loop over all queries, key-value(query-rank)
				int phoName=Key.get();
				innerPhoInds.add(phoName);
				//get maxSize
				pho_maxSize.add(Math.max(Value.width, Value.height));
				//get vws and geos
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
				DataOutputStream bin_vws = new DataOutputStream(new FileOutputStream(binFeatPath+phoName+".vw"));
				DataOutputStream bin_geometric = new DataOutputStream(new FileOutputStream(binFeatPath+phoName+".geo"));
				bin_vws.write(buffer_vws.array()); bin_geometric.write(buffer_geo.array());
				bin_vws.close(); bin_geometric.close();
				//record bin file name
				outputStream_vw.println(binFeatPath+phoName+".vw");
				outputStream_geo.println(binFeatPath+phoName+".geo");
				phoNum++;
			}
			outputStream_vw.close();
			outputStream_geo.close();
			//save photo max size
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
			General.dispInfo(outputStream_report, "prepareDataForHPMbin done! phoNum:"+phoNum+", minScale:"+minScale+", maxScale:"+maxScale+" ..... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
		}else {
			while(MapFileReader.next(Key, Value)){ //loop over all queries, key-value(query-rank)
				for (SURFpointVWs onePoint : Value.feats.getArr()) {
					//updata minScale and maxScale
					minScale=Math.min(minScale, onePoint.point.scale);
					maxScale=Math.max(maxScale, onePoint.point.scale);
				}
			}
			//done
			General.dispInfo(outputStream_report, "prepareDataForHPMbin done! feat alreay exist, only need minScale and maxScale for params! phoNum:"+phoNum+", minScale:"+minScale+", maxScale:"+maxScale+" ..... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
		}
		MapFileReader.close();
		return new double[]{minScale, maxScale};
		
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
	
	public static String runHPM(int vwNum, String indexPath, String paramPath, String hpmBinPath, String phoVWPath_db, String phoGeoPath_db, String phoVWPath_query, String phoGeoPath_query, PrintWriter outputStream_report, long startTime) throws Exception {
		//******* run HPM bin  ***********
		//1.index
		//./hpm -nvw 100000 -ofile SURF_VW100k_5KCutQ.index -p params -vw SURFFeat_VW100k_SA_Oxford5KCutQ_DataBase_vwFiles.txt -feat SURFFeat_VW100k_SA_Oxford5KCutQ_DataBase_geoFiles.txt
		if (new File(indexPath).exists()) {
			General.dispInfo(outputStream_report, "indexPath:"+indexPath+" is already exist, no need to build .... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
		}else {
			General.runSysCommand(Arrays.asList(
					hpmBinPath, "-nvw", ""+vwNum, "-ofile", indexPath, "-p", paramPath, "-vw", phoVWPath_db, "-feat", phoGeoPath_db
					), null, true);
			General.dispInfo(outputStream_report, "build index done! "+indexPath+" .... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
		}
		
		//2.retrieval
		//./hpm -nvw 100000 -ofile SURF_VW100k_5KCutQ.index -p params -qvw SURFFeat_VW100k_SA_OxfordCutQ_Query_vwFiles.txt -qfeat SURFFeat_VW100k_SA_OxfordCutQ_Query_geoFiles.txt
		General.runSysCommand(Arrays.asList(
				hpmBinPath, "-nvw", ""+vwNum, "-ofile", indexPath, "-p", paramPath, "-qvw", phoVWPath_query, "-qfeat", phoGeoPath_query
				), null, true);
		General.dispInfo(outputStream_report, "retrieval done! .... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
		
		return phoVWPath_query+".ranks.bin"; //result rank file
	}

	@SuppressWarnings("unchecked")
	public static void readRankFromHPMbin(String dataLabel, String resulRank, String innerPhoInds_dbPath, String innerPhoInds_queryPath, String s_to_L, String grondTruthPath, String junkPath, String buildingInd_NamePath, PrintWriter outputStream_report) throws Exception {

		int[] s_to_l=(int[]) General.readObject(s_to_L);
		HashMap<Integer, HashSet<Integer>> groundTrue=(HashMap<Integer, HashSet<Integer>>) General.readObject(grondTruthPath);
		HashMap<Integer, HashSet<Integer>> junks=(HashMap<Integer, HashSet<Integer>>) General.readObject(junkPath);
		HashMap<Integer, HashSet<Integer>> buildingInd_Name=(HashMap<Integer, HashSet<Integer>>) General.readObject(buildingInd_NamePath);
		int dispInter=20;
		DataInputStream rank = new DataInputStream( new FileInputStream(resulRank));
		Integer[] innerPhoInds_db=(Integer[]) General.readObject(innerPhoInds_dbPath);
		Integer[] innerPhoInds_query=(Integer[]) General.readObject(innerPhoInds_queryPath);
		byte[] buffer_double = new byte[8]; 
		int queryNum=(int) General.byteArr_to_double(General.readByteArr(buffer_double, rank));
		float MAP=0; float HR_1=0; 
		for (int i = 0; i < queryNum; i++) {
			int queryName=innerPhoInds_query[i];
			//get rank
			int dataNum=(int) General.byteArr_to_double(General.readByteArr(buffer_double, rank));
			int[] rankedDoc=General.read_intArr_littleEndian(dataNum, rank);
			ArrayList<Integer> oriIDs=new ArrayList<Integer>(rankedDoc.length);
			for (int one : rankedDoc) {
				oriIDs.add(s_to_l[innerPhoInds_db[one]]);
			}
			HashSet<Integer> relPhos=null;
			String queryInfoToShow=", queryName:"+queryName;
			if (dataLabel.contains("Oxford")) {
				int buildingInd=queryName/1000;//oxford use 1000 to group
				relPhos=groundTrue.get(buildingInd);
				General.dispInfo_ifNeed(relPhos==null, "", "err! relPhos==null, queryName:"+queryName+", buildingInd:"+buildingInd+", groundTrue:"+groundTrue);
				oriIDs.removeAll(junks.get(buildingInd));//remove junks
				queryInfoToShow+=", buildingName:"+buildingInd_Name.get(buildingInd);
			}else if (dataLabel.contains("Barcelona")){
				int buildingInd=queryName/10000;//Barcelona use 10000 to group
				relPhos=groundTrue.get(buildingInd);
				General.dispInfo_ifNeed(relPhos==null, "", "err! relPhos==null, queryName:"+queryName+", buildingInd:"+buildingInd+", groundTrue:"+groundTrue);
				queryInfoToShow+=", buildingName:"+buildingInd;
			}else {
				relPhos=groundTrue.get(queryName);
			}
			float AP=General_IR.AP_smoothed(relPhos, oriIDs);
			boolean cartoCorrect=relPhos.contains(oriIDs.get(0));
			MAP+=AP;
			HR_1+=cartoCorrect?1:0;
			General.dispInfo_ifNeed(i%dispInter==0,"---", "current queryNums:"+i+queryInfoToShow+", AP:"+AP+", 1NN-cartoCorrect:"+cartoCorrect);
		}
		General.dispInfo(outputStream_report, "\t tot "+queryNum+" querys, MAP:"+MAP/queryNum+", HR_1:"+HR_1/queryNum);	
	}
}
