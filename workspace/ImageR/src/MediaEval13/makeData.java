package MediaEval13;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.tools.zip.ZipEntry;
import org.apache.tools.zip.ZipOutputStream;

import MyAPI.General.General;
import MyAPI.General.General_JAI;
import MyAPI.General.General_Lire;
import MyAPI.General.General_http;
import MyAPI.Obj.Disp;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
import MyCustomedHaoop.ValueClass.FloatArr;

import com.almworks.sqlite4java.SQLParts;
import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.sun.org.apache.bcel.internal.classfile.Field;

public class makeData{
	
	public static void main(String[] args) throws Exception {
		mapTaskData();
//		checkData();
//		getLostPhotos();
//		extractTestPhotos();
		
//		getNewAddedMissingBloPhos();
//		savePhoToZip_NewAddedMissingBloPhos();
//		savePhoFeatStr_NewAddedMissingBloPhos();
		
//		checkFeatExtractionCode();
	}
	
	@SuppressWarnings("unchecked")
	public static void mapTaskData() throws IOException, SQLiteException, ClassNotFoundException, InterruptedException {
		
		String basePath="Q:/FlickrCrawler/MetaData/";
//		String basePath="/tmp/";
		
		String savePath="O:/MediaEval13/";
//		String savePath="/tudelft.net/staff-bulk/ewi/insy/D-MIRLab/XinchaoLi/MediaEval13/";
		
		String oriDataPath="P:/Ori-Data/Medieval task/2013/";
//		String oriDataPath=savePath; 
		
		//set MetaData DataBase
		String dataBase=basePath+"FlickrPhotoMeta_new.db";
		SQLParts sql; SQLiteStatement stmt,stmt_missingBlock,stmt_current;
		String oneLine;
////		PrintWriter outputStream_Report = new PrintWriter(new OutputStreamWriter(new FileOutputStream(savePath+"makeData.report",false), "UTF-8"),true); 
//		PrintWriter outputStream_Report=new PrintWriter(new OutputStreamWriter(new FileOutputStream(savePath+"makeData_userIDs.report",false), "UTF-8"),true); 
//		PrintWriter outputStream_Report=new PrintWriter(new OutputStreamWriter(new FileOutputStream(savePath+"makeData_tags.report",false), "UTF-8"),true); 
//		General.dispInfo(outputStream_Report, "start processing....");
		
		//read dataBase
		sql = new SQLParts("SELECT * FROM photoMeta WHERE photoID=?");  //SELECT * FROM photoMeta WHERE photoIndex=?
		SQLiteConnection db_read = new SQLiteConnection(new File(dataBase));
		SQLiteConnection db_read_missingBlock = new SQLiteConnection(new File(basePath+"patch/FlickrPhotoMeta_new_missingBlocks.db"));
//		SQLiteConnection db_read_missingBlock = new SQLiteConnection(new File(basePath+"FlickrPhotoMeta_new_missingBlocks.db"));
		db_read.open(); stmt = db_read.prepare(sql);
		db_read_missingBlock.open(); stmt_missingBlock=db_read_missingBlock.prepare(sql);
				
//		//get trainTestNum
//		int procPhotos=0;
//		String[] dataPath={"training_latlng","training_missingBlocks_latlng","test5.txt","test5_missingBlocks.txt"};
//		int[] trainTestNum=new int[2];
//		for (int i = 0; i < dataPath.length; i++) {
//			BufferedReader inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(oriDataPath+dataPath[i]), "UTF-8"));
//			inStr_photoMeta.readLine();//1st line is not useful
//			int train_test=i/2;
//			while ((oneLine=inStr_photoMeta.readLine())!=null) {
//				trainTestNum[train_test]++;
//				procPhotos++;
//			}
//			inStr_photoMeta.close();
//		}
//		General.Assert(procPhotos==trainTestNum[0]+trainTestNum[1], "error, procPhotos should == trainTestNum[0]+trainTestNum[1]");
//		General.dispInfo(outputStream_Report, "total procPhotos:"+procPhotos+", trainTestNum: "+General.IntArrToString(trainTestNum, "_"));
		
		long startTime=System.currentTimeMillis(); //start time 
		
		//make latlons, s_to_photoID, L_to_S_trainTest
//		float[][] latlons=new float[2][procPhotos];  String[] s_to_photoID=new String[procPhotos];
//		long[] userIDs_0=new long[procPhotos]; int[] userIDs_1=new int[procPhotos];
//		ArrayList<HashMap<Integer, Integer>> L_to_S_trainTest=new ArrayList<HashMap<Integer, Integer>>(2);
//		for (int i = 0; i < 2; i++) {
//			L_to_S_trainTest.add(new HashMap<Integer, Integer>(trainTestNum[i]));
//		}
//		int photoIndex_S=0; int dispInter=100*1000; int[] noTagNum=new int[2]; int[] oneTagNum=new int[2];
//		for (int i = 0; i < dataPath.length; i++) {
//			BufferedReader inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(oriDataPath+dataPath[i]), "UTF-8"));
//			inStr_photoMeta.readLine();//1st line is not useful
//			int train_test=i/2;
//			if (i%2==0) {
//				stmt_current=stmt;
//			}else {//for missing block photos
//				stmt_current=stmt_missingBlock;
//			}
//			while ((oneLine=inStr_photoMeta.readLine())!=null) {
//				String photoID=oneLine.split(" ")[0];
//				stmt_current.bind(1, photoID);
//				int photoIndex_L=-1; float[] latlon=null; boolean photoExist=false; long userID_0 = 0; int userID_1 = 0;
//				while (stmt_current.step()) {
////					photoIndex_L = Integer.valueOf(stmt_current.columnString(0)); //sql, column start from 0
////					latlon= General.StrArrToFloatArr(stmt_current.columnString(2).split(","));
////					String userID=stmt_current.columnString(4);
////					userID_0=Long.valueOf(userID.split("@")[0]);
////					userID_1=Integer.valueOf(userID.split("@")[1].substring(1,3));
//					String tags=stmt_current.columnString(6);
//					int tagNum=tags.split(" ").length;
//					if (tagNum==1) {
//						if (tags.equalsIgnoreCase("")) {
//							noTagNum[train_test]++;
//						}else {
//							oneTagNum[train_test]++;
//						}
//					}
//					photoExist=true;
//				}
//				stmt_current.reset();
//				General.Assert(photoExist, "photoID:"+photoID+" from dataPath:"+dataPath[i]+" is not exist in current db file!");
//				//save
////				latlons[0][photoIndex_S]=latlon[0]; latlons[1][photoIndex_S]=latlon[1];
////				L_to_S_trainTest.get(train_test).put(photoIndex_L, photoIndex_S);
////				s_to_photoID[photoIndex_S]=photoID;
////				userIDs_0[photoIndex_S]=userID_0; userIDs_1[photoIndex_S]=userID_1;
//				//
//				photoIndex_S++;
//				//disp
//	    		if((photoIndex_S)%dispInter==0){ 							
//					General.dispInfo(outputStream_Report, "tran test data making.. "+photoIndex_S+" photos finished!! "+new DecimalFormat("00%").format((double)photoIndex_S/procPhotos)+", ......"+General.dispTime(System.currentTimeMillis()-startTime, "min"));
//					outputStream_Report.flush();
//	    		}
//			}
//			inStr_photoMeta.close();
//		}
//		stmt.dispose();db_read.dispose();
//		stmt_missingBlock.dispose();db_read_missingBlock.dispose();
//		
//		//get tagNum statistic
//		General.dispInfo(outputStream_Report, "in trainData, "+General.getPercentInfo(noTagNum[0], trainTestNum[0], "0.0%")+", no tag. "
//							+General.getPercentInfo(oneTagNum[0], trainTestNum[0], "0.0%")+", one tag.");
//		General.dispInfo(outputStream_Report, "in testData, "+General.getPercentInfo(noTagNum[1], trainTestNum[1], "0.0%")+", no tag. "
//				+General.getPercentInfo(oneTagNum[1], trainTestNum[1], "0.0%")+", one tag.");
//		General.dispInfo(outputStream_Report, "in total, "+General.getPercentInfo(General.sum_IntArr(noTagNum), General.sum_IntArr(trainTestNum), "0.0%")+", no tag. "
//							+General.getPercentInfo(General.sum_IntArr(oneTagNum), General.sum_IntArr(trainTestNum), "0.0%")+", one tag.");
		
//		photoIndex_S=0;
//		//check training data latlon
//		for (int i = 0; i < 2; i++) {
//			BufferedReader inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(oriDataPath+dataPath[i]), "UTF-8"));
//			inStr_photoMeta.readLine();//1st line is not useful
//			while ((oneLine=inStr_photoMeta.readLine())!=null) {
//				float[] photoID_lat_lon=General.StrArrToFloatArr(oneLine.split(" "));
//				General.Assert(General.isEqual_float(latlons[0][photoIndex_S], photoID_lat_lon[1], (float) 0.0001), 
//						"lat is not the same, photoIndex_S:"+photoIndex_S+", "+latlons[0][photoIndex_S]+", "+photoID_lat_lon[1]);
//				General.Assert(General.isEqual_float(latlons[1][photoIndex_S], photoID_lat_lon[2], (float) 0.0001), 
//						"lon is not the same, photoIndex_S:"+photoIndex_S+", "+latlons[1][photoIndex_S]+", "+photoID_lat_lon[2]);
//				photoIndex_S++;
//			}
//			inStr_photoMeta.close();
//		}
		
		//save latlons, s_to_photoID, L_to_S_trainTest, userIDs
//		General.writeObject(savePath+"MEval13_latlons.floatArr", latlons);
//		General.writeObject(savePath+"MEval13_s_to_photoID.strArr", s_to_photoID);
//		General.writeObject(savePath+"MEval13_L_to_S_train.hashMap", L_to_S_trainTest.get(0));
//		General.writeObject(savePath+"MEval13_L_to_S_test.hashMap", L_to_S_trainTest.get(1));
//		General.writeObject(savePath+"MEval13_userIDs_0.long", userIDs_0);
//		General.writeObject(savePath+"MEval13_userIDs_1.int", userIDs_1);
//		General.dispInfo(outputStream_Report,"save latlons, s_to_photoID, L_to_S_trainTest, userIDs done! total time for all: "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
//		outputStream_Report.close();
		
//		//make S_to_S_test.hashSet
//		HashMap<Integer, Integer> L_to_S_test=(HashMap<Integer, Integer>) General.readObject(savePath+"MEval13_L_to_S_test.hashMap");
//		HashMap<Integer, Integer> S_to_S_test=new HashMap<Integer, Integer>(L_to_S_test.size());
//		for (Entry<Integer, Integer> oneKV : L_to_S_test.entrySet()) {
//			S_to_S_test.put(oneKV.getValue(), oneKV.getValue());
//		}
//		General.Assert(L_to_S_test.size()==S_to_S_test.size(), "err! size of L_to_S_test and S_to_S_test should be the same! L_to_S_test:"+L_to_S_test.size()+", S_to_S_test:"+S_to_S_test.size());
//		System.out.println("S_to_S_test:"+S_to_S_test.size());
//		General.writeObject(savePath+"MEval13_S_to_S_test.hashMap", S_to_S_test);
		
//		//merge train and test selected photos
//		HashMap<Integer, Integer> L_to_S_train=(HashMap<Integer, Integer>) General.readObject(savePath+"MEval13_L_to_S_train.hashMap");
//		HashMap<Integer, Integer> L_to_S_test=(HashMap<Integer, Integer>) General.readObject(savePath+"MEval13_L_to_S_test.hashMap");
//		HashMap<Integer, Integer> L_to_S=new HashMap<Integer, Integer>(L_to_S_train.size()+L_to_S_test.size());
//		L_to_S.putAll(L_to_S_train); L_to_S.putAll(L_to_S_test);
//		General.Assert(L_to_S.size()==(L_to_S_train.size()+L_to_S_test.size()), 
//				"megered L_to_S should be:"+(L_to_S_train.size()+L_to_S_test.size())+", act:"+L_to_S.size());
//		General.writeObject(savePath+"MEval13_L_to_S.hashMap", L_to_S);
		
//		//make S_to_L
//		HashMap<Integer, Integer> L_to_S=(HashMap<Integer, Integer>) General.readObject(savePath+"MEval13_L_to_S.hashMap");
//		int[] S_to_L=new int[L_to_S.size()];
//		for (Entry<Integer, Integer> oneKV : L_to_S.entrySet()) {
//			S_to_L[oneKV.getValue()]=oneKV.getKey();
//		}
//		General.writeObject(savePath+"MEval13_S_to_L.intArr", S_to_L);
			
//		//make developemnt querys
//		HashMap<Integer, Integer> L_to_S_train=(HashMap<Integer, Integer>) General.readObject(savePath+"MEval13_L_to_S_train.hashMap");
//		Random rand=new Random();
//		HashMap<Integer, Integer> L_to_S_devQ=General.randSplitHashMap(rand, L_to_S_train , 0, 300*1000).get(0);
//		General.writeObject(savePath+"MEval13_L_to_S_devQ.hashMap", L_to_S_devQ);
		
		//make small querys
		HashMap<Integer, Integer> L_to_S_test5=(HashMap<Integer, Integer>) General.readObject(savePath+"MEval13_L_to_S_test.hashMap");
		HashMap<Integer, Integer> L_to_S_test1=new HashMap<Integer, Integer>();
		String[] dataPath={"test1.txt","test1_missingBlocks.txt"};
		int photoNum=0; int dispIn=1000;
		for (int i = 0; i < dataPath.length; i++) {
			BufferedReader inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(oriDataPath+dataPath[i]), "UTF-8"));
			inStr_photoMeta.readLine();//1st line is not useful
			if (i%2==0) {
				stmt_current=stmt;
			}else {//for missing block photos
				stmt_current=stmt_missingBlock;
			}
			while ((oneLine=inStr_photoMeta.readLine())!=null) {
				String photoID=oneLine.split(" ")[0];
				stmt_current.bind(1, photoID);
				int photoIndex_L=-1; boolean photoExist=false; 
				while (stmt_current.step()) {
					photoIndex_L = Integer.valueOf(stmt_current.columnString(0)); //sql, column start from 0
					photoExist=true;
				}
				stmt_current.reset();
				General.Assert(photoExist, "photoID:"+photoID+" from dataPath:"+dataPath[i]+" is not exist in current db file!");
				//save
				L_to_S_test1.put(photoIndex_L, L_to_S_test5.get(photoIndex_L));
				//
				photoNum++;
				//disp
	    		if((photoNum)%dispIn==0){ 							
					General.dispInfo(null, "test1 data making.. "+photoNum+" photos finished!! ......"+General.dispTime(System.currentTimeMillis()-startTime, "min"));
	    		}
			}
			inStr_photoMeta.close();
		}
		General.dispInfo(null, "test1 data making done!!  "+photoNum+" photos ......"+General.dispTime(System.currentTimeMillis()-startTime, "min"));
		stmt.dispose();db_read.dispose();
		stmt_missingBlock.dispose();db_read_missingBlock.dispose();
		General.writeObject(savePath+"MEval13_L_to_S_test1.hashMap", L_to_S_test1);
	}

	@SuppressWarnings("unchecked")
	public static void checkData() throws IOException, SQLiteException, ClassNotFoundException, InterruptedException {
		String savePath="O:/MediaEval13/";
		String basePath="Q:/FlickrCrawler/MetaData/";
		
		String dataBase=basePath+"FlickrPhotoMeta_new.db";
		SQLParts sql; SQLiteStatement stmt,stmt_missingBlock;
		
		PrintWriter outputStream = new PrintWriter(new OutputStreamWriter(new FileOutputStream(savePath+"missingPhos.txt"), "UTF-8")); 
		
		Configuration conf = new Configuration();
		FileSystem hdfs  = FileSystem.get(conf);
		
		//read dataBase
		sql = new SQLParts("SELECT * FROM photoMeta WHERE photoIndex=?");  //SELECT * FROM photoMeta WHERE photoIndex=?
		SQLiteConnection db_read = new SQLiteConnection(new File(dataBase));
		SQLiteConnection db_read_missingBlock = new SQLiteConnection(new File(basePath+"patch/FlickrPhotoMeta_new_missingBlocks.db"));
		db_read.open(); stmt = db_read.prepare(sql);
		db_read_missingBlock.open(); stmt_missingBlock=db_read_missingBlock.prepare(sql);
				
//		//load latlons, s_to_photoID, L_to_S_trainTest
//		float[][] latlons=(float[][]) General.readObject(savePath+"latlons.floatArr");
		HashMap<Integer, Integer> L_to_S=(HashMap<Integer, Integer>) General.readObject(savePath+"MEval13_L_to_S.hashMap");
		
		//set SequenceFile.Reader
		String SeqFile_path=savePath+"indexedPhos_all70M/part-r-00000/data";
		SequenceFile.Reader seqFile=new SequenceFile.Reader(hdfs, new Path(SeqFile_path), conf);
		IntWritable key = new IntWritable();
		IntWritable value = new IntWritable();
		
		//********* check exist data ******************
		boolean[] isExist=new boolean[73*1000*1000]; int existNum_totCol=0;
		while (seqFile.next(key, value)) {//key: index_L,  value: index_S
			int index_L=key.get();
			isExist[index_L]=true;
			existNum_totCol++;
		}
		seqFile.close();
		General.dispInfo(outputStream,"existNum_totCol:"+existNum_totCol);
		
		HashSet<String> noExist=new HashSet<String>(); int noExist_inOri = 0, noExist_inMisBlo=0;
		for (Integer index_L : L_to_S.keySet()) {
			boolean isExistInDB=false;
			if (isExist[index_L]==false) {
				//check stmt
				stmt.bind(1, index_L);
				while (stmt.step()) {
					//check url
//					String url = stmt.columnString(5); //sql, column start from 0
//					BufferedImage image= General_JAI.readImage_url(new URL(url));
//					ImageIO.write(image, "jpg", new File("O:/MediaEval13/lostPhotos/"+index_L+".jpg"));
					//save photoID
					outputStream.println(stmt.columnString(1));
					noExist.add(stmt.columnString(1));
					isExistInDB=true;
					noExist_inOri++;
				}
				stmt.reset();
				//check stmt_missingBlock
				if (!isExistInDB) {
					stmt_missingBlock.bind(1, index_L);
					while (stmt_missingBlock.step()) {
						//check url
//						String url = stmt.columnString(5); //sql, column start from 0
//						BufferedImage image= General_JAI.readImage_url(new URL(url));
//						ImageIO.write(image, "jpg", new File("O:/MediaEval13/lostPhotos/"+index_L+".jpg"));
						//save photoID
						outputStream.println(stmt_missingBlock.columnString(1));
						noExist.add(stmt.columnString(1));
						isExistInDB=true;
						noExist_inMisBlo++;
					}
					stmt_missingBlock.reset();
				}
				if (!isExistInDB) {
					General.dispInfo(outputStream, "index_L:"+index_L+" not exist in DB!");
				}
			}
		}
		General.dispInfo(outputStream,  "tot num:"+L_to_S.size()+", noExist:"+noExist.size()+", noExist_inOri:"+noExist_inOri+", noExist_inMisBlo:"+noExist_inMisBlo);
		outputStream.close();
		
		General.writeObject(savePath+"MEv13_noExistPhoIDs.hashSet", noExist);
		
		stmt.dispose();db_read.dispose();
		stmt_missingBlock.dispose();db_read_missingBlock.dispose();
	}

	@SuppressWarnings("unchecked")
	public static void getLostPhotos() throws Exception {
		String localPath="/media/sf_O_Research/MediaEval13/";
		String remotePath="/media/MyBook/PlacingTask2013_Images/";
		HashSet<String> noExist=(HashSet<String>) General.readObject(localPath+"MEv13_noExistPhoIDs.hashSet");
		System.out.println("photos need to be copied: "+noExist.size());
		HashSet<String> fisrt_Letters=new HashSet<String>();
		for (String phoID:noExist) {
			fisrt_Letters.add(phoID.substring(0, 1));
		}
		System.out.println("folders need to be checked: "+fisrt_Letters.size()+", fisrt_Letters:"+fisrt_Letters);
		long startTime=System.currentTimeMillis();
		for (String fisrt_Letter : fisrt_Letters) {
			String dir=remotePath+fisrt_Letter+"/";
			System.out.println("listing file info for folder: "+dir);
			ProcessBuilder pb = new ProcessBuilder(Arrays.asList("ls", "-f", dir));
		    Process process = pb.start();
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
			
//			System.out.println("list file info finished, "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
//			BufferedReader inStr_dirInfo = new BufferedReader(new InputStreamReader(new FileInputStream(dirInfoPath), "UTF-8"));
			String oneLine; int samNum=0;
			while ((oneLine=stdInput.readLine())!=null) {
				String phoID=oneLine.split("_")[0];
				if (noExist.contains(phoID)) {
					try {
						BufferedImage img=ImageIO.read(new File(dir+oneLine));
						ImageIO.write(img, "jpg", new File(localPath+"lostPhotos/"+oneLine));
						System.out.println("phoID-: "+phoID+", "+dir+oneLine+" is copied throw ImageIO!");
					} catch (Exception e) {
						General.forTransfer(dir+oneLine, localPath+"lostPhotos/"+oneLine);
						System.out.println("phoID-: "+phoID+", "+dir+oneLine+" cannot be read with ImageIO, it is copied throw General.forTransfer!");
					}					
				}
				if (++samNum%10000==0) {
					System.out.println(samNum+" photos is checked! currrent-pho:"+oneLine+", "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
				}
			}
			stdInput.close();
			process.waitFor();
			System.out.println("folder- "+fisrt_Letter+" is done! totFiles: "+samNum+", "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
		}
	}
	
	public static void extractTestPhotos() throws IOException, ClassNotFoundException, InterruptedException {
		String basePath="O:/MediaEval13/";
		String savePath="D:/xinchaoli/Desktop/My research/My Code/DataSaved/MediEval/MediEval13/";
		
		String[] s_to_photoID=(String[]) General.readObject(savePath+"s_to_photoID.strArr");
//		//set save class
//		IntWritable MapFile_key = new IntWritable();
//		BufferedImage_jpg MapFile_value = new BufferedImage_jpg();
//		//set MapFile.Reader
//		Configuration conf = new Configuration();
//		FileSystem hdfs  = FileSystem.get(conf);
//		String MapFile_path=basePath+"testPhotos.MapFile/";
//		
//		String testPhotosPath=basePath+"testPhotos/";
//
//		long startTime=System.currentTimeMillis(); //end time 
		int totIndexedNum=0; int intervel=10000; int totphotoNum=25*1000;
//		// ******* read from MapFile to image *************
//		int mapFileNum=5;
//		for (int i = 0; i < mapFileNum; i++) {
//			MapFile.Reader MapFileReader=new MapFile.Reader(hdfs, MapFile_path+"part-r-"+General.StrleftPad(i+"", 0, 5, "0"), conf);
//			String testPhotosFolder=testPhotosPath+"testPhotos_"+i+"/";
//			General.makeFolder(testPhotosFolder);
//			while(MapFileReader.next(MapFile_key, MapFile_value)){
//				int photo_sInd=MapFile_key.get();
//				ImageIO.write(MapFile_value.getBufferedImage(), "jpg", new File(testPhotosFolder+photo_sInd+"_"+s_to_photoID[photo_sInd]+".jpg"));
//				totIndexedNum++;
//				if (totIndexedNum%intervel==0){
//					//disp info
//					double percentage=(double)totIndexedNum/totphotoNum;
//					System.out.println("extracting "+totphotoNum+" image, now photo-"+totIndexedNum+" finished ......"
//					+new DecimalFormat("0.0%").format(percentage)+", running time:"+General.dispTime(System.currentTimeMillis()-startTime, "min"));
//				}
//			}
//			MapFileReader.close();
//		}
		System.out.println("totIndexedNum: "+totIndexedNum);
	}
	
	public static void getNewAddedMissingBloPhos() throws IOException, SQLiteException {
		
		String basePath="O:/MediaEval13/";
			
		//set MetaData DataBase
		String dataBase="Q:/FlickrCrawler/MetaData/patch/FlickrPhotoMeta_new_missingBlocks_onlyExist.db";
		SQLParts sql; SQLiteStatement stmt;
		String oneLine;
//		PrintWriter outputStream_Report = new PrintWriter(new OutputStreamWriter(new FileOutputStream(savePath+"makeData.report"), "UTF-8")); 
//		General.dispInfo(outputStream_Report, "start processing....");
		
		//read dataBase
		sql = new SQLParts("SELECT * FROM photoMeta WHERE photoID=?");  //SELECT * FROM photoMeta WHERE photoIndex=?
		SQLiteConnection db_read = new SQLiteConnection(new File(dataBase));
		db_read.open(); stmt = db_read.prepare(sql);
		
		long startTime=System.currentTimeMillis();
		
		//get trainTestNum
		int procPhotos=0; int dispInter=10000;
		HashMap<Integer, Integer> L_to_L=new HashMap<Integer, Integer>();
		BufferedReader inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(basePath+"mediaeval2013_dataset_update"), "UTF-8"));
		while ((oneLine=inStr_photoMeta.readLine())!=null) {
			String photoID=oneLine.split("\t")[1];
			stmt.bind(1, photoID);
			int photoIndex_L=-1; 
			while (stmt.step()) {
				photoIndex_L = Integer.valueOf(stmt.columnString(0)); //sql, column start from 0
			}
			stmt.reset();
			General.Assert(photoIndex_L!=-1, "error, photoID:"+photoID+" not exist in meta data base!");
			//save
			L_to_L.put(photoIndex_L, photoIndex_L);
			//disp
			procPhotos++;
    		if((procPhotos)%dispInter==0){ 							
				System.out.println( "data making.. "+procPhotos+" photos finished!! "+new DecimalFormat("00%").format((double)procPhotos/procPhotos)+", ......"+General.dispTime(System.currentTimeMillis()-startTime, "min"));
    		}
		}
		inStr_photoMeta.close();
		System.out.println(  "done! total procPhotos:"+procPhotos);
		
		stmt.dispose();db_read.dispose();
		
		//save L_to_L
		General.writeObject(basePath+"L_to_L_newAddMisBloPho.hashMap", L_to_L);

	}
	
	public static void savePhoToZip_NewAddedMissingBloPhos() throws IOException, SQLiteException {
	
//		String basePath="O:/MediaEval13/";
		String basePath="/tudelft.net/staff-bulk/ewi/insy/D-MIRLab/XinchaoLi/MediaEval13/";
		
		//set MetaData DataBase
//		String dataBase="Q:/FlickrCrawler/MetaData/patch/FlickrPhotoMeta_new_missingBlocks_onlyExist.db";
		String dataBase="/tmp/FlickrPhotoMeta_new_missingBlocks.db";
		SQLParts sql; SQLiteStatement stmt;
		
		//set save class
		IntWritable SeqFile_key = new IntWritable();
		BufferedImage_jpg SeqFile_value = new BufferedImage_jpg();
		//set SeqFile
		Configuration conf = new Configuration();
		FileSystem hdfs  = FileSystem.get(conf);
		String SeqFiles_path=basePath+"photoFile_newAddedMisBloPho/";
				
		//read dataBase
		sql = new SQLParts("SELECT * FROM photoMeta WHERE photoIndex=?");  //SELECT * FROM photoMeta WHERE photoIndex=?
		SQLiteConnection db_read = new SQLiteConnection(new File(dataBase));
		db_read.open(); stmt = db_read.prepare(sql);
		
		long startTime=System.currentTimeMillis();
		
		//get trainTestNum
		int procPhotos_tot=0;  int fileNum=100;
		for (int i = 27; i < fileNum; i++) {
			String zipFileName=i+"_misBloPho";
			String zipFilePath=SeqFiles_path+zipFileName+".zip";
			ZipOutputStream zos = new ZipOutputStream(new File(zipFilePath));
			zos.setEncoding("gb2312");
			SequenceFile.Reader SeqFileReader=new SequenceFile.Reader(hdfs, new Path(SeqFiles_path+"part-r-"+General.StrleftPad(i+"", 0, 5, "0")+"/data"), conf);
			int procPhotos_this=0;
			while(SeqFileReader.next(SeqFile_key, SeqFile_value)){
				//get phoID
				int photoIndex_L=SeqFile_key.get();
				stmt.bind(1, photoIndex_L);
				String photoID=""; 
				while (stmt.step()) {
					photoID = stmt.columnString(1); //sql, column start from 0
				}
				stmt.reset();
				//save to zip
				ImageIO.write(SeqFile_value.getBufferedImage("photoID:"+photoID, Disp.getNotDisp()), "jpg", new File(SeqFiles_path+"temp.jpg"));
				ZipEntry ze = new ZipEntry(photoID+".jpg");
				zos.putNextEntry(ze);
				//read file to zip
				FileInputStream fis = new FileInputStream(new File(SeqFiles_path+"temp.jpg"));
				byte[] bf = new byte[2048];
				int location = 0;
				while ((location = fis.read(bf)) != -1) {
					zos.write(bf, 0, location);
				}
				//
				procPhotos_this++;
			}
			//disp
			System.out.println( "data making.. file-"+i+", "+procPhotos_this+" photos, finished!! "+new DecimalFormat("00%").format((double)i/fileNum)+", ......"+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			SeqFileReader.close();
			//save zip file
			zos.closeEntry();
			zos.close();
			procPhotos_tot+=procPhotos_this;
		}
	
		
		System.out.println(  "done! total procPhotos:"+procPhotos_tot);
		
		stmt.dispose();db_read.dispose();

	}
	
	public static void savePhoFeatStr_NewAddedMissingBloPhos() throws IOException, SQLiteException {
		
		String basePath="O:/MediaEval13/";
////		String basePath="/tudelft.net/staff-bulk/ewi/insy/D-MIRLab/XinchaoLi/MediaEval13/";
		
		//set MetaData DataBase
		String dataBase="Q:/FlickrCrawler/MetaData/patch/FlickrPhotoMeta_new_missingBlocks_onlyExist.db";
//		String dataBase="/tmp/FlickrPhotoMeta_new_missingBlocks.db";
		SQLParts sql; SQLiteStatement stmt;
		
		//set save class
		IntWritable SeqFile_key = new IntWritable();
		Text SeqFile_value = new Text();
		//set SeqFile
		Configuration conf = new Configuration();
		FileSystem hdfs  = FileSystem.get(conf);
		String SeqFiles_path=basePath+"MisBloPho_feat/";
				
		//read dataBase
		sql = new SQLParts("SELECT * FROM photoMeta WHERE photoIndex=?");  //SELECT * FROM photoMeta WHERE photoIndex=?
		SQLiteConnection db_read = new SQLiteConnection(new File(dataBase));
		db_read.open(); stmt = db_read.prepare(sql);
		
		//test one photo meta 
		int tagetPhoInd=70304333;
		System.out.println("test one photo meta , tagetPhoInd: "+tagetPhoInd);
		stmt.bind(1, tagetPhoInd);
		while (stmt.step()) {
			for (int i = 0; i < stmt.columnCount(); i++) {
				System.out.println(stmt.columnString(i));
			}
		}
		stmt.reset();
		
		long startTime=System.currentTimeMillis();
		
		//get trainTestNum
		int procPhotos_tot=0;  int fileNum=20; 
		for (int i = 0; i < fileNum; i++) {
			SequenceFile.Reader SeqFileReader=new SequenceFile.Reader(hdfs, new Path(SeqFiles_path+"part-r-"+General.StrleftPad(i+"", 0, 5, "0")), conf);
			PrintWriter outputStream_feat = new PrintWriter(new OutputStreamWriter(new FileOutputStream(SeqFiles_path+"MisBloPho_feat_"+i), "UTF-8")); 
			int procPhotos_this=0;
			while(SeqFileReader.next(SeqFile_key, SeqFile_value)){
				//get phoID
				int photoIndex_L=SeqFile_key.get();
				stmt.bind(1, photoIndex_L);
				String photoID=""; 
				while (stmt.step()) {
					photoID = stmt.columnString(1); //sql, column start from 0
				}
				stmt.reset();
				//save feat data
				outputStream_feat.println(photoID+SeqFile_value.toString());
				//
				procPhotos_this++;
			}
			//disp
			System.out.println( "data making.. file-"+i+", "+procPhotos_this+" photos, finished!! "+new DecimalFormat("00%").format((double)i/fileNum)+", ......"+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			//save file
			SeqFileReader.close();
			outputStream_feat.close();
			procPhotos_tot+=procPhotos_this;
		}
	
		
		System.out.println(  "done! total procPhotos:"+procPhotos_tot);
		
		stmt.dispose();db_read.dispose();
		
//		//check saved text feat file
//		int procPhotos_tot=0;  int fileNum=20; String oneLine;
//		for (int i = 0; i < fileNum; i++) {
//			BufferedReader inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(basePath+"MisBloPho_feat/MisBloPho_feat_"+i), "UTF-8"));
//			while ((oneLine=inStr_photoMeta.readLine())!=null) {
//				procPhotos_tot++;
//			}
//			inStr_photoMeta.close();
//		}
//		System.out.println(  "done! total procPhotos:"+procPhotos_tot);

	}

	public static void checkFeatExtractionCode() throws Exception {
		
		//set MetaData DataBase
		String dataBase="Q:/FlickrCrawler/MetaData/FlickrPhotoMeta_new.db";
		SQLParts sql; SQLiteStatement stmt;
				
		//read dataBase
		sql = new SQLParts("SELECT * FROM photoMeta WHERE photoID=?");  //SELECT * FROM photoMeta WHERE photoID=?
		SQLiteConnection db_read = new SQLiteConnection(new File(dataBase));
		db_read.open(); stmt = db_read.prepare(sql);
		
		BufferedReader inStr_photoFeat = new BufferedReader(new InputStreamReader(new FileInputStream("P:/Ori-Data/Medieval task/2013/imagefeatures_9"), "UTF-8"));
		String line=null;
		int phoNum=0;
		while (((line=inStr_photoFeat.readLine())!=null)&&(phoNum<2)) {
			String photoID=line.split(" ")[0];
			String feat_inStore=line.substring(line.indexOf(" "));
			stmt.bind(1, photoID); String url=null;
			while (stmt.step()) {
				//check metadata
//				for (int i = 0; i < stmt.columnCount(); i++) {
//					System.out.println(stmt.columnString(i));
//				}
				//get url
				url=stmt.columnString(5);
			}
			stmt.reset();
			BufferedImage img=ImageIO.read(new URL(url));
			String feat_extr=General_Lire.extractFeat_inStr_lire093(img);
			if(!feat_inStore.equalsIgnoreCase(feat_extr))
				System.out.println("photoID:"+photoID+", url:"+url+", feat not match,\n" +
					" feat_inStore:\t"+feat_inStore+"\n feat_extr:\t"+feat_extr);
			phoNum++;
		}
		inStr_photoFeat.close();
		stmt.dispose();db_read.dispose();
	}
}
