package MediaEval14;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import javax.imageio.ImageIO;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;
import MyAPI.Obj.Disp;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
import MyCustomedHaoop.ValueClass.VideoBytes;
import MyCustomedHaoop.ValueClass.forTest;

import com.almworks.sqlite4java.SQLParts;
import com.almworks.sqlite4java.SQLite;
import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

public class makeData {

	/**
	 * krenew -s -- java -Xms70g -Xmx100g -cp makeData.jar:$CLASSPATH MediaEval14.makeData 0 "/tudelft.net/staff-bulk/ewi/insy/D-MIRLab/XinchaoLi/MediaEval14/" "inMemory" "/tudelft.net/staff-bulk/ewi/insy/D-MIRLab/XinchaoLi/MediaEval14/"
	 * krenew -s -- java -Xms4g -Xmx10g -cp makeData_groupSeqFile.jar:$CLASSPATH MediaEval14.makeData /tudelft.net/staff-bulk/ewi/insy/D-MIRLab/XinchaoLi/MediaEval14/ME14Data/GoogleDrive/1 0
	 * 
	 * @param args
	 * @throws NumberFormatException
	 * @throws SQLiteException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws NumberFormatException, SQLiteException, IOException, ClassNotFoundException, InterruptedException, SQLException {
//		General.dispTopLines_FromFile("Q:/PhotoDataBase/YFCC100M_Webscope100M/yfcc100m_dataset-0",10);//yfcc100m_hash, yfcc100m_dataset-0
		
//		General.dispTopLines_FromFile("P:/Ori-Data/Medieval task/2014/mediaeval2014_placing_train",28000);

//		indexData_2014YFCC100M_Webscope100M(1, "Q:/PhotoDataBase/YFCC100M_Webscope100M/", "Q:/PhotoDataBase/YFCC100M_Webscope100M/");
		
//		indexData_2014YFCC100M_Webscope100M_MD5HashCode(0, "Q:/PhotoDataBase/YFCC100M_Webscope100M/", "Q:/PhotoDataBase/YFCC100M_Webscope100M/");
		
//		mapTaskData(Integer.valueOf(args[0]), args[1], args[2]);// 0 "/tudelft.net/staff-bulk/ewi/insy/D-MIRLab/XinchaoLi/MediaEval14/" "/tudelft.net/staff-bulk/ewi/insy/D-MIRLab/XinchaoLi/MediaEval14/"
//		mapTaskData(0, "O:/MediaEval14/", "Q:/PhotoDataBase/YFCC100M_Webscope100M/");// 0 "/tudelft.net/staff-bulk/ewi/insy/D-MIRLab/XinchaoLi/MediaEval14/" null
		
//		mapTaskData2(Integer.valueOf(args[0]), args[1], args[2], args[3]);// 0 "/tudelft.net/staff-bulk/ewi/insy/D-MIRLab/XinchaoLi/MediaEval14/" "/tmp/" "/tudelft.net/staff-bulk/ewi/insy/D-MIRLab/XinchaoLi/MediaEval14/"
//		mapTaskData2(0, "O:/MediaEval14/", "Q:/PhotoDataBase/YFCC100M_Webscope100M/", "Q:/PhotoDataBase/YFCC100M_Webscope100M/");

//		testExtarctPhotoTarFiles();
		
//		String basePath="/tudelft.net/staff-bulk/ewi/insy/D-MIRLab/XinchaoLi/MediaEval14/";
//		groupPhotoFileIntoSeq(basePath+"ME14Data/SeqFiles/", basePath+"MEval14_photos_s_to_photoID_md5_phoIndInL.txt", args[0], Integer.valueOf(args[1])); //  /tudelft.net/staff-bulk/ewi/insy/D-MIRLab/XinchaoLi/MediaEval14/ME14Data/GoogleDrive/0 0
	
		makeVideoFrameQueries();
		
//		combine_L_to_S_trainTest();
		
//		findTargetPhoto();
	}

	private static SQLiteConnection create_db(String filename) throws SQLiteException{
		SQLiteConnection db = new SQLiteConnection(filename.equalsIgnoreCase("inMemory")? null: new File(filename));
		db.open();
		db.exec("CREATE TABLE photoMeta(photoIndex Integer, photoID CHARACTER(20), latlon CHARACTER(20), accuracy Integer, userID CHARACTER(30), " +
				"photoLink CHARACTER(100), photoTags CHARACTER(5000), DateTaken CHARACTER(30), DateUploaded CHARACTER(30), Title CHARACTER(30), Description CHARACTER(100), PageURL CHARACTER(100))");
		return db;
	}
	
	private static SQLiteConnection create_db_MD5(String filename) throws SQLiteException{
		SQLiteConnection db = new SQLiteConnection(filename.equalsIgnoreCase("inMemory")? null: new File(filename));
		db.open();
		db.exec("CREATE TABLE photoMD5(photoID CHARACTER(20), MD5 CHARACTER(40))");
		return db;
	}
	
	private static Connection indexData_2014YFCC100M_Webscope100M(int target, String metaDataPath, String savePath_forDB, boolean isInMerory, String METaskLabel) throws SQLiteException, NumberFormatException, IOException, SQLException, ClassNotFoundException{
		DecimalFormat percformat = (DecimalFormat)NumberFormat.getPercentInstance();
	    percformat.applyPattern("00.0%");
	 	    
//		String dataSavePath="Q:/PhotoDataBase/YFCC100M_Webscope100M/";
//		String dataSavePath="/tudelft.net/staff-bulk/ewi/mm/D-MIRLab/XinchaoLi/FlickrCrawler/MetaData/";
		
//		int target=0; //0 for photos, 1 for videos
		String[] marker=new String[]{"_photos","_videos"};
		
		String dataBase=isInMerory?":memory:":savePath_forDB+"FlickrMeta_Webscope100M"+marker[target]+".sqlite";

//		String dataBase=dataSavePath+"FlickrMeta_Webscope100M"+marker[target]+".sqlite";
				
		if (isInMerory || !new File(dataBase).exists()) {
			int photoNumEst=100*1000*1000; int startPhoIndex=target==0?72403217:0;
			int intervel=1000*1000;
			
			BufferedReader inStr_photoMeta; 
			String line1Photo; String[] metaData;
			PrintWriter outputStream_report = new PrintWriter(new OutputStreamWriter(new FileOutputStream((isInMerory?savePath_forDB+"InMemoryDBFor_"+METaskLabel+"_":savePath_forDB)+"saveMetaDB"+marker[target]+".report"), "UTF-8")); 
			General.dispInfo(outputStream_report, "start build MetaData database:"+dataBase+", isInMerory:"+isInMerory);

			// load the sqlite-JDBC driver using the current class loader
			Class.forName("org.sqlite.JDBC");
		    // create a database connection
		    Connection db_connection = DriverManager.getConnection("jdbc:sqlite:"+dataBase);
		    // creat a new table in this db
		    Statement db_stmt = db_connection.createStatement();
		    db_stmt.executeUpdate("CREATE TABLE photoMeta(photoIndex Integer, photoID CHARACTER(20), latlon CHARACTER(20), accuracy Integer, userID CHARACTER(30), " +
				"photoLink CHARACTER(100), photoTags CHARACTER(5000), DateTaken CHARACTER(30), DateUploaded CHARACTER(30), Title CHARACTER(30), Description CHARACTER(100), PageURL CHARACTER(100))");
		    // prepare the insert query
		    PreparedStatement db_stmt_pre = db_connection.prepareStatement("INSERT INTO photoMeta values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		    db_connection.setAutoCommit(false);
		    
			//loop over all folders in root folder, fixed order! 9,8,7...0		    
		    int maxFileInd=9;
			int photoIndex=startPhoIndex; long startTime=System.currentTimeMillis(); //start time 
			int procNum=0; int ignorNum=0; String target_inString=target+"";
			for (int file_i = maxFileInd; file_i > -1; file_i--) {//fixed order! 9,8,7...0		    
				File oneFile=new File(metaDataPath+"yfcc100m_dataset-"+file_i);
				General.Assert(oneFile.exists(), "err! metaDataFile not exist:"+oneFile);
				General.dispInfo(outputStream_report, "now processing: "+oneFile.getPath());
				//process one meta-data file
				inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(oneFile), "UTF-8"));
				while((line1Photo=inStr_photoMeta.readLine())!=null){
					if (line1Photo.endsWith(target_inString)) {//0 is for photo, 1 is for video
						//insert data
						metaData=line1Photo.split("\t");
						long photoID=Long.valueOf(metaData[0]);
						db_stmt_pre.setInt(1, photoIndex); //column index begin from 1
						db_stmt_pre.setString(2, photoID+"");
						db_stmt_pre.setString(3, metaData[11]+","+metaData[10]); //lat,lon
						int acc=metaData[12].isEmpty()?-1:Integer.valueOf(metaData[12]); db_stmt_pre.setInt(4, acc);
						String UserID=metaData[1]; db_stmt_pre.setString(5, UserID);
						String photoLink=metaData[14]; db_stmt_pre.setString(6, photoLink);	//Photo Link						
						String photoTags=metaData[8]; db_stmt_pre.setString(7, photoTags);	//Photo tags						
						String DateTaken=metaData[3]; db_stmt_pre.setString(8, DateTaken);	//DateTaken		
						String DateUploaded=metaData[4]; db_stmt_pre.setString(9, DateUploaded);	//DateUploaded		
						String Title=metaData[6]; db_stmt_pre.setString(10, Title);	//Title		
						String Description=metaData[7]; db_stmt_pre.setString(11, Description);	//Description		
						String PageURL=metaData[13]; db_stmt_pre.setString(12, PageURL);	//PageURL	
						db_stmt_pre.execute();
						db_stmt_pre.clearParameters();
						//updata
						photoIndex++;
						procNum++;
						if((procNum)%intervel==0){ 							
							General.dispInfo(outputStream_report, "Processing "+photoNumEst+"(estimated) image metaData, now file:"+oneFile.getName()+" procNum:"+procNum+" finished.."+percformat.format((double)procNum/photoNumEst)+", ignorNum not index:"+ignorNum
									+" ......"+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
						}
					}else {
						ignorNum++;
					}
				}
				inStr_photoMeta.close();
			}
			int lastIndex=photoIndex-1;
			db_stmt_pre.close(); db_connection.commit();
			General.dispInfo(outputStream_report, "index done! total procNum:"+procNum+", ignorNum:"+ignorNum+" .... "+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
			if (!isInMerory) {
				db_stmt.executeUpdate("CREATE UNIQUE INDEX indexPhotoIndex on photoMeta(photoIndex)");
				General.dispInfo(outputStream_report, "build UNIQUE INDEX indexPhotoIndex done! .... "+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
			}
			db_stmt.executeUpdate("CREATE UNIQUE INDEX indexPhotoID on photoMeta(photoID)");
			General.dispInfo(outputStream_report, "build UNIQUE INDEX indexPhotoID done! .... "+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
			db_stmt.executeUpdate("ANALYZE");
			General.dispInfo(outputStream_report, "ANALYZE sqlite db to make query time fast done! .... "+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
			db_stmt.close();db_connection.commit();
			General.dispInfo(outputStream_report, "done! , startPhoIndex:"+startPhoIndex+", endPhoIndex:"+lastIndex+", total procNum:"+procNum+", ignorNum not index:"+ignorNum+" ......"+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
			outputStream_report.close(); 
			return db_connection;
		}else {
			// load the sqlite-JDBC driver using the current class loader
			Class.forName("org.sqlite.JDBC");
		    // create a database connection
		    Connection db_connection = DriverManager.getConnection("jdbc:sqlite:"+dataBase);		    
			return db_connection;
		}
	}
		
	private static SQLiteConnection indexData_2014YFCC100M_Webscope100M_MD5HashCode(int target, String dataSavePath, String basePath_forDB) throws SQLiteException, NumberFormatException, IOException{
		DecimalFormat percformat = (DecimalFormat)NumberFormat.getPercentInstance();
	    percformat.applyPattern("00.0%");
		boolean isInMerory=basePath_forDB.equalsIgnoreCase("inMemory");
//		String dataSavePath="Q:/PhotoDataBase/YFCC100M_Webscope100M/";
//		String dataSavePath="/tudelft.net/staff-bulk/ewi/insy/D-MIRLab/XinchaoLi/MediaEval14/";
		
//		int target=0; //0 for photos, 1 for videos
		String[] marker=new String[]{"_photos","_videos"};
		
		String dataBase=isInMerory?basePath_forDB:basePath_forDB+"FlickrMD5Hash_Webscope100M"+marker[target]+".sqlite";

//		String dataBase=dataSavePath+"FlickrMD5Hash_Webscope100M"+marker[target]+".sqlite";
		
		int photoNumEst=100*1000*1000;
		int intervel=1000*1000;
		
		BufferedReader inStr_photoMeta; 
		SQLParts sql; SQLiteStatement stmt;
		String line1Photo; String[] photoID_MD5;
		PrintWriter outputStream_report = new PrintWriter(new OutputStreamWriter(new FileOutputStream(dataSavePath+"saveMD5HashDB"+marker[target]+".report"), "UTF-8")); 
		General.dispInfo(outputStream_report, "start build MD5 database:"+dataBase+", isInMerory:"+isInMerory);

		// build dataBase
		if (!isInMerory) {
			General.deleteAll(new  File(dataBase));
		}
		SQLiteConnection db = create_db_MD5(dataBase);
		db.exec("BEGIN TRANSACTION");
		sql = new SQLParts("INSERT INTO photoMD5 values(?, ?)");
		stmt = db.prepare(sql, false);
		//loop over all folders in root folder
		long startTime=System.currentTimeMillis(); //start time 
		int procPhotos=0;
		//process one meta-data file
		inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(dataSavePath+"yfcc100m_hash"), "UTF-8"));
		while((line1Photo=inStr_photoMeta.readLine())!=null){
			photoID_MD5=line1Photo.split("\t");
			long photoID=Long.valueOf(photoID_MD5[0]);
			stmt.bind(1, photoID+"");//column index begin from 1
			stmt.bind(2, photoID_MD5[1]); //MD5
			if(stmt.step()) {
				General.dispInfo(outputStream_report, "stmt.step() Error in insert data, line1Photo:"+line1Photo);
			}
			stmt.reset();
			procPhotos++;
			if((procPhotos)%intervel==0){ 							
				General.dispInfo(outputStream_report, "Processing "+photoNumEst+"(estimated) image photoID_MD5, now "+procPhotos+" finished.."+percformat.format((double)procPhotos/photoNumEst)
						+" ......"+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo()+", SQlite memory: highWater"+SQLite.getMemoryHighwater(true)/1024/1024+"MB, used"+SQLite.getMemoryUsed()/1024/1024+"MB");
				db.exec("COMMIT TRANSACTION");
				db.exec("BEGIN TRANSACTION");
			}
		}
		inStr_photoMeta.close();
		db.exec("COMMIT TRANSACTION");
		General.dispInfo(outputStream_report, "index done! total "+procPhotos+" photos and videos .... "+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
		if (!isInMerory) {
			db.exec("CREATE UNIQUE INDEX indexPhotoID on photoMD5(photoID)");
			General.dispInfo(outputStream_report, "build UNIQUE INDEX indexPhotoID done! .... "+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
		}
		db.exec("CREATE UNIQUE INDEX indexPhotoMD5 on photoMD5(MD5)");
		General.dispInfo(outputStream_report, "build UNIQUE INDEX indexPhotoMD5 done! .... "+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
		db.exec("ANALYZE");
		General.dispInfo(outputStream_report, "ANALYZE sqlite db to make query time fast done! .... "+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
		stmt.dispose(); 
		General.dispInfo(outputStream_report, "done! , ......"+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
		outputStream_report.close();
		if (isInMerory) {
			return db;
		}else {
			db.dispose();
			return null;
		}
		
	}

	public static String[] translateMD5_to_phoID(String yfcc100m_hash, String[] toTranslatePaths, String dataSavePath) throws IOException, InterruptedException {
		DecimalFormat percformat = (DecimalFormat)NumberFormat.getPercentInstance();
	    percformat.applyPattern("00.0%");
	    
//		String dataSavePath="Q:/PhotoDataBase/YFCC100M_Webscope100M/";
//		String dataSavePath="/tudelft.net/staff-bulk/ewi/insy/D-MIRLab/XinchaoLi/MediaEval14/";
	    String reprotPath=dataSavePath+"translateMD5_to_phoID.report";
	    
	    String[] resPaths=new String[toTranslatePaths.length]; boolean isAllExist=true;
	    for (int i = 0; i < toTranslatePaths.length; i++) {
	    	resPaths[i]=dataSavePath+new File(toTranslatePaths[i]).getName()+"_inPhoID";
	    	isAllExist=isAllExist && new File(resPaths[i]).exists();
	    }
	    
	    if (!isAllExist) {//res path does not exit before
	    	//load all Webscope100M data's  MD5 hashMap into Memory
		    int photoNumEst=100*1000*1000;
			int intervel=1000*1000;
		    HashMap<String,Long> MD5_PhoID=new HashMap<String, Long>(photoNumEst);

			BufferedReader inStr_photoMeta; 
			String line1Photo; String[] photoID_MD5;
			PrintWriter outputStream_report = new PrintWriter(new OutputStreamWriter(new FileOutputStream(reprotPath), "UTF-8")); 
			General.dispInfo(outputStream_report, "start build all Webscope100M data's  MD5 hashMap into Memory");

			//loop over all folders in root folder
			long startTime=System.currentTimeMillis(); //start time 
			int procPhotos=0;
			//process one meta-data file
			inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(yfcc100m_hash), "UTF-8"));
			while((line1Photo=inStr_photoMeta.readLine())!=null){
				photoID_MD5=line1Photo.split("\t");
				long photoID=Long.valueOf(photoID_MD5[0]);
				Long previous=MD5_PhoID.put(photoID_MD5[1], photoID);
				General.Assert(previous==null, "err! duplicated key in hashMap MD5_PhoID, MD5:"+photoID_MD5[1]+", its previous phoID:"+photoID);
				procPhotos++;
				if((procPhotos)%intervel==0){ 							
					General.dispInfo(outputStream_report, "Processing "+photoNumEst+"(estimated) image photoID_MD5, now "+procPhotos+" finished.."+percformat.format((double)procPhotos/photoNumEst)
							+" ......"+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
				}
			}
			inStr_photoMeta.close();
			General.Assert(MD5_PhoID.size()==procPhotos,"err! MD5_PhoID.size() should ==procPhotos, but MD5_PhoID.size():"+MD5_PhoID.size()+", procPhotos:"+procPhotos);
			General.dispInfo(outputStream_report, "index done! total "+procPhotos+" photos and videos .... "+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
			
			//translate MD5 to PhoID
		    for (int i = 0; i < toTranslatePaths.length; i++) {
				//translate
				General.dispInfo(outputStream_report, "start translate MD5 to PhoID for file:"+toTranslatePaths[i]);
				PrintWriter outputStream_res = new PrintWriter(new OutputStreamWriter(new FileOutputStream(resPaths[i]), "UTF-8")); 
				inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(toTranslatePaths[i]), "UTF-8"));
				int lineInd=0;
				while((line1Photo=inStr_photoMeta.readLine())!=null){
					lineInd++;
					String photoMD5=line1Photo.split("\t")[0]; //MD5
					long photoID=-1; 
					//get photoID from MD5 db
					photoID=MD5_PhoID.get(photoMD5);
					General.Assert(photoID>0, lineInd+"-th line, photoMD5:"+photoMD5+" from toTranslatePath:"+toTranslatePaths[i]+" is not exist in MD5 db file!");
					outputStream_res.println(line1Photo+"\t"+photoID);
				}
				General.dispInfo(outputStream_report, "done! translate MD5 to PhoID for file:"+toTranslatePaths[i]+" .... "+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
				outputStream_res.close();
				inStr_photoMeta.close();
			}
			outputStream_report.close();
		}
		return resPaths;

		
	}
		
	public static void mapTaskData(int target, String savePath, String basePath_forDB) throws IOException, SQLiteException, ClassNotFoundException, InterruptedException {
			
//		String basePath_forDB="Q:/PhotoDataBase/YFCC100M_Webscope100M/";
//		String basePath_forDB="/tmp/";

//		String savePath="O:/MediaEval14/";
//		String savePath="/tudelft.net/staff-bulk/ewi/insy/D-MIRLab/XinchaoLi/MediaEval14/";
		
//		String oriDataPath="P:/Ori-Data/Medieval task/2014/";
		String oriDataPath=savePath; 
		
		String[] dataPaths=new String[]{oriDataPath+"mediaeval2014_placing_train", oriDataPath+"mediaeval2014_placing_test"}; 
		
//		int target=0; //0 for photos, 1 for videos
		String[] marker=new String[]{"_photos","_videos"};
		
		//set MetaData DataBase
		String dataBase_Meta=basePath_forDB+"FlickrMeta_Webscope100M"+marker[target]+".sqlite";
//		String dataBase_Meta="inMemory";
//		String dataBase_MD5=basePath_forDB+"FlickrMD5Hash_Webscope100M"+marker[target]+".sqlite";
		String dataBase_MD5="inMemory";
		SQLParts sql, sql_MD5; SQLiteStatement stmt,stmt_MD5;
		String oneLine;
		
		PrintWriter outputStream_Report = new PrintWriter(new OutputStreamWriter(new FileOutputStream(savePath+"makeData"+marker[target]+".report",false), "UTF-8"),true); 
		General.dispInfo(outputStream_Report, "start processing....");
		
		//read dataBase
		sql = new SQLParts("SELECT * FROM photoMeta WHERE photoID=?");  //SELECT * FROM photoMeta WHERE photoIndex=?
		sql_MD5 = new SQLParts("SELECT * FROM photoMD5 WHERE MD5=?");  //SELECT * FROM photoMeta WHERE photoIndex=?
		SQLiteConnection db_read = new SQLiteConnection(new File(dataBase_Meta)); db_read.open();
//		SQLiteConnection db_read = indexData_2014YFCC100M_Webscope100M(target, savePath, dataBase_Meta);
//		SQLiteConnection db_read_MD5 = new SQLiteConnection(new File(dataBase_MD5)); db_read_MD5.open();
		SQLiteConnection db_read_MD5 = indexData_2014YFCC100M_Webscope100M_MD5HashCode(target, savePath, dataBase_MD5);
		stmt = db_read.prepare(sql);
		stmt_MD5=db_read_MD5.prepare(sql_MD5);
					
		long startTime=System.currentTimeMillis(); //start time 
		
		//get trainTestNum
		int procPhotos=0;
		int[] trainTestNum=new int[2];
		for (int i = 0; i < dataPaths.length; i++) {
			BufferedReader inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(dataPaths[i]), "UTF-8"));
			int train_test=i;
			while ((oneLine=inStr_photoMeta.readLine())!=null) {
				boolean isTarget=(Integer.valueOf(oneLine.split("\t")[1])==target); //0 for photos, 1 for videos
				if (isTarget) {
					trainTestNum[train_test]++;
					procPhotos++;
				}
//				else {
//					System.out.println(oneLine);
//				}
			}
			inStr_photoMeta.close();
		}
		General.Assert(procPhotos==trainTestNum[0]+trainTestNum[1], "error, procPhotos should == trainTestNum[0]+trainTestNum[1]");
		General.dispInfo(outputStream_Report, "total procPhotos:"+procPhotos+", trainTestNum: "+General.IntArrToString(trainTestNum, "_"));
		
		//make latlons, s_to_photoID, L_to_S_trainTest
		float[][] latlons=new float[2][procPhotos];  String[] s_to_photoID=new String[procPhotos];
		long[] userIDs_0=new long[procPhotos]; int[] userIDs_1=new int[procPhotos];
		LinkedList<HashMap<Integer, Integer>> L_to_S_trainTest=new LinkedList<HashMap<Integer, Integer>>();
		for (int i = 0; i < 2; i++) {
			L_to_S_trainTest.add(new HashMap<Integer, Integer>(trainTestNum[i]));
		}
		int photoIndex_S=0; int dispInter=1*1000; int[] noTagNum=new int[2]; int[] oneTagNum=new int[2]; 
		for (int i = 0; i < dataPaths.length; i++) {
			BufferedReader inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(dataPaths[i]), "UTF-8"));
			int train_test=i; int lineInd=0;
			while ((oneLine=inStr_photoMeta.readLine())!=null) {
				lineInd++;
				String[] infos=oneLine.split("\t");
				String photoMD5=infos[0]; //MD5
				boolean isTarget=(Integer.valueOf(infos[1])==target); //0 for photos, 1 for videos
				if (isTarget) {
					int photoExist=0; String photoID=null; 
					//get photoID from MD5 db
					try {
						stmt_MD5.bind(1, photoMD5);
						while (stmt_MD5.step()) {
							photoID=stmt_MD5.columnString(0); //sql, column start from 0
							photoExist++;
						}
						General.Assert(photoExist==1, photoExist==0 ? "photoMD5:"+photoMD5+" from dataPath:"+dataPaths[i]+" is not exist in MD5 db file!"
								: "photoMD5:"+photoMD5+" from dataPath:"+dataPaths[i]+" has multi-"+photoExist+" entry in MD5 db file!");
						stmt_MD5.reset();
					} catch (Exception e) {
						System.out.println(lineInd+"-th line, photoMD5:"+photoMD5+" from dataPath:"+dataPaths[i]+" has err! "+e.getLocalizedMessage());
					}
					//get photo info from photoMeta db
					int photoIndex_L=-1; float[] latlon=null; photoExist=0; long userID_0 = 0; int userID_1 = 0;
					stmt.bind(1, photoID);
					while (stmt.step()) {
						photoIndex_L = Integer.valueOf(stmt.columnString(0)); //sql, column start from 0
						latlon= General.StrArrToFloatArr(stmt.columnString(2).split(","));
						String userID=stmt.columnString(4);
						userID_0=Long.valueOf(userID.split("@")[0]);
						userID_1=Integer.valueOf(userID.split("@")[1].substring(1,3));
						String tags=stmt.columnString(6);
						int tagNum=tags.split(",").length;
						if (tagNum==1) {
							if (tags.equalsIgnoreCase("")) {
								noTagNum[train_test]++;
							}else {
								oneTagNum[train_test]++;
							}
						}
						photoExist++;
					}
					General.Assert(photoExist==1, photoExist==0 ? "photoID:"+photoID+" from dataPath:"+dataPaths[i]+" is not exist in photoMeta db file!"
							: "photoID:"+photoID+" from dataPath:"+dataPaths[i]+" has multi-"+photoExist+" entry in photoMeta db file!");
					stmt.reset();
					//save
					latlons[0][photoIndex_S]=latlon[0]; latlons[1][photoIndex_S]=latlon[1];
					L_to_S_trainTest.get(train_test).put(photoIndex_L, photoIndex_S);
					s_to_photoID[photoIndex_S]=photoID;
					userIDs_0[photoIndex_S]=userID_0; userIDs_1[photoIndex_S]=userID_1;
					//
					photoIndex_S++;
				}
				//disp
	    		if((photoIndex_S)%dispInter==0){ 							
					General.dispInfo(outputStream_Report, "tran test data making.. "+photoIndex_S+" photos finished!! "+new DecimalFormat("00%").format((double)photoIndex_S/procPhotos)+", ......"+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
					outputStream_Report.flush();
	    		}
			}
			inStr_photoMeta.close();
		}
		stmt.dispose();db_read.dispose();
		stmt_MD5.dispose();db_read_MD5.dispose();
		
		//get tagNum statistic
		General.dispInfo(outputStream_Report, "in trainData, "+General.getPercentInfo(noTagNum[0], trainTestNum[0])+", no tag. "
							+General.getPercentInfo(oneTagNum[0], trainTestNum[0])+", one tag.");
		General.dispInfo(outputStream_Report, "in testData, "+General.getPercentInfo(noTagNum[1], trainTestNum[1])+", no tag. "
				+General.getPercentInfo(oneTagNum[1], trainTestNum[1])+", one tag.");
		General.dispInfo(outputStream_Report, "in total, "+General.getPercentInfo(General.sum_IntArr(noTagNum), General.sum_IntArr(trainTestNum))+", no tag. "
							+General.getPercentInfo(General.sum_IntArr(oneTagNum), General.sum_IntArr(trainTestNum))+", one tag.");
			
		//save latlons, s_to_photoID, L_to_S_trainTest, userIDs
		General.writeObject(savePath+"MEval14"+marker[target]+"_latlons.floatArr", latlons);
		General.writeObject(savePath+"MEval14"+marker[target]+"_s_to_photoID.strArr", s_to_photoID);
		General.writeObject(savePath+"MEval14"+marker[target]+"_L_to_S_train.hashMap", L_to_S_trainTest.get(0));
		General.writeObject(savePath+"MEval14"+marker[target]+"_L_to_S_test.hashMap", L_to_S_trainTest.get(1));
		General.writeObject(savePath+"MEval14"+marker[target]+"_userIDs_0.long", userIDs_0);
		General.writeObject(savePath+"MEval14"+marker[target]+"_userIDs_1.int", userIDs_1);
		General.dispInfo(outputStream_Report,"save latlons, s_to_photoID, L_to_S_trainTest, userIDs done! total time for all: "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
		outputStream_Report.close();

	}

	public static void mapTaskData2(int target, String yfcc100m_hash, String oriDataPath_train, String oriDataPath_test, String METaskLabel, String savePath, String savePath_forDB, String metaDataPath) throws IOException, SQLiteException, ClassNotFoundException, InterruptedException, SQLException {
		
//		String basePath_forDB="Q:/PhotoDataBase/YFCC100M_Webscope100M/";
//		String basePath_forDB="/tmp/";

//		String savePath="O:/MediaEval14/";
//		String savePath="/tudelft.net/staff-bulk/ewi/insy/D-MIRLab/XinchaoLi/MediaEval14/";
		
//		String oriDataPath="P:/Ori-Data/Medieval task/2014/";
		
		String[] dataPaths=new String[]{oriDataPath_train, oriDataPath_test}; 
		//read MD5 and translateMD5_to_phoID
		String[] dataPaths_inPhoID=translateMD5_to_phoID(yfcc100m_hash, dataPaths, savePath); 
				
//		int target=0; //0 for photos, 1 for videos
		String[] marker=new String[]{"_photos","_videos"};
		
		//set MetaData DataBase
//		String dataBase_Meta=basePath_forDB+"FlickrMeta_Webscope100M"+marker[target]+".sqlite";
//		String dataBase_Meta="inMemory";

		PrintWriter outputStream_Report = new PrintWriter(new OutputStreamWriter(new FileOutputStream(savePath+"makeData"+marker[target]+".report",false), "UTF-8"),true); 
		General.dispInfo(outputStream_Report, "start processing...."); 
		String oneLine;
		
		//********** read 2014YFCC100M metaData dataBase ********************
		// load the sqlite-JDBC driver using the current class loader
		Class.forName("org.sqlite.JDBC");
	    // create a database connection
	    Connection db_connection = indexData_2014YFCC100M_Webscope100M(target, metaDataPath, savePath_forDB, true, METaskLabel);//here, isDBInMem should ==true, as large amount of query SQL from Disk is slow.
		General.dispInfo(outputStream_Report, "2014YFCC100M db opened!"); 
	    // prepare the select query
	    PreparedStatement db_stmt_pre = db_connection.prepareStatement("SELECT * FROM photoMeta WHERE photoID=?");
	    db_connection.setAutoCommit(false);
					
		long startTime=System.currentTimeMillis(); //start time 
		//get trainTestNum
		int procPhotos=0;
		int[] trainTestNum=new int[2];
		for (int i = 0; i < dataPaths_inPhoID.length; i++) {
			BufferedReader inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(dataPaths_inPhoID[i]), "UTF-8"));
			int train_test=i;
			while ((oneLine=inStr_photoMeta.readLine())!=null) {
				boolean isTarget=(Integer.valueOf(oneLine.split("\t")[1])==target); //0 for photos, 1 for videos
				if (isTarget) {
//					if (train_test==0 || (Integer.valueOf(oneLine.split("\t")[3])==5)) {//train, use all; test, only use 5-th test set
						trainTestNum[train_test]++;
						procPhotos++;
//					}
				}
			}
			inStr_photoMeta.close();
		}
		General.Assert(procPhotos==trainTestNum[0]+trainTestNum[1], "error, procPhotos should == trainTestNum[0]+trainTestNum[1]");
		General.dispInfo(outputStream_Report, "total procPhotos:"+procPhotos+", trainTestNum: "+General.IntArrToString(trainTestNum, "_"));
		
		Configuration conf=new Configuration();
		//make latlons, s_to_photoID, L_to_S_trainTest
		float[][] latlons=new float[2][procPhotos];  
		PrintWriter outputStream_s_to_photoID_MD5 = new PrintWriter(new OutputStreamWriter(new FileOutputStream(savePath+METaskLabel+marker[target]+"_s_to_photoID_md5_phoIndInL.txt",false), "UTF-8"),true); 
		long[] userIDs_0=new long[procPhotos]; int[] userIDs_1=new int[procPhotos];
		SequenceFile.Writer uRLsWriter=new SequenceFile.Writer(FileSystem.get(conf), conf, new Path(savePath+METaskLabel+marker[target]+"_URLs.seq"), IntWritable.class, Text.class);
		LinkedList<HashMap<Integer, Integer>> L_to_S_trainTest=new LinkedList<HashMap<Integer, Integer>>();
		for (int i = 0; i < 2; i++) {
			L_to_S_trainTest.add(new HashMap<Integer, Integer>(trainTestNum[i]));
		}
		int photoIndex_S=0; int dispInter=10*1000; int[] noTagNum=new int[2]; int[] oneTagNum=new int[2]; 
		for (int i = 0; i < dataPaths_inPhoID.length; i++) {
			BufferedReader inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(dataPaths_inPhoID[i]), "UTF-8"));
			int train_test=i;
			while ((oneLine=inStr_photoMeta.readLine())!=null) {
				String[] infos=oneLine.split("\t");
				boolean isTarget=(Integer.valueOf(infos[1])==target); //0 for photos, 1 for videos
				if (isTarget) {
//					if (train_test==0 || (Integer.valueOf(infos[3])==5)) {//train, use all; test, only use 5-th test set
						String photoID=infos[infos.length-1]; //photoID
						//get photo info from photoMeta db
						int photoIndex_L=-1; float[] latlon=null; int photoExist=0; long userID_0 = 0; int userID_1 = 0; String url=null;
						db_stmt_pre.setString(1, photoID); 
						ResultSet result =db_stmt_pre.executeQuery();
						while (result.next()) {
							photoIndex_L = result.getInt(1); //sql, column start from 1
							latlon= General.StrArrToFloatArr(result.getString(3).split(","));
							String userID=result.getString(5);
							userID_0=Long.valueOf(userID.split("@")[0]);
							userID_1=Integer.valueOf(userID.split("@")[1].substring(1,3));
							url=result.getString(6);
							//tagNum statistics
							String tags=result.getString(7);
							int tagNum=tags.split(",").length;
							if (tagNum==1) {
								if (tags.equalsIgnoreCase("")) {
									noTagNum[train_test]++;
								}else {
									oneTagNum[train_test]++;
								}
							}
							photoExist++;
						}
						General.Assert(photoExist==1, photoExist==0 ? "photoID:"+photoID+" from dataPath:"+dataPaths_inPhoID[i]+" is not exist in photoMeta db file!"
								: "photoID:"+photoID+" from dataPath:"+dataPaths_inPhoID[i]+" has multi-"+photoExist+" entry in photoMeta db file!");
						db_stmt_pre.clearParameters();
						//save
						latlons[0][photoIndex_S]=latlon[0]; latlons[1][photoIndex_S]=latlon[1];
						L_to_S_trainTest.get(train_test).put(photoIndex_L, photoIndex_S);
						outputStream_s_to_photoID_MD5.println(photoID+"\t"+infos[0]+"\t"+photoIndex_L);
						userIDs_0[photoIndex_S]=userID_0; userIDs_1[photoIndex_S]=userID_1;
						uRLsWriter.append(new IntWritable(photoIndex_L), new Text(url));
						//disp
						photoIndex_S++;
			    		if((photoIndex_S)%dispInter==0){ 							
							General.dispInfo(outputStream_Report, "tran test data making.. "+photoIndex_S+" photos finished!! "+new DecimalFormat("00%").format((double)photoIndex_S/procPhotos)+", ......"+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
							outputStream_Report.flush();
			    		}
//					}
				}
			}
			inStr_photoMeta.close();
		}
		db_stmt_pre.close();db_connection.close();
		uRLsWriter.close();
		outputStream_s_to_photoID_MD5.close();
		
		//get tagNum statistic
		General.dispInfo(outputStream_Report, "in trainData, "+General.getPercentInfo(noTagNum[0], trainTestNum[0])+", no tag. "
							+General.getPercentInfo(oneTagNum[0], trainTestNum[0])+", one tag.");
		General.dispInfo(outputStream_Report, "in testData, "+General.getPercentInfo(noTagNum[1], trainTestNum[1])+", no tag. "
				+General.getPercentInfo(oneTagNum[1], trainTestNum[1])+", one tag.");
		General.dispInfo(outputStream_Report, "in total, "+General.getPercentInfo(General.sum_IntArr(noTagNum), General.sum_IntArr(trainTestNum))+", no tag. "
							+General.getPercentInfo(General.sum_IntArr(oneTagNum), General.sum_IntArr(trainTestNum))+", one tag.");
			
		//save latlons, s_to_photoID, L_to_S_trainTest, userIDs
		General.writeObject(savePath+METaskLabel+marker[target]+"_latlons.floatArr", latlons);
		General.writeObject(savePath+METaskLabel+marker[target]+"_L_to_S_train.hashMap", L_to_S_trainTest.get(0));
		General.writeObject(savePath+METaskLabel+marker[target]+"_L_to_S_test.hashMap", L_to_S_trainTest.get(1));
		General.writeObject(savePath+METaskLabel+marker[target]+"_userIDs_0.long", userIDs_0);
		General.writeObject(savePath+METaskLabel+marker[target]+"_userIDs_1.int", userIDs_1);
		General.dispInfo(outputStream_Report,"save latlons, s_to_photoID, L_to_S_trainTest, userIDs, urls done! total time for all: "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
		outputStream_Report.close();

	}

	public static void testExtarctPhotoTarFiles() throws FileNotFoundException, IOException {
		String tarFilePath="N:/ewi/insy/D-MIRLab/XinchaoLi/MediaEval14/ME14Data/GoogleDrive/1/186.tar.gz";
		long startTime=System.currentTimeMillis(); int dispInter=1000;
		//read images in .tar.gz file
		TarArchiveInputStream tarInput = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(tarFilePath)));
		TarArchiveEntry currentEntry = null; int fileInd=0;
		while((currentEntry = tarInput.getNextTarEntry()) != null) {
			File oneImage = currentEntry.getFile();
			if (oneImage!=null) {
				if (oneImage.isDirectory()) {
					System.out.println("Dir: "+oneImage.getName());
				}else {
					System.out.println("file: "+oneImage.getName());
				}
			}else {
				System.out.println("byteEntriy:"+currentEntry.getName()+", isNormalFile:"+currentEntry.isFile());
				if (currentEntry.isFile() && !currentEntry.getName().contains("list") && !currentEntry.getName().contains("script")) {//this is the photo!
//					System.out.println(", its name:"+currentEntry.getName().split("/")[1]);
					fileInd++;
					if (fileInd%dispInter==0) {
						System.out.println(fileInd+"-th file done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
					}
//					//show photo
//					byte[] byteContent = new byte[(int) currentEntry.getSize()];
//	                tarInput.read(byteContent, 0, byteContent.length);
//					General.dispPhoto(General_BoofCV.BytesToBufferedImage(byteContent));
				}
			}
		}
		System.out.println("one tar file done! total-"+fileInd+" files, "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
		tarInput.close();
	}
	
	private static void groupPhotoFileIntoSeq(String dataSavePath, String s_to_MD5DataPath, String imageFilePath, int target) throws IOException, InterruptedException, SQLiteException {
		DecimalFormat percformat = (DecimalFormat)NumberFormat.getPercentInstance();
	    percformat.applyPattern("00.0%");
	    
//		String dataSavePath="Q:/PhotoDataBase/YFCC100M_Webscope100M/";
//		String dataSavePath="/tudelft.net/staff-bulk/ewi/insy/D-MIRLab/XinchaoLi/MediaEval14/";
				
	    String[] marker=new String[]{"_photos","_videos"};
	    boolean isPhoto=(target==0);
	    
		PrintWriter outputStream_report = new PrintWriter(new OutputStreamWriter(new FileOutputStream(dataSavePath+"groupFileIntoSeq_"+imageFilePath.charAt(imageFilePath.length()-1)+marker[target]+".report", true), "UTF-8")); 
		
		//set SeqFile
		Configuration conf = new Configuration();
		FileSystem hdfs  = FileSystem.get(conf);
				
    	//********** load all train-test data's  MD5 hashMap into Memory **********//
	    int photoNumEst=6*1000*1000;
		int intervel=1000*1000;
	    HashMap<String,Integer> MD5_PhoInd_L=new HashMap<String, Integer>(photoNumEst);
		BufferedReader inStr_photoMeta; 
		String line1Photo; 
		General.dispInfo(outputStream_report, "\n start build all train-test data's MD5 into hashMap in Memory");
		//loop over all folders in root folder
		long startTime=System.currentTimeMillis(); //start time 
		int procPhotos=0;
		//process one meta-data file
		inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(s_to_MD5DataPath), "UTF-8"));
		while((line1Photo=inStr_photoMeta.readLine())!=null){
			String[] infos=line1Photo.split("\t");//photoID md5 phoIndInL
			int phoInd_L=Integer.valueOf(infos[2]);
			Integer previous=MD5_PhoInd_L.put(infos[1], phoInd_L);
			General.Assert(previous==null, "err! duplicated key in hashMap MD5_PhoInd_L, MD5:"+infos[1]+", its previous phoInd_L:"+previous);
			procPhotos++;
			if((procPhotos)%intervel==0){ 							
				General.dispInfo(outputStream_report, "Processing "+photoNumEst+"(estimated) image MD5_PhoInd_L, now "+procPhotos+" finished.."+percformat.format((double)procPhotos/photoNumEst)
						+" ......"+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
			}
		}
		General.dispInfo(outputStream_report, "done for s_to_MD5DataPath:"+s_to_MD5DataPath);
		inStr_photoMeta.close();
		General.Assert(MD5_PhoInd_L.size()==procPhotos,"err! MD5_PhoID.size() should ==procPhotos, but MD5_PhoInd_L.size():"+MD5_PhoInd_L.size()+", procPhotos:"+procPhotos);
		General.dispInfo(outputStream_report, "index done! total "+procPhotos+" photos and videos .... "+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
		
    	//********** Transfer MD5 to PhotoID, and group images into SeqFile **********//
		File[] imageTars=new File(imageFilePath).listFiles(); int dispInter=1000; int tarFileInd=0;
		General.dispInfo(outputStream_report, "start Transfer MD5 to PhotoID, and group images into SeqFile from: "+imageFilePath+"\n intotal:"+imageTars.length+" tar files.");
	    for (int i = 0; i < imageTars.length; i++) {
	    	String seqFilePath=dataSavePath+imageTars[i].getName().split("\\.")[0]+".seq";
	    	if (imageTars[i].getName().endsWith(".tar.gz") && !(new File(seqFilePath).exists()) ) {
				General.dispInfo(outputStream_report, "start translate MD5 to PhoID to PhotoIndex_L for images in tar file: "+imageTars[i]);
				SequenceFile.Writer seqFile=new SequenceFile.Writer(hdfs, conf, new Path(seqFilePath), IntWritable.class, isPhoto?BufferedImage_jpg.class:VideoBytes.class);
				//read images in .tar.gz file
				TarArchiveInputStream tarInput = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(imageTars[i])));
				TarArchiveEntry currentEntry = null; int fileInd=0;
				while((currentEntry=tarInput.getNextTarEntry()) != null) {
					if (currentEntry.isFile() && !currentEntry.getName().contains("list") && !currentEntry.getName().contains("script")) {
						String MD5=currentEntry.getName().split("/")[1];
						General.Assert(!MD5.equalsIgnoreCase(""), fileInd+"-th file's name not in control!, name:"+currentEntry.getName());
						int phoInd_L=-1;
						try{
							phoInd_L=MD5_PhoInd_L.get(MD5);
						}catch (Exception e){
							System.out.println(fileInd+"-th file's name not in control!, name:"+currentEntry.getName());
						}
						General.Assert(phoInd_L>0, fileInd+"-th file, photoMD5:"+MD5+" from tarFile:"+imageTars[i]+" is not exist in MD5_PhoInd_L! phoInd_L:"+phoInd_L);
						//save into SeqFile
						byte[] byteContent = new byte[(int) currentEntry.getSize()];
		                tarInput.read(byteContent, 0, byteContent.length);
						seqFile.append(new IntWritable(phoInd_L), isPhoto?new BufferedImage_jpg(byteContent):new VideoBytes(byteContent));
						//show progress
						fileInd++;
						if (fileInd%dispInter==0) {
							General.dispInfo(outputStream_report, "\t-- "+fileInd+"-th file done! current photo's MD5:"+MD5+", phoInd_L:"+phoInd_L+", ..."+General.dispTime(System.currentTimeMillis()-startTime, "min"));
						}
					}
				}
				seqFile.close(); tarInput.close();
				//show progress
				tarFileInd++;
				General.dispInfo(outputStream_report, "done for "+tarFileInd+"-th tar file:"+imageTars[i]+", in this tarFile, total "+fileInd+" items .... "+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
				outputStream_report.flush();
	    	}
		}
		outputStream_report.close();
	}
	
	private static void makeVideoFrameQueries() throws IOException, InterruptedException{
		String basePath="O:/MediaEval14/VideoFrameTest/";
//		String videoFramePath=basePath+"2012TestVideoFrames/";
//		String seqFilePath=basePath+"2012TestVideoFrames.seq";
//		BufferedReader groundTruth = new BufferedReader(new InputStreamReader(new FileInputStream(basePath+"2012TestVideoFrames.gt.txt"), "UTF-8"));
//		float[][] latlons_ME14_pho=(float[][]) General.readObject("O:/MediaEval14/MEval14_photos_latlons.floatArr");
//		//set SeqFile
//		Configuration conf = new Configuration();
//		FileSystem hdfs  = FileSystem.get(conf);
//		SequenceFile.Writer seqFile=new SequenceFile.Writer(hdfs, conf, new Path(seqFilePath), IntWritable.class, BufferedImage_jpg.class);
//		String line1; 
//		int queryInd=latlons_ME14_pho[0].length;//queryId start from latlons_ME14_pho
//		//count frame number
//		int frameNum=0;
//		while((line1=groundTruth.readLine())!=null){
//			frameNum++;
//		}
//		groundTruth.close();
//		//add latlons_ME14_pho
//		float[][] latlons_combined=new float[2][latlons_ME14_pho[0].length+frameNum];
//		for (int i = 0; i < latlons_ME14_pho[0].length; i++) {
//			latlons_combined[0][i]=latlons_ME14_pho[0][i];
//			latlons_combined[1][i]=latlons_ME14_pho[1][i];
//		}
//		//process frames
//		groundTruth = new BufferedReader(new InputStreamReader(new FileInputStream(basePath+"2012TestVideoFrames.gt.txt"), "UTF-8"));
//		HashMap<Integer, Integer> queries_L_to_L=new HashMap<Integer, Integer>();
//		while((line1=groundTruth.readLine())!=null){
//			String[] info=line1.split("\t");//photoName, lat, lon
//			String photoName=info[0];
//			//add latlon
//			float[] latlon=General.StrArrToFloatArr(new String[]{info[1],info[2]});
//			latlons_combined[0][queryInd]=latlon[0];
//			latlons_combined[1][queryInd]=latlon[1];
//			//save pho
//			seqFile.append(new IntWritable(queryInd), new BufferedImage_jpg(ImageIO.read(new File(videoFramePath+photoName)), "jpg"));
//			queries_L_to_L.put(queryInd, queryInd);
//			queryInd++;
//		}
//		seqFile.close();
//		groundTruth.close();
//		General.writeObject(basePath+"2012TestVideoFrames_L_to_L.hashMap", queries_L_to_L);
//		General.writeObject(basePath+"2012TestVideoFrames_combinedME14_latlons.floatArr", latlons_combined);
		
		//debug one query
		int query_in_L=5500000+233-1;
		HashMap<Integer, Integer> queries_L_to_L_=new HashMap<Integer, Integer>();
		queries_L_to_L_.put(query_in_L, query_in_L);
		General.writeObject(basePath+"2012TestVideoFrames_L_to_L_"+query_in_L+".hashMap", queries_L_to_L_);

	}

	@SuppressWarnings("unchecked")
	private static void combine_L_to_S_trainTest() throws InterruptedException, FileNotFoundException, IOException{
		HashMap<Integer, Integer> L_to_S_train=(HashMap<Integer, Integer>) General.readObject("O:/MediaEval14/MEval14_photos_L_to_S_train.hashMap");
		HashMap<Integer, Integer> L_to_S_test=(HashMap<Integer, Integer>) General.readObject("O:/MediaEval14/MEval14_photos_L_to_S_test.hashMap");
		System.out.println("L_to_S_train: "+L_to_S_train.size()+", L_to_S_test: "+L_to_S_test.size());
		L_to_S_train.putAll(L_to_S_test);
		System.out.println("L_to_S: "+L_to_S_train.size());
		General.writeObject("O:/MediaEval14/MEval14_photos_L_to_S.hashMap", L_to_S_train);
	}
	
	private static void findTargetPhoto() throws InterruptedException, FileNotFoundException, IOException{
//		//check target doc photos
//		int[] targetPhoto_inL=new int[]{81418570,83047878,100250317};
//		HashSet<Integer> selPhotos=new HashSet<Integer>();
//		for (int one : targetPhoto_inL) {
//			selPhotos.add(one);
//		}
//		General.writeObject("O:/MediaEval14/MEval14_TargetPhotos_L_"+General.IntArrToString(targetPhoto_inL, "-")+".hashSet", selPhotos);
		//check query
		Configuration conf = new Configuration();
		FileSystem hdfs  = FileSystem.get(conf);
		SequenceFile.Reader queryPhotos=new SequenceFile.Reader(hdfs, new Path("O:/MediaEval14/VideoFrameTest/2012TestVideoFrames.seq"), conf);
		IntWritable key=new IntWritable(); BufferedImage_jpg value=new BufferedImage_jpg();
		while (queryPhotos.next(key, value)) {
			if ((key.get()-5500000+1)==233) {
				General_BoofCV.showImage(value.getBufferedImage("", Disp.getNotDisp()), "query:"+key);
			}
		}
		queryPhotos.close();
	}
}
