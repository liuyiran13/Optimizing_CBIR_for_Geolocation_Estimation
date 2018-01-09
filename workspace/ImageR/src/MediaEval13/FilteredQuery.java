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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.General.General_geoRank;
import MyAPI.Geo.UserID;
import MyAPI.Geo.groupDocs.UserIDs;
import MyAPI.Obj.Disp;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
import MyCustomedHaoop.ValueClass.ComposeWritableClassCollection.PhotoPointsLoc_DID_Score_ImageRegionMatch_ShortArr_Arr;
import MyCustomedHaoop.ValueClass.IntList_FloatList;

import com.almworks.sqlite4java.SQLParts;
import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

public class FilteredQuery {

	/**
	 * krenew -s -- java -Xms4g -Xmx10g -cp FilteredQuery.jar:$CLASSPATH MediaEval13.FilteredQuery
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws SQLiteException
	 */
	
	public static void main(String[] args) throws IOException, InterruptedException, SQLiteException {
//		indexQ();
		
		makeQSeqFile();
		
//		makeRes();
		
	}
	
	@SuppressWarnings("unchecked")
	protected static void indexQ() throws IOException, InterruptedException, SQLiteException {		
		String dataBase="Q:/PhotoDataBase/FlickrCrawler/MetaData/FlickrPhotoMeta_new.db";
		
		String savePath="O:/MediaEval13/Jaeyoung/filtered/";
		
		//set MetaData DataBase
		SQLParts sql; SQLiteStatement stmt;
		PrintWriter outputStream_Report = new PrintWriter(new OutputStreamWriter(new FileOutputStream(savePath+"indexQ_2nd.report",false), "UTF-8"),true); 
		Disp disp=new Disp(true, "", outputStream_Report);
		disp.disp("start processing....");
		
		//get selected queries
		sql = new SQLParts("SELECT * FROM photoMeta WHERE photoID=?");  //SELECT * FROM photoMeta WHERE photoIndex=?
		SQLiteConnection db_read = new SQLiteConnection(new File(dataBase));
		db_read.open(); stmt = db_read.prepare(sql);
		HashMap<Integer, Integer> ME13Qset_L_to_S=(HashMap<Integer, Integer>) General.readObject("O:/MediaEval13/MEval13_L_to_S_test.hashMap");
		for (File oneFilter : new File(savePath+"oriData/2nd").listFiles()) {//loop over different filtering scheme
			if (oneFilter.isDirectory()) {
				disp.disp("process oneFilter: "+oneFilter.getAbsolutePath());
				HashMap<Integer, Integer> selQ_L_to_S=new HashMap<Integer, Integer>();
				PrintWriter outputStream_photoName_photoIndex_L = new PrintWriter(new OutputStreamWriter(new FileOutputStream(oneFilter.getAbsolutePath()+"_QPhoName_Index_L.txt",false), "UTF-8"),true); 
				int noImgPho=0;
				for (File onePho : oneFilter.listFiles()) {
					Disp dispOneFilter=Disp.makeHardCopyAddSpacer(disp, "\t");
					//find photoIndex_L
					String flickrID=onePho.getName().split("_")[0];
					stmt.bind(1, flickrID);
					int photoIndex_L=-1; boolean photoExist=false; 
					while (stmt.step()) {
						photoIndex_L = Integer.valueOf(stmt.columnString(0)); //sql, column start from 0
						photoExist=true;
					}
					stmt.reset();
					General.Assert(photoExist, "flickrID:"+flickrID+" from dataPath:"+onePho.getAbsolutePath()+" is not exist in current db file!");
					//find this photoIndex_L in ME13Qset_L_to_S
					Integer photoIndex_S=ME13Qset_L_to_S.get(photoIndex_L);
					General.Assert(photoIndex_S!=null, "flickrID:"+flickrID+" from dataPath:"+onePho.getAbsolutePath()+", photoIndex_L: "+photoIndex_L+", is not exist in ME13Qset_L_to_S!");
					//save this photo into seqFile
					BufferedImage img = new BufferedImage_jpg(onePho, false, "").getBufferedImage("flickrID:"+flickrID+" from dataPath:"+onePho.getAbsolutePath()+", photoIndex_L: "+photoIndex_L, dispOneFilter);
					if (img!=null) {
//						General_BoofCV.showImage(img.getBufferedImage(flickrID), flickrID);
						selQ_L_to_S.put(photoIndex_L, photoIndex_S);
						outputStream_photoName_photoIndex_L.println(onePho.getName()+"\t"+photoIndex_L);
					}else {
						noImgPho++;
					}
				}
				disp.disp("done! within tot-"+oneFilter.listFiles().length+" photos, noImgPho:"+noImgPho);
				General.writeObject(oneFilter.getAbsolutePath()+"_selQ_L_to_S.hashMap", selQ_L_to_S);
				outputStream_photoName_photoIndex_L.close();
			}
		}
		outputStream_Report.close();
		stmt.dispose();db_read.dispose();
	}
	
	protected static void makeQSeqFile() throws IOException, InterruptedException, SQLiteException {
		org.apache.log4j.BasicConfigurator.configure();
		
		String savePath="O:/MediaEval13/Jaeyoung/filtered/";
//		String savePath="/tudelft.net/staff-bulk/ewi/insy/mmc/XinchaoLi/MediaEval13/Jaeyoung/filtered/";
		
		//set FileSystem
		Configuration conf = new Configuration();
		//set MetaData DataBase
		PrintWriter outputStream_Report = new PrintWriter(new OutputStreamWriter(new FileOutputStream(savePath+"makeQSeqFile_2nd.report",false), "UTF-8"),true); 
		Disp disp=new Disp(true, "", outputStream_Report);
		disp.disp("start processing....");
		//get selected queries
		for (File oneFilter : new File(savePath+"oriData/2nd").listFiles()) {//loop over different filtering scheme
			if (oneFilter.isDirectory()) {
				disp.disp("process oneFilter: "+oneFilter.getAbsolutePath());
				SequenceFile.Writer seqFile= General_Hadoop.createSeqFileWriter(conf, new Path(oneFilter.getAbsolutePath()+"_QinL.seq"), IntWritable.class, BufferedImage_jpg.class);
				BufferedReader inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(oneFilter.getAbsolutePath()+"_QPhoName_Index_L.txt"), "UTF-8"));
				String line1; int phoNum=0;
				while ((line1=inStr_photoMeta.readLine())!=null) {
					String[] photoFile_phoIndL=line1.split("\t");
					seqFile.append(new IntWritable(Integer.valueOf(photoFile_phoIndL[1])), new BufferedImage_jpg(oneFilter.getAbsolutePath()+"/"+photoFile_phoIndL[0], false, ""));
					phoNum++;
				}
				inStr_photoMeta.close();
				seqFile.close();
				disp.disp("done! tot-"+phoNum+" photos");
			}
		}
		outputStream_Report.close();
	}

	protected static void makeRes() throws IOException, InterruptedException, SQLiteException {
		//set MetaData DataBase
		SQLParts sql; SQLiteStatement stmt, stmt_missingBlock;
		String dataBase="Q:/PhotoDataBase/FlickrCrawler/MetaData/FlickrPhotoMeta_new.db";
		String dataBase_missingBlocks="Q:/PhotoDataBase/FlickrCrawler/MetaData/patch/FlickrPhotoMeta_new_missingBlocks.db";
		sql = new SQLParts("SELECT photoID FROM photoMeta WHERE photoIndex=?");  //SELECT * FROM photoMeta WHERE photoIndex=?
		SQLiteConnection db_read = new SQLiteConnection(new File(dataBase));
		SQLiteConnection db_read_missingBlock = new SQLiteConnection(new File(dataBase_missingBlocks));
		db_read.open(); stmt = db_read.prepare(sql);
		db_read_missingBlock.open(); stmt_missingBlock=db_read_missingBlock.prepare(sql);
		
		String savePath="O:/MediaEval13/Jaeyoung/filtered/visRank/";
		float[][] latlons=(float[][]) General.readObject("O:/MediaEval13/MEval13_latlons.floatArr");
		UserIDs userIDs=new UserIDs((long[]) General.readObject("O:/MediaEval13/MEval13_userIDs_0.long"),(int[]) General.readObject("O:/MediaEval13/MEval13_userIDs_1.int")); 
		int[] s_to_l=(int[]) General.readObject("O:/MediaEval13/MEval13_S_to_L.intArr");
//		String[] schemes=new String[]{"Gotham", "Kelvin", "cropped60p", "cropped80p", "tiltshift"};
//		String[] schemes=new String[]{"Ori", "Toaster", "Lomo", "Nashville"};
		String[] schemes=new String[]{"Ori"};
		PrintWriter outputStream_Report = new PrintWriter(new OutputStreamWriter(new FileOutputStream(savePath+General.StrArrToStr(schemes, "_")+".report",false), "UTF-8"),true); 
		Disp disp=new Disp(true, "", outputStream_Report);
		for (String filterScheme : schemes) {
			disp.disp("start process filterScheme: "+filterScheme);
			String rankPath=savePath+filterScheme+"_MEva13_9M_20K-VW_SURF_iniR-BurstIntraInter_HDs20-HMW12_ReR1K_HDr20_Top1K_1vs1AndHistAndAngle@0.52@0.2@1@0@0@0@0@0@0@0_rankDocScore";
			MapFile.Reader rankReader=new MapFile.Reader(new Path(rankPath), new Configuration());
			IntWritable Key_queryName=new IntWritable();
			IntList_FloatList Value_RankScores= new IntList_FloatList();
			int query_num=0; int query_existRank=0; int correctQ=0; float isOneLocScale=0.01f;
			HashSet<UserID> userIDs_Q=new HashSet<>(); HashSet<UserID> userIDs_corr=new HashSet<>();
			int topDocNum=10; 
//			PrintWriter outputStream_Res = new PrintWriter(new OutputStreamWriter(new FileOutputStream(savePath+filterScheme+".res",false), "UTF-8"),true); 
			while (rankReader.next(Key_queryName, Value_RankScores)) {
				int qID_s=Key_queryName.get();
				ArrayList<Integer> docs = Value_RankScores.getIntegers();
				ArrayList<Float> scores = Value_RankScores.getFloats();
				userIDs_Q.add(userIDs.getOneUsr(qID_s));
				String flickrID_q = getFlickrID(s_to_l[qID_s],stmt,stmt_missingBlock);
				query_num++;
				if (docs.size()>0) {
					//check prediction
					if (General_geoRank.isOneLocation_approximate(latlons[0][qID_s], latlons[1][qID_s], latlons[0][docs.get(0)], latlons[1][docs.get(0)], isOneLocScale)) {
						correctQ++;
						userIDs_corr.add(userIDs.getOneUsr(qID_s));
					}
					query_existRank++;
//					//output
//					StringBuffer oneQRes=new StringBuffer(flickrID_q+"\t");
//					for (int i = 0; i < Math.min(docs.size(), topDocNum); i++) {
//						String flickrID_doc = getFlickrID(s_to_l[docs.get(i)],stmt,stmt_missingBlock);
//						oneQRes.append(flickrID_doc+"_"+scores.get(i)+"\t");
//					}
//					outputStream_Res.println(oneQRes);
//					disp.disp(100, query_num, query_num+" queries finished! query_existRank: "+query_existRank+", correctQ: "+correctQ+", currentQ: "+ oneQRes.toString());
				}else{
					disp.disp("qID_s: "+qID_s+", flickrID_q:"+flickrID_q+", no rank exist!");
				}
			}
			rankReader.close();
			disp.disp(filterScheme+" done! query_num: "+query_num+", query_existRank: "+query_existRank+", correctQ: "+correctQ
					+", unique users: "+userIDs_Q.size()+", userIDs_corr: "+userIDs_corr.size()+"\n");
//			outputStream_Res.close();
		}
		stmt.dispose();db_read.dispose();
		stmt_missingBlock.dispose();db_read_missingBlock.dispose();
	}
	
	private static String getFlickrID(int phoInd_L, SQLiteStatement stmt, SQLiteStatement stmt_missingBlock) throws SQLiteException{
		//find flickrID
		stmt.bind(1, phoInd_L);
		String flickrID=null; boolean photoExist=false; 
		while (stmt.step()) {
			flickrID = stmt.columnString(0); //sql, column start from 0
			photoExist=true;
		}
		stmt.reset();
		if (!photoExist) {//try stmt_missingBlock
			stmt_missingBlock.bind(1, phoInd_L);
			while (stmt_missingBlock.step()) {
				flickrID = stmt_missingBlock.columnString(0); //sql, column start from 0
				photoExist=true;
			}
			stmt_missingBlock.reset();
		}
		General.Assert(photoExist, "photoInd_L:"+phoInd_L+" is not exist in current db file!");
		return flickrID;
	}

}
