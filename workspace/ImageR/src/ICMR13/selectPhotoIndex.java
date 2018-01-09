package ICMR13;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.HashSet;

import MyAPI.General.General;

import com.almworks.sqlite4java.SQLParts;
import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

public class selectPhotoIndex{
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException, SQLiteException, ClassNotFoundException, InterruptedException {
		

		String basePath="D:/xinchaoli/Desktop/My research/My Code/DataSaved/ICMR2013/";
//		String basePath="/tudelft.net/staff-bulk/ewi/mm/D-MIRLab/XinchaoLi/ICMR2013/";
		
		
//		//********* select photos for image search database  ************//
		int SelNum=10*1000*1000; //selected photo nums
////		String dataBasePath="Q:\\FlickrCrawler\\MetaData\\";
//		String dataBasePath="/tudelft.net/staff-bulk/ewi/mm/D-MIRLab/XinchaoLi/FlickrCrawler/MetaData/";
////		PrintWriter outputStream_Report = new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"selectPhoto_"+SelNum/1000/1000+"M.report"), "UTF-8")); 
//		long startTime, endTime; //start time 
//		int dispInter=1000*1000;
//		//set MetaData DataBase
//		String dataBase=dataBasePath+"FlickrPhotoMeta_new.db";
//		SQLParts sql; SQLiteStatement stmt; SQLiteConnection db_read;
//		//read total photo number in the metaData
//		sql = new SQLParts("SELECT * FROM photoMeta");  //SELECT * FROM photoMeta WHERE photoIndex=?
//		db_read = new SQLiteConnection(new File(dataBase));
//		db_read.open(); stmt = db_read.prepare(sql);
//		int procPhotos=0;  int startPhoIndex=0;
//		startTime=System.currentTimeMillis(); //start time 
//		while (stmt.step()) {
//			procPhotos++;
//			int photoIndex = Integer.valueOf(stmt.columnString(0)); //sql, column start from 0
//			//get startPhoIndex
//			if (procPhotos==1)
//				startPhoIndex=photoIndex;
//			//disp
//    		if((procPhotos)%dispInter==0){ 							
//				endTime=System.currentTimeMillis(); //end time 
////				General.dispInfo(outputStream_Report, "Flickr photo DataBase reading: "+new DecimalFormat("0.0").format((double)procPhotos/1000/1000)+" million photos finished!!, "+" ......"+new DecimalFormat("0.0").format((double)((endTime-startTime)/1000)/60)+"mins");
//			}
//		}
//		endTime=System.currentTimeMillis(); //end time 
//		//random choose and save GPS 
//		General.Assert(startPhoIndex==3185258+1, "startPhoIndex="+startPhoIndex+", not equal to "+(3185258+1));
////		int PhoNum=procPhotos; // total photo number in the metaData
////		int[] random=General.randIndex(PhoNum);
////		SelNum=Math.min(SelNum, PhoNum); //select all photos as query
//		HashMap<Integer,Integer> transIndex_LtoS=new HashMap<Integer,Integer>(SelNum); //save map from large index(oringinal index in 60Million) to index in selected photo.
////		int[] transIndex_StoL=new int[SelNum];//save map from index in selected photo to large index
////		for(int i=0;i<SelNum;i++){
////			transIndex_LtoS.put(random[i]+startPhoIndex,i); //random is from 0~ , 
////			transIndex_StoL[i]=random[i]+startPhoIndex;
////		}
////		General.dispInfo(outputStream_Report,"total "+PhoNum+" photos, random selected num :"+transIndex_LtoS.size());
////		General.writeObject(basePath+SelNum/1000/1000+"M_transIndex_LtoS.hashMap", transIndex_LtoS);
////		General.writeObject(basePath+SelNum/1000/1000+"M_transIndex_StoL.intArr", transIndex_StoL);
//		//read dataBase and save GPS
//		stmt.reset();
//		transIndex_LtoS=(HashMap<Integer, Integer>) General.readObject(basePath+SelNum/1000/1000+"M_transIndex_LtoS.hashMap");
//		float[][] latlons=new float[2][SelNum]; //latlons[0]:lats, latlons[1]:lons
//		procPhotos=0; 
//		startTime=System.currentTimeMillis(); //start time 
//		while (stmt.step()) {
//			int photoIndex = Integer.valueOf(stmt.columnString(0)); //sql, column start from 0
//			if(transIndex_LtoS.containsKey(photoIndex)){
//				//read GPS
//				float[] oneLatLon=General.StrArrToFloatArr(stmt.columnString(2).split(","));
//				latlons[0][transIndex_LtoS.get(photoIndex)]=oneLatLon[0]; //index in latlon is small index!
//				latlons[1][transIndex_LtoS.get(photoIndex)]=oneLatLon[1];
//				procPhotos++;
//			}
//			//disp
//    		if((procPhotos)%dispInter==0){ 							
//				endTime=System.currentTimeMillis(); //end time 
////				General.dispInfo(outputStream_Report, "Flickr photo GPS extraction: "+new DecimalFormat("0.0").format((double)procPhotos/1000/1000)+" million photos finished!!, "+" ......"+new DecimalFormat("0.0").format((double)((endTime-startTime)/1000)/60)+"mins");
//			}
//		}
//		endTime=System.currentTimeMillis(); //end time 
//		General.Assert(latlons[0].length==transIndex_LtoS.size(), "latlons[0].length:"+latlons[0].length+", transIndex_LtoS.size():"+transIndex_LtoS.size());
//		//save GPS
//		General.writeObject(basePath+SelNum/1000/1000+"M_selectedPhotos_LatLon.float2", latlons);
////		General.dispInfo(outputStream_Report,"Flickr photo GPS extraction done!!, total "+new DecimalFormat("0.0").format((double)procPhotos/1000/1000)+" million photos, "+" ......"+new DecimalFormat("0.0").format((double)((endTime-startTime)/1000)/60)+"mins");
//		//clean-up
//		stmt.dispose();db_read.dispose();
		
		//********* select querys from image search dataBase  ************//
		int queryNum=100*1000; 
//		int[] transIndex_StoL=(int[]) General.readObject(basePath+SelNum/1000/1000+"M_transIndex_StoL.intArr");
//		int[] random=General.randIndex(transIndex_StoL.length);
//		queryNum=Math.min(queryNum, transIndex_StoL.length);
//		HashMap<Integer,Integer> querys=new HashMap<Integer,Integer>(queryNum); 
//		for(int i=0;i<queryNum;i++){
//			querys.put(transIndex_StoL[(random[i])],random[i]); 
//		}
//		General.writeObject(basePath+"Querys_"+querys.size()/1000+"K_LtoS_from_D10M.hashMap", querys);
//		General.dispInfo(outputStream_Report,"query selection done, total selected querys: "+querys.size());
//		outputStream_Report.close();
		
		int targetQ_S=2576565;
		HashMap<Integer,Integer> querys=(HashMap<Integer, Integer>) General.readObject(basePath+"Querys_"+100+"K_LtoS_from_D10M.hashMap");
		for(int query_L:querys.keySet()){
			if(querys.get(query_L)==targetQ_S){
				System.out.println(query_L);
			}
		}
		int[] transIndex_StoL=(int[]) General.readObject(basePath+SelNum/1000/1000+"M_transIndex_StoL.intArr");
		System.out.println(transIndex_StoL[targetQ_S]);

	}
}
