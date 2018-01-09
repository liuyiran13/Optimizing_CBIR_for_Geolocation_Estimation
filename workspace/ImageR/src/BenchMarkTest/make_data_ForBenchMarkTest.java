package BenchMarkTest;

import java.awt.List;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Random;
import java.util.Map.Entry;

import javax.imageio.ImageIO;

import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;

import boofcv.gui.image.ShowImages;

import com.almworks.sqlite4java.SQLParts;
import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.sun.org.apache.bcel.internal.generic.NEW;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;
import MyAPI.General.General_EJML;
import MyAPI.General.General_Hadoop;
import MyAPI.General.General_IR;
import MyAPI.General.General_geoRank;
import MyAPI.General.General_http;
import MyAPI.Obj.Conf_localMachine;
import MyAPI.Obj.Disp;
import MyAPI.imagR.ImageDataManager;
import MyAPI.imagR.PhotoAllFeats_orgVW;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
import MyCustomedHaoop.ValueClass.forTest;

public class make_data_ForBenchMarkTest{

	public static void main(String[] args) throws Exception {
//		forHerverHoliday();
		
//		forOxfordBuilding();
		System.load("/home/yiran/Desktop/Project_jars/sqlite-linux-amd64/libsqlite4java-linux-amd64-1.0.392.so");
		
		forSanFrancisco();
		
//		forBarcelona();
		
//		MakeUniDistractors();
		
//		test();
	}
	
	public static void forHerverHoliday() throws Exception {
		String photoBasePath="O:/ImageRetrieval/Herve1.5K/ori_Data/Jpeg/";
		String photoBasePath_org="O:/ImageRetrieval/Herve1.5K/ori_Data/Jpeg_organised/";
		String saveBasePath="O:/ImageRetrieval/Herve1.5K/";
		String queryPath="O:/ImageRetrieval/Herve1.5K/ori_Data/eval_holidays/perfect_result.dat";
		
		//query and relPho are marked with negative value!
		//as there is a bug in the dataset, query:103100 and 103900 are exactly the same! so here we only use 103100
		
//		//******************* read groundTruth ****************//
//        HashMap<Integer, HashSet<Integer>> groundTrue=new HashMap<Integer, HashSet<Integer>>();
//		BufferedReader inputStreamFeat = new BufferedReader(new InputStreamReader(new FileInputStream(queryPath), "UTF-8"));
//		String line1_fromFeat; int relPhoNum=0;
//		while((line1_fromFeat=inputStreamFeat.readLine())!=null){
//			String[] info=line1_fromFeat.split(" "); 
//			int queryName=Integer.valueOf(info[0].split("\\.")[0]);
//			if (queryName!=103900) {
//				int relNum=(info.length-1)/2;
//				HashSet<Integer> onePhotoInfo=new HashSet<Integer>(relNum);
//				for (int i = 0; i < relNum; i++) {
//					onePhotoInfo.add(-Integer.valueOf(info[(i+1)*2].split("\\.")[0]));
//				}
//				groundTrue.put(-queryName, onePhotoInfo);
//				relPhoNum+=relNum;
//			}else {
//				System.err.println("current query is "+queryName+", as it is duplicated with 103100, so ignore!");
//			}
//		}
//		inputStreamFeat.close();
//		int queryNum=groundTrue.size();
//		System.out.println("query num in groundTrue:"+queryNum+", relPhoNum:"+relPhoNum);
////		General.writeObject(saveBasePath+"Herve_groundTruth.hashMap", groundTrue);
//		
//		ArrayList<Integer> querys= new ArrayList<Integer>(groundTrue.keySet());
//		
//		//************************* imag seq file ****************//
//		//set Conf_localMachine
//		Conf_localMachine conf_LocMac=new Conf_localMachine();
////        General.makeORdelectFolder(photoBasePath_org);
//        ImageDataManager imageDataManager =new ImageDataManager("IndexIsName", 0, photoBasePath, null, 0, null, null);
//        //set seqFile
//        SequenceFile.Writer seqFile_query_relImg= 
////        		new SequenceFile.Writer(FileSystem.get(conf), conf, new Path(saveBasePath+"HerverImage_test.seq"), IntWritable.class, BufferedImage_jpg.class);
//        		General_Hadoop.createSeqFileWriter(conf_LocMac.conf, General_Hadoop.getLocalPath(saveBasePath+"HerverImage_test0.seq", conf_LocMac.conf), IntWritable.class, BufferedImage_jpg.class);
//        //read Img, save to seq file
//        int relPhoNum_seq=0;
//        for (int queryID:querys) {
//			BufferedImage img_q=imageDataManager.getImage(-queryID, new Disp(true, "", null));
//			seqFile_query_relImg.append(new IntWritable(queryID), new BufferedImage_jpg(img_q, "jpg"));
//        	General.forTransfer(photoBasePath+(-queryID)+".jpg", photoBasePath_org+(queryID)+".jpg");
//        	for (int relID:groundTrue.get(queryID)) {
//        		BufferedImage img_rel=imageDataManager.getImage(-relID, new Disp(true, "", null));
//        		seqFile_query_relImg.append(new IntWritable(relID), new BufferedImage_jpg(img_rel, "jpg"));
//        		General.forTransfer(photoBasePath+(-relID)+".jpg", photoBasePath_org+(relID)+".jpg");
//        		relPhoNum_seq++;
//			}
//		}
//        System.out.println("done! querNum_seq:"+querys.size()+", relPhoNum_seq:"+relPhoNum_seq);
//        seqFile_query_relImg.close();
		
//		//**************** make selPho_transIndex, PhoIDMap ***************
//		int[] noisePhoInds=(int[]) General.readObject("O:/ImageRetrieval/UniDistractors_10M_fromFlickr66MWithPatch");
//		int M=1*1000*1000; 
//		int[] noisyPhoNums= new int[]{9000,99*1000,1*M,10*M};//0; 10k,100k,1M,10M
//		for (int noisyPhoNum : noisyPhoNums) {
//			int totPhoNum=noisyPhoNum+relPhoNum;
//			String dataLabel=noisyPhoNum==0?"_ori1.5K":"_"+General_IR.makeNumberLabel(totPhoNum,"0");
//			HashMap<Integer, Integer> selPho_transIndex=new HashMap<Integer, Integer>(totPhoNum);
//			int[] s_to_l=new int[totPhoNum]; int ind_s=0;
//			//add relevant photos
//			for (int queryID:querys) {
//	        	for (int relID:groundTrue.get(queryID)) {
//	        		selPho_transIndex.put(relID, ind_s);
//	        		s_to_l[ind_s]=relID;
//	    			ind_s++;
//				}
//			}
//			//add noisy photos
//			for (int i = 0; i < noisyPhoNum; i++) {
//				int ind_in_L=noisePhoInds[i];
//				selPho_transIndex.put(ind_in_L, ind_s);
//				s_to_l[ind_s]=ind_in_L;
//				ind_s++;
//			}
//			General.Assert(selPho_transIndex.size()==totPhoNum, "misMatch in selPhoForRetr!");
//			System.out.println("make selPho_transIndex done! for :"+ dataLabel);
////			General.writeObject(saveBasePath+"Herve"+dataLabel+"_SelPhos_L_to_S.hashMap", selPho_transIndex);
////			General.writeObject(saveBasePath+"Herve"+dataLabel+"_SelPhos_S_to_L.intArr", s_to_l);
//		}
		
//		//**************** make selQuery_transIndex, PhoIDMap ***************
//		HashMap<Integer, Integer> selQuery_transIndex=new HashMap<Integer, Integer>(queryNum);
//		for (int queryID:querys) {
//			selQuery_transIndex.put(queryID, queryID);
//		}
//		General.writeObject(saveBasePath+"Herve_querys_L_to_L.hashMap", selQuery_transIndex);
		
	}
	
	@SuppressWarnings("unchecked")
	public static void forOxfordBuilding() throws Exception {
		String saveBasePath="O:/ImageRetrieval/Oxford5K/";
		String photoBasePath=saveBasePath+"ori_Data/oxbuild_images/";
		String photoBasePath_copy=saveBasePath+"ori_Data/oxbuild_images_copy/";
		String photoBasePath_organised=saveBasePath+"ori_Data/oxbuild_images_organised/";
		String queryPath=saveBasePath+"ori_Data/GroundTruth_gt_files_170407/";
		String line1;
		
//		General.makeORdelectFolder(photoBasePath_copy);
//        General.makeORdelectFolder(photoBasePath_organised);
//
//		HashSet<String> buildingNames=new HashSet<String>();
//		for (File oneFile : new File(queryPath).listFiles()) {
//			String[] info=oneFile.getName().split("_");
//			String name = "";
//			for (int i = 0; i < info.length-2; i++) {
//				name+=info[i]+"_";
//			}
//			buildingNames.add(name);
//		}
//		System.out.println(buildingNames);
//		
//		//check whether all 1-5 query's groundTruth are the same
//		for (String label : new String[]{"good","ok","junk"}) {
//			for (String oneBuilding : buildingNames) {
//				String[] infos=new String[6];
//				for (int i = 1; i < 6; i++) {
//					BufferedReader inputStreamFeat = new BufferedReader(new InputStreamReader(new FileInputStream(queryPath+oneBuilding+i+"_"+label+".txt"), "UTF-8"));
//					StringBuffer buffer=new StringBuffer();
//					while((line1=inputStreamFeat.readLine())!=null){
//						buffer.append(line1);
//					}
//					inputStreamFeat.close();
//					infos[i-1]=buffer.toString();
//				}
//				//check if same
//				String oneInfo=infos[0];
//				for (int i = 1; i < 5; i++) {
//					if (!oneInfo.equalsIgnoreCase(infos[i])) {
//						System.out.println("oneBuilding:"+oneBuilding+", file-"+label+"-"+(i+1)+"is not the same!");
//					}
//				}
//			}
//		}
//		
//		//copy photos from ori-data:photoBasePath to photoBasePath_copy
//		int phoNum=0;
//		for (File oneFile : new File(photoBasePath).listFiles()) {
//			File newFile=new File(photoBasePath_copy+oneFile.getName());
//			if(!General.forTransfer(oneFile,newFile)){
//				System.out.println("fail to copy "+oneFile.getAbsolutePath()+" to "+newFile.getAbsolutePath());
//			}
//			phoNum++;
//		}
//		System.out.println(phoNum+" photos copied to "+photoBasePath_copy);
//		
//		//******************* read groundTruth ****************//
//		HashMap<Integer, HashSet<Integer>> query=new HashMap<Integer, HashSet<Integer>>(); //1 building has 1 query set
//        HashMap<Integer, HashSet<Integer>> groundTrue=new HashMap<Integer, HashSet<Integer>>(); //1 building has 1 groundTruth set
//        HashMap<Integer, HashSet<Integer>> junks=new HashMap<Integer, HashSet<Integer>>(); //1 building has 1 junk set
//        HashMap<Integer, String> buildingInd_Name=new HashMap<Integer, String>();
//        HashMap<String, Integer> photoName_ind=new HashMap<String, Integer>();
//		HashMap<Integer, float[]> QueryID_Postions=new HashMap<Integer, float[]>();
//        int buildingInd=-1; int phoInd=0;
//        for (String oneBuilding : buildingNames) {
//        	phoInd=buildingInd*1000;
//        	buildingInd_Name.put(buildingInd, oneBuilding);
//        	//querys
//        	HashSet<Integer> queries=new HashSet<Integer>();
//        	for (int i = 1; i < 6; i++) {
//        		BufferedReader inputStreamFeat = new BufferedReader(new InputStreamReader(new FileInputStream(queryPath+oneBuilding+i+"_query"+".txt"), "UTF-8"));
//        		line1=inputStreamFeat.readLine();inputStreamFeat.close(); //line1: phoName, ObjPostion:[xmin ymin xmax ymax]
//        		String[] phoName_ObjPostion=line1.split(" ");
//        		//get jpgName
//        		String[] temp=phoName_ObjPostion[0].split("_");
//        		String jpgName="";
//        		for (int j = 1; j < temp.length-1; j++) {
//					jpgName+=temp[j]+"_";
//				}
//        		jpgName+=temp[temp.length-1];
//        		//copy photo in organised index
//        		copyAndIndexOnePhoto_forOxfordBuilding(photoName_ind, jpgName, photoBasePath_copy, photoBasePath_organised, phoInd, queries);
//        		//get position
//        		float[] pos=General.StrArrToFloatArr(General.selectArrStr(phoName_ObjPostion, new int[]{1,2,3,4},0,0));
//        		pos[2]-=pos[0]; pos[3]-=pos[1];
//        		QueryID_Postions.put(phoInd, pos);
//        		//updata ind
//        		phoInd--;
//        	}
//        	System.out.println("oneBuilding:"+oneBuilding+", buildingInd:"+buildingInd+",  index queires finished, current phoInd:"+phoInd);
//        	//relevant photos
//        	HashSet<Integer> relPhotos=new HashSet<Integer>();
//        	for (String label : new String[]{"good","ok"}) {
//        		BufferedReader inputStreamFeat = new BufferedReader(new InputStreamReader(new FileInputStream(queryPath+oneBuilding+"1_"+label+".txt"), "UTF-8"));
//        		while((line1=inputStreamFeat.readLine())!=null){
//        			Integer phoInd_exist=photoName_ind.get(line1);
//        			if (phoInd_exist==null) {//this photo is not indexed
//        				copyAndIndexOnePhoto_forOxfordBuilding(photoName_ind, line1, photoBasePath_copy, photoBasePath_organised, phoInd, relPhotos);
//                		phoInd--;
//					}else {//this photo is indexed, only need to be add to groundTruth
//            			relPhotos.add(phoInd_exist);
//					}
//				}
//        		inputStreamFeat.close();
//        	}
//        	System.out.println("buildingInd:"+buildingInd+",  index relevant photos finished, current phoInd:"+phoInd);
//        	//junk photos
//        	HashSet<Integer> junkPhotos=new HashSet<Integer>();
//        	BufferedReader inputStreamFeat = new BufferedReader(new InputStreamReader(new FileInputStream(queryPath+oneBuilding+"1_junk.txt"), "UTF-8"));
//        	while((line1=inputStreamFeat.readLine())!=null){
//    			Integer phoInd_exist=photoName_ind.get(line1);
//    			if (phoInd_exist==null) {//this photo is not indexed
//    				copyAndIndexOnePhoto_forOxfordBuilding(photoName_ind, line1, photoBasePath_copy, photoBasePath_organised, phoInd, junkPhotos);
//            		phoInd--;
//				}else {//this photo is indexed, only need to be add to junkPhotos
//					junkPhotos.add(phoInd_exist);
//				}
//			}
//    		inputStreamFeat.close();    
//    		System.out.println("buildingInd:"+buildingInd+",  index junk photos finished, current phoInd:"+phoInd);
//    		//add query, relevent photos and junk photos
//    		query.put(buildingInd, queries);
//    		groundTrue.put(buildingInd, relPhotos);
//    		junks.put(buildingInd, junkPhotos);
//        	//updata building index
//        	buildingInd--;
//        }
//        General.writeObject(saveBasePath+"OxfordBuilding_query.hashMap", query);
//        General.writeObject(saveBasePath+"OxfordBuilding_groundTruth.hashMap", groundTrue);
//        General.writeObject(saveBasePath+"OxfordBuilding_junks.hashMap", junks);
//        General.writeObject(saveBasePath+"OxfordBuilding_buildingInd_Name.hashMap", buildingInd_Name);
//        General.writeObject(saveBasePath+"QueryID_Postions.hashMap", QueryID_Postions);
//        //******************* index noise photos ****************//
//        phoInd=buildingInd*1000;
//        System.out.println("begin index noise photos, virtual buildingInd:"+buildingInd+",  current phoInd:"+phoInd);
//		for (File oneFile : new File(photoBasePath_copy).listFiles()) {
//			File newFile=new File(photoBasePath_organised+phoInd+".jpg");
//			if(!General.forTransfer(oneFile,newFile)){
//				System.out.println("fail to copy "+oneFile.getAbsolutePath()+" to "+newFile.getAbsolutePath());
//			}
//			phoInd--;
//		}
//		System.out.println("index noisy photos finished, current phoInd:"+phoInd);
//		//**** delete photoBasePath_copy ***********
//		General.deleteAll(new File(photoBasePath_copy));
		
//        //************************* imag seq file ****************//
//		HashMap<Integer, float[]> QueryID_Postions=(HashMap<Integer, float[]>) General.readObject(saveBasePath+"QueryID_Postions.hashMap");
//		//set FileSystem
//		Configuration conf = new Configuration();
//        FileSystem hdfs  = FileSystem.get(conf);
//        //set SeqFile
//        SequenceFile.Writer seqFile_allImg=new SequenceFile.Writer(hdfs, conf, new Path(saveBasePath+"OxfordBuilding_cutQ.seq"), IntWritable.class, BufferedImage_jpg.class); 
        //read all Img, save to seq file
		ImageDataManager imageDataManager=new ImageDataManager("IndexIsName", 0, photoBasePath_organised, null, 0, null, null, null);
        int seqPhotoNum=0; HashSet<Integer> errPhotos=new HashSet<Integer>();
        for (File oneFile : new File(photoBasePath_organised).listFiles()) {
			int photoID=Integer.valueOf(oneFile.getName().split(".j")[0]);
			try {
				BufferedImage img_q=imageDataManager.getImage(photoID, new Disp(false,"",null));
//				//check query size
//				float[] position=QueryID_Postions.get(photoID);
//				if (position!=null) {//this is a query
//					System.out.println(photoID+" has bounding box:"+General.floatArrToString(position, ", ", "0"));
////					ShowImages.showWindow(img_q, img_q.getWidth()+"_"+img_q.getHeight());
//					img_q=img_q.getSubimage((int)position[0], (int)position[1], (int)(position[2]), (int)(position[3]));
////					ShowImages.showWindow(img_cut, img_cut.getWidth()+"_"+img_cut.getHeight()+"_"+General.floatArrToString(position, ", ", "0"));
//				}
//				seqFile_allImg.append(new IntWritable(photoID), new BufferedImage_jpg(img_q, "jpg"));
				seqPhotoNum++;
			} catch (Exception e) {
				System.err.println("read jpg img err for pho:"+photoID);
				errPhotos.add(photoID);
			}
		}
//        seqFile_allImg.close();
        System.out.println("done! save all images into SeqFile, seqPhoto_num:"+seqPhotoNum+", errPhotos:"+errPhotos.size()+", they are "+errPhotos);

        
        //**************** make selPho_transIndex, PhoIDMap ***************
		int[] noisePhoInds=(int[]) General.readObject("O:/ImageRetrieval/UniDistractors_10M_fromFlickr66MWithPatch");
		int M=1*1000*1000; 
		int[] noisyPhoNums= new int[]{5000,95*1000,1*M,10*M};//0; 10k,100k,1M,10M
		for (int noisyPhoNum : noisyPhoNums) {
			int totPhoNum=noisyPhoNum+seqPhotoNum;
			String dataLabel=noisyPhoNum==0?"_ori5K":"_"+General_IR.makeNumberLabel(totPhoNum,"0");
			HashMap<Integer, Integer> selPho_transIndex=new HashMap<Integer, Integer>(totPhoNum);
			int[] s_to_l=new int[totPhoNum]; int ind_s=0;
			//add photos from oxford
  			for (File oneFile : new File(photoBasePath_organised).listFiles()) {
  				int photoID=Integer.valueOf(oneFile.getName().split(".j")[0]);
  				if (!errPhotos.contains(photoID)) {
  					selPho_transIndex.put(photoID, ind_s);
  			    	s_to_l[ind_s]=photoID;
  					ind_s++;
  				}
  			}
			//add noisy photos
			for (int i = 0; i < noisyPhoNum; i++) {
				int ind_in_L=noisePhoInds[i];
				selPho_transIndex.put(ind_in_L, ind_s);
				s_to_l[ind_s]=ind_in_L;
				ind_s++;
			}
			General.Assert(selPho_transIndex.size()==totPhoNum, "misMatch in selPhoForRetr!");
			System.out.println("make selPho_transIndex done! for :"+ dataLabel);
			General.writeObject(saveBasePath+"Oxford"+dataLabel+"_SelPhos_L_to_S.hashMap", selPho_transIndex);
			General.writeObject(saveBasePath+"Oxford"+dataLabel+"_SelPhos_S_to_L.intArr", s_to_l);
		}
      		
//		//**************** make selQuery_L_to_L, PhoIDMap ***************
////		HashMap<Integer, HashSet<Integer>> query= (HashMap<Integer, HashSet<Integer>>) General.readObject(saveBasePath+"OxfordBuilding_query.hashMap");
//		HashMap<Integer, Integer> selQuery_transIndex=new HashMap<Integer, Integer>(query.size());
//		for (Entry<Integer, HashSet<Integer>> building_queries:query.entrySet()) {
//        	for (int queryID : building_queries.getValue()) {
//        		selQuery_transIndex.put(queryID, queryID);
//        	}
//		}
//		General.writeObject(saveBasePath+"Oxford_querys_L_to_L.hashMap", selQuery_transIndex);
		
        
        
        System.out.println("done! ");
		
	}

	public static void forBarcelona() throws Exception {
		String photoBasePath="O:/ImageRetrieval/Barcelona1K/ori_Data/ec1m_landmark_images/";
		String saveBasePath="O:/ImageRetrieval/Barcelona1K/";
		String queryPath="O:/ImageRetrieval/Barcelona1K/ori_Data/Queries.txt";
		
		//query and relPho are marked with negative value! e.g., ec1m_00010003.jpg is mapped to -10003
		
		//******************* read groundTrue_BuildingID and make imag seq file ****************//
        HashMap<Integer, HashSet<Integer>> groundTrue_BuildingID=new HashMap<Integer, HashSet<Integer>>();
//        //set FileSystem
//      	Configuration conf = new Configuration();
//        FileSystem hdfs  = FileSystem.get(conf);
//        //set seqFile
//        SequenceFile.Writer seqFile_query_relImg=new SequenceFile.Writer(hdfs, conf, new Path(saveBasePath+"Barcelona1K.seq"), IntWritable.class, BufferedImage_jpg.class);  
        //run
        int relPhoNum=0;//add photos (include querys) are relpho and will be indexed as the retrieval set
//        HashMap<Integer, Integer> allPhotoAsQuery_transIndex=new HashMap<Integer, Integer>();//for pairwise PRCurve
        for (File oneImage : new File(photoBasePath).listFiles()) {
        	int photoID=-Integer.valueOf(oneImage.getName().split("\\.")[0].split("_")[1]);//photoID is negative
        	//add to groundTrue_BuildingID
        	int buildingID=photoID/10000;//building ID is negative
        	HashSet<Integer> relPhotos=groundTrue_BuildingID.get(buildingID);
        	if (relPhotos!=null) {
        		relPhotos.add(photoID);
			}else {
				relPhotos=new HashSet<Integer>();
				relPhotos.add(photoID);
				groundTrue_BuildingID.put(buildingID, relPhotos);
			}
        	//read Img, save to seq file
//        	BufferedImage img=General_BoofCV.readUniOrintaionImg_from_file(oneImage, true, "");//General_BoofCV.readUniOrintaionImg_from_file
//			General.dispPhoto(img);
//        	seqFile_query_relImg.append(new IntWritable(photoID), new BufferedImage_jpg(img,"jpg"));
//        	if (relPhoNum==0) {//check photo
//            	General.dispPhoto(new BufferedImage_jpg(oneImage, false, "").getBufferedImage());
//            	General.dispPhoto(new BufferedImage_jpg(General_BoofCV.readUniOrintaionImg_from_file(oneImage),"jpg").getBufferedImage());//in retrieval test, jpg is better!
//            	General.dispPhoto(new BufferedImage_jpg(General_BoofCV.readUniOrintaionImg_from_file(oneImage),"png").getBufferedImage());
//			}
			relPhoNum++;
//			allPhotoAsQuery_transIndex.put(photoID, photoID);
		}
//        seqFile_query_relImg.close();
		System.out.println("building num:"+groundTrue_BuildingID.size()+", relPhoNum:"+relPhoNum);
//		General.writeObject(saveBasePath+"Barcelona_groundTruthBuildingID.hashMap", groundTrue_BuildingID);
//		General.writeObject(saveBasePath+"Barcelona_allPhotoAsQuery_L_to_L.hashMap", allPhotoAsQuery_transIndex);
//        //******************* read query, make selQuery_transIndex, PhoIDMap *******************//
//  		HashMap<Integer, Integer> selQuery_transIndex=new HashMap<Integer, Integer>();  		
//		BufferedReader inputStreamFeat = new BufferedReader(new InputStreamReader(new FileInputStream(queryPath), "UTF-8"));
//		String line1;
//		while((line1=inputStreamFeat.readLine())!=null){
//			int photoID=-Integer.valueOf(line1.split("\\.")[0].split("_")[1]);//photoID is negative
//			selQuery_transIndex.put(photoID, photoID);
//		}
//		inputStreamFeat.close();
//		System.out.println("query num:"+selQuery_transIndex.size());
//  		General.writeObject(saveBasePath+"Barcelona_querys_L_to_L.hashMap", selQuery_transIndex);
  		//**************** make selPho_transIndex, PhoIDMap ***************
		int[] noisePhoInds=(int[]) General.readObject("O:/ImageRetrieval/UniDistractors_10M_fromFlickr66MWithPatch");
		int M=1*1000*1000; 
		int[] noisyPhoNums= new int[]{9000,99*1000,1*M,10*M};//0; 10k,100k,1M,10M
		for (int noisyPhoNum : noisyPhoNums) {
			int totPhoNum=noisyPhoNum+relPhoNum;
			String dataLabel=noisyPhoNum==0?"_ori1K":"_"+General_IR.makeNumberLabel(totPhoNum,"0");
			HashMap<Integer, Integer> selPho_transIndex=new HashMap<Integer, Integer>(totPhoNum);
			int[] s_to_l=new int[totPhoNum]; int ind_s=0;
			//add relevant photos
			for (HashSet<Integer> oneBuildingPhos:groundTrue_BuildingID.values()) {
	        	for (int phoID:oneBuildingPhos) {
	        		selPho_transIndex.put(phoID, ind_s);
	        		s_to_l[ind_s]=phoID;
	    			ind_s++;
				}
			}
			//add noisy photos
			for (int i = 0; i < noisyPhoNum; i++) {
				int ind_in_L=noisePhoInds[i];
				selPho_transIndex.put(ind_in_L, ind_s);
				s_to_l[ind_s]=ind_in_L;
				ind_s++;
			}
			General.Assert(selPho_transIndex.size()==totPhoNum, "misMatch in selPhoForRetr!");
			System.out.println("make selPho_transIndex done! for :"+ dataLabel);
			General.writeObject(saveBasePath+"Barcelona"+dataLabel+"_SelPhos_L_to_S.hashMap", selPho_transIndex);
			General.writeObject(saveBasePath+"Barcelona"+dataLabel+"_SelPhos_S_to_L.intArr", s_to_l);
		}
		
	}
	
	public static void copyAndIndexOnePhoto_forOxfordBuilding(HashMap<String, Integer> photoName_ind, String photoName, String oriPath, String destPath, int photoInd, HashSet<Integer> photIndSet) throws Exception{
		Integer phoInd_exist=photoName_ind.get(photoName);
		if (phoInd_exist==null) {//this photo is not indexed
			//copy photo in organised index
    		File oldfile=new File(oriPath+photoName+".jpg");
    		File newfile=new File(destPath+photoInd+".jpg");
			if(!General.forTransfer(oldfile,newfile)){
				System.out.println("fail to copy "+oldfile.getAbsolutePath()+" to "+newfile.getAbsolutePath());
			}
			oldfile.delete();
			photoName_ind.put(photoName, photoInd);
			//add to groundTruth
			photIndSet.add(photoInd);
		}else {//this photo is indexed, only need to be add to groundTruth
			photIndSet.add(phoInd_exist);
		}
	}
	
	public static SQLiteConnection create_db(String filename) throws SQLiteException{
		SQLiteConnection db = new SQLiteConnection(new File(filename));
		db.open();
		db.exec("CREATE TABLE photoMeta(photoIndex Integer, photoCartoID Integer, photoType Integer, "
				+ "photoPanoramaID Integer, latlon CHARACTER(20), fileName CHARACTER(100))");
		return db;
	}
	
	
	
	@SuppressWarnings({ "unchecked", "resource" })
	public static void forSanFrancisco() throws Exception {
		String photoBasePath="/home/yiran/Desktop/Sanfrancisco_QT_removed_0.2/San_tar/";
		String saveBasePath="/home/yiran/Desktop/Sanfrancisco_QT_removed_0.2/";		
		String dataBase=saveBasePath+"SanFranciscoMeta.db"; //only save training image metaData
		int globalStartIndex=0; //this index is based on the last ind in 65M flickr photos
		//set FileSystem
		Configuration conf = new Configuration();
        MapFile.Writer.setIndexInterval(conf, 10); //set MapFileOutputFormat index intervel
        MapFile.Writer mapFile_writer = null; MapFile.Reader mapFile_reader;
       
        ArrayList<Integer> queryIDs; ArrayList<String> queryFileNames;  ArrayList<ArrayList<Integer>> query_cartoIDs; ArrayList<float[]> queryLatLons;
        BufferedReader inStr_photoMeta; 
        SQLParts sql; SQLiteStatement stmt; SQLiteConnection db_connection;
		String line1; PrintWriter outputStream_report = null;
			
		int globalIndex=globalStartIndex; //save the current index
		outputStream_report = new PrintWriter(new OutputStreamWriter(new FileOutputStream(saveBasePath+"indexPhotos.report"), "UTF-8")); 
		
		
		
//        //******************* index query image && groundTruth ****************//
		General.dispInfo(outputStream_report,"******************* index query image && groundTruth **************");
        queryFileNames=new ArrayList<String>();
        query_cartoIDs=new ArrayList<ArrayList<Integer>>();
        //get queryName, and cartogoryID
//		inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(photoBasePath+"Evaluation_Package/cartoid_groundTruth.txt"), "UTF-8"));
		inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(photoBasePath+"cartoid_groundTruth_2014_04.txt"), "UTF-8"));
		while((line1=inStr_photoMeta.readLine())!=null){
			String[] info=line1.split(" "); 
			queryFileNames.add(info[0]);
			ArrayList<Integer> cartoIDs=new ArrayList<Integer>();
			for (int i = 1; i < info.length; i++) {
				try {
					cartoIDs.add(Integer.valueOf(info[i]));
				} catch (NumberFormatException e) {
					General.dispInfo(outputStream_report, "NumberFormatException for "+info[i]+", from "+line1);
				}
			}
			query_cartoIDs.add(cartoIDs);
			System.out.println("cartoIDs number" + cartoIDs);
		}
		inStr_photoMeta.close();
        System.out.println("queryFileNames is" + queryFileNames);
		//get latlon and index query
		queryIDs=new ArrayList<Integer>();
        queryLatLons=new ArrayList<float[]>();
        mapFile_writer=new MapFile.Writer(conf, new Path(saveBasePath+"SanFrancisco_MFile_inLindex"), MapFile.Writer.keyClass(IntWritable.class), MapFile.Writer.valueClass(BufferedImage_jpg.class) ); 
		String queryDir=photoBasePath+"BuildingQueryImagesCartoIDCorrected-Upright/";
        for (int i = 0; i < queryFileNames.size(); i++) {
			String queryFileName=queryFileNames.get(i);
			//get latlon
			inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(queryDir+queryFileName+".gps"), "UTF-8"));
			queryLatLons.add(General.StrArrToFloatArr(inStr_photoMeta.readLine().split(" ")));
			inStr_photoMeta.close();
			//index image
			queryIDs.add(globalIndex);
			mapFile_writer.append(new IntWritable(globalIndex), new BufferedImage_jpg(queryDir+queryFileName+".jpg", false, ""));
			//updata index
			globalIndex++;
        }
        System.out.println("queryIDs is" + queryIDs);
        System.out.println("queryLatLons is" + queryLatLons);
        General.dispInfo(outputStream_report, "index query done! tot-"+queryFileNames.size()+" querys, current globalIndex:"+globalIndex);
        //save queryInfo
		General.writeObject(saveBasePath+"SanFrancisco_queryFileNames.arrList", queryFileNames);
		General.writeObject(saveBasePath+"SanFrancisco_queryIDs.arrList", queryIDs);
		General.writeObject(saveBasePath+"SanFrancisco_queryCartoIDs.arrList", query_cartoIDs);
		General.writeObject(saveBasePath+"SanFrancisco_queryCartoIDs_corr2014.arrList", query_cartoIDs);
		General.writeObject(saveBasePath+"SanFrancisco_queryLatLons.arrList", queryLatLons);
////
////		
////		
////		
////        //******************* index training image ****************//
		General.dispInfo(outputStream_report,"******************* index training image **************");
		TarArchiveInputStream tarInputStream;
		TarArchiveEntry entry;
		//build dataBase
 		File dataBaseFile  =   new  File(dataBase); 
 		General.deleteAll(dataBaseFile);
 		db_connection = create_db(dataBase);
 		db_connection.exec("BEGIN TRANSACTION");
 		sql = new SQLParts("INSERT INTO photoMeta values(?, ?, ?, ?, ?, ?)");
 		stmt = db_connection.prepare(sql);
 		long startTime=System.currentTimeMillis(); int tarFileNum=0;
        for(File oneFile:new File(photoBasePath).listFiles()){
        	if (oneFile.getName().endsWith(".tar")) {
        		//open tar
        		tarInputStream = (TarArchiveInputStream) new ArchiveStreamFactory().createArchiveInputStream("tar", new FileInputStream(oneFile));
        	    int photoNum_inTar=0;
        		while ((entry = (TarArchiveEntry)tarInputStream.getNextEntry()) != null) {
        	    	String entryName=entry.getName().split("/")[1];
        	    	General.Assert(entryName.endsWith(".jpg"), entry.getName()+" should end with .jpg");
        	    	//get meta
        	    	String[] meta=entryName.split("\\.jpg")[0].split("_");
        	    	stmt.bind(1, globalIndex);//column index begin from 1
        	    	stmt.bind(2, Integer.valueOf(meta[7]));//cartoID
        	    	stmt.bind(3, meta[0].equalsIgnoreCase("PCI")?0:1);//photoType, 0:PCI, 1:PFI
        	    	stmt.bind(4, Integer.valueOf(meta[2]));//photoPanoramaID
        	    	stmt.bind(5, meta[3]+","+meta[4]);//lat,lon
        	    	stmt.bind(6, entryName);//fileName:PCI_sp_11841_37.789727_-122.386935_937762214_0_727224067_246.264_-37.8198.jpg
        	    	if(stmt.step()) {
        	    		General.dispInfo(outputStream_report, "stmt.step() Error in insert data, photoIndex-"+globalIndex+", entryName:"+entryName);
        			}
        			stmt.reset();
        	    	//index image
        			mapFile_writer.append(new IntWritable(globalIndex), new BufferedImage_jpg(ImageIO.read(tarInputStream),"jpg"));
        			//updata index
        			globalIndex++;
        			photoNum_inTar++;
        		}
        		tarInputStream.close();
        		tarFileNum++;
        		General.dispInfo(outputStream_report, oneFile+" done! tarFileNum:"+tarFileNum+", photoNum_inTar:"+photoNum_inTar+", current globalIndex:"+globalIndex+", "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
        	}
        }
        mapFile_writer.close();
        General.dispInfo(outputStream_report, "index train photos done! current globalIndex:"+globalIndex+", "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
        db_connection.exec("COMMIT TRANSACTION");
        db_connection.exec("CREATE UNIQUE INDEX index_PhotoIndex on photoMeta(photoIndex)");
        db_connection.exec("CREATE INDEX index_PhotoCartoID on photoMeta(photoCartoID)");
        db_connection.exec("CREATE INDEX index_PhotoType on photoMeta(photoType)");
        db_connection.exec("CREATE INDEX index_PhotoPanoramaID on photoMeta(photoPanoramaID)");
		stmt.dispose();db_connection.dispose();
        General.dispInfo(outputStream_report, "creat index on train photos db done! "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
        outputStream_report.close();
////      
////		
////		
////		
//        //******************* make data for MAP evaluation ****************//
//		outputStream_report = new PrintWriter(new OutputStreamWriter(new FileOutputStream(saveBasePath+"makeDataForMAP.report"), "UTF-8")); 
		outputStream_report = new PrintWriter(new OutputStreamWriter(new FileOutputStream(saveBasePath+"makeDataForMAP_corr2014.report"), "UTF-8")); 
		queryIDs=(ArrayList<Integer>) General.readObject(saveBasePath+"SanFrancisco_queryIDs.arrList");
//		query_cartoIDs=(ArrayList<ArrayList<Integer>>) General.readObject(saveBasePath+"SanFrancisco_queryCartoIDs.arrList");
		query_cartoIDs=(ArrayList<ArrayList<Integer>>) General.readObject(saveBasePath+"SanFrancisco_queryCartoIDs_corr2014.arrList");
        HashMap<Integer, HashSet<Integer>> groundTrue=new HashMap<Integer, HashSet<Integer>>();
        //set database
        db_connection = new SQLiteConnection(new File(dataBase)); 
	    db_connection.open(); 
		sql = new SQLParts("SELECT photoIndex FROM photoMeta WHERE photoType=0 AND photoCartoID=?");  //SELECT photoIndex FROM photoMeta WHERE photoType=0 AND photoCartoID=?
		stmt = db_connection.prepare(sql); int gtSize_min=Integer.MAX_VALUE; int gtSize_max=Integer.MIN_VALUE; int totRelNum=0;
		for (int i = 0; i < queryIDs.size(); i++) {
			ArrayList<Integer> relPhotos=new ArrayList<Integer>(); StringBuffer cartoID_phoNum=new StringBuffer();
			for (int cartoID : query_cartoIDs.get(i)) {
				stmt.bind(1, cartoID);
				int phoNum=0;
				while (stmt.step()){
					relPhotos.add(stmt.columnInt(0)); //sql, column start from 0
					System.out.println("stmt value is" + stmt.columnInt(0));
					phoNum++;
				}
				stmt.reset();
				cartoID_phoNum.append(cartoID+"-"+phoNum+" ");
			}
			groundTrue.put(queryIDs.get(i), new HashSet<Integer>(relPhotos));
			//update gtSize statistics
			gtSize_min=Math.min(gtSize_min, relPhotos.size());
			gtSize_max=Math.max(gtSize_max, relPhotos.size());
			totRelNum+=relPhotos.size();
			General.dispInfo(outputStream_report, i+"-th query is finished! queryID:"+queryIDs.get(i)+", cartoIDs: "+cartoID_phoNum.toString()
					+", relPhotos:"+relPhotos.size()+", current gtSize_min:"+gtSize_min+", gtSize_max:"+gtSize_max+", totRelNum:"+totRelNum);
		}
		stmt.dispose();db_connection.dispose();
		General.writeObject(saveBasePath+"SanFrancisco_groundTruth_onlyPCI_inLInd.hashMap", groundTrue);
		General.writeObject(saveBasePath+"SanFrancisco_groundTruth_onlyPCI_inLInd_corr2014.hashMap", groundTrue);
		General.dispInfo(outputStream_report, "make data for MAP evaluation done! tot-query num:"+queryIDs.size()+", gtSize_min:"+gtSize_min+", gtSize_max:"+gtSize_max+", totRelNum:"+totRelNum);
		outputStream_report.close();
////
////        
////        
//		
////		//**************** only Sanfransico data for geo-prediction: make selQuery_transIndex, selPho_transIndex, PhoIDMap ***************
		//**** get totPhoNum
		int totPhoNum=0;
		//doc num
		sql = new SQLParts("SELECT photoIndex, latlon, photoCartoID FROM photoMeta WHERE photoType=0");  //only PCI, SELECT * FROM photoMeta WHERE photoCartoID=?
        db_connection = new SQLiteConnection(new File(dataBase)); 
        db_connection.open(); 
		stmt = db_connection.prepare(sql);
		while (stmt.step()){
			totPhoNum++;
		}
		stmt.dispose();db_connection.dispose();
		System.out.println("Sanfransico data for geo-prediction, tot doc PhoNum:"+totPhoNum);
		//query num
		queryIDs=(ArrayList<Integer>) General.readObject(saveBasePath+"SanFrancisco_queryIDs.arrList");
		queryLatLons=(ArrayList<float[]>) General.readObject(saveBasePath+"SanFrancisco_queryLatLons.arrList");
		query_cartoIDs=(ArrayList<ArrayList<Integer>>) General.readObject(saveBasePath+"SanFrancisco_queryCartoIDs.arrList");
		query_cartoIDs=(ArrayList<ArrayList<Integer>>) General.readObject(saveBasePath+"SanFrancisco_queryCartoIDs_corr2014.arrList");
		totPhoNum+=queryIDs.size();
		System.out.println("Sanfransico data for geo-prediction, queryNum:"+queryIDs.size()+", totPhoNum:"+totPhoNum);
		//**** make latlons/cartoIDs, transIndex
		float[][] latlons=new float[2][totPhoNum]; int[] s_to_l=new int[totPhoNum];
		int[] cartoIDs_db=new int[totPhoNum];//only save db photo cartoID, not for query, as query has mutiple cartoIDs and each db only have 1 cartoID. note: query's cartoID is in SanFrancisco_queryCartoIDs.arrList with the same order of index_inS
		LinkedList<HashSet<Integer>> cartoIDs_q=new LinkedList<>();
		int index_inS=0;
		//query
		HashMap<Integer, Integer> selQuery_transIndex=new HashMap<Integer, Integer>(queryIDs.size());
		for (int i = 0; i < queryIDs.size(); i++) {
			selQuery_transIndex.put(queryIDs.get(i), index_inS); //each index is changed to begin with 0 from begin with globalStartIndex
			latlons[0][index_inS]=queryLatLons.get(i)[0];
			latlons[1][index_inS]=queryLatLons.get(i)[1];
			s_to_l[index_inS]=queryIDs.get(i);
			cartoIDs_q.add(new HashSet<>(query_cartoIDs.get(i)));
			index_inS++;
		}
		//docs
		HashMap<Integer, Integer> selDocs_transIndex=new HashMap<Integer, Integer>(totPhoNum);
		db_connection = new SQLiteConnection(new File(dataBase)); 
        db_connection.open(); 
		stmt = db_connection.prepare(sql);
		int dispInter=totPhoNum/10;
		while (stmt.step()){
			selDocs_transIndex.put(stmt.columnInt(0), index_inS); //each index is changed to begin with 0 from begin with globalStartIndex
			float[] thisLatLon=General.StrArrToFloatArr(stmt.columnString(1).split(","));
			latlons[0][index_inS]=thisLatLon[0];
			latlons[1][index_inS]=thisLatLon[1];
			s_to_l[index_inS]=stmt.columnInt(0);
			cartoIDs_db[index_inS]=stmt.columnInt(2);
			index_inS++;
			if (index_inS%dispInter==0) {
				System.out.println("index_inS finished:"+index_inS+", current db photo: photoIndex in L (stmt.columnInt(0)):"+stmt.columnInt(0)
						+", thisLatLon"+General.floatArrToString(thisLatLon, ",", "0.000")
						+", cartoID:"+stmt.columnInt(2));
			}
		}
		stmt.dispose();db_connection.dispose();
		//save
		General.Assert((selDocs_transIndex.size()+selQuery_transIndex.size())==totPhoNum, "misMatch in totPhoNum!");
		General.writeObject(saveBasePath+"SanFrancisco_querys_transIndex_L_to_S.hashMap", selQuery_transIndex);
		General.writeObject(saveBasePath+"SanFrancisco_docsPCI_transIndex_L_to_S.hashMap", selDocs_transIndex);
		selDocs_transIndex.putAll(selQuery_transIndex);
		General.Assert(selDocs_transIndex.size()==totPhoNum, "misMatch in totPhoNum!");
		General.writeObject(saveBasePath+"SanFrancisco_Q-DPCI_transIndex_L_to_S.hashMap", selDocs_transIndex);
		General.writeObject(saveBasePath+"SanFrancisco_Q-DPCI_transIndex_S_to_L.intArr", s_to_l);
		General.writeObject(saveBasePath+"SanFrancisco_Q-DPCI_latlons.floatArr", latlons);
		General.writeObject(saveBasePath+"SanFrancisco_Q-DPCI_cartoIDs_db.intArr", cartoIDs_db);
		General.writeObject(saveBasePath+"SanFrancisco_Q-DPCI_cartoIDs_q_corr2014.hashSetArr", cartoIDs_q.toArray(new HashSet[0]));
		System.out.println("File generation finished!!!!");
	
		

		
		
//		//******************* transefer groundTruth ***************
//		HashMap<Integer, HashSet<Integer>> groundTrue_inLInd=(HashMap<Integer, HashSet<Integer>>) General.readObject(saveBasePath+"SanFrancisco_groundTruth_onlyPCI_inLInd.hashMap");
		HashMap<Integer, HashSet<Integer>> groundTrue_inLInd=(HashMap<Integer, HashSet<Integer>>) General.readObject(saveBasePath+"SanFrancisco_groundTruth_onlyPCI_inLInd_corr2014.hashMap");
		HashMap<Integer, Integer> selDocs_transIndex1=(HashMap<Integer, Integer>) General.readObject(saveBasePath+"SanFrancisco_Q-DPCI_transIndex_L_to_S.hashMap");
		HashMap<Integer, HashSet<Integer>> groundTrue_inSInd=new HashMap<Integer, HashSet<Integer>>();
		//for show groundTruth photos
		int showGT_max=10; 
		MapFile.Reader[] image_mapFiles=General_Hadoop.openAllMapFiles(new String[]{"/home/yiran/Desktop/Sanfrancisco_QT_removed_0.5/SanFrancisco_MFile_inLindex/"});
//        String groundTruthDir=saveBasePath+"GTruth_inSind/";
        String groundTruthDir=saveBasePath+"GTruth_inSind_corr2014/";
        General.makeORdelectFolder(groundTruthDir);
		for (Entry<Integer, HashSet<Integer>> one : groundTrue_inLInd.entrySet()) {
			int query_inS=selDocs_transIndex1.get(one.getKey());
			HashSet<Integer> relDocs_inS=new HashSet<Integer>();
			for (Integer doc : one.getValue()) {
				relDocs_inS.add(selDocs_transIndex1.get(doc));
			}
			groundTrue_inSInd.put(query_inS, relDocs_inS);
			//show Q
			General_IR.addPhotoPath_MovePhoto("MapFile", query_inS, groundTruthDir, 100000, null, 0, image_mapFiles);
			//show groundTruth photos
			String thisQPath=groundTruthDir+query_inS+"_"+relDocs_inS.size()+"/";
			General.makeORdelectFolder(thisQPath);
			General_IR.addPhotoPath_MovePhoto("MapFile", query_inS, thisQPath, 100000, null, 0, image_mapFiles);
			int[] randInd=General.randIndex(relDocs_inS.size());
			ArrayList<Integer> relDocs_inS_arr=new ArrayList<>(relDocs_inS);
			for (int j=0;j<Math.min(randInd.length, showGT_max);j++) {
				General_IR.addPhotoPath_MovePhoto("MapFile", relDocs_inS_arr.get(randInd[j]), thisQPath, 100000, null, 0, image_mapFiles);
			}
		}
		General_Hadoop.closeAllMapFiles(image_mapFiles);
		General.writeObject(saveBasePath+"SanFrancisco_groundTruth_onlyPCI_inSInd.hashMap", groundTrue_inSInd);
		General.writeObject(saveBasePath+"SanFrancisco_groundTruth_onlyPCI_inSInd_corr2014.hashMap", groundTrue_inSInd);

		
		
		
			
//		//******************* make Querys_S_to_S, docID_S_to_S ***************
		System.out.println("writing started!!!");
//		HashMap<Integer, Integer> L_to_S=(HashMap<Integer, Integer>) General.readObject(saveBasePath+"SanFrancisco_querys_transIndex_L_to_S.hashMap");
		HashMap<Integer, Integer> L_to_S=(HashMap<Integer, Integer>) General.readObject(saveBasePath+"SanFrancisco_docsPCI_transIndex_L_to_S.hashMap");
		HashMap<Integer, Integer> S_to_S=new HashMap<>();
		for (Entry<Integer, Integer> one_L_to_S : L_to_S.entrySet()) {
			S_to_S.put(one_L_to_S.getValue(), one_L_to_S.getValue());
		}
//		General.writeObject(saveBasePath+"SanFrancisco_querys_transIndex_S_to_S.hashMap", S_to_S);
		General.writeObject(saveBasePath+"SanFrancisco_docsPCI_transIndex_S_to_S.hashMap", S_to_S);
		System.out.println("writing finished!!!");
//		General_Hadoop.closeAllMapFiles(image_mapFiles);
		
		
		
		
		
		//******************* check image and feat files ***************
//		outputStream_report = new PrintWriter(new OutputStreamWriter(new FileOutputStream(saveBasePath+"checkImageAndFeat_inGTruth.report"), "UTF-8")); 
//		Disp disp=new Disp(true, "", outputStream_report);
////		String noFeatPhotos=saveBasePath+"noFeatPhotos/";
//		String noFeatPhotos=saveBasePath+"noFeatPhotos_inGTruth_corr2014/";
//		General.makeORdelectFolder(noFeatPhotos);
//		
//		ImageDataManager imDataManager_Q=new ImageDataManager(100000, "/home/yiran/Desktop/Sanfrancisco/SanFrancisco_MFile_inLindex/", 100000, "/home/yiran/Desktop/Sanfrancisco/feats/SIFTUPRightINRIA2_QDPCIVW20k_MA_SanFran_Q/", null);
//		ImageDataManager imDataManager_D=new ImageDataManager(100000, "/home/yiran/Desktop/Sanfrancisco/SanFrancisco_MFile_inLindex/", 100000, "/home/yiran/Desktop/Sanfrancisco/feats/SIFTUPRightINRIA2_QDPCIVW20k_SA_SanFran_DPCI/", null);
////		HashMap<Integer, Integer> doc_S_to_S=(HashMap<Integer, Integer>) General.readObject(saveBasePath+"SanFrancisco_docsPCI_transIndex_S_to_S.hashMap");
//		HashMap<Integer, HashSet<Integer>> gtruth=(HashMap<Integer, HashSet<Integer>>) General.readObject(saveBasePath+"SanFrancisco_groundTruth_onlyPCI_inSInd_corr2014.hashMap");
//		int i=0; int noFeatNum_Q=0; int noPhotNum_Q=0; int noFeatNum_D=0; int noPhotNum_D=0;
//		for (Entry<Integer, HashSet<Integer>> one : gtruth.entrySet()) {
//			//check Q
//			System.out.println(one.getKey());
//			System.out.println(Disp.getNotDisp());
//			BufferedImage imgQ=imDataManager_Q.getImage(one.getKey(), Disp.getNotDisp());
// 			PhotoAllFeats_orgVW featQ = imDataManager_Q.getPhoFeat(one.getKey(), Disp.getNotDisp());
//			if (imgQ==null || featQ==null) {
//				disp.disp("for query:"+one.getKey()+", imgExist:"+(imgQ!=null)+", featExist:"+(featQ!=null));
//				if (imgQ==null) {
//					noPhotNum_Q++;
//					General.Assert(featQ==null, "err! img==null so feat==null");
//					noFeatNum_Q++;
//				}else if (featQ==null) {
//					ImageIO.write(imgQ, "jpg", new File(noFeatPhotos+"Q_"+one.getKey()+".jpg"));
//					noFeatNum_Q++;
//				}
//			}
//			//check gtruth
//			for (Integer relDoc : one.getValue()) {
//				BufferedImage imgD=imDataManager_D.getImage(relDoc, Disp.getNotDisp());
//				PhotoAllFeats_orgVW featD = imDataManager_D.getPhoFeat(relDoc, Disp.getNotDisp());
//				if (imgD==null || featD==null) {
//					String thisQFolder=noFeatPhotos+one.getKey()+"/";
//					General.makeFolder(thisQFolder);
//					ImageIO.write(imgQ, "jpg", new File(thisQFolder+"Q_"+one.getKey()+".jpg"));
//					disp.disp("for gtruth: "+relDoc+" of query:"+one.getKey()+", imgExist:"+(imgD!=null)+", featExist:"+(featD!=null));
//					if (imgD==null) {
//						noPhotNum_D++;
//						General.Assert(featD==null, "err! img==null so feat==null");
//						noFeatNum_D++;
//					}else if (featD==null) {
//						ImageIO.write(imgD, "jpg", new File(thisQFolder+"D_"+relDoc+".jpg"));
////						General.dispPhoto(imgD);
//						noFeatNum_D++;
//					}
//				}
//			}
//			i++;
//			disp.disp(10, i, i+" querys finished! noFeatNum_Q:"+noFeatNum_Q+", noPhotNum_Q:"+noPhotNum_Q+", noFeatNum_D:"+noFeatNum_D+", noPhotNum_D:"+noPhotNum_D);
//		}
//		disp.disp("done! tot "+i+" querys finished! noFeatNum_Q:"+noFeatNum_Q+", noPhotNum_Q:"+noPhotNum_Q+", noFeatNum_D:"+noFeatNum_D+", noPhotNum_D:"+noPhotNum_D);
//		outputStream_report.close();
	}

	public static void MakeUniDistractors() throws SQLiteException, IOException, InterruptedException{
		float[][] latlon_toEscape=new float[][]{{(float) 51.75194, (float) -1.25777},{(float) 41.38333, (float) 2.18333}};
		int M=1*1000*1000; float escapeRang=(float) 0.1; int distractorNum=10*M;
		//set MetaData DataBase
		SQLParts sql = new SQLParts("SELECT * FROM photoMeta WHERE photoIndex=?");  //SELECT * FROM photoMeta WHERE photoIndex=?
		SQLiteConnection db_read = new SQLiteConnection(new File("Q:/PhotoDataBase/FlickrCrawler/MetaData/FlickrPhotoMeta_new.db"));//
		db_read.open(); SQLiteStatement stmt = db_read.prepare(sql);

		//checkExistPhotosOnHDFS
		boolean[] existPhos=checkExistPhotosOnHDFS("Q:/PhotoDataBase/FlickrCrawler/indexed_Photos_allColl/part-r-00000");//
		System.out.println("check exist done!, memory:"+General.memoryInfo());
		//sequentially sellect, not rand select 
		int startInd=4*M; //the first 3185258 photos are 3M photos, used for train VW
		int latlonScapeNum=0; int[] res=new int[distractorNum]; int res_ind=0; int dispInter=1000*1000;
		for (int i = 0; i < distractorNum; i++) {
			//check exist and latlon_toEscape
			int photoIndex_L=i+startInd;
			if (existPhos[photoIndex_L]) {
				//check latlon_toEscape
				stmt.bind(1, photoIndex_L); float[] latlon=null;
				while (stmt.step()) {
					latlon=General.StrArrToFloatArr(stmt.columnString(2).split(","));
				}
				stmt.reset();
				boolean isEscape=false;
				for (float[] oneLoc:latlon_toEscape) {
					if (General_geoRank.isOneLocation_approximate(oneLoc[0], oneLoc[1], latlon[0], latlon[1], escapeRang)) {
						isEscape=true;
						break;
					}
				}
				if (isEscape) {
					latlonScapeNum++;
					distractorNum++; //this one can not be used, then need add another one!
				}else {
					res[res_ind]=photoIndex_L;
					res_ind++;
				}
				//disp
				General.dispInfo_ifNeed(i%dispInter==0, "\t", i+"-th sample is done! current latlonScapeNum:"+latlonScapeNum+", memory:"+General.memoryInfo());
			}else {
				distractorNum++; //this one can not be used, then need add another one!
			}
		}
		General.Assert(res_ind==res.length, "res_ind should ==res.length");
		stmt.dispose();db_read.dispose();
		System.out.println("done! tot checked dataNum:"+distractorNum+", latlonScapeNum:"+latlonScapeNum);
		
		General.writeObject("O:/ImageRetrieval/UniDistractors_"+General_IR.makeNumberLabel(res.length,"0")+"_fromFlickr66MWithPatch", res);
		
		//make L_to_L hashMap for group photos for rankShow
		String label="UniDistractors_10M_fromFlickr66MWithPatch";
		int[] distractors_inL=(int[]) General.readObject("O:/ImageRetrieval/"+label);
		HashSet<Integer> phoInL=new HashSet<Integer>(distractors_inL.length*2);
		for (int L : distractors_inL) {
			phoInL.add(L);
		}
		General.writeObject("O:/ImageRetrieval/"+label+".hashSet",phoInL);
	}
	
	public static boolean[] checkExistPhotosOnHDFS(String SeqFile_path) throws IOException{
		Configuration conf = new Configuration();
		FileSystem hdfs  = FileSystem.get(conf);
		
		//set key/value class
		IntWritable key_indexL = new IntWritable();
		IntWritable value_indexL = new IntWritable();
				
		//set SeqFile.Reader
		SequenceFile.Reader seqFile=new SequenceFile.Reader(hdfs, new Path(SeqFile_path), conf);
		
		//check total dataNum
		int existNum=0; int lastInd=0;
		while (seqFile.next(key_indexL, value_indexL)) {//key: index_L,  value: value_indexL
			lastInd=key_indexL.get();
			existNum++;
		}
		seqFile.close();
		System.out.println("in SeqFile_path:"+SeqFile_path+", existNum:"+existNum+", lastInd:"+lastInd);
		
		//get exist photo statistic
		seqFile=new SequenceFile.Reader(hdfs, new Path(SeqFile_path), conf);
		
		boolean[] isExist=new boolean[lastInd+1];
		while (seqFile.next(key_indexL, value_indexL)) {//key: index_L,  value: index_L
			int index_L=key_indexL.get();
			isExist[index_L]=true;
			existNum++;
		}
		seqFile.close();
		return isExist;
	}

	public static void test() throws IOException{
		Configuration conf = new Configuration();
		System.out.println(General.StrArrToStr(ImageIO.getReaderFormatNames(), "\n"));
		//set key/value class
		IntWritable key = new IntWritable();
		BufferedImage_jpg value = new BufferedImage_jpg();
				
		//set SeqFile.Reader
		SequenceFile.Reader seqFile=new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path("O:/ImageRetrieval/Herve1.5K/HerverImage.seq")));
		int file_i=0;
		while(seqFile.next(key, value)){
			BufferedImage img=value.getBufferedImage("phoID:"+key, Disp.getNotDisp());
			if (img==null) {
				System.out.println("error! .........  file_i:"+file_i+", key:"+key);
			}else{
				System.out.println("file_i:"+file_i+", key:"+key);
			}
			if (key.get()==-136800) {
				General_BoofCV.showImage(img, key+"");
			}
			if (key.get()==-127900) {
				General_BoofCV.showImage(img, key+"");
			}
			file_i++;
		}
		seqFile.close();
	}

}
