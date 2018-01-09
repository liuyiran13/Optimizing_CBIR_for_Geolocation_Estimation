package MediaEval15;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import com.almworks.sqlite4java.SQLiteException;

import MyAPI.General.General;
import MyAPI.Obj.Disp;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
import MyCustomedHaoop.ValueClass.VideoBytes;

public class makeData {
	
	/**
	 * java -Xms70g -Xmx100g -cp mkData_ME15.jar:$CLASSPATH MediaEval15.makeData
	 * 
	 */
	public static void main(String[] args) throws NumberFormatException, SQLiteException, IOException, ClassNotFoundException, InterruptedException, SQLException {
		
//		checkOriData();
		
//		splitTestSetByYear();
		
//		checkTaskData();
		
//		String basePath="/tudelft.net/staff-bulk/ewi/insy/mmc/XinchaoLi/";
//		MediaEval14.makeData.mapTaskData2(0, basePath+"MediaEval14/YFCC100M_metaData/yfcc100m_hash", basePath+"MediaEval15/DataSet/OriData/mediaeval2015_placing_locale_train", basePath+"MediaEval15/DataSet/OriData/MediaEval15/mediaeval2015_placing_locale_test", 
//				"ME15", basePath+"MediaEval15/DataSet/", basePath+"MediaEval14/", basePath+"/MediaEval14/YFCC100M_metaData/");
		
//		String basePath="F:/Experiments/MediaEval15/DataSet/";
//		groupPhotoFileIntoSeq(basePath+"2015NewAddedImages_seq/", basePath+"/ME15_photos_s_to_photoID_md5_phoIndInL.txt", 
//				basePath+"/2015NewAddedImages/",0);
		
		makeQuerySets();

	}
	
	public static void checkOriData() throws IOException{
		String filePath="F:/Experiments/MediaEval15/mediaeval2015_placing_locale_test";
		General.dispTopLines_FromFile(filePath,100);
		
		BufferedReader inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"));
		String line1Photo; int ind=0; int[][] PhoVid_TargTot=new int[2][2];//phot:[2014,2015], vid[2014,2015]
		while((line1Photo=inStr_photoMeta.readLine())!=null){
			String[] infos=line1Photo.split("\t");
			int isPho=infos[1].equalsIgnoreCase("0")?0:1;
			int is2014=infos[2].equalsIgnoreCase("2014")?0:1;
			PhoVid_TargTot[isPho][is2014]++;
			if (is2014!=1) {
				System.out.println(ind+"-th Line is not 2014:"+line1Photo);
			}
			ind++;
		}
		inStr_photoMeta.close();
		System.out.println("phot_2014:"+PhoVid_TargTot[0][0]+", phot_2015:"+PhoVid_TargTot[0][1]+", vid_2014:"+PhoVid_TargTot[1][0]+", vid_2015:"+PhoVid_TargTot[1][1]);
	}
	
	@SuppressWarnings("unchecked")
	public static void checkTaskData() throws IOException, InterruptedException{
		//1. as train data for 2015 and 2014 are the same, so the first part of latLons should be the same.
		float[][] latlons_2014=(float[][]) General.readObject("N:/ewi/insy/MMC/XinchaoLi/MediaEval14/MEval14_photos_latlons.floatArr");
		float[][] latlons_2015=(float[][]) General.readObject("N:/ewi/insy/MMC/XinchaoLi/MediaEval15/DataSet/ME15_photos_latlons.floatArr");
		HashMap<Integer, Integer> L_to_S_2014_train=(HashMap<Integer, Integer>) General.readObject("N:/ewi/insy/MMC/XinchaoLi/MediaEval14/MEval14_photos_L_to_S_train.hashMap");
		HashMap<Integer, Integer> L_to_S_2014_test=(HashMap<Integer, Integer>) General.readObject("N:/ewi/insy/MMC/XinchaoLi/MediaEval14/MEval14_photos_L_to_S_test.hashMap");

		BufferedReader inStr_photoMeta_2015 = new BufferedReader(new InputStreamReader(new FileInputStream("N:/ewi/insy/MMC/XinchaoLi/MediaEval15/DataSet/ME15_photos_s_to_photoID_md5_phoIndInL.txt"), "UTF-8"));
		String line1Photo; int Sind_2015=0; int notExistIn2014=0; int existInTrain2014=0; int existInTest2014=0;
		while((line1Photo=inStr_photoMeta_2015.readLine())!=null){
			int ind_L_2015=Integer.valueOf(line1Photo.split("\t")[2]);
			Integer Sind_2014_train=L_to_S_2014_train.get(ind_L_2015);
			Integer Sind_2014_test=L_to_S_2014_test.get(ind_L_2015);
			if (Sind_2014_train!=null) {
				boolean isSame=General.isEqual_float(latlons_2014[0][Sind_2014_train], latlons_2015[0][Sind_2015], 0.0001f) && General.isEqual_float(latlons_2014[1][Sind_2014_train], latlons_2015[1][Sind_2015], 0.0001f);
				if (!isSame) {
					System.out.println("err! notSame in Sind_2014_train, "+Sind_2015+"-th line "+latlons_2014[0][Sind_2014_train]+", "+latlons_2014[1][Sind_2014_train]);
					System.out.println("err! notSame in Sind_2014_train, "+Sind_2015+"-th line "+latlons_2015[0][Sind_2015]+", "+latlons_2015[1][Sind_2015]);
				}
				existInTrain2014++;
			}else if (Sind_2014_test!=null) {
				boolean isSame=General.isEqual_float(latlons_2014[0][Sind_2014_test], latlons_2015[0][Sind_2015], 0.0001f) && General.isEqual_float(latlons_2014[1][Sind_2014_test], latlons_2015[1][Sind_2015], 0.0001f);
				if (!isSame) {
					System.out.println("err! notSame in Sind_2014_test, "+Sind_2015+"-th line "+latlons_2014[0][Sind_2014_test]+", "+latlons_2014[1][Sind_2014_test]);
					System.out.println("err! notSame in Sind_2014_test, "+Sind_2015+"-th line "+latlons_2015[0][Sind_2015]+", "+latlons_2015[1][Sind_2015]);
				}
				existInTest2014++;
			}else {
				notExistIn2014++;
//				System.out.println("warn! "+Sind_2015+"-th line not exist in L_to_S_2014");
			}
			Sind_2015++;
		}
		inStr_photoMeta_2015.close();
		System.out.println("done! tot2015:"+Sind_2015+", existInTrain2014:"+existInTrain2014+", existInTest2014:"+existInTest2014+", tot2014_train:"+L_to_S_2014_train.size()+", tot2014_test:"+L_to_S_2014_test.size()+", notExistIn2014:"+notExistIn2014);
	}
	
	@SuppressWarnings("unchecked")
	public static void splitTestSetByYear() throws IOException, InterruptedException{
		String basePath="F:/Experiments/MediaEval15/DataSet/";
		HashMap<Integer, Integer> L_to_S_test=(HashMap<Integer, Integer>) General.readObject(basePath+"ME15_photos_L_to_S_test.hashMap");
		HashMap<Integer, Integer> L_to_S_2014_test=(HashMap<Integer, Integer>) General.readObject("N:/ewi/insy/MMC/XinchaoLi/MediaEval14/MEval14_photos_L_to_S_test.hashMap");
		//split 2015 test set by old 2014 and new 2015
		HashMap<Integer, Integer> L_to_S_test_2014=new HashMap<>(); HashMap<Integer, Integer> L_to_S_test_2015=new HashMap<>();
		for (Entry<Integer, Integer> one : L_to_S_test.entrySet()) {
			if (L_to_S_2014_test.containsKey(one.getKey())) {
				L_to_S_test_2014.put(one.getKey(), one.getValue());
			}else {
				L_to_S_test_2015.put(one.getKey(), one.getValue());
			}
		}
		System.out.println("L_to_S_test: "+L_to_S_test.size()+", L_to_S_test2014: "+L_to_S_test_2014.size()+", L_to_S_test2015: "+L_to_S_test_2015.size());
		General.writeObject(basePath+"ME15_photos_L_to_S_test2014.hashMap", L_to_S_test_2014);
		General.writeObject(basePath+"ME15_photos_L_to_S_test2015.hashMap", L_to_S_test_2015);
	}
	
	public static void groupPhotoFileIntoSeq(String dataSavePath, String s_to_photoID_md5_phoIndInL, String imageFilePath, int target) throws InterruptedException, SQLiteException, IOException {
		DecimalFormat percformat = (DecimalFormat)NumberFormat.getPercentInstance();
	    percformat.applyPattern("00.0%");
	    
//		String dataSavePath="Q:/PhotoDataBase/YFCC100M_Webscope100M/";
//		String dataSavePath="/tudelft.net/staff-bulk/ewi/insy/D-MIRLab/XinchaoLi/MediaEval14/";
				
	    String[] marker=new String[]{"_photos","_videos"};
	    boolean isPhoto=(target==0);
	    
		PrintWriter outputStream_report = new PrintWriter(new OutputStreamWriter(new FileOutputStream(dataSavePath+"groupFileIntoSeq_"+marker[target]+".report", false), "UTF-8")); 
		Disp disp=new Disp(true, "", outputStream_report);
		//set SeqFile
		Configuration conf = new Configuration();
		FileSystem hdfs  = FileSystem.get(conf);
				
    	//********** load all test data's  MD5 hashMap into Memory, 2015's training data is the same with 2014, test data is combination of 2014 test and 2015 test **********//
	    int photoNumEst=5000*1000;
		int intervel=100*1000;
	    HashMap<String,Integer> MD5_PhoInd_L=new HashMap<String, Integer>(photoNumEst);
		BufferedReader inStr_photoMeta; 
		String line1Photo; 
		General.dispInfo(outputStream_report, "\n start build all 2015 data's MD5 into hashMap in Memory from "+s_to_photoID_md5_phoIndInL);
		//loop over all folders in root folder
		long startTime=System.currentTimeMillis(); //start time 
		int procPhotos=0;
		//process one meta-data file
		inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(s_to_photoID_md5_phoIndInL), "UTF-8"));
		HashSet<Integer> checkDupPhoIndL=new HashSet<>();
		while((line1Photo=inStr_photoMeta.readLine())!=null){
			String[] infos=line1Photo.split("\t");//photoID_md5_phoIndInL
			int phoInd_L=Integer.valueOf(infos[2]);
			Integer previous=MD5_PhoInd_L.put(infos[1], phoInd_L);
			General.Assert(previous==null, "err! duplicated key in hashMap MD5_PhoInd_L, MD5:"+infos[1]+", its previous phoInd_L:"+previous+", its current phoInd_L:"+phoInd_L);
			General.Assert(checkDupPhoIndL.add(phoInd_L), "err! duplicated PhoInd_L in MD5_PhoInd_L, multi phoIndS have this same phoIndL:"+phoInd_L);
			procPhotos++;
			disp.disp(intervel, procPhotos, "Processing "+photoNumEst+"(estimated) image MD5_PhoIndInL, now "+procPhotos+" finished.."+percformat.format((double)procPhotos/photoNumEst)
					+" ......"+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
			
		}
		General.dispInfo(outputStream_report, "done for build all 2015 data's MD5 into hashMap in Memory from "+s_to_photoID_md5_phoIndInL);
		inStr_photoMeta.close();
		General.Assert(MD5_PhoInd_L.size()==procPhotos,"err! MD5_PhoID.size() should ==procPhotos, but MD5_PhoInd_L.size():"+MD5_PhoInd_L.size()+", procPhotos:"+procPhotos);
		General.dispInfo(outputStream_report, "index done! total "+procPhotos+" photos .... "+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
		
    	//********** Transfer MD5 to PhotoID, and group images into SeqFile **********//
		File[] imageTars=new File(imageFilePath).listFiles(); int procPhoNum=0; int phoSaveInter=100*1000; int tarFileInd=0;
		SequenceFile.Writer seqFile=null;
		General.dispInfo(outputStream_report, "start Transfer MD5 to PhotoID, and group images into SeqFile from: "+imageFilePath+"\n intotal:"+imageTars.length+" tar files.");
		HashSet<String> badTarFiles=new HashSet<>(); checkDupPhoIndL=new HashSet<>();
		for (File oneTar: imageTars) {
			String tarFileName=oneTar.getName();
	    	if (tarFileName.endsWith(".tar.gz")) {
//				General.dispInfo(outputStream_report, "\t --start translate MD5 to PhotoIndex_L for images in tar file: "+tarFileName);
				//read images in .tar.gz file
	    		int fileInd=0;
				try {
					TarArchiveInputStream tarInput=new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(oneTar)));
					TarArchiveEntry currentEntry = null; 
					while((currentEntry=tarInput.getNextTarEntry()) != null) {
						if (currentEntry.isFile() && !currentEntry.getName().contains("list") && !currentEntry.getName().contains("script")) {
							String MD5=currentEntry.getName().split("/")[1];
							General.Assert(!MD5.equalsIgnoreCase(""), fileInd+"-th file's name not in control!, name:"+currentEntry.getName());
							int phoInd_L=-1;
							try{
								phoInd_L=MD5_PhoInd_L.get(MD5);
							}catch (Exception e){
								System.out.println("\t --from tarFile:"+tarFileName+", "+fileInd+"-th file's name not in control! not in hashMap of MD5_PhoInd_L, name:"+currentEntry.getName());
							}
							General.Assert(phoInd_L>0, fileInd+"-th file, photoMD5:"+MD5+" from tarFile:"+tarFileName+" is not exist in MD5_PhoInd_L! phoInd_L:"+phoInd_L);
							//get byte content
							byte[] byteContent = new byte[(int) currentEntry.getSize()];
							tarInput.read(byteContent, 0, byteContent.length);
							//save into SeqFile
				            if (procPhoNum%phoSaveInter==0) {//needs to change to a new seq file
				            	if (seqFile!=null) {
									seqFile.close();
								}
				            	seqFile=new SequenceFile.Writer(hdfs, conf, new Path(dataSavePath+procPhoNum/phoSaveInter+".seq"), IntWritable.class, isPhoto?BufferedImage_jpg.class:VideoBytes.class);
				            	disp.disp("current is "+procPhoNum+"-th photos, it is the "+fileInd+"-th file of "+tarFileInd+"-th tarfile:"+tarFileName+", photo's MD5:"+MD5+", phoInd_L:"+phoInd_L+", made a new seq file to save this and the following photos.");
							}
				            General.Assert(checkDupPhoIndL.add(phoInd_L), "err! duplicated PhoInd_L, multi photo hava this same phoIndL:"+phoInd_L+", current is "+procPhoNum+"-th photos, it is the "+fileInd+"-th file of "+tarFileInd+"-th tarfile:"+tarFileName+", photo's MD5:"+MD5+", phoInd_L:"+phoInd_L);
							seqFile.append(new IntWritable(phoInd_L), isPhoto?new BufferedImage_jpg(byteContent):new VideoBytes(byteContent));
							procPhoNum++;
							fileInd++;
						}
					}
					tarInput.close();
				} catch (IllegalArgumentException | IOException e) {
					disp.disp("\t --err! tarfile is bad! it is "+tarFileInd+"-th tarfile:"+tarFileName);
                	badTarFiles.add(tarFileName);
				}
				//show progress
				disp.disp(100, ++tarFileInd, "-- done for "+tarFileInd+" tar files, badTarFiles:"+badTarFiles+", currnet finished is:"+tarFileName+", in this tarFile, total "+fileInd+" items .... "+General.dispTime(System.currentTimeMillis()-startTime, "min")+", memory: "+General.memoryInfo());
				outputStream_report.flush();
	    	}
		}
		seqFile.close();
    	disp.disp("done! procPhoNum: "+procPhoNum+", tot tarfiles: "+tarFileInd);
		outputStream_report.close();
	}
	
	@SuppressWarnings("unchecked")
	public static void makeQuerySets() throws FileNotFoundException, IOException, InterruptedException{
		String basePath="F:/Experiments/MediaEval15/DataSet/";
		String QMarker="2015";//"", "2014", "2015"
		//make S_to_S
		HashMap<Integer, Integer> L_to_S_test=(HashMap<Integer, Integer>) General.readObject(basePath+"ME15_photos_L_to_S_test"+QMarker+".hashMap");
		HashMap<Integer, Integer> S_to_S_test=new HashMap<Integer, Integer>(L_to_S_test.size());
		for (Entry<Integer, Integer> oneKV : L_to_S_test.entrySet()) {
			S_to_S_test.put(oneKV.getValue(), oneKV.getValue());
		}
		General.Assert(L_to_S_test.size()==S_to_S_test.size(), "err! size of L_to_S_test and S_to_S_test should be the same! L_to_S_test:"+L_to_S_test.size()+", S_to_S_test:"+S_to_S_test.size());
		System.out.println("S_to_S_test:"+S_to_S_test.size());
		General.writeObject(basePath+"ME15_photos_S_to_S_test"+QMarker+".hashMap", S_to_S_test);
		//make small query sets
		Random rand=new Random(); int subSetSize=30*1000; 
		String resPath=basePath+"Q"+QMarker+"_S_to_S_sub"+subSetSize/1000+"K/";
		ArrayList<HashMap<Integer, Integer>> S_to_S=General.randSplitHashMap(rand, S_to_S_test , 0, subSetSize);
		General.makeORdelectFolder(resPath);
		for (int i = 0; i < S_to_S.size(); i++) {
			General.writeObject(resPath+"Q"+i+".hashMap", S_to_S.get(i));
			System.out.println("set-"+i+" size:"+S_to_S.get(i).size());
		}
		
		
	}
}
