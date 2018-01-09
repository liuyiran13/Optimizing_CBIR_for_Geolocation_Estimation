package MyCustomedHaoop.Mapper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyCustomedHaoop.KeyClass.SelSampleKey;

public class SelectSamples{
	
	public boolean isSelection; 
	public boolean isHashMap; //either HashMap or HashSet
	String selectSapmlePath;
	boolean isKeyIsSelSampleKey;
	public static final String selSamples="SelSamples.file"; 
	
	public SelectSamples(String selectSapmlePath, boolean isKeyIsSelSampleKey){
		if (selectSapmlePath!=null) {
			isSelection=true;
			isHashMap=selectSapmlePath.endsWith("hashMap");//either hashMap or hashSet
			this.selectSapmlePath=selectSapmlePath;
			this.isKeyIsSelSampleKey=isKeyIsSelSampleKey;
		}else{
			isSelection=false;
		}
	}
	
	public static void setRandomSelectedSample(int randPhoNum, String randSelSampMark, String selectSapmlePath_mark, String startInd_totNum, String workPath, String dataLabel, Configuration conf) throws NumberFormatException, FileNotFoundException, IOException, ClassNotFoundException{
		if (conf.get(selectSapmlePath_mark)==null) {//no pre-rang, so need to use startInd_totNum to make range
			SelectSamples.setRandSelSam_coarseRang(randSelSampMark, startInd_totNum, randPhoNum, workPath+"randSel_"+randPhoNum+dataLabel, conf);
		}else{
			SelectSamples.setRandSelSam_spicificRang(randSelSampMark, selectSapmlePath_mark, randPhoNum, workPath+"randSel_"+randPhoNum+dataLabel, conf);
		}
	}
			
	@SuppressWarnings("unchecked")
	private static void setRandSelSam_spicificRang(String randSelSampMark, String selectSapmlePath_mark, int randPhoNum, String new_selectSapmlePath, Configuration conf) throws NumberFormatException, FileNotFoundException, IOException, ClassNotFoundException{
		String ini_selectSapmlePath=conf.get(selectSapmlePath_mark);//e.g., "selectPhotosPath"
		if (ini_selectSapmlePath.endsWith("hashMap")) {
			new_selectSapmlePath+=".hashMap";
			HashMap<Integer, Integer> iniSel_hashMap=(HashMap<Integer, Integer>) General_Hadoop.readObject_HDFS(FileSystem.get(conf), ini_selectSapmlePath);
			if (iniSel_hashMap.size()>=randPhoNum) {//need random selection
				if (!FileSystem.get(conf).exists(new Path(new_selectSapmlePath))) {//new_selectSapmlePath not exist
					HashMap<Integer, Integer> selectPhotos= General.randSel(new Random(), iniSel_hashMap, randPhoNum);
					General_Hadoop.writeObject_HDFS(FileSystem.get(conf), new_selectSapmlePath, selectPhotos);
					System.out.println("selSam is not existed, creat a new one and saved to: "+new_selectSapmlePath);
				}else {
					System.out.println("selSam is existed: "+new_selectSapmlePath);
				}
				conf.set(randSelSampMark, new_selectSapmlePath);
			}
		}else {
			new_selectSapmlePath+=".hashSet";
			HashSet<Integer> iniSel_hashSet=(HashSet<Integer>) General_Hadoop.readObject_HDFS(FileSystem.get(conf), ini_selectSapmlePath);
			if (iniSel_hashSet.size()>=randPhoNum) {//need random selection
				if (!FileSystem.get(conf).exists(new Path(new_selectSapmlePath))) {//new_selectSapmlePath not exist
					HashSet<Integer> selectPhotos= General.randSel(new Random(), iniSel_hashSet, randPhoNum);
					General_Hadoop.writeObject_HDFS(FileSystem.get(conf), new_selectSapmlePath, selectPhotos);
					System.out.println("selSam is not existed, creat a new one and saved to: "+new_selectSapmlePath);
				}else {
					System.out.println("selSam is existed: "+new_selectSapmlePath);
				}
				conf.set(randSelSampMark, new_selectSapmlePath);
			}
		}
	}
	
	private static void setRandSelSam_coarseRang(String randSelSampMark, String startInd_totNum, int randomPhoNum, String new_selectSapmlePath, Configuration conf) throws NumberFormatException, FileNotFoundException, IOException{
		General.Assert(startInd_totNum!=null, "err! startInd_totNum should not be null");
		new_selectSapmlePath+=".hashMap";
		if (!FileSystem.get(conf).exists(new Path(new_selectSapmlePath))) {//hashMap not exist
			String[] info=startInd_totNum.split("_");//
			int startInd=Integer.valueOf(info[0]);//1
			int totNum=Integer.valueOf(info[1]);//3185258
			HashMap<Integer, Integer> selectPhotos= General.randSelect_returnHashMap(new Random(), totNum, randomPhoNum, startInd);
			General_Hadoop.writeObject_HDFS(FileSystem.get(conf), new_selectSapmlePath, selectPhotos);
			System.out.println("selSam is not existed, creat a new one and saved to: "+new_selectSapmlePath);
		}else {
			System.out.println("selSam is existed: "+new_selectSapmlePath);
		}
		conf.set(randSelSampMark, new_selectSapmlePath);
	}
	
	public void addDistriCache_SelectSamples(ArrayList<String> cacheFilePaths){
		if (isSelection) {
			cacheFilePaths.add(selectSapmlePath+"#"+selSamples); //SelSamples path with symLink
		}
	}
	
	@SuppressWarnings("unchecked")
	public static HashMap<Integer, Integer> getSelSamples_HashMap() throws InterruptedException{
		return (HashMap<Integer, Integer>)  General.readObject("SelSamples.file");
	}
	
	@SuppressWarnings("unchecked")
	public static HashSet<Integer> getSelSamples_HashSet() throws InterruptedException{
		return (HashSet<Integer>)  General.readObject("SelSamples.file");
	}
	
	@SuppressWarnings("rawtypes")
	public Class getMapper(){
		if (isSelection) {
			if (isHashMap) {
				if (isKeyIsSelSampleKey) {
					return Mapper_selectSamples_hashMap_SelSampleKey.class;
				} else {
					return Mapper_selectSamples_hashMap.class;
				}
			}else{
				return Mapper_selectSamples_hashSet.class;
			}
		}else{
			return null;
		}
	}
	
	@SuppressWarnings("unchecked")
	public int getMaxS(String maxS_inStr, FileSystem hdfs, boolean disp) throws ClassNotFoundException, IOException, InterruptedException{
		if (isSelection) {
			int maxS=0; 
			if (maxS_inStr!=null) {//alreay set in conf
				maxS=Integer.valueOf(maxS_inStr);
				General.dispInfo_ifNeed(disp, "", "maxS_inStr alreay setted in conf, it is "+maxS);
			}else{//no info in conf, need to read hashMap or hashSet to find maxS, when hdfs==null, it read from local machine
				if (isHashMap) {//L_to_S
					HashMap<Integer, Integer> L_to_S= (hdfs==null)?(HashMap<Integer, Integer>) General.readObject(selectSapmlePath):(HashMap<Integer, Integer>) General_Hadoop.readObject_HDFS(hdfs, selectSapmlePath);
					for (java.util.Map.Entry<Integer, Integer> one:L_to_S.entrySet()){
						maxS=Math.max(one.getValue(), maxS);
					}
					General.dispInfo_ifNeed(disp, "", "maxS not setted in conf, read (HashMap, L_to_S), total selected photos to save: "+L_to_S.size()+", maxS: "+maxS);
				}else {//L_to_L, so S==L
					HashSet<Integer> selPho_inL=(hdfs==null)?(HashSet<Integer>) General.readObject(selectSapmlePath):(HashSet<Integer>) General_Hadoop.readObject_HDFS(hdfs, selectSapmlePath);
					for (int L : selPho_inL) {
						maxS=Math.max(L, maxS);
					}
					General.dispInfo_ifNeed(disp, "", "maxS not setted in conf, read (HashSet, ID_in_L), total selected photos to save: "+selPho_inL.size()+", maxS: "+maxS);
				}
			}
			return maxS;
		}else{
			throw new InterruptedException("isSelection is false, so there is no getEqualAssignRedNum");
		}
	}
	
	@SuppressWarnings("unchecked")
	public int getSelSampleNum(FileSystem hdfs, boolean disp) throws ClassNotFoundException, IOException{
		if (isHashMap) {//L_to_S
			HashMap<Integer, Integer> L_to_S= (HashMap<Integer, Integer>) General_Hadoop.readObject_HDFS(hdfs, selectSapmlePath);
			General.dispInfo_ifNeed(disp, "", "total selected photos (HashMap, L_to_S) to save: "+L_to_S.size());
			return  L_to_S.size();
		}else {//L_to_L, so S==L
			HashSet<Integer> selPho_inL=(HashSet<Integer>) General_Hadoop.readObject_HDFS(hdfs, selectSapmlePath);
			General.dispInfo_ifNeed(disp, "", "total selected photos (HashSet, ID_in_L) to save: "+selPho_inL.size());
			return  selPho_inL.size();
		}
	}
	
	public static class Mapper_selectSamples_hashMap <K extends Writable> extends Mapper<IntWritable,K,IntWritable,K>{
		
		private HashMap<Integer,Integer> selectedSamples;
		private int procSamples;
		private int procSelSamples;
		private int dispInter;
		private long startTime, endTime;
		
		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			selectedSamples= (HashMap<Integer, Integer>)  General.readObject("SelSamples.file");
			System.out.println("total selected Samples:"+selectedSamples.size());
			//set procSamples
			procSamples=0;
			procSelSamples=0;
			//set dispInter
			dispInter=5000;
			startTime=System.currentTimeMillis(); //startTime
			
			System.out.println("mapper setup finsihed!");
			
	 	}
		
		@Override
		protected void map(IntWritable key, K value, Context context) throws IOException, InterruptedException {
			//key: SampleName in L-index
			//value: file content
			procSamples++;
			int SampleName=key.get();// SampleName
			Integer SampleName_inS= selectedSamples.get(SampleName);
			if (SampleName_inS!=null) {
				procSelSamples++;
				context.write(new IntWritable(SampleName_inS), value);
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
	
	public static class Mapper_selectSamples_hashMap_SelSampleKey <K extends SelSampleKey, V extends Writable> extends Mapper<K,V,K,V>{
		
		private HashMap<Integer,Integer> selectedSamples;
		private int procSamples;
		private int procSelSamples;
		private int dispInter;
		private long startTime, endTime;
		
		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			selectedSamples= (HashMap<Integer, Integer>)  General.readObject("SelSamples.file");
			System.out.println("total selected Samples:"+selectedSamples.size());
			//set procSamples
			procSamples=0;
			procSelSamples=0;
			//set dispInter
			dispInter=5000;
			startTime=System.currentTimeMillis(); //startTime
			
			System.out.println("mapper setup finsihed!");
			
	 	}
		
		@Override
		protected void map(K key, V value, Context context) throws IOException, InterruptedException {
			//key: SampleName in L-index
			//value: file content
			procSamples++;
			int SampleName=key.getSelSampleKey();// SampleName
			Integer SampleName_inS= selectedSamples.get(SampleName);
			if (SampleName_inS!=null) {
				procSelSamples++;
				key.setSelSampleKey(SampleName_inS);
				context.write(key, value);
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

	public static class Mapper_selectSamples_hashSet <K extends Writable> extends Mapper<IntWritable,K,IntWritable,K>{
		
		private HashSet<Integer> selectedSamples;
		private int procSamples;
		private int procSelSamples;
		private int dispInter;
		private long startTime;
		
		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			selectedSamples= (HashSet<Integer>)  General.readObject("SelSamples.file");
			System.out.println("total selected Samples:"+selectedSamples.size());
			//set procSamples
			procSamples=0;
			procSelSamples=0;
			//set dispInter
			dispInter=5000;
			startTime=System.currentTimeMillis(); //startTime
			
			System.out.println("mapper setup finsihed!");
			
	 	}
		
		@Override
		protected void map(IntWritable key, K value, Context context) throws IOException, InterruptedException {
			//key: SampleName in L-index
			//value: file content
			procSamples++;
			int SampleName=key.get();// SampleName
			if(selectedSamples.contains(SampleName)){
				procSelSamples++;
				context.write(new IntWritable(SampleName), value);
			}
			//disp
			if((procSamples)%dispInter==0){ 							
				System.out.println( "select Samples, "+procSamples+" Samples finished!! selected: "+procSelSamples+" ......"+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one mapper finished! total Samples in this Mapper: "+procSamples+", selected: "+procSelSamples+" ....."+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
			
	 	}
	}

}

