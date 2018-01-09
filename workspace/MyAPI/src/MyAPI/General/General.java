package MyAPI.General;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.RenderingHints;
import java.awt.Transparency;
import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.lang.reflect.Array;
import java.math.BigInteger;

import javax.imageio.ImageIO;
import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.border.EmptyBorder;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Decompressor;

import org.apache.commons.math3.complex.Complex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;













import MyAPI.General.myComparator.ValueComparator_Dou_ASC;
import MyAPI.Obj.Disp;
import MyAPI.Obj.Statistics;
import MyAPI.SystemCommand.MySystemCommandExecutor;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
import MyCustomedHaoop.ValueClass.Int_Float;



public class General {
	
	public static String runSysCommand(List<String> commands, String usrInput, boolean isPrintImediatlyOnScreen) throws IOException, InterruptedException{
//		List<String> commands = Arrays.asList(command); // build the system command we want to run, Arrays.asList("cmd", "/c", "dir")	    
	    // execute the command
		MySystemCommandExecutor commandExecutor = new MySystemCommandExecutor(commands,true,isPrintImediatlyOnScreen);
		int result = commandExecutor.executeCommand(true, "\t", "s",usrInput);

	    // get the stdout and stderr from the command that was run
	    String stdout = commandExecutor.getStandardOutputFromCommand();
	    String stderr = commandExecutor.getStandardErrorFromCommand();
	    return "RepCode:"+result+","+stdout+stderr;
	}
	
	public static void Assert(boolean state, String message) {
		if(state==false)
			throw new IllegalArgumentException("Assertion Failed: "+message);
	}
	
	public static void Assert_onlyWarning(boolean state, String message) {
		if(state==false)
			System.out.println("-Warn! Assertion Failed: "+message);
	}
	
	public static String[] selectArrStr(String[] Arr, int beg, int end, int[] ArrIndex) {
		// select part of Arr into Arr_selected, from beg to end in ArrIndex, ArrIndex 应该跟Arr的长度一致，如随机打乱顺序后的INDEX
		String[] Arr_selected=new String[end-beg+1];
		for(int i=0;i<Arr_selected.length;i++){
			Arr_selected[i]=Arr[ArrIndex[beg+i]];
		}
		return Arr_selected;
	}
	
	public static void checkDir(Disp disp, String dirPath){//check current workDir: General.checkDir(new Disp(true, "\t", null),".");
		File SeqFile=new File(dirPath);
		String absolutePath=SeqFile.getAbsolutePath();
		String filesInfo=General.listFilesInfo(new File(absolutePath), -1);
		disp.disp("absolutePath for "+dirPath+" is: "+absolutePath+"\n"+filesInfo);
	}
	
	public static String[] selectArrStr(String[] Arr, int[] selectedInd, int top, int bottom) throws InterruptedException {
		if (selectedInd==null && top>0 && bottom==0) {//select top
			top=Math.min(Arr.length, top);
			String[] Arr_selected=new String[top];
			for (int i = 0; i < top; i++) {
				Arr_selected[i]=Arr[i];
			}
			return Arr_selected;
		}else if (selectedInd==null && top==0 && bottom>0){// select bottom, 
			String[] Arr_selected=new String[bottom];
			bottom=Math.min(Arr.length, bottom); 
			int ind=0;
			for (int i = Arr.length-bottom; i < Arr.length; i++) {
				Arr_selected[ind]=Arr[i];
				ind++;
			}
			return Arr_selected;
		}else if (selectedInd!=null && top==0 && bottom==0){// select part of Arr into Arr_selected, from beg to end in ArrIndex, 
			String[] Arr_selected=new String[selectedInd.length];
			for(int i=0;i<Arr_selected.length;i++){
				Arr_selected[i]=Arr[selectedInd[i]];
			}
			return Arr_selected;
		}else {
			throw new InterruptedException("err in selectArrStr: either selectedInd=null, top>0, bottom==0; or selectedInd!=null, top=0, bottom=0; or selectedInd=null, top==0, bottom>0");
		}
	}
	
	public static float[] selectArrFloat(float[] Arr, int[] selectedInd, int top) throws InterruptedException {
		if (selectedInd==null && top>0) {//select top
			top=Math.min(Arr.length, top);
			float[] Arr_selected=new float[top];
			for (int i = 0; i < top; i++) {
				Arr_selected[i]=Arr[i];
			}
			return Arr_selected;
		}else if (selectedInd!=null && top==0){// select part of Arr into Arr_selected, from beg to end in ArrIndex, 
			float[] Arr_selected=new float[selectedInd.length];
			for(int i=0;i<Arr_selected.length;i++){
				Arr_selected[i]=Arr[selectedInd[i]];
			}
			return Arr_selected;
		}else {
			throw new InterruptedException("err in selectArrFloat, use --selectedInd==null, top>0 or --selectedInd!=null, top==0");
		}
	}
	
	public static float[][] selectArrFloat(float[][] Arr, int[][] selectedInd) throws InterruptedException {//selectedInd.length must == Arr dimension number
		float[][] res=new float[selectedInd[0].length][selectedInd[1].length]; //each row in selectedInd save selected index for one dimension
		for (int i = 0; i < selectedInd[0].length; i++) {
			for (int j = 0; j < selectedInd[1].length; j++) {
				res[i][j]=Arr[selectedInd[0][i]][selectedInd[1][j]];
			}
		}
		return res;
	}
	
	public static int[] selectArrInt(int[] Arr, int[] selectedInd, int top) throws InterruptedException {
		if (selectedInd==null && top>0) {//select top
			top=Math.min(Arr.length, top);
			int[] Arr_selected=new int[top];
			for (int i = 0; i < top; i++) {
				Arr_selected[i]=Arr[i];
			}
			return Arr_selected;
		}else if (selectedInd!=null && top==0){// select part of Arr into Arr_selected, from beg to end in ArrIndex, 
			int[] Arr_selected=new int[selectedInd.length];
			for(int i=0;i<Arr_selected.length;i++){
				Arr_selected[i]=Arr[selectedInd[i]];
			}
			return Arr_selected;
		}else {
			throw new InterruptedException("err in selectArrInt, use --selectedInd==null, top>0 or --selectedInd!=null, top==0");
		}
	}
	
	public static <V extends Object> ArrayList<V> selectArr(V[] Arr, int[] selectedInd, int top) throws InterruptedException {
		if (selectedInd==null && top>0) {//select top
			top=Math.min(Arr.length, top);
			ArrayList<V> Arr_selected=new ArrayList<V>(top);
			for (int i = 0; i < top; i++) {
				Arr_selected.add(Arr[i]);
			}
			return Arr_selected;
		}else if (selectedInd!=null && top==0){// select part of Arr into Arr_selected, from beg to end in ArrIndex, 
			ArrayList<V> Arr_selected=new ArrayList<V>(selectedInd.length);
			for(int i=0;i<selectedInd.length;i++){
				Arr_selected.add(Arr[selectedInd[i]]);
			}
			return Arr_selected;
		}else {
			throw new InterruptedException("err in selectArr, use --selectedInd==null, top>0 or --selectedInd!=null, top==0");
		}
	}
	
	public static <V extends Object> ArrayList<V> selectList(ArrayList<V> Arr, List<Integer> selectedInd) {
		// select part of Arr into Arr_selected, from beg to end in ArrIndex, 
		ArrayList<V> Arr_selected=new ArrayList<V>(selectedInd.size());
		for(int ind:selectedInd){
			Arr_selected.add(Arr.get(ind));
		}
		return Arr_selected;
	}
	
	public static float[] scaleValue(float[] arr, float lower,  float upper, float lowerPercent, float upperPercent) throws InterruptedException{//linear scale elements in arr to the range lower ~ upper
		float[] res=new float[arr.length];
		float upper_lower=upper-lower;
		//truncate the arr
		lowerPercent/=100; upperPercent/=100;//the input parameter is in percent
		int lowerNum=(int) (res.length*lowerPercent); int upperNum=(int) (res.length*upperPercent);
		//run
		if (lowerNum<2 && upperNum<2) {//do not do top-merge
			int[] min_max_ind=getMinMax_ind(arr);
			float min=arr[min_max_ind[0]]; float max=arr[min_max_ind[1]];
			float max_min=max-min; 
			for (int i = 0; i < arr.length; i++) {
				res[i]=scaleValue(arr[i], min, max_min, lower, upper_lower);
			}
		}else {//merge top upperNum into upper, and last lowerNum into lower
			Statistics<Integer> state=new Statistics<>(Math.max(lowerNum, upperNum));
			for (int i = 0; i < arr.length; i++) {
				state.addSample(arr[i], i);
			}
			//use the last sample in the top list as the max or min
			float max=state.getMaxValues().last().getMaster();
			float min=state.getMinValues().last().getMaster();
			float max_min=max-min; 
			for (int i = 0; i < arr.length; i++) {
				float newSam=arr[i];
				if (arr[i]>max) {
					newSam=max;
				}
				if (arr[i]<min) {
					newSam=min;
				}
				res[i]=scaleValue(newSam, min, max_min, lower, upper_lower);
			}
		}
		return res;
	}
	
	public static float scaleValue(float oneValue, float min, float max_min, double lower,  double upper_lower) {//max_min: max-min
		//scale data
//		return (float) (lower + (upper-lower) * (oneValue-scalingInfo[0]) / (scalingInfo[1]-scalingInfo[0]));
		return (float) (lower + (upper_lower) * (oneValue-min) / (max_min));
	}
	
	public static double scaleValue(double oneValue, double[] scalingInfo, double lower,  double upper) {// scaling data according to scalingInfo, scalingInfo[min_max], 
		//scale data
		if (scalingInfo[0]==scalingInfo[1]) {
			return upper;
		}else {
			return (lower + (upper-lower) * (oneValue-scalingInfo[0]) / (scalingInfo[1]-scalingInfo[0]));
		}
	}
	
	public static double scaleValue(double oneValue, float[] scalingInfo, double lower,  double upper) {// scaling data according to scalingInfo, scalingInfo[min_max], 
		//scale data
		return (lower + (upper-lower) * (oneValue-scalingInfo[0]) / (scalingInfo[1]-scalingInfo[0]));
	}
	
	public static float[] scaleArr(float[] rang, float scaleRate){
		//shrik or enlarge one rang
		Assert(rang[0]<rang[1], "err! rang0 shoud < rang[1]");
		float mid=(rang[0]+rang[1])/2;
		float halfBin=(rang[1]-mid)*scaleRate;
		rang[0]=mid-halfBin;rang[1]=mid+halfBin;
		return rang;
	}
	
	public static int getCommonNum(ArrayList<Integer> longlist, ArrayList<Integer> shortlist){
		int commonNum=0;
		HashSet<Integer> longList=new HashSet<Integer>(longlist);
		for(int one:shortlist){
			if(longList.contains(one)){
				commonNum++;
			}
		}
		return commonNum;
	}
	
	public static String getUserInput(){
		Scanner scan = new Scanner(System.in);
		String res=scan.nextLine();
		scan.close();
		return res;
	}
	
	public static String getUserInputHidden(){
		return new String(System.console().readPassword());
	}
	
	public static int[][] getAllCombinations(int[][] valEachDim) {
		/*
		 * valEachDim[i] is all the possible values in the i-th dim
		 */
		int dimNum=valEachDim.length;
		int totCombNum=1; int[] numEachDim=new int[dimNum];
		for (int i=0;i<dimNum;i++) {
			numEachDim[i]=valEachDim[i].length;
			totCombNum*=numEachDim[i];
		}
		//ini combinations
		int[][] combinations=new int[totCombNum][dimNum];
		//process, every time update the lowest dim index, similar to count number, from low dim to high dim, if lower dim is full, then upgrade
		int[] ind_eachDim=new int[dimNum];
		int lastDim=dimNum-1; int combinInd=0; 
		while (ind_eachDim[lastDim]<numEachDim[lastDim]) {//the boarder is ind in lastDim==lastDim's possible values num
			//add this combination
			for (int i = 0; i < dimNum; i++) {
				combinations[combinInd][i]=valEachDim[i][ind_eachDim[i]];
			}
			combinInd++;
			//update 
			ind_eachDim[0]++;
			for (int i = 0; i < lastDim; i++) {
				if (ind_eachDim[i]==numEachDim[i]) {//judge whether this i-position needs upgrade
					ind_eachDim[i]=0;
					ind_eachDim[i+1]++;
				}else {
					break;
				}
			}
		}
		General.Assert(combinInd==totCombNum, "err! combinInd should==totCombNum, combinInd:"+combinInd+", totCombNum:"+totCombNum);
		return combinations;
	}
	
	public static int[] randIndex(int n) {// random produce n no duplicate value with rang 0~n-1, with random order  
	   ArrayList<Integer> list = new ArrayList<Integer>();  
	   Random rand = new Random();  
	   boolean[] bool = new boolean[n];  int num =0;  
	   for (int i = 0; i<n; i++){  
	       do{  
	           //如果产生的数相同继续循环  
	           num = rand.nextInt(n);   // 0 ~ n-1   
	       }while(bool[num]);  
	       bool[num] =true;  
	       list.add(num);  
	   }  
	   Integer[] Index = new Integer[n];
	   list.toArray(Index);
	   int[] Index_int=new int[Index.length];
	   for(int i=0;i<Index.length;i++){
		   Index_int[i]=Index[i].intValue();
	   }
//			   System.out.println (list);
//			   System.out.println (Index_int[0]);
	   return Index_int;
	}
	
	public static double[] randDouArr(int n, long seed) {// random produce n value with rang 0~1,
	   Random random=new Random(seed);
	   double[] douArr=new double[n];
	   for (int i = 0; i < douArr.length; i++) {
		   douArr[i]=random.nextDouble();
	   }
	   return douArr;
	}
	
	public static double[] randDouArr(int n) {// random produce n value with rang 0~1,
	   Random random=new Random();
	   double[] douArr=new double[n];
	   for (int i = 0; i < douArr.length; i++) {
		   douArr[i]=random.nextDouble();
	   }
	   return douArr;
	}
	
	public static double[] randDouArr_withScal(int n, double scale) {// random produce n value with rang 0~1,
	   Random random=new Random();
	   double[] douArr=new double[n];
	   for (int i = 0; i < douArr.length; i++) {
		   douArr[i]=random.nextDouble()*scale;
	   }
	   return douArr;
	}
	
	public static int[] randIndex(Random rand, int n) {// random produce n no duplicate value with rang 0~n-1, with random order  
	   ArrayList<Integer> list = new ArrayList<Integer>();  
	   boolean[] bool = new boolean[n];  int num =0;  
	   for (int i = 0; i<n; i++){  
	       do{  
	           //如果产生的数相同继续循环  
	           num = rand.nextInt(n);   // 0 ~ n-1   
	       }while(bool[num]);  
	       bool[num] =true;  
	       list.add(num);  
	   }  
	   Integer[] Index = new Integer[n];
	   list.toArray(Index);
	   int[] Index_int=new int[Index.length];
	   for(int i=0;i<Index.length;i++){
		   Index_int[i]=Index[i].intValue();
	   }
//				   System.out.println (list);
//				   System.out.println (Index_int[0]);
	   return Index_int;
	}
	
	public static LinkedList<Integer> randSelMSamples(Random rand, int m, int tot){// rand get M samples from tot samaples, M<=N
		LinkedList<Integer> list = new LinkedList<Integer>();  
		boolean[] bool = new boolean[tot];  int num =0;  
		for (int i = 0; i<m; i++){  
			do{  
				num = rand.nextInt(tot);   // 0 ~ n-1   
			}while(bool[num]);  
			bool[num] =true;  
			list.add(num);  
		}                       
		return list;
	}
	
	public static <K , V> HashMap<K, V> randSel(Random rand, HashMap<K, V> totH, int randomNum) {// random Split data into ration.length parts, according to the ratio, like 3:2:3, used for random divide data 
		if (randomNum>=totH.size()) {
			return totH;
		}else {
			boolean isSet=randomNum<(totH.size()/2);
			HashSet<Integer> randSam=new HashSet<>(randSelMSamples(rand, isSet?randomNum:totH.size()-randomNum, totH.size()));
			HashMap<K,V> res=new HashMap<>(); 
			int ind=0;
			if (isSet) {//randSam is the selected
				for (Entry<K, V> one : totH.entrySet()) {
					if (randSam.contains(ind)) {
						res.put(one.getKey(), one.getValue());
					}
					ind++;
				}
			}else {//randSam is the not selected
				for (Entry<K, V> one : totH.entrySet()) {
					if (!randSam.contains(ind)) {
						res.put(one.getKey(), one.getValue());
					}
					ind++;
				}
			}
			Assert(res.size()==randomNum, "err! res.size() should ==randomNum, totH.size(): "+totH.size()+", res.size(): "+res.size()+", randomNum: "+randomNum);
			return res;
		}
	}
	
	public static <K> HashSet<K> randSel(Random rand, HashSet<K> totH, int randomNum) {// random Split data into ration.length parts, according to the ratio, like 3:2:3, used for random divide data 
		if (randomNum>=totH.size()) {
			return totH;
		}else {
			boolean isSet=randomNum<(totH.size()/2);
			HashSet<Integer> randSam=new HashSet<>(randSelMSamples(rand, isSet?randomNum:totH.size()-randomNum, totH.size()));
			HashSet<K> res=new HashSet<>(); 
			int ind=0;
			if (isSet) {//randSam is the selected
				for (K one : totH) {
					if (randSam.contains(ind)) {
						res.add(one);
					}
					ind++;
				}
			}else {//randSam is the not selected
				for (K one : totH) {
					if (!randSam.contains(ind)) {
						res.add(one);
					}
					ind++;
				}
			}
			Assert(res.size()==randomNum, "err! res.size() should ==randomNum, totH.size(): "+totH.size()+", res.size(): "+res.size()+", randomNum: "+randomNum);
			return res;
		}
	}
	
	public static int[] randIndex(Random rand, int n, int startInd) {// random produce n no duplicate value with rang startInd~startInd+n-1, with random order  
		int[] randInds_from0=randIndex(rand,n);
		return elementAdd(randInds_from0, startInd);
	}
	
	public static <K , V> ArrayList<HashMap<K, V>> randSplitHashMap  (Random rand, HashMap<K, V> totH, int subSetNum, int subSetSize) {// random Split data into ration.length parts, according to the ratio, like 3:2:3, used for random divide data 
		if (subSetNum<=0) {
			General.Assert(subSetSize>0, "err in randSplitHashMap, either provide useful subSetNum or subSetSize, here subSetNum:"+subSetNum+", subSetSize:"+subSetSize);
			subSetNum=totH.size()/subSetSize+1;
		}
		ArrayList<HashMap<K, V>> sets=new ArrayList<HashMap<K,V>>(subSetNum);
		//Initialize
		for (int i = 0; i < subSetNum; i++) {
			sets.add(new HashMap<K, V>());
		}
		//rand
		for (Entry<K, V> oneKV : totH.entrySet()) {
			sets.get(rand.nextInt(subSetNum)).put(oneKV.getKey(), oneKV.getValue());
		}
		return sets;
	}
	
	public static int randSplit(Random rand, int[] ratio) {// random Split data into ration.length parts, according to the ratio, like 3:2:3, used for random divide data 
		int[] bin=new int[ratio.length];
		bin[0]=ratio[0]-1;
		for(int i=1;i<bin.length;i++){//3:2:3 ---> 2,4,7
			bin[i]=bin[i-1]+ratio[i];
		}
		int max=bin[bin.length-1];
		int randInd=rand.nextInt(max+1);
		int part=getBinInd_linear(bin, randInd);
		General.Assert(part<ratio.length, "part <="+(ratio.length-1)+", but:"+part);
		return part;
	}
	
	public static LinkedList<Integer> randSelect(Random rand, int selNum, ArrayList<Integer> samples) {// random select K samples from N datas, return data-index, from 0.
		LinkedList<Integer> selInds=General.randSelMSamples(new Random(2), selNum, samples.size());
		LinkedList<Integer> res=new LinkedList<Integer>();
		for (Integer randID : selInds) {
			res.add(samples.get(randID));
		}
		return res;
	}
	
	public static HashSet<Integer> randSelect(Random rand, int N, int K, int startInd) {// random select K samples from N datas, return data-index, from 0.
		int[] randOrder=randIndex(rand, N, startInd);
		HashSet<Integer> selected=new HashSet<Integer>(K);
		for (int i = 0; i < K; i++) {//choose top K
			selected.add(randOrder[i]);
		}
		return selected;
	}
	
	public static HashMap<Integer,Integer> randSelect_returnHashMap(Random rand, int N, int K, int startInd) {// random select K samples from N datas, return data-index, from 0.
		int[] randOrder=randIndex(rand, N, startInd);
		HashMap<Integer,Integer> selected=new HashMap<Integer,Integer>(K);
		for (int i = 0; i < K; i++) {//choose top K
			selected.put(randOrder[i],randOrder[i]);
		}
		return selected;
	}
	
	public static int[] makeAccum(int[] accumLevel, int[] hist) {//make accumulated data from hist[accumLevel[0]]
		int[] accuHist=new int[accumLevel.length];
		int start=accumLevel[0]; //start from accumLevel[0]!!
		for(int i=0;i<accuHist.length;i++){
			for(int j=start;j<=accumLevel[i];j++)
				accuHist[i]+=hist[j];
		}
		return accuHist;
	}
	
	public static int[] makeAccum(int[] hist) {//make accumulated data from 0,1,2,...
		int[] accuHist=new int[hist.length];
		for(int i=0;i<accuHist.length;i++){
			for(int j=0;j<=i;j++)
				accuHist[i]+=hist[j];
		}
		return accuHist;
	}
	
	public static String IntArrToString(int[] intArr, String delimiter) {
		StringBuffer str=new StringBuffer();
		for(int i:intArr){
			str.append(i+delimiter);
		}
		return str.toString();
	}
	
	public static String BooleanArrToString(boolean[] Arr, String delimiter) {
		StringBuffer str=new StringBuffer();
		for(boolean i:Arr){
			str.append(i+delimiter);
		}
		return str.toString();
	}
	
	public static <T> String ArrToString(T[] Arr, String delimiter) {
		StringBuffer str=new StringBuffer();
		for(T i:Arr){
			str.append(i+delimiter);
		}
		return str.toString();
	}
		
	public static String IntArrToString(Integer[] intArr, String delimiter) {
		StringBuffer str=new StringBuffer();
		for(int i:intArr){
			str.append(i+delimiter);
		}
		return str.toString();
	}
	
	public static byte[] readByteArr(byte [] byteBarray, DataInputStream dis) throws IOException{
		int bytesRead = dis.read(byteBarray);  // Bytes are read into buffer
	    if (bytesRead != byteBarray.length) {
	    	throw new IOException("Unexpected End of Stream, should read "+byteBarray.length+" bytes, but only: "+bytesRead);
	    }
	    return byteBarray;
	}
	
	public static byte[] int_to_byteArr(int myInteger){
	    return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(myInteger).array();
	}
	
	public static int byteArr_to_int(byte [] byteBarray, int start, int length){
	    return ByteBuffer.wrap(byteBarray, start, length).order(ByteOrder.LITTLE_ENDIAN).getInt();
	}
	
	public static int byteArr_to_int(byte [] byteBarray){
	    return ByteBuffer.wrap(byteBarray).order(ByteOrder.LITTLE_ENDIAN).getInt();
	}
	
	public static byte[] float_to_byteArr(float myFloat){
	    return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(myFloat).array();
	}
	
	public static float byteArr_to_float(byte [] byteBarray, int start, int length){
	    return ByteBuffer.wrap(byteBarray, start, length).order(ByteOrder.LITTLE_ENDIAN).getFloat();
	}
	
	public static float byteArr_to_float(byte [] byteBarray){
	    return ByteBuffer.wrap(byteBarray).order(ByteOrder.LITTLE_ENDIAN).getFloat();
	}
	
	public static double byteArr_to_double(byte [] byteBarray){
	    return ByteBuffer.wrap(byteBarray).order(ByteOrder.LITTLE_ENDIAN).getDouble();
	}
	
	public static ArrayList<Integer> findCommonElement_multipleSorted_ASC_loopShotArr(ArrayList<ArrayList<Integer>> input){//each row is one arr
		ArrayList<Integer> commonLocs=input.get(0);
		for (int i = 1; i < input.size(); i++) {
			commonLocs=commonLocs.size()<input.get(i).size()? General.findCommonElement_twoSorted_ASC_loopShotArr(commonLocs, input.get(i))
					:General.findCommonElement_twoSorted_ASC_loopShotArr(input.get(i), commonLocs);
		}
		return commonLocs;
	}
	
	public static ArrayList<Integer> findCommonElement_twoSorted_ASC_loopShotArr(Integer[] aa_short, Integer[] bb_long){//aa_short and bb_long should be sorted in ASC!
		//compare to findCommonElement_twoSorted_ASC_onebyone, this one have similar efficiency but easier to understand
		if (aa_short[aa_short.length-1]>=bb_long[0] && aa_short[0]<=bb_long[bb_long.length-1]) {//aa and bb do have overlap rang!
			ArrayList<Integer> commons=new ArrayList<Integer>(aa_short.length);
			int beginInd=0;
			for (int oneTopDoc : aa_short) {
				if (oneTopDoc>bb_long[bb_long.length-1]) {
					break;
				}
				for (int i = beginInd; i < bb_long.length; i++) {
					if (oneTopDoc<bb_long[i]) {//< then stop, and update beginInd to current position of bblong
						beginInd=i;
						break;
					}else if (oneTopDoc==bb_long[i]) {// == then stop, and add this element, and update beginInd to the next position of bblong
						commons.add(oneTopDoc);
						beginInd=i+1;//next
						break;
					}
				}
			}
			return commons;
		}else {
			return null;
		}
	}
	
	public static ArrayList<Integer> findCommonElement_twoSorted_ASC_loopShotArr(ArrayList<Integer> aa, ArrayList<Integer> bb){//aa_short and bb_long should be sorted in ASC!
		//compare to findCommonElement_twoSorted_ASC_onebyone, this one have similar efficiency but easier to understand
		ArrayList<Integer> shortList=aa; ArrayList<Integer> longList=bb;
		if (aa.size()>bb.size()) {
			shortList=bb; longList=aa;
		}
		int longList_lastOne=longList.get(longList.size()-1);
		if (shortList.get(shortList.size()-1)>=longList.get(0) && shortList.get(0)<=longList_lastOne) {//aa and bb do have overlap rang!
			ArrayList<Integer> commons=new ArrayList<Integer>(shortList.size());
			int beginInd=0;
			for (int oneTopDoc : shortList) {
				if (oneTopDoc>longList_lastOne) {
					break;
				}
				for (int i = beginInd; i < longList.size(); i++) {
					if (oneTopDoc<longList.get(i)) {//< then stop, and update beginInd to current position of bblong
						beginInd=i;
						break;
					}else if (oneTopDoc==longList.get(i)) {// == then stop, and add this element, and update beginInd to the next position of bblong
						commons.add(oneTopDoc);
						beginInd=i+1;//next
						break;
					}
				}
			}
			return commons;
		}else {
			return null;
		}
	}
	
	public static ArrayList<Integer> findCommonElement_twoSorted_ASC_onebyone(int[] aa_short, int[] bb_long){//aa_short and bb_long should be sorted in ASC!
		if (aa_short[aa_short.length-1]>=bb_long[0] && aa_short[0]<=bb_long[bb_long.length-1]) {//aa and bb do have overlap rang!
			ArrayList<Integer> commons=new ArrayList<Integer>(aa_short.length);	
			int a_ind=0, b_ind=0;
			while (a_ind<aa_short.length && b_ind<bb_long.length) {
				if (aa_short[a_ind]>bb_long[b_ind]) {
					b_ind++;
				}else if (aa_short[a_ind]==bb_long[b_ind]) {
						commons.add(aa_short[a_ind]);
						a_ind++;
						b_ind++;
				}else {
					a_ind++;
				}
			}
			return commons;
		}else {
			return null;
		}
	}
	
	public static ArrayList<Integer> findCommonElement_twoSorted_ASC_booleanInd(int[] aa_short, int[] bb_long){//aa_short and bb_long should be sorted in ASC!
		if (aa_short[aa_short.length-1]>=bb_long[0] && aa_short[0]<=bb_long[bb_long.length-1]) {//aa and bb do have overlap rang!
			ArrayList<Integer> commons=new ArrayList<Integer>(aa_short.length);
			int maxEle=Math.max(aa_short[aa_short.length-1], bb_long[bb_long.length-1]);
			boolean[] exist=new boolean[maxEle+1];
			for (int one : bb_long) {
				exist[one]=true;
			}
			for (int one : aa_short) {
				if (exist[one]) {
					commons.add(one);
				}
			}
			return commons;
		}else {
			return null;
		}
	}
	
	public static ArrayList<int[]> findCommonElementInds_twoSorted_ASC_loopShotArr(int[] aa_short, int[] bb_long){//aa_short and bb_long should be sorted in ASC!
		if (aa_short[aa_short.length-1]>=bb_long[0] && aa_short[0]<=bb_long[bb_long.length-1]) {//aa and bb do have overlap rang!
			ArrayList<int[]> commons=new ArrayList<int[]>(aa_short.length);
			int beginInd=0;
			for (int a_i=0; a_i<aa_short.length;a_i++) {
				int oneTopDoc=aa_short[a_i];
				if (oneTopDoc>bb_long[bb_long.length-1]) {
					break;
				}
				for (int b_i = beginInd; b_i < bb_long.length; b_i++) {//< then stop, and update beginInd to current position of bblong
					if (oneTopDoc<bb_long[b_i]) {
						beginInd=b_i;
						break;
					}else if (oneTopDoc==bb_long[b_i]) {// == then stop, and add this element, and update beginInd to the next position of bblong
						commons.add(new int[]{oneTopDoc,a_i,b_i});
						beginInd=b_i+1;//ne
						break;
					}
				}
			}
			return commons;
		}else {
			return null;
		}
	}
	
	public static ArrayList<int[]> findCommonElementInds_twoSorted_ASC_booleanInd(int[] aa_short, int[] bb_long){//aa_short and bb_long should be sorted in ASC!
		if (aa_short[aa_short.length-1]>=bb_long[0] && aa_short[0]<=bb_long[bb_long.length-1]) {//aa and bb do have overlap rang!
			ArrayList<int[]> commons=new ArrayList<int[]>(aa_short.length);
			int maxEle=Math.max(aa_short[aa_short.length-1], bb_long[bb_long.length-1]);
			int[] indInLong=new int[maxEle+1];
			for (int i=0;i<bb_long.length;i++) {
				indInLong[bb_long[i]]=i+1;
			}
			for (int i=0;i<aa_short.length;i++) {
				if (indInLong[aa_short[i]]!=0) {
					commons.add(new int[]{aa_short[i],i,indInLong[aa_short[i]]-1});
				}
			}
			return commons;
		}else {
			return null;
		}
	}
	
	public static byte[] short_to_byteArr(short myShort){
	    return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putShort(myShort).array();
	}
	
	public static short byteArr_to_short(byte [] byteBarray, int start, int length){
	    return ByteBuffer.wrap(byteBarray, start, length).order(ByteOrder.LITTLE_ENDIAN).getShort();
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Integer[] getSortOrder_ArraySort(Comparator comp, int dataSize){
		//to use: Integer[] asc=General.getSortOrder_ArraySort(new Comparator_IntArr(qi_matchNum, "ASC"), qi_matchNum.size());
		Integer[] sortOrder = new Integer[dataSize];		       
		for(int i=0; i<sortOrder.length; i++){
            sortOrder[i] = i;
        }	
		Arrays.sort(sortOrder, comp);
		return sortOrder;
	}
	
	public static int getOneDimInd_forMutiDimArr(int[] ind_eachDim, int[] size_eachDim) {
		int oneDimInd=0;
		for (int i = 0; i < ind_eachDim.length-1; i++) {
			oneDimInd+=(ind_eachDim[i])*Multiplicative_IntArr(size_eachDim,i+1,size_eachDim.length);
		}
		oneDimInd+=ind_eachDim[ind_eachDim.length-1];
		return oneDimInd;
	}
	
	public static int[] getMutiDimArr_FromOneDimInd(int oneDimInd, int[] size_eachDim) {
		int[] ind_eachDim=new int[size_eachDim.length]; int currentV=oneDimInd;
		for (int i = 0; i < ind_eachDim.length-1; i++) {
			int[] Int_mod=mod_withInt(currentV, Multiplicative_IntArr(size_eachDim,i+1,size_eachDim.length));
			ind_eachDim[i]=Int_mod[0];
			currentV=Int_mod[1];
		}
		ind_eachDim[ind_eachDim.length-1]=currentV;
		return ind_eachDim;
	}

	public static String IntArrArrListToString(ArrayList<int[]> intArrArrList, String delimiterForArrList, String delimiterForIntArr) {
		StringBuffer str=new StringBuffer();
		for (int[] intArr : intArrArrList) {
			for(int i:intArr){
				str.append(i+delimiterForIntArr);
			}
			str.append(delimiterForArrList);
		}
		return str.toString();
	}
	
	public static int[] assign_by_ratio(int[] ratio, int maxSum) {
		int ratioSum=General.sum_IntArr(ratio);
		int[] bounds=General.makeAccum(ratio);
		Random rand=new Random();
		int[] partNums=new int [ratio.length];
		for (int i = 0; i < maxSum; i++) {
			int sig=rand.nextInt(ratioSum);
			for (int j = 0; j < bounds.length; j++) {
				if (sig<bounds[j]) {
					partNums[j]++;
					break;
				}
			}
		}
		return partNums;
	}
	
	public static float[] normliseArr(int[] hist, float sum) {
		if(sum<0){//if no sum, then use hist[] sum as sum
			for(int i:hist){
				sum+=i;
			}
		}
		float[] percents=new float[hist.length];
		if (sum!=0) {
			for(int i=0;i<hist.length;i++){
				percents[i]=(float)hist[i]/sum;
			}
		}
		return percents;
	}
	
	public static float[] normliseArr(float[] hist, float sum) {
		if( sum<0){//if no sum, then use hist[] sum as sum
			for(float i:hist){
				sum+=i;
			}
		}
		float[] percents=new float[hist.length];
		if (sum!=0) {
			for(int i=0;i<hist.length;i++){
				percents[i]=(float)hist[i]/sum;
			}
		}
		return percents;
	}
	
	public static <T extends Number> float[] normliseArr(T[] hist, T sum) {
		double SUM=sum.doubleValue();
		if( SUM<0){//if no sum, then use hist[] sum as sum
			for(T i:hist){
				SUM+=i.doubleValue();
			}
		}
		float[] percents=new float[hist.length];
		for(int i=0;i<hist.length;i++){
			percents[i]=(float) (hist[i].doubleValue()/SUM);
		}
		return percents;
	}
	
	public static void normliseArr_L1NormLise(double[] arr) {
		double sum=sum_DouArr(arr);
		for(int i=0;i<arr.length;i++){
			arr[i]/=sum;
		}
	}
	
	public static <T extends Number> float[] mkAccumPercent(T data[]) { //make accumulated percentage for data
		double sum=0;
		for(T i:data){
			sum=sum+i.doubleValue();
		}
		float[] percents=new float[data.length];
		percents[0]=(float) (data[0].doubleValue()/sum);
		for(int i=1;i<data.length;i++){
			percents[i]=(float) (percents[i-1]+data[i].doubleValue()/sum);
		}
		return percents;
	}
	
	public static String douArrToString(double[] douArr, String delimiter, String format) { // format, "0.0"
		StringBuffer str=new StringBuffer();
		for(double i:douArr){
			str.append(new DecimalFormat( format).format(i)+delimiter);
		}
		return str.toString();
	}
	
	public static String douToString(double dou, String format) { // format, "0.0"
		return new DecimalFormat( format).format(dou);
	}
	
	public static String floatArrToString(float[] floatArr, String delimiter, String format) { // format, "0.0"
		if (floatArr!=null) {
			StringBuffer str=new StringBuffer();
			for(float i:floatArr){
				str.append(new DecimalFormat( format).format(i)+delimiter);
			}
			return str.toString();
		}else{
			return "floatArr is null in floatArrToString";
		}
		
	}
	
	public static String floatArrToString_nolastDelimiter(float[] floatArr, String delimiter, String format) { // format, "0.0"
		StringBuffer str=new StringBuffer();
		for (int j = 0; j < floatArr.length; j++) {
			if (j==floatArr.length-1) {//last element
				str.append(new DecimalFormat( format).format(floatArr[j]));
			}else {
				str.append(new DecimalFormat( format).format(floatArr[j])+delimiter);
			}
		}			
		return str.toString();
	}
	
	public static float[] cutOff_floatArr(float[] floatArr, float thr, String mode, float v) throws Exception { 
		if (mode.equalsIgnoreCase("floor")) {//cut if value < thr
			for (int i = 0; i < floatArr.length; i++) {
				if (floatArr[i]<thr) {
					floatArr[i]=v;
				}
			}
		}else if (mode.equalsIgnoreCase("ceil")) { //cut if value > thr
			for (int i = 0; i < floatArr.length; i++) {
				if (floatArr[i]>thr) {
					floatArr[i]=v;
				}
			}
		}else {
			throw new Exception("mode name mismatch, mode name should be floor or ceil!");
		}
		return floatArr;
	}
	
	public static byte[] intArr_to_byteArr(ArrayList<Integer> target) throws IOException{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		out.writeInt(target.size());
		for (Integer integer : target) {
			out.writeInt(integer);
		}
		byte[] bytes = baos.toByteArray();
		return bytes;
	}
	
	public static int[] byteArr_to_intArr(byte[] target) throws IOException{
		ByteArrayInputStream bais = new ByteArrayInputStream(target);
		DataInputStream in = new DataInputStream(bais);
		int arrSize=in.readInt();
		int[] res=new int[arrSize];
		for (int i = 0; i < arrSize; i++) {
			res[i]=in.readInt();
		}
		return res;
	}
	
	public static int[] read_intArr_littleEndian(int dataNum, DataInputStream stream) throws IOException{
		byte[] buffer_int = new byte[4]; //int
		int[] rankedDoc=new int[dataNum];
		for (int j = 0; j < dataNum; j++) {
			rankedDoc[j]=byteArr_to_int(readByteArr(buffer_int, stream));
		}
		return rankedDoc;
	}
	
	public static double[] read_doubleArr_littleEndian(int dataNum, DataInputStream stream) throws IOException{
		byte[] buffer_double = new byte[8]; //double
		double[] res=new double[dataNum];
		for (int j = 0; j < dataNum; j++) {
			res[j]=byteArr_to_double(readByteArr(buffer_double, stream));
		}
		return res;
	}
	
	public static byte[] compress_LZ4_intList(byte[] target, LZ4Compressor compressor){

		int oriLength = target.length;

		// compress data
		int maxCompressedLength = compressor.maxCompressedLength(oriLength);
		byte[] compressed = new byte[maxCompressedLength];
		int actLength=compressor.compress(target, 0, oriLength, compressed, 0, maxCompressedLength);
		
		return Arrays.copyOf(compressed, actLength);
	}
	
	public static byte[] decompress_LZ4_intList(byte[] compressed, int oriLength, LZ4Decompressor decompressor){
		byte[] restored = new byte[oriLength];
		decompressor.decompress(compressed, 0, restored, 0, oriLength);
		return restored;
	}
	
	public static double[] floatArrToDouArr(float[] floatArr) { 
		double[] douArr=new double[floatArr.length];
		for (int i = 0; i < douArr.length; i++) {
			douArr[i]=floatArr[i];
		}
		return douArr;
	}
	
	public static int[] floatArrToIntArr(float[] floatArr) { 
		int[] resArr=new int[floatArr.length];
		for (int i = 0; i < resArr.length; i++) {
			resArr[i]=(int) floatArr[i];
		}
		return resArr;
	}
	
	public static void assignArrValue(float[] floatArr, int[] selectedInds, float[] values) {// floatArr(selectedInds)=values
		for (int i = 0; i < selectedInds.length; i++) {
			floatArr[selectedInds[i]]=values[i];
		}
	}
	
	public static float[] arrArrToArr(float[][] floatArr, String model) throws InterruptedException { 
		//each row/colomn contains same numble of samples
		float[] arr=new float[floatArr.length*floatArr[0].length];
		int ind=0;
		if (model.equalsIgnoreCase("rowFirst")) {
			for (int i = 0; i < floatArr.length; i++) {
				for (int j = 0; j < floatArr[0].length; j++) {
					arr[ind++]=floatArr[i][j];
				}
			}
			return arr;
		}else if (model.equalsIgnoreCase("colomnFirst")) {
			for (int i = 0; i < floatArr[0].length; i++) {
				for (int j = 0; j < floatArr.length; j++) {
					arr[ind++]=floatArr[j][i];
				}
			}
			return arr;
		}else {
			throw new InterruptedException("err in arrArrToArr, model shoud be rowFirst or colomnFirst, here:"+model);
		}
	}
	
	public static <T extends Object> ArrayList<T> arrArrToArrList(T[][] ArrArr, String model) throws InterruptedException { 
		//each row/colomn contains same numble of samples
		ArrayList<T> arrayList=new ArrayList<T>();
		if (model.equalsIgnoreCase("rowFirst")) {
			for (int i = 0; i < ArrArr.length; i++) {
				for (int j = 0; j < ArrArr[i].length; j++) {
					arrayList.add(ArrArr[i][j]);
				}
			}
			return arrayList;
		}else if (model.equalsIgnoreCase("colomnFirst")) {
			for (int i = 0; i < ArrArr[0].length; i++) {
				for (int j = 0; j < ArrArr.length; j++) {
					arrayList.add(ArrArr[j][i]);
				}
			}
			return arrayList;
		}else {
			throw new InterruptedException("err in arrArrToArr, model shoud be rowFirst or colomnFirst, here:"+model);
		}
	}
	
	public static float[][] arrToArrArr(float[] floatArr, String model, int rowNum, int colomnNum) throws InterruptedException { 
		General.Assert(floatArr.length==rowNum*colomnNum, "err in arrToArrArr, floatArr.length should == rowNum*colomnNum, here:"+floatArr.length);
		float[][] arrArr=new float[rowNum][colomnNum];
		if (model.equalsIgnoreCase("rowFirst")) {
			for (int i = 0; i < rowNum; i++) {
				for (int j = 0; j < colomnNum; j++) {
					arrArr[i][j]=floatArr[i*colomnNum+j];
				}
			}
			return arrArr;
		}else if (model.equalsIgnoreCase("colomnFirst")) {
			for (int i = 0; i < colomnNum; i++) {
				for (int j = 0; j < rowNum; j++) {
					arrArr[j][i]=floatArr[i*rowNum+j];
				}
			}
			return arrArr;
		}else {
			throw new InterruptedException("err in arrArrToArr, model shoud be rowFirst or colomnFirst, here:"+model);
		}
	}
	
	public static int[][] ArrListToIntArrArr(ArrayList<int[]> tt) { 
		int[][] kk=new int[tt.size()][];
		for (int i = 0; i < tt.size(); i++) {
			kk[i]=tt.get(i);
		}
		return kk;
	}
	
	public static String DouArrToString(Double[] douArr, String delimiter) {
		StringBuffer str=new StringBuffer();
		for(double i:douArr){
			str.append(i+delimiter);
		}
		return str.toString();
	}
	
	public static String ObjArrToString(Object[] objArr, String delimiter) {
		StringBuffer str=new StringBuffer();
		for(Object i:objArr){
			str.append(i+delimiter);
		}
		return str.toString();
	}
	
	public static float[][] padArr_symmetric(float[][] arr, int[] padSize, String model) throws InterruptedException {
		int numDims = padSize.length;
		Assert(numDims==2, "err in padArr_symmetric_both! numDims = padSize.length should ==2, here:"+numDims);
		int[] arrSize=new int[]{arr.length,arr[0].length};
		int[][] padInd= new int[numDims][];
		if (model.equalsIgnoreCase("pre")) {
			for (int i = 0; i < numDims; i++) {
				int M=arrSize[i];
				int[] dimNums=concateArr(new int[][]{makeRange(new int[]{0,M-1,1}), makeRange(new int[]{M-1,0,-1})});
				int p=padSize[i];
				padInd[i]=selectArrInt(dimNums, elementMod(makeRange(new int[]{-p,M-1,1}), 2*M), 0);//pre direction 
			}
			return selectArrFloat(arr, padInd);
		}else if (model.equalsIgnoreCase("post")) {
			for (int i = 0; i < numDims; i++) {
				int M=arrSize[i];
				int[] dimNums=concateArr(new int[][]{makeRange(new int[]{0,M-1,1}), makeRange(new int[]{M-1,0,-1})});
				int p=padSize[i];
				padInd[i]=selectArrInt(dimNums, elementMod(makeRange(new int[]{0,M+p-1,1}), 2*M), 0);//post direction 
			}
			return selectArrFloat(arr, padInd);
		}else if (model.equalsIgnoreCase("both")) {
			for (int i = 0; i < numDims; i++) {
				int M=arrSize[i];
				int[] dimNums=concateArr(new int[][]{makeRange(new int[]{0,M-1,1}), makeRange(new int[]{M-1,0,-1})});
				int p=padSize[i];
				padInd[i]=selectArrInt(dimNums, elementMod(makeRange(new int[]{-p,M+p-1,1}), 2*M), 0);//both direction 
			}
			return selectArrFloat(arr, padInd);
		}else {
			throw new InterruptedException("err in padArr_symmetric! model should be pre, post or both! here:"+model);
		}
	}
	
	public static HashSet<Integer> intArr_To_HashSet(int[] objArr) {
		HashSet<Integer> hashSet=new HashSet<Integer>(objArr.length);
		for (int i : objArr) {
			hashSet.add(i);
		}
		return hashSet;
	}
	
	public static float[] intArr_To_FloatArr(Integer[] objArr) {
		float[] res=new float[objArr.length];
		for (int i = 0; i < objArr.length; i++) {
			res[i]=objArr[i].floatValue();
		}
		return res;
	}
	
	public static String douArrArrToStr(double[][] douArr) {
		// in row by row
		StringBuffer strBuf = new StringBuffer();
		for(int i=0;i<douArr.length;i++){
			for(int j=0;j<douArr[0].length;j++){
				strBuf.append(douArr[i][j]+",");
			}
		}
		return strBuf.toString();
	}
	
	public static String StrArrToStr(String[] strArr, String delimeter) {
		StringBuffer str=new StringBuffer();
		for(String oneStr: strArr){
			if(oneStr!=null && !oneStr.isEmpty()){
				str.append(oneStr+delimeter);
			}
		}
		return str.toString();
	}

	public static int[] StrArrToIntArr(String[] strArr) { 
		int[] intArr=new int[strArr.length];
		for(int i=0;i<strArr.length;i++){
			intArr[i]=Integer.valueOf(strArr[i]);
		}
		return intArr;
	}
	
	public static Integer[] StrArrToIntegerArr(String[] strArr) { 
		Integer[] intArr=new Integer[strArr.length];
		for(int i=0;i<strArr.length;i++){
			intArr[i]=Integer.valueOf(strArr[i]);
		}
		return intArr;
	}
	
	public static boolean[] StrArrToBooleanArr(String[] strArr) { 
		boolean[] booleanArr=new boolean[strArr.length];
		for(int i=0;i<strArr.length;i++){
			booleanArr[i]=Boolean.valueOf(strArr[i]);
		}
		return booleanArr;
	}
	
	public static HashSet<Integer> StrArr_To_HashSetInt(String[] strArr) { 
		HashSet<Integer> hashSet=new HashSet<Integer>(strArr.length);
		for(int i=0;i<strArr.length;i++){
			hashSet.add(Integer.valueOf((strArr[i])));
		}
		return hashSet;
	}
	
	public static double[] StrArrToDouArr(String[] strArr) {
		double[] douArr=new double[strArr.length];
		for(int i=0;i<strArr.length;i++){
			douArr[i]=Double.valueOf(strArr[i]);
		}
		return douArr;
	}
	
	public static float[] StrArrToFloatArr(String[] strArr) {
		float[] floatArr=new float[strArr.length];
		for(int i=0;i<strArr.length;i++){
			floatArr[i]=Float.valueOf(strArr[i]);
		}
		return floatArr;
	}
	
	public static boolean xor(boolean a, boolean b) {
	    return (a && !b) || (!a && b);
	}
	
	public static float log(float base, float k) {
	    return (float) (Math.log(k)/Math.log(base));
	}
	
	public static double log(double base, double k) {
	    return (double) (Math.log(k)/Math.log(base));
	}
	
	public static void max(float[] max, float[] one) {
		for (int i = 0; i < max.length; i++) {
			if (one[i]>max[i]) {
				max[i]=one[i];
			}
		}
	}
	
	public static void min(float[] min, float[] one) {
		for (int i = 0; i < min.length; i++) {
			if (one[i]<min[i]) {
				min[i]=one[i];
			}
		}
	}
	
	public static float[] mul(float[] arr, float one) {
		float[] C=new float[arr.length];
		for (int i = 0; i < arr.length; i++) {
			C[i]=arr[i]*one;
		}
		return C;
	}
	
	public static int mod(int a, int b) {
		return a - (int)Math.floor((double)a/b)*b;
	}
	
	public static int[] mod_withInt(int a, int b) {
		int Int=(int)Math.floor((double)a/b);
		int mod=a-Int*b;
		return new int[]{Int,mod};
	}
	
	public static String memoryInfo() {
		return "Used_"+(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())/1024/1024+"MB/Free_"+Runtime.getRuntime().freeMemory()/1024/1024+"MB/Tot_"+Runtime.getRuntime().totalMemory()/1024/1024+"MB/Max_"+Runtime.getRuntime().maxMemory()/1024/1024+"MB";
	}
	
	public static float[] sub(float[] A, float[] B) {
		float[] C=new float[A.length];
		for (int i = 0; i < A.length; i++) {
			C[i]=A[i]-B[i];
		}
		return C;
	}
	
	public static float[] abs(float[] A) {
		float[] C=new float[A.length];
		for (int i = 0; i < A.length; i++) {
			C[i]=Math.abs(A[i]);
		}
		return C;
	}
	
	public static <T extends Object> boolean isSameArr(T[] Arr1, T[] Arr2) {
		boolean isSame=true;
		if ((Arr1==null)!=(Arr2==null)) {
			isSame=false;
		}else if (Arr1==null) {
			isSame=true;
		}else if (Arr1.length!=Arr2.length) {
			isSame=false;
		}else {
			for (int i = 0; i < Arr1.length; i++) {
				if(!Arr1[i].equals(Arr2[i])){
					isSame=false;
					break;
				}
			}
		}
		return isSame;
	}
	
	public static boolean isSameArr(int[] Arr1, int[] Arr2) {
		Assert(Arr1!=null && Arr2!=null, "err! both Arr1 and Arr2 must be not null!");
		boolean isSame=true;
		if (Arr1.length!=Arr2.length) {
			isSame=false;
		}else {
			for (int i = 0; i < Arr1.length; i++) {
				if(Arr1[i]!=Arr2[i]){
					isSame=false;
					break;
				}
			}
		}
		return isSame;
	}
	
	public static boolean isOrdered(Integer[] arr, boolean isAscOrder) {
		boolean res=true;
		if (isAscOrder) {//in ascending order
			for (int i = 1; i < arr.length; i++) {
				if (arr[i]<arr[i-1]) {
					res= false;
					break;
				}
			}
		}else {//in descending order
			for (int i = 1; i < arr.length; i++) {
				if (arr[i]>arr[i-1]) {
					res= false;
					break;
				}
			}
		}
		return res;
	}
	
	public static boolean isSameArr(boolean[] Arr1, boolean[] Arr2) {
		boolean isSame=true;
		if ((Arr1==null)!=(Arr2==null)) {
			isSame=false;
		}else if (Arr1==null) {
			isSame=true;
		}else if (Arr1.length!=Arr2.length) {
			isSame=false;
		}else {
			for (int i = 0; i < Arr1.length; i++) {
				if(Arr1[i]!=Arr2[i]){
					isSame=false;
					break;
				}
			}
		}
		return isSame;
	}
	
	public static boolean isSameArr(double[] Arr1, double[] Arr2) {
		boolean isSame=true;
		if ((Arr1==null)!=(Arr2==null)) {
			isSame=false;
		}else if (Arr1==null) {
			isSame=true;
		}else if (Arr1.length!=Arr2.length) {
			isSame=false;
		}else {
			for (int i = 0; i < Arr1.length; i++) {
				if(Arr1[i]!=Arr2[i]){
					isSame=false;
					break;
				}
			}
		}
		return isSame;
	}
	
	public static boolean isSameArr(float[] Arr1, float[] Arr2, float tollerant) {
		boolean isSame=true;
		if ((Arr1==null)!=(Arr2==null)) {
			isSame=false;
		}else if (Arr1==null) {
			isSame=true;
		}else if (Arr1.length!=Arr2.length) {
			isSame=false;
		}else {
			for (int i = 0; i < Arr1.length; i++) {
				if(!isEqual_float(Arr1[i], Arr2[i], tollerant)){
					isSame=false;
					break;
				}
			}
		}
		return isSame;
	}
	
	public static boolean isSameArrArr(double[][] Arr1, double[][] Arr2) {
		boolean isSame=true;
		if ((Arr1==null)!=(Arr2==null)) {
			isSame=false;
		}else if (Arr1==null) {
			isSame=true;
		}else if (Arr1.length!=Arr2.length) {
			isSame=false;
		}else {
			for (int i = 0; i < Arr1.length; i++) {
				if(!General.isSameArr(Arr1[i], Arr2[i])){
					isSame=false;
					break;
				}
			}
		}
		return isSame;
	}
	
	public static float[] DouArrToFloatArr(double[] douArr) {
		float[] floatArr=new float[douArr.length];
		for(int i=0;i<douArr.length;i++){
			floatArr[i]=(float) douArr[i];
		}
		return floatArr;
	}
	
	public static float[][] DouArrArrToFloatArrArr(double[][] douArrArr) {
		float[][] floatArr=new float[douArrArr.length][];
		for(int i=0;i<douArrArr.length;i++){
			floatArr[i]=DouArrToFloatArr( douArrArr[i]);
		}
		return floatArr;
	}
	
	public static <T> ArrayList<T> createArrayList(T fillValue, int size) {
		ArrayList<T> res = new ArrayList<T>(size);
		for(int i=0;i<size;i++){
			res.add(fillValue);
		}
		return res;
	}
	
	public static int[] concateArr(int[][] arrs){
		int totEleNum=0;
		for (int[] is : arrs) {
			totEleNum+=is.length;
		}
		int ind=0; int[] oneArr=new int[totEleNum];
		for (int[] is : arrs) {
			for (int i : is) {
				oneArr[ind]=i;
				ind++;
			}
		}
		return oneArr;
	}
	
	public static void closePrintWriterOnExist(PrintWriter outStr_report){
		if (outStr_report!=null) {
			outStr_report.close();
		}
	}
	
	public static int[] makeRange(int[] setup){ 
		//this function assume, (end-begin) can be fully divided by step
		/*  setup[]={begin, end, step}*/
		int begin=setup[0], end=setup[1], step=setup[2];
		int intervalNum=(int) ((end-begin)/step);//fully divided
		int[] rangs=new int[intervalNum+1];
		Assert(intervalNum>0, "err in makeRange with int[] setup:"+General.IntArrToString(setup, ",")+", intervalNum=((end-begin)/step) should >0");
		rangs[0]=begin;//first one is begin
		for (int i = 1; i <= intervalNum; i++) {
			rangs[i]=rangs[i-1]+step;
		}
		return rangs;
	}
	
	public static Integer[] makeRange(Integer[] setup){ 
		//this function assume, (end-begin) can be fully divided by step
		/*  setup[]={begin, end, step}*/
		int begin=setup[0], end=setup[1], step=setup[2];
		int intervalNum=(int) ((end-begin)/step);//fully divided
		Integer[] rangs=new Integer[intervalNum+1];
		Assert(intervalNum>0, "err in makeRange with int[] setup:"+General.IntArrToString(setup, ",")+", intervalNum=((end-begin)/step) should >0");
		rangs[0]=begin;//first one is begin
		for (int i = 1; i <= intervalNum; i++) {
			rangs[i]=rangs[i-1]+step;
		}
		return rangs;
	}
	
	public static int[] makeAllOnes_intArr(int arrSize, int v){
		int[] res=new int[arrSize];
		for (int i = 0; i < res.length; i++) {
			res[i]=v;
		}
		return res;
	}
	
	public static float[] makeAllOnes_floatArr(int arrSize, float v){
		float[] res=new float[arrSize];
		for (int i = 0; i < res.length; i++) {
			res[i]=v;
		}
		return res;
	}
	
	public static double[] makeAllOnes_doubleArr(int arrSize, double v){
		double[] res=new double[arrSize];
		for (int i = 0; i < res.length; i++) {
			res[i]=v;
		}
		return res;
	}
	
	public static float[] makeRange(float[] setup){ //[begin....end]
		//float value has some rounding problem, if 0~1, then, 0,0.1,0.2,0.3,0.400000006,0.5000007.....
		//this function assume, (end-begin) can be fully divided by step
		/*  setup[]={begin, end, step}*/
		float begin=setup[0], end=setup[1], step=setup[2];
		int intervalNum=(int) Math.ceil((end-begin)/step);//fully divided
		float[] rangs=new float[intervalNum+1];
		Assert(intervalNum>0, "err in makeRange with float[] setup:"+General.floatArrToString(setup, ",", "0.00000")+", intervalNum=((end-begin)/step) should >0");
		rangs[0]=begin;//first one is begin
		for (int i = 1; i <= intervalNum; i++) {
			rangs[i]=rangs[i-1]+step;
		}
		if (rangs[rangs.length-1]>end) {
			rangs[rangs.length-1]=end;
		}
		return rangs;
	}
	
	public static double[] makeRange(double[] setup){ //[begin....end]
		//float value has some rounding problem, if 0~1, then, 0,0.1,0.2,0.3,0.400000006,0.5000007.....
		//this function assume, (end-begin) can be fully divided by step
		/*  setup[]={begin, end, step}*/
		double begin=setup[0], end=setup[1], step=setup[2];
		int intervalNum=(int) ((end-begin)/step);//fully divided
		double[] rangs=new double[intervalNum+1];
		Assert(intervalNum>0, "err in makeRange with double[] setup:"+General.douArrToString(setup, ",", "0.00000")+", intervalNum=((end-begin)/step) should >0");
		rangs[0]=begin;//first one is begin
		for (int i = 1; i <= intervalNum; i++) {
			rangs[i]=rangs[i-1]+step;
		}
		return rangs;
	}
	
	public static float[] makeMiddleBins(float[] bins){
		//for assign one sample to two nearby bins, here make the middle of each bin
		//bins: from small to large
		float[] middleBins=new float[bins.length];
		middleBins[0]=bins[0];
		for (int i = 1; i < middleBins.length; i++) {
			middleBins[i]=(bins[i-1]+bins[i])/2;
		}
		return middleBins;
	}
	
	public static float[] makeLinearSpace(float begin, float end, int Num){ //[begin....end]
		//float value has some rounding problem, if 0~1, then, 0,0.1,0.2,0.3,0.400000006,0.5000007.....
		//this function generate N values between begin and end
		/*  {begin, end, Num}*/
		return makeRange(new float[]{begin,end,(end-begin)/(Num-1)}); //Num should > 1
	}

	public static double calculateGeoDistance(int pho1, int pho2, float[][] latlon, String method){//latlon should be float[lat_lon][photoNum];
		return calculateGeoDistance(latlon[0][pho1],latlon[1][pho1],latlon[0][pho2],latlon[1][pho2],method);
	}
	
	public static double calculateGeoDistance(double Lat1,double Lon1,double Lat2,double Lon2, String method){//DistInKm
		//method GreatCircle,Cartesian
		double geoDistance=-1;
		//Check for illegal input
		boolean outofbounds=Math.abs(Lat1)>90|Math.abs(Lon1)>180|Math.abs(Lat2)>90|Math.abs(Lon2)>180;
		if (outofbounds){
			System.err.println("calculateGeoDistance error!..lat long data out of normal boundary");
			return geoDistance;
		}
		if (Lon1 < 0)
	    	Lon1 = Lon1 + 360;
	    if (Lon2 < 0)
	    	Lon2 = Lon2 + 360;
	    if(method.equalsIgnoreCase("Cartesian")){
	    	double km_per_deg_la = 111.3237;
	        double km_per_deg_lo = 111.1350;
	        double km_la = km_per_deg_la * (Lat1-Lat2);
	        double dif_lo;
	        // Always calculate the shorter arc.
	        if (Math.abs(Lon1-Lon2) > 180)
	            dif_lo = Math.abs(Lon1-Lon2)-180;
	        else
	            dif_lo = Math.abs(Lon1-Lon2);
	        double km_lo = km_per_deg_lo * dif_lo * Math.cos((Lat1+Lat2)*Math.PI/360);
	        geoDistance = Math.sqrt(Math.pow(km_la, 2)+ Math.pow(km_lo,2));
	    }else{
	    	double R_aver = 6374;
	    	double deg2rad = Math.PI/180;
	        Lat1 = Lat1 * deg2rad;
	        Lon1 = Lon1 * deg2rad;
	        Lat2 = Lat2 * deg2rad;
	        Lon2 = Lon2 * deg2rad;
	        double a = Math.pow(Math.sin((Lat1-Lat2)/2),2) + Math.cos(Lat1) * Math.cos(Lat2) * Math.pow(Math.sin((Lon1-Lon2)/2),2);
	        geoDistance = 2*R_aver * Math.asin(Math.sqrt(a));
	    }
	    return geoDistance;
	}
  		
	public static int calculateHashCode(int a, int b){	
	    return a * 31 + b;
	}
	
	public static void makeORdelectFolder(String Strpath){	
		File dataSave  =   new  File(Strpath);
		if (dataSave.exists()){ // 注意!!, 首次运行才用！！
			deleteAll(dataSave); // 删除 dataSave中的所有文件
		}
		dataSave.mkdirs();
	}
	
	public static String listFilesInfo(File aFile, int count) { //count: label parent and child space, count = -1
		StringBuffer fileInfo=new StringBuffer();
		count++;
	    String spcs = "";
	    for (int i = 0; i < count; i++){
	      spcs += " ";
	    }
	    if(aFile.isFile())
	    	fileInfo.append(spcs + "[FILE] " + aFile.getName()+"\n");
	    else if (aFile.isDirectory()) {
	    	fileInfo.append(spcs + "[DIR] " + aFile.getName()+"\n");
		    File[] listOfFiles = aFile.listFiles();
		    if(listOfFiles!=null) {
		    	for (int i = 0; i < listOfFiles.length; i++)
		    		fileInfo.append(spcs +listFilesInfo(listOfFiles[i],count));
		    } else {
	      		fileInfo.append(spcs + " [ACCESS DENIED]"+"\n");
	      	}
	    }
	    count--;
		return fileInfo.toString();
	}
	
	public static void makeFolder(String Strpath){	
		File dataSave  =   new  File(Strpath);
		if (!dataSave.exists()){ // 注意!!, 首次运行才用！！
			dataSave.mkdirs(); // 删除 dataSave中的所有文件
		}
	}
	
	public static void deleteAll(File path){
		if(!path.exists())   return; 
		if(path.isFile()){ 
			path.delete();	
			return; 
		}	
		File[]   files   =   path.listFiles();	     
		for(int   i=0;i <files.length;i++){ 
			deleteAll(files[i]); 
		}	
		path.delete();  // delete the path itself
	}

	public static int assignFeatToCenter_fastGANN(double[] feat, float[][] centers, float[][] nodes, ArrayList<HashSet<Integer>> node_vw_links) {
		//find NN-node
		int node=assignFeatToCenter(feat,nodes);
		//get links for this node
		HashSet<Integer> links=node_vw_links.get(node);
		int minValueIndex=-1; double mindistance=999999;
		for(int vw:links){
			double thisDis=squaredEuclidian(feat,centers[vw]);
			if (thisDis<mindistance){
				mindistance=thisDis;
				minValueIndex=vw;
			}
		}
		return minValueIndex;
	}
	
	public static int assignFeatToCenter_fastGANN(float[] feat, float[][] centers, float[][] nodes, ArrayList<HashSet<Integer>> node_vw_links) {
		//find NN-node
		int node=assignFeatToCenter(feat,nodes);
		//get links for this node
		HashSet<Integer> links=node_vw_links.get(node);
		int minValueIndex=-1; double mindistance=999999;
		for(int vw:links){
			double thisDis=squaredEuclidian(feat,centers[vw]);
			if (thisDis<mindistance){
				mindistance=thisDis;
				minValueIndex=vw;
			}
		}
		return minValueIndex;
	}
	
	public static LinkedList<Int_Float> assignFeatToCenter_fastGANN_MultiAss(double[] feat, float[][] centers, float[][] nodes, ArrayList<HashSet<Integer>> node_vw_links, double alph, int NN, double deta) throws InterruptedException {
		//search in single node
		if (NN>1) {
			ArrayList<Integer> vws=new ArrayList<Integer>(); ArrayList<Float> dis=new ArrayList<Float>();
			//the following two methods: a, b can get similar performance but a is much faster, e.g., MAP=0.74 for indexLabel:_Oxford_5K_20K-VW_SURF, rankLabel: _iniR-BurstIntraInter@20@12_reR@1000@1000_reRHE@20@12_1vs1AndHistAndAngle@true@true@false@0.52@0.2@1@0@0@0@0@0@0@0
			//a. find NN-node
			int node=assignFeatToCenter(feat,nodes);
			//get links for this node
			HashSet<Integer> links=node_vw_links.get(node);
			for(int vw:links){
				float thisDis=(float) squaredEuclidian(feat,centers[vw]);
				vws.add(vw);
				dis.add(thisDis);
			}
//			//b. do not use node, use center directly
//			for (int vw = 0; vw < centers.length; vw++) {
//				float thisDis=(float) squaredEuclidian(feat,centers[vw]);
//				vws.add(vw);
//				dis.add(thisDis);
//			}
			//***** sort vw_dis *********
			ArrayList<Integer> topVWs=new ArrayList<Integer>();
			ArrayList<Float> topDists=new ArrayList<Float>();
			General_IR.rank_get_TopDocScores_PriorityQueue( vws, dis,  NN, topVWs, topDists, "ASC", true, true); //false: only select top samples, not rank these tops
			//***** select nearest vws *******
			float nnDist=(float) (topDists.get(0)*alph);
			LinkedList<Int_Float> vws_NN=new LinkedList<Int_Float>(); float L1Norm_ofSoftWeight=0;
			for (int top_i=0; top_i<topVWs.size();top_i++) {
				if (topDists.get(top_i)<nnDist) {
					float softWeight=(float) Math.exp(-Math.pow(topDists.get(top_i)/deta, 2)/2);
					L1Norm_ofSoftWeight+=softWeight;
					vws_NN.add(new Int_Float(topVWs.get(top_i), softWeight));
				}
			}
			//L1-normalise weight
			for (Int_Float oneVW : vws_NN) {
				oneVW.floatV/=L1Norm_ofSoftWeight;
			}
			return vws_NN;
		}else if(NN==1){
			int minValueIndex= assignFeatToCenter_fastGANN(feat, centers, nodes,  node_vw_links);
			LinkedList<Int_Float> vws_NN=new LinkedList<Int_Float>();
			vws_NN.add(new Int_Float(minValueIndex,1));
			return vws_NN;
		}else {
			return null;
		}
		
	}
	
	public static ArrayList<Integer> assignFeatToCenter_fastGANN_MultiAss(float[] feat, float[][] centers, float[][] nodes, ArrayList<HashSet<Integer>> node_vw_links, double alph, int NN, int TopNNnodes) throws InterruptedException {
		//search in multiple nodes
		
		HashMap<Integer, Float> vw_dis=new HashMap<Integer, Float>();
		
		//find NN-nodes
		int[] nnNodes=assignFeatToCenter_MultiAss(feat,nodes,TopNNnodes);
		
		//find NN-centers
		for (int node : nnNodes) {
			//get links for this node
			HashSet<Integer> links=node_vw_links.get(node);
			for(int vw:links){
				float thisDis=(float) squaredEuclidian(feat,centers[vw]);
				vw_dis.put(vw, thisDis);
			}
		}
		
		//***** sort vw_dis *********
		ArrayList<Integer> topVWs=new ArrayList<Integer>();
		ArrayList<Float> topDists=new ArrayList<Float>();
		General_IR.rank_get_TopDocScores_treeMap( vw_dis,  NN, topVWs, topDists, "ASC");
		
		//***** select nearest vws *******
		float nnDist=(float) (topDists.get(0)*alph);
		ArrayList<Integer> vws_NN=new ArrayList<Integer>(NN);
		for (int top_i=0; top_i<topVWs.size();top_i++) {
			if (topDists.get(top_i)<nnDist) {
				vws_NN.add(topVWs.get(top_i));
			}
		}
		return vws_NN;
	
	}
	
	public static int assignFeatToCenter(double[] feat, double[][] centers) {
		int minValueIndex=-1; double mindistance=999999;
		for(int i=0;i<centers.length;i++){
			double thisDis=squaredEuclidian(feat,centers[i]);
			if (thisDis<mindistance){
				mindistance=thisDis;
				minValueIndex=i;
			}
		}
		return minValueIndex;
	}
	
	public static int assignFeatToCenter(float[] feat, float[][] centers) {
		int minValueIndex=-1; float minDistance=Integer.MAX_VALUE;
		for(int i=0;i<centers.length;i++){
			float thisDis=squaredEuclidian(feat,centers[i]);
			if (thisDis<minDistance){
				minDistance=thisDis;
				minValueIndex=i;
			}
		}
		return minValueIndex;
	}
	
	public static int assignFeatToCenter(double[] feat, float[][] centers) {
		int minValueIndex=-1; double mindistance=999999;
		for(int i=0;i<centers.length;i++){
			double thisDis=squaredEuclidian(feat,centers[i]);
			if (thisDis<mindistance){
				mindistance=thisDis;
				minValueIndex=i;
			}
		}
		return minValueIndex;
	}
	
	public static int assignFeatToCenter(double[] feat, ArrayList<double[]> centers) {
		int minValueIndex=-1; double mindistance=999999;
		for(int i=0;i<centers.size();i++){
			double thisDis=squaredEuclidian(feat,centers.get(i));
			if (thisDis<mindistance){
				mindistance=thisDis;
				minValueIndex=i;
			}
		}
		return minValueIndex;
	}
	
	public static int[] assignFeatToCenter_MultiAss(float[] feat, float[][] centers, int TopNN) {
		HashMap<Integer, Double> vw_dis=new HashMap<Integer, Double>();
		for (int i = 0; i < centers.length; i++) {
			double thisDis=squaredEuclidian(feat,centers[i]);
			vw_dis.put(i, thisDis);
		}
		//***** sort vw_dis *********
		ValueComparator_Dou_ASC mvCompartor = new ValueComparator_Dou_ASC(vw_dis);
		TreeMap<Integer,Double> vw_dis_ASC = new TreeMap<Integer,Double>(mvCompartor);
		vw_dis_ASC.putAll(vw_dis);
		//return
		int[] topNN_index=new int[TopNN];
		int i=0;
		for (int ind : vw_dis_ASC.keySet()) {
			if (i<TopNN) {
				topNN_index[i++]=ind;
			}
		}
		return topNN_index;
	}
	
	public static double squaredEuclidian(double[] feat1, double[] feat2) {
		double thisDis=0; double dif=0;
		for (int j=0;j<feat1.length;j++){
			dif=feat1[j]-feat2[j];
			thisDis=thisDis+dif*dif;
		}
		return thisDis;
	}
	
	public static float squaredEuclidian(float[] feat1, float[] feat2) {
		Assert(feat1.length==feat2.length, "err, feat1.length should == feat2.length, but "+feat1.length+", "+feat2.length);
		float thisDis=0; float dif=0;
		for (int j=0;j<feat1.length;j++){
			dif=feat1[j]-feat2[j];
			thisDis=thisDis+dif*dif;
		}
		return thisDis;
	}
		
	public static double squaredEuclidian(double[] feat1, float[] feat2) {
		double thisDis=0; double dif=0;
		for (int j=0;j<feat1.length;j++){
			dif=feat1[j]-feat2[j];
			thisDis=thisDis+dif*dif;
		}
		return thisDis;
	}
	
	public static float vectorRotationAngle(float[] feat1, float[] feat2, double vectorLength_1, double vectorLength_2){//two 2D-vector's rotation angle, feat2->feat1, in (-pi,pi] format
		//counterclockwise is positive rotation direction!!
		float crossProduct=feat1[0]*feat2[1]-feat1[1]*feat2[0];
		if (crossProduct>=0) {//feat2 is counterclockwise from feat1, 
			return (float) Math.acos(vectorAngleCos(feat1, feat2, vectorLength_1, vectorLength_2));
		}else {//feat2 is clockwise from feat1, when crossProduct==0, two vector is parallel, then angle ==0 or pi
			return (float) -Math.acos(vectorAngleCos(feat1, feat2, vectorLength_1, vectorLength_2));
		}
	}
	
	public static float vectorAngleCos(float[] feat1, float[] feat2, double vectorLength_1, double vectorLength_2) {//two vector's angle in cos format
		if (vectorLength_1>0 && vectorLength_2>0) {//already have vectorLength in previous, so save computing time
			return (float) (vectorInnerMut(feat1, feat2)/vectorLength_1/vectorLength_2);
		}else {
			return (float) (vectorInnerMut(feat1, feat2)/Math.sqrt(vectorInnerMut(feat1,feat1))/Math.sqrt(vectorInnerMut(feat2,feat2)));
		}
	}
	
	public static float vectorInnerMut(float[] feat1, float[] feat2) {//two vector's inner product
		float res=0; 
		for (int j=0;j<feat1.length;j++){
			res+=feat1[j]*feat2[j];
		}
		return res;
	}
	
	public static boolean isInRange(float v, float[] low_high) {
		if (v>=low_high[0] && v<=low_high[1]) {
			return true;
		}else {
			return false;
		}
	}
	
	public static int[] ListToIntArr(List<Integer> arrList) { 
		int[] intArr=new int[arrList.size()];
		int i=0;
		for(Integer one :arrList){
			intArr[i]=one;
			i++;
		}
		return intArr;
	}
	
	public static void writeMatrixTxt(double[][] data, String dataPath) throws UnsupportedEncodingException, FileNotFoundException {
		// write [][] to a txt file
		PrintWriter outStr_data = new PrintWriter(new OutputStreamWriter(new FileOutputStream(dataPath), "UTF-8")); 
		for(int i=0;i<data.length;i++){
			StringBuffer oneRow = new StringBuffer();
			for(int j=0;j<data[i].length;j++){
				oneRow.append(data[i][j]+",");
			}
			outStr_data.println(oneRow.toString());
		}
		outStr_data.close();
	}
	
	public static double[][] readMatrixTxt(String dataPath) throws IOException {
		// read [][] from a txt file
		int rowNum=0;
		
		BufferedReader inStr_data= new BufferedReader(new InputStreamReader(new FileInputStream(dataPath), "UTF-8"));
		while(inStr_data.readLine()!=null){
			rowNum++;
		}
		inStr_data.close();
		
		double[][] data=new double[rowNum][]; 
		inStr_data= new BufferedReader(new InputStreamReader(new FileInputStream(dataPath), "UTF-8"));
		for(int i=0;i<rowNum;i++){
			data[i]=StrArrToDouArr(inStr_data.readLine().split(","));
		}
		inStr_data.close();

		return data;
	}
	
	public static void copyFrames(HashMap<String, Integer> hashMap_VideoIDFraNum, ArrayList<String> Arr_selected, String PhotoPath,String destBasePath) throws Exception {
		// select(copy) photos from BasePath to destiPath
		for(String videoID:hashMap_VideoIDFraNum.keySet()){
			if(Arr_selected.contains(videoID)){
				int frameNum=hashMap_VideoIDFraNum.get(videoID);
				int middle=(frameNum+1)/2; //middle frame index, from 1, 加1避免frameNum是1的情况
				String targetPho=videoID+"_"+middle+".jpg";
				File oldfile=new File(PhotoPath+targetPho); 
				if (oldfile.exists()){ // photo exist
					File newfile=new File(destBasePath+targetPho); 
					if(!forTransfer(oldfile,newfile)){
						System.out.println("fail to copy "+oldfile.getAbsolutePath()+" to "+newfile.getAbsolutePath());
					}
				}else{
					System.err.println("error! file not exist! "+ oldfile.toString());
				}
			}
		}
	}
	
	public static <K,V> HashMap<K,ArrayList<V>> copy_HashMap_K_ArrListV (HashMap<K,ArrayList<V>>  hashMap_A){
		HashMap<K,ArrayList<V>> hashMap_B=new HashMap<K,ArrayList<V>>(hashMap_A.size());
		for (Entry<K,ArrayList<V>> oneEntry : hashMap_A.entrySet()) {
			ArrayList<V> oneList_B=new ArrayList<V>();
			for (V oneDj:oneEntry.getValue()) {
				oneList_B.add(oneDj);
			}
			hashMap_B.put(oneEntry.getKey(), oneList_B);
		}
		return hashMap_B;
	}
	
	public static short[] copy_ShortArr (short[] shortArr_A){
		short[] shortArr_B=new short[shortArr_A.length];
		for (int i = 0; i < shortArr_A.length; i++) {
			shortArr_B[i]=shortArr_A[i];
		}
		return shortArr_B;
	}
	
	/**
	 * // wrapper all exceptions to InterruptedException
	 * @param classK
	 * @return
	 * @throws InterruptedException
	 */
	public static <K extends Object> K creatNewInstance(Class<K> classK) throws InterruptedException{
		try {
			return classK.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			// wrapper all exceptions to InterruptedException
			e.printStackTrace();
			throw new InterruptedException("err in creatNewInstance, e:"+e.getMessage());
		}
	}
	
	public static boolean isDuplicated_Rows(float[][] centers, int[] dataIndInfo, float tollerance, boolean disp) {
		//check whether have duplicated rows 
		boolean isDuplicated=false;
		boolean needDataIndInfo=(dataIndInfo!=null);
		for (int i = 0; i < centers.length; i++) {
			for (int j = i+1; j < centers.length; j++) {
				if (General.isSameArr(centers[i], centers[j], tollerance)) {
					isDuplicated=true;
					if (disp) {
						System.out.println("info in isDuplicated_Rows:");
						System.out.println(i+"-th row:"+General.floatArrToString(centers[i], "_", "0.0"));
						System.out.println(j+"-th row:"+General.floatArrToString(centers[j], "_", "0.0"));
						dispInfo_ifNeed(needDataIndInfo, "", "corresponding dataInd: "+dataIndInfo[i]+"_"+dataIndInfo[j]);
					}else {
						return isDuplicated;
					}
				}
			}
		}
		return isDuplicated;
	}
	
	public static void showPhoto_BinIndex(int[] bins, int[] showNum, int binIndex,int max_show, String[] photoPaths, String[] photoDiscrptions, String folderName) throws Exception{	
		//int[] showNum=new int[bins.length+1]; //last one for binIndex==-1: value > bins' last value
		if(binIndex==0){//groundTruth size==0
			folderName=folderName+"--"+bins[0]+"/";
			if(showNum[0]==0)
				General.makeFolder(folderName);
			if(showNum[0]<max_show){
				for(int i=0;i<photoPaths.length;i++){
					forTransfer(photoPaths[i],folderName+photoDiscrptions[i]+".jpg");
				}
			}
		}else{
			if(binIndex==-1){//groundTruth size > bins' last value
				folderName=folderName+bins[bins.length-1]+"--init/";
				if(showNum[showNum.length-1]==0)
					General.makeFolder(folderName);
				if(showNum[showNum.length-1]<max_show){
					for(int i=0;i<photoPaths.length;i++){
						forTransfer(photoPaths[i],folderName+photoDiscrptions[i]+".jpg");
					}
				}
			}else{
				folderName=folderName+(bins[binIndex-1]+1)+"--"+bins[binIndex]+"/";
				if(showNum[binIndex]==0)
					General.makeFolder(folderName);
				if(showNum[binIndex]<max_show){
					for(int i=0;i<photoPaths.length;i++){
						forTransfer(photoPaths[i],folderName+photoDiscrptions[i]+".jpg");
					}
				}
			}
		}
	}
	
	public static void showLocationList_BinIndex(int[] bins, int[] showNum, int binIndex,int max_show_eachBin, String folderName, ArrayList<ArrayList<Integer>> locPhotosList, ArrayList<ArrayList<String>> locPhotosDiscriptions, MapFile.Reader MapFileReader) throws Exception{	
		//int[] showNum=new int[bins.length+1]; //last one for binIndex==-1: value > bins' last value
		if(binIndex==0){//groundTruth size==0
			folderName=folderName+"--"+bins[0]+"/";
			int binIndex_act=0;
			//the-same
			if(showNum[binIndex_act]==0)
				General.makeFolder(folderName);
			if(showNum[binIndex_act]<max_show_eachBin){
				for(int i=0;i<locPhotosList.size();i++){
					for(int j=0;j<locPhotosList.get(i).size();j++){
						savePhotoFromMapFile(MapFileReader, locPhotosList.get(i).get(j), folderName+locPhotosDiscriptions.get(i).get(j));
					}
				}
				showNum[binIndex_act]++;
			}
		}else{
			if(binIndex==-1){//groundTruth size > bins' last value
				folderName=folderName+bins[bins.length-1]+"--init/";
				int binIndex_act=showNum.length-1;
				//the-same
				if(showNum[binIndex_act]==0)
					General.makeFolder(folderName);
				if(showNum[binIndex_act]<max_show_eachBin){
					for(int i=0;i<locPhotosList.size();i++){
						for(int j=0;j<locPhotosList.get(i).size();j++){
							savePhotoFromMapFile(MapFileReader, locPhotosList.get(i).get(j), folderName+locPhotosDiscriptions.get(i).get(j));
						}
					}
					showNum[binIndex_act]++;
				}
			}else{
				folderName=folderName+(bins[binIndex-1]+1)+"--"+bins[binIndex]+"/";
				int binIndex_act=binIndex;
				//the-same
				if(showNum[binIndex_act]==0)
					General.makeFolder(folderName);
				if(showNum[binIndex_act]<max_show_eachBin){
					for(int i=0;i<locPhotosList.size();i++){
						for(int j=0;j<locPhotosList.get(i).size();j++){
							savePhotoFromMapFile(MapFileReader, locPhotosList.get(i).get(j), folderName+locPhotosDiscriptions.get(i).get(j));
						}
					}
					showNum[binIndex_act]++;
				}
			}
		}
	}
	
	public static void showPhoto(int PhotoIndex, String BasePath, String destiPath) throws Exception {
		// select(copy) photo from BasePath to destiPath
		int saveInterval=100000; int total_photos=3185258;  
		String folder=(PhotoIndex/saveInterval*saveInterval+1)+"-"+(PhotoIndex/saveInterval+1)*saveInterval;
		String filename=PhotoIndex+"_"+total_photos+".jpg";
		File oldfile=new File(BasePath+folder+"\\"+filename); 
		if (oldfile.exists()){ // photo exit
			File newfile=new File(destiPath); 
			if(!forTransfer(oldfile,newfile)){
				System.out.println("fail to copy "+oldfile.getAbsolutePath()+" to "+newfile.getAbsolutePath());
			}
		}else{
			System.out.println(filename+" does not exist!!");
		}
	}
	
	public static void showPhoto_inHTML(PrintWriter htmlText, String HtmlTitle, int htmlSentenceType, 
			String imageBasePath, String queryImage, String queryImage_caption, String queryColor,
			ArrayList<String> gTruth, ArrayList<String> gTruth_Caption,  ArrayList<String> gTruth_Color, int smallPhoColNum_gTru,
			ArrayList<String> candidateImages, ArrayList<String> candidateImages_Caption, ArrayList<String> candidateImages_Color,
			ArrayList<ArrayList<String>> Neighbours, ArrayList<ArrayList<String>> Neighbours_Caption, ArrayList<ArrayList<String>> Neighbours_Color, ArrayList<String> groupCaption, int smallPhoColNum_GVNeig) {

		String bigPhoSize_W="max-width:100px;"; String bigPhoSize_H="max-height:100px;"; String smallPhoSize_W="max-width:50px;"; String smallPhoSize_H="max-height:50px;";
//		String bigPhoSize_W="width:100px;"; String bigPhoSize_H="height:100px;"; String smallPhoSize_W="width:100px;"; String smallPhoSize_H="height:100px;";

		String cellMinWidth="min-width:50px;";
		
		if (htmlSentenceType==0 || htmlSentenceType==-1) {//begin of one html file, -1 mark this html only one query, one list
			htmlText.print("<html>\n" +
	                "<head><title>show photos</title></head>\n" +
	                "<body bgcolor=\"#FFFFFF\">\n" +
	                "<h2>"+HtmlTitle+"</h2>");
			htmlText.print("<table>\n");
		}

		//*********** add one new cell in the big table for one query **************
        htmlText.print("<tr>\n");
        //add query
        if (queryImage!=null) {
            htmlText.print("<td style=\"vertical-align:bottom\"><a href=\""+imageBasePath+queryImage+"\""+"><img style=\""+bigPhoSize_W+bigPhoSize_H+"border:medium solid "+queryColor+";\"src=\""+imageBasePath+queryImage+"\" border=\"5\" style=\"border: 3px black solid;\"></a><p>" + queryImage_caption + "</p></td>\n");
		}
        //add query's ground truth
        if (gTruth!=null) {
        	htmlText.print("\t<td><table border=\"4\" style=\"border: 2px solid green;\">\n");
        	htmlText.print("\t\t<tr>");
            for (int x = 0; x< gTruth.size(); x++) {
                htmlText.print("<td style=\"vertical-align:bottom;"+cellMinWidth+"\"><center><a href=\""+ imageBasePath+gTruth.get(x) +"\"><img style=\""+smallPhoSize_W+smallPhoSize_H+"border:medium solid "+gTruth_Color.get(x)+";\"src=\""+ imageBasePath+gTruth.get(x) +"\" border=\"5\" style=\"border: 3px black solid;\"></a><p>" + gTruth_Caption.get(x) + "</p></center></td>");
                if ((x+1) % smallPhoColNum_gTru == 0 && x<(gTruth.size()-1)) 
                	htmlText.print("</tr>\n\t\t<tr>");
            }
            htmlText.print("</tr>\n\t</table></td>\n");
		}
        //add rank list
        if (candidateImages!=null || Neighbours!=null) {
        	int listNum=Math.max(candidateImages==null?0:candidateImages.size(), Neighbours==null?0:Neighbours.size());//for: candidateImages is null, but Neighbours is not, vise varse
        	for (int j = 0;j<listNum;j++) {
        		//add candidateImages
        		if (candidateImages!=null) {
                    htmlText.print("\t<td style=\"vertical-align:bottom;"+cellMinWidth+"\"><center><a href=\"" + imageBasePath+candidateImages.get(j) + "\"><img style=\""+bigPhoSize_W+bigPhoSize_H+"border:medium solid "+candidateImages_Color.get(j)+";\"src=\""+ imageBasePath+candidateImages.get(j) +"\" border=\"5\" style=\"border: 3px tblack solid;\"></a><p>" + candidateImages_Caption.get(j) + "</p></center></td>\n");
				}
                //add neighbor list   
                if (Neighbours!=null) {
                	htmlText.print("\t\t<td><table border=\"4\">\n");
                	htmlText.print("\t\t\t<tr>");
                    for (int x = 0;x<Neighbours.get(j).size();x++) {
                        htmlText.print("<td style=\"vertical-align:bottom;"+cellMinWidth+"\"><center><a href=\""+ imageBasePath+Neighbours.get(j).get(x) +"\"><img style=\""+smallPhoSize_W+smallPhoSize_H+"border:medium solid "+Neighbours_Color.get(j).get(x)+";\"src=\""+ imageBasePath+Neighbours.get(j).get(x) +"\" border=\"5\" style=\"border: 3px black solid;\"></a><p>" + Neighbours_Caption.get(j).get(x) + "</p></center></td>");
                        if ((x+1) % smallPhoColNum_GVNeig == 0 && x<(Neighbours.get(j).size()-1)) 
                        	htmlText.print("</tr>\n\t\t\t<tr>");
                    }
                    htmlText.print("</tr>\n\t\t</table><p>" + groupCaption.get(j) + "</p></td>\n");
    			}
            }
		}
        //cell end
        htmlText.print("</tr>\n");
        
        if (htmlSentenceType==1 || htmlSentenceType==-1) {//end of one html file, -1 mark this html only one query, one list
        	htmlText.print("</table>\n</body>\n</html>");
        	htmlText.close();
        }
	}
	
	public static void savePhotoFromMapFile(MapFile.Reader MapFileReader, Integer photoName, String PhotoDestination) throws IOException{
		IntWritable MapFile_key = new IntWritable(photoName);
		BufferedImage_jpg MapFile_value = new BufferedImage_jpg();
		MapFileReader.get(MapFile_key, MapFile_value);
		ImageIO.write(MapFile_value.getBufferedImage("phoID:"+MapFile_key, Disp.getNotDisp()), "jpg", new File(PhotoDestination+".jpg"));
	}
	
	public static void forTransfer(String BasePath, String destiPath) throws Exception {
		// select(copy) photo from BasePath to destiPath
		File oldfile=new File(BasePath); 
		if (oldfile.exists()){ // photo exit
			File newfile=new File(destiPath); 
			if (newfile.exists()) {
				throw new InterruptedException("destiPath exists: "+destiPath);
			}else{
				if(!forTransfer(oldfile,newfile)){
					throw new InterruptedException("fail to copy "+oldfile.getAbsolutePath()+" to "+newfile.getAbsolutePath());
				}
			}
		}else{
			throw new InterruptedException(BasePath+" does not exist!!");
		}
	}
	
	@SuppressWarnings("resource")
	public static boolean forTransfer(File f1,File f2) throws Exception{// copy f1 to f2, 对输入输出流获得其管道,然后分批次的从f1的管道中像f2的管道中输入数据每次输入的数据最大为ChannDataSize
		int ChannDataSize=1024*1024*5;  //5MB
		int length=ChannDataSize;
		FileInputStream in=new FileInputStream(f1);
		FileOutputStream out=new FileOutputStream(f2);
		FileChannel inC=in.getChannel();
		FileChannel outC=out.getChannel();
		while(true){
			if(inC.position()==inC.size()){ //inc end
				inC.close();
				outC.close();
				return true;
			}
			if((inC.size()-inC.position())<ChannDataSize)
				length=(int)(inC.size()-inC.position());
			else
				length=ChannDataSize;
			inC.transferTo(inC.position(),length,outC);
			inC.position(inC.position()+length);
		}
	}
	
	public static void writeObject(String objectPath, Object object) throws FileNotFoundException, IOException {
		// serialize model
		 ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(objectPath));
		 oos.writeObject(object);
		 oos.flush();
		 oos.close();
	}
	
	public static Object readObject(String objectPath) throws InterruptedException {
		/**
		 * toUse: classIndLab=(ClassIndexLabel) readObject(Path_regionIndex+tr_percentage+"_classIndLab");
		 */
		if (objectPath!=null && new File(objectPath).exists()) {
			try {// deserialize model
				ObjectInputStream ois = new ObjectInputStream( new FileInputStream(objectPath));
				Object obj = ois.readObject();
				ois.close();
				return obj;
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				throw new InterruptedException("err in readObject, FileNotFoundException:"+e.getMessage());
			} catch (IOException e) {
				e.printStackTrace();
				throw new InterruptedException("err in readObject, IOException:"+e.getMessage());
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				throw new InterruptedException("err in readObject, ClassNotFoundException:"+e.getMessage());
			}
		}else {
			return null;
		}
	}
	
	/** Read the given binary file, and return its contents as a byte array.
	 * @throws InterruptedException */ 
	public static byte[] readBinaryFile(File file, boolean disp, String spacer) throws InterruptedException{
		dispInfo_ifNeed(disp, spacer, "info in readBinaryFile: Reading in binary file named: " + file.getAbsolutePath()+", its size: "+file.length());
	    byte[] result = new byte[(int)file.length()];
	    try {
	      InputStream input = null;
	      try {
	        int totalBytesRead = 0;
	        input = new BufferedInputStream(new FileInputStream(file));
	        while(totalBytesRead < result.length){
	          int bytesRemaining = result.length - totalBytesRead;
	          //input.read() returns -1, 0, or more :
	          int bytesRead = input.read(result, totalBytesRead, bytesRemaining); 
	          if (bytesRead > 0){
	            totalBytesRead+=bytesRead;
	          }
	        }
	        /*
	         the above style is a bit tricky: it places bytes into the 'result' array; 
	         'result' is an output parameter;
	         the while loop usually has a single iteration only.
	        */
	        dispInfo_ifNeed(disp, spacer, "Num bytes read: " + dispNum(result.length, "M"));
	      }finally {
	    	  input.close();
	      }
	    }catch (FileNotFoundException ex) {
	    	dispInfo_ifNeed(disp, spacer, "File not found.");
	    	throw new InterruptedException(ex.toString());
	    }catch (IOException ex) {
	    	dispInfo_ifNeed(disp, spacer, ex.toString());
	    	throw new InterruptedException(ex.toString());
	    }
	    return result;
	}

	public static byte[] readBinaryFromUnknowSize(InputStream inStream, int bufferSize) throws IOException{
		int byteread = 0;
        LinkedList<byte[]> content=new LinkedList<byte[]>();
		byte[] buffer = new byte[bufferSize];
		while ((byteread = inStream.read(buffer)) != -1) {
		    if (byteread<buffer.length) {//one read does not necessarily read the full length of buffer, depend one the url connection, at least 1 byte if data not end, at most buffer.length
		    	byte[] temp=new byte[byteread];
		        for (int i = 0; i < temp.length; i++) {
		        	temp[i]=buffer[i];
				}
		        content.add(temp);
			}else {
				content.add(buffer.clone());
			}                
		}
		
		int totByteNum=0;
        for (byte[] oneBuffer: content) {
        	totByteNum+=oneBuffer.length;
		}
        
        byte[] content_byteArr=new byte[totByteNum];
        int ind=0;
        for (byte[] oneBuffer: content) {
        	for (byte oneByte: oneBuffer) {
            	content_byteArr[ind++]=oneByte;
			}
		}
        return content_byteArr;
	}
	
	/**
	   Write a byte array to the given file. 
	   Writing binary data is significantly simpler than reading it. 
	 * @throws InterruptedException 
	  */
	public static void writeBinaryFile(byte[] aInput, String aOutputFileName, boolean disp, String spacer) throws InterruptedException{
		dispInfo_ifNeed(disp, spacer, "info in writeBinaryFile: Writing binary file to "+aOutputFileName);
	    try {
	      OutputStream output = null;
	      try {
	        output = new BufferedOutputStream(new FileOutputStream(aOutputFileName,true));
	        output.write(aInput);
	      }finally {
	        output.close();
	      }
	    }catch(FileNotFoundException ex){
	    	dispInfo_ifNeed(disp, spacer, "FileError: "+ex.toString());
	    	throw new InterruptedException(ex.toString());
	    }catch(IOException ex){
	    	dispInfo_ifNeed(disp, spacer, ex.toString());
	    	throw new InterruptedException(ex.toString());
	    }
	    dispInfo_ifNeed(disp, spacer, "Writing binary file done! total bytes num:"+dispNum(aInput.length, "M"));
	}
	  
	public static byte[] BitSettoByteArray(BitSet bits) {
		int bitSize=bits.size(); // bits.length --> the index of the highest set bit(true bit) + 1, bits.size --> total size of bitSet
		/** should use bits.size() here!
		 * 	if use: int bitSize=bits.size();
		 * 	then	HESig2.toString(): {0, 25, 50}
		 *	 		byte[] kk=BitSettoByteArray(HESig2); kk.length: 8
		 *
		 *	if use: int bitSize=bits.length;
		 *	then	HESig2.toString(): {0, 25, 50}
		 *			byte[] kk=BitSettoByteArray(HESig2); kk.length: 7 !!
		 */
		byte[] bytes = new byte[(bitSize+ 7) / 8];
	    for (int i=0; i<bitSize; i++) {
	        if (bits.get(i)) {
	            bytes[bytes.length-i/8-1] |= 1<<(i%8);
	        }
	    }
	    return bytes;
	}
	
	public static BitSet BytetoBitSet(byte Byte) {
		BitSet bits = new BitSet(8);
		for (int i = 0; i < 8; i++) {
			if ((Byte & (1 << i )) > 0) {
				bits.set(i);
			}
		}
		return bits;
	}
	
	public static BitSet ByteArraytoBitSet(byte[] bytes) {
		BitSet bits = new BitSet(bytes.length * 8);
		for (int i = 0; i < bytes.length * 8; i++) {
			if ((bytes[bytes.length - i / 8 - 1] & (1 << (i % 8))) > 0) {
				bits.set(i);
			}
		}
		return bits;
	}
	
	public static int get_DiffBitNum(byte[] bytes1, byte[] bytes2) {
		return get_DiffBitNum(bytes1, bytes2, 0, bytes1.length);
	}
	
	public static int get_DiffBitNum(byte[] bytes1, byte[] bytes2, int compareRange_start, int compareRange_end) {
		//calculate Hamming distance, count number of different bits between bytes1 and bytes2 from compareRange_start to (compareRange_end-1)
		//compareRange should be integer times of 4!!
		int hammingDist=0;
		for(int i=compareRange_start; i<compareRange_end; i+=4){
			byte t1=(byte) (bytes1[i]^bytes2[i]);
			byte t2=(byte) (bytes1[i+1]^bytes2[i+1]);
			byte t3=(byte) (bytes1[i+2]^bytes2[i+2]);
			byte t4=(byte) (bytes1[i+3]^bytes2[i+3]);
			hammingDist+= Integer.bitCount( t1 << 24 | (t2& 0xFF) << 16 | (t3& 0xFF) << 8 | t4 & 0xFF);
		}
		return hammingDist;
	}
	
	public static int[] get_shang_remainder(int big, int small){
		int shang=big/small;
		int remainder=big-shang*small;
		return new int[]{shang,remainder};
	}
	
	public static String StrleftPad(String str, int padMode, int LENGTH, String addStr) { 
		//fill addStr to String to make length==LENGTH, padMode==0, left fill
		String result = str;
		int i = 0;
		while (i < (LENGTH - str.length())) {
			if (padMode == 0) {
				result = addStr + result;
			} else {
				result = result + addStr;
			}
			i++;
		}
		return result;
	}
	
	public static int HammingDis(String str1, String str2, int RADIX) { 
		//Calculate hamming distance between two strings, RADIX is the radix of the String represent, e.g., 2 for binary
		BigInteger integ_1=new BigInteger(str1,RADIX);
		BigInteger integ_2=new BigInteger(str2,RADIX);
		int HM_dis=integ_1.xor(integ_2).bitCount();
		return HM_dis;
	}
	
	public static byte[] intToByteArr (int iNT) { 
		byte[] Byte = ByteBuffer.allocate(4).putInt(iNT).array();// int to byte[]
		return Byte;
	}
	
	public static <M extends Object> ArrayList<ArrayList<M>> ini_ArrayList_ArrayList(int size, int subSize){
		ArrayList<ArrayList<M>> res=new ArrayList<ArrayList<M>>((int) (size*1.5));
		for (int i = 0; i < size; i++) {
			res.add(new ArrayList<M>((int) (subSize*1.5)));
		}
		return res;
	}
	
	public static <M extends Object> ArrayList<LinkedList<M>> ini_ArrayList_LinkedList(int size){
		ArrayList<LinkedList<M>> res=new ArrayList<LinkedList<M>>(size);
		for (int i = 0; i < size; i++) {
			res.add(new LinkedList<M>());
		}
		return res;
	}
	
	public static <K extends Object, V extends Object> ArrayList<HashMap<K,V>> ini_ArrayList_HashMap(int size){
		ArrayList<HashMap<K,V>> res=new ArrayList<HashMap<K,V>>(size);
		for (int i = 0; i < size; i++) {
			res.add(new HashMap<K,V>());
		}
		return res;
	}
	
	public static <M extends Object> ArrayList<HashSet<M>> ini_ArrayList_HashSet(int size){
		ArrayList<HashSet<M>> res=new ArrayList<HashSet<M>>(size);
		for (int i = 0; i < size; i++) {
			res.add(new HashSet<M>());
		}
		return res;
	}
	
	public static float[][] ini_floatArrArr(int rowNum, int colNum){
		float[][] newCenters=new float[rowNum][colNum]; 
		for (int i = 0; i < newCenters.length; i++) {
			newCenters[i]= new float[colNum];
		}
		return newCenters;
	}
	
 	public static int ByteArrToInt (byte[] iNT) { 
		return ByteBuffer.wrap(iNT).getInt();
	}
	
	public static ArrayList<byte[]> ByteArrArrToListByteArr (byte[][] ByteArrArr) { 
		ArrayList<byte[]> ListByteArr=new ArrayList<byte[]>(ByteArrArr.length);
		for (byte[] bs : ByteArrArr) {
			ListByteArr.add(bs);
		}
		return ListByteArr;
	}
	
	public static byte[] combine2ByteArr (byte[] A, byte[] B) { 
		byte[] combined=new byte[A.length+B.length];
		System.arraycopy(A, 0, combined, 0, A.length);
		System.arraycopy(B, 0, combined, A.length, B.length);
		return combined;
	}
	
	public static void addIntArr(int[] A, int[] B){//A=A+B
		for(int i=0;i<A.length;i++){
			A[i]+=B[i];
		}
	}
	
	public static void addStrArr_append(String[] A, String B){//A=A+B
		for(int i=0;i<A.length;i++){
			A[i]+=B;
		}
	}
	
	public static String[] addStrArr_prefx(String A, String[] B){//B=A+B
		for(int i=0;i<B.length;i++){
			B[i]=A+B[i];
		}
		return B;
	}
	
	public static void addStrArr(String[] A, String[] B){//A=A+B
		for(int i=0;i<A.length;i++){
			A[i]+=B[i];
		}
	}
	
	public static void addSampeToList(List<String> list, String toAdd, String delimeter, String preFix) {
		if (toAdd!=null) {
			for (String one:toAdd.split(delimeter)) {
				list.add(preFix+one); 
			}
		}
	}

	public static void addFloatArr(float[] A, float[] B){//A=A+B
		for(int i=0;i<A.length;i++){
			A[i]+=B[i];
		}
	}
	
	public static void addDoubleArr(double[] A, double[] B){//A=A+B
		for(int i=0;i<A.length;i++){
			A[i]+=B[i];
		}
	}
	
	public static boolean[] orBooleanArr(boolean[] A, boolean[] B){//A=A+B
		boolean[] res=new boolean[A.length];
		for(int i=0;i<A.length;i++){
			res[i]=A[i] || B[i];
		}
		return res;
	}
	
	public static int[] elementAdd(int[] aa, int b){//res=A.+b
		int[] res=new int[aa.length];
		for(int i=0;i<aa.length;i++){
			res[i]=aa[i]+b;
		}
		return res;
	}
	
	public static void elementAdd(double[] aa, double b){//A=A+B
		for(int i=0;i<aa.length;i++){
			aa[i]+=b;
		}
	}
	
	public static float[][] elementAdd(float[][] aa, float b){//res=A.+b
		float[][] res=new float[aa.length][aa[0].length];
		for(int i=0;i<aa.length;i++){
			for (int j = 0; j < aa[0].length; j++) {
				res[i][j]=aa[i][j]+b;
			}
		}
		return res;
	}
	
	public static float[][] elementAdd(float[][] aa, float[][] bb){//res=A.+b
		float[][] res=new float[aa.length][aa[0].length];
		for(int i=0;i<aa.length;i++){
			for (int j = 0; j < aa[0].length; j++) {
				res[i][j]=aa[i][j]+bb[i][j];
			}
		}
		return res;
	}
	
	public static void elementAdd_saveInA(float[][] aa, float[][] bb){//A=A.+b
		for(int i=0;i<aa.length;i++){
			for (int j = 0; j < aa[0].length; j++) {
				aa[i][j]+=bb[i][j];
			}
		}
	}
	
	public static Complex[][] elementAdd(Complex[][] aa, Complex[][] bb){//res=A.+b
		Complex[][] res=new Complex[aa.length][aa[0].length];
		for(int i=0;i<aa.length;i++){
			for (int j = 0; j < aa[0].length; j++) {
				res[i][j]=aa[i][j].add(bb[i][j]);
			}
		}
		return res;
	}
	
	public static float[][] elementDiv(float[][] aa, float[][] bb){//res=A./b
		float[][] res=new float[aa.length][aa[0].length];
		for(int i=0;i<aa.length;i++){
			for (int j = 0; j < aa[0].length; j++) {
				res[i][j]=aa[i][j]/bb[i][j];
			}
		}
		return res;
	}
	
	public static void elementDiv(float[] aa, float bb){//res=A./b
		for(int i=0;i<aa.length;i++){
			aa[i]/=bb;
		}
	}
	
	public static int[] elementFloor(float[] aa){//fix(a)
		int[] res=new int[aa.length];
		for(int i=0;i<aa.length;i++){
			res[i]=(int) Math.floor(aa[i]);
		}
		return res;
	}
	
	public static void elementMut(double[] aa, double bb){//res=A.*b
		for(int i=0;i<aa.length;i++){
			aa[i]*=bb;
		}
	}
	
	public static double[] elementMut_returnNew(double[] aa, double bb){//res=A.*b
		double[] res=new double[aa.length];
		for(int i=0;i<aa.length;i++){
			res[i]=aa[i]*bb;
		}
		return res;
	}
	
	public static float[] elementMut_returnNew(float[] aa, float bb){//res=A.*b
		float[] res=new float[aa.length];
		for(int i=0;i<aa.length;i++){
			res[i]=aa[i]*bb;
		}
		return res;
	}
	
	public static float[][] elementMut(float[][] aa, float b){//res=A.*b
		float[][] res=new float[aa.length][aa[0].length];
		for(int i=0;i<aa.length;i++){
			for (int j = 0; j < aa[0].length; j++) {
				res[i][j]=aa[i][j]*b;
			}
		}
		return res;
	}
	
	public static float[][] elementMut(int[][] aa, float b){//res=A.*b
		float[][] res=new float[aa.length][aa[0].length];
		for(int i=0;i<aa.length;i++){
			for (int j = 0; j < aa[0].length; j++) {
				res[i][j]=aa[i][j]*b;
			}
		}
		return res;
	}
	
	public static Complex[][] elementMut(Complex[][] aa, Complex[][] bb){//res=A.+b
		Complex[][] res=new Complex[aa.length][aa[0].length];
		for(int i=0;i<aa.length;i++){
			for (int j = 0; j < aa[0].length; j++) {
				res[i][j]=aa[i][j].multiply(bb[i][j]);
			}
		}
		return res;
	}
	
	public static Complex[][] elementMut(Complex[][] aa, float[][] bb){//res=A.+b
		Complex[][] res=new Complex[aa.length][aa[0].length];
		for(int i=0;i<aa.length;i++){
			for (int j = 0; j < aa[0].length; j++) {
				res[i][j]=aa[i][j].multiply(bb[i][j]);
			}
		}
		return res;
	}
	
	public static int[] elementMod(int[] aa, int b){//res=A.mod b
		int[] res=new int[aa.length];
		for(int i=0;i<aa.length;i++){
			res[i]=mod(aa[i],b);
		}
		return res;
	}
	
	public static float[][] elementPower(float[][] aa, float b){//res=A.^b
		float[][] res=new float[aa.length][aa[0].length];
		for(int i=0;i<aa.length;i++){
			for (int j = 0; j < aa[0].length; j++) {
				res[i][j]=(float) Math.pow(aa[i][j],b);
			}
		}
		return res;
	}
	
	public static void elementPower(double[] aa, double b){//res=A.^b
		for(int i=0;i<aa.length;i++){
			aa[i]=Math.pow(aa[i],b); 
		}
	}
	
	public static float[][] elementABS(Complex[][] aa){//res=abs(A)
		float[][] res=new float[aa.length][aa[0].length];
		for(int i=0;i<aa.length;i++){
			for (int j = 0; j < aa[0].length; j++) {
				res[i][j]=(float) aa[i][j].abs();
			}
		}
		return res;
	}
	
	public static float[][] elementExp(float[][] aa){//res=exp(A)
		float[][] res=new float[aa.length][aa[0].length];
		for(int i=0;i<aa.length;i++){
			for (int j = 0; j < aa[0].length; j++) {
				res[i][j]=(float) Math.exp(aa[i][j]);
			}
		}
		return res;
	}
	
	public static float[][] elementLog(float[][] aa){//res=log(A)
		float[][] res=new float[aa.length][aa[0].length];
		for(int i=0;i<aa.length;i++){
			for (int j = 0; j < aa[0].length; j++) {
				res[i][j]=(float) Math.log(aa[i][j]);
			}
		}
		return res;
	}
	
	public static Complex[][] elementSub(Complex[][] aa, Complex[][] bb){//res=A.+b
		Complex[][] res=new Complex[aa.length][aa[0].length];
		for(int i=0;i<aa.length;i++){
			for (int j = 0; j < aa[0].length; j++) {
				res[i][j]=aa[i][j].subtract(bb[i][j]);
			}
		}
		return res;
	}
	
	public static float[][] elementSub(float[][] aa, float[][] bb){//res=A.-b
		float[][] res=new float[aa.length][aa[0].length];
		for(int i=0;i<aa.length;i++){
			for (int j = 0; j < aa[0].length; j++) {
				res[i][j]=aa[i][j]-bb[i][j];
			}
		}
		return res;
	}
	
	public static void getUniNum(double[] arr, HashMap<Double, Integer> statics){//Calculate histogram of objs at the bins
		for (double one : arr) {
			if (statics.containsKey(one)) {
				statics.put(one,statics.get(one)+1);
			}else {
				statics.put(one,1);
			}
		}
	}
	
	public static String getPercentInfo(int num, int tot){
		return num+" of "+tot+", "+new DecimalFormat("00.0%").format((double)num/tot);
	}
	
	public static int getBinInd_linear(int[] bins, int obj){//assign obj to one bin in bins, return index binInd, let bins[binInd-1]<obj<=bins[binInd]
		int binInd=bins.length; //obj > bins' last-value
		for(int i=0;i<bins.length;i++){
			if(obj<=bins[i]){
				binInd=i;
				break;
			}
		}
		return binInd;
	}
	
	public static int getBinInd_linear(float[] bins, float obj){//assign obj to one bin in bins, return index binInd, let bins[binInd-1]<obj<=bins[binInd]
		//bins: from small to large
		int binInd=bins.length; //obj > bins' last-value
		for(int i=0;i<bins.length;i++){
			if(obj<=bins[i]){
				binInd=i;
				break;
			}
		}
//		//for debug show
//		if (binInd==bins.length || binInd==0) {
//			System.out.println("binInd:"+binInd+", obj:"+obj);
//		}
		return binInd;
	}
	
	public static int getBinInd_equalBin(float[] start_end_step, int lastBinInd, float obj){//assign obj to one bin in bins, return index binInd, let bins[binInd-1]<obj<=bins[binInd]
		//bins: from small to large, and with equal size of step!
		if (obj<=start_end_step[0]) {
			return 0;
		}else if (obj>start_end_step[1]) {//obj > bins' last-value
			return lastBinInd;
		}else {
			return (int) Math.ceil((obj-start_end_step[0])/start_end_step[2]);
		}
	}
	
	public static <K extends Comparable<K>> int getBinInd_linear(K[] bins, K obj){//assign obj to one bin in bins, return index binInd, let bins[binInd-1]<obj<=bins[binInd]
		//bins: from small to large
		int binInd=bins.length; //obj > bins' last-value
		for(int i=0;i<bins.length;i++){
			if(obj.compareTo(bins[i]) <= 0){
				binInd=i;
				break;
			}
		}
		return binInd;
	}
	
	public static int[] getBinInd_linear_assignTwoBins(float[] bins, float[] middleBins, float obj){//assign obj to one bin in bins, return index binInd, let bins[binInd-1]<obj<=bins[binInd]
		//bins: from small to large
		int binInd=getBinInd_linear(bins,obj);
		if (binInd!=0 && binInd!=bins.length) {
			return new int[]{binInd,obj<middleBins[binInd]?binInd-1:binInd+1};
		}else {
			return new int[]{binInd};//when binInd==0 or bins.length, it only votes for 1 bin.
		}
	}
	
	public static int[] getBinInd_equalBin_assignTwoBins(float[] start_end_step, int lastBinInd, float obj, float[] middleBins){//assign obj to one bin in bins, return index binInd, let bins[binInd-1]<obj<=bins[binInd]
		//bins: from small to large
		int binInd=getBinInd_equalBin(start_end_step,lastBinInd,obj);
		if (binInd!=0 && binInd!=lastBinInd) {
			return new int[]{binInd,obj<middleBins[binInd]?binInd-1:binInd+1};
		}else {
			return new int[]{binInd};//when binInd==0 or bins.length, it only votes for 1 bin.
		}
	}
	
	public static <T extends Comparable<T>> double[] latlonToCartesian (T lat, T lon){
		double[] xyz =new double[3];
		double lat_pi=Double.valueOf(lat.toString())* Math.PI/180;
		double lon_pi=Double.valueOf(lon.toString())* Math.PI/180;
		xyz[0]=Math.cos(lat_pi)*Math.cos(lon_pi);
		xyz[1]=Math.cos(lat_pi)*Math.sin(lon_pi);
		xyz[2]=Math.sin(lat_pi);
		return xyz;
	}
	
	@SuppressWarnings("unchecked")
	public static <T extends Object> T[] list_to_arr(List<T> list, Class<T> clazz){
		T[] ObjArr=(T[]) Array.newInstance(clazz, list.size());
		int i=0;
		for(T one:list){
			ObjArr[i]=one;
			i++;
		}
		return ObjArr;
	}
	
	@SuppressWarnings("unchecked")
	public static <T extends Object> T[][] listList_to_arrArr(List<ArrayList<T>> list, Class<T> clazz){
		T[][] ObjArr=(T[][]) Array.newInstance(clazz, list.size());
		int i=0;
		for(List<T> one:list){
			ObjArr[i]=list_to_arr(one, clazz);
			i++;
		}
		return ObjArr;
	}
	
	public static float[] CartesianTolatlon (double[] xyz){
		float[] latlon =new float[2];
		double x=xyz[0]; double y=xyz[1]; double z=xyz[2];
		double lat=(float) Math.atan2(z,Math.sqrt(x*x+y*y));
		double lon=(float) Math.atan2(y,x);
		
		latlon[0]=(float) (lat*180/Math.PI);
		latlon[1]=(float) (lon*180/Math.PI);
		return latlon;
	}
	
	public static int sum_IntArr (int[] toSum){
		int sum=0;
		for(int one : toSum){
			sum+=one;
		}
		return sum;
	}
	
	public static double sum_DouArr (double[] toSum){
		double sum=0;
		for(double one : toSum){
			sum+=one;
		}
		return sum;
	}
	
	public static float sum_FloatArr (float[] toSum){
		float sum=0;
		for(float one : toSum){
			sum+=one;
		}
		return sum;
	}
	
	public static long sum_LongArr (long[] toSum){
		long sum=0;
		for(long one : toSum){
			sum+=one;
		}
		return sum;
	}
	
	public static int Multiplicative_IntArr (int[] arr){
		int res=1;
		for(int one : arr){
			res*=one;
		}
		return res;
	}
	
	public static int Multiplicative_IntArr (int[] arr, int[] selectedInds){
		int res=1;
		for (int i = 0; i < selectedInds.length; i++) {
			res*=arr[selectedInds[i]];
		}
		return res;
	}
	
	public static int Multiplicative_IntArr (int[] arr, int start, int end){
		int res=1;
		for (int i = start; i < end; i++) {
			res*=arr[i];
		}
		return res;
	}
	
	public static double[] getMax_ind_val (double[] array) { 
		int ind=-1; double max=0;
		for (int i = 0; i < array.length; i++) {
			if(max<array[i]){
				ind=i;
				max=array[i];
			}
		}
		double[] ret=new double[2]; ret[0]=ind; ret[1]=max;
		return ret;
	}
	
	public static int[] getMax_ind_val (int[] array) { 
		int ind=-1; int max=0;
		for (int i = 0; i < array.length; i++) {
			if(max<array[i]){
				ind=i;
				max=array[i];
			}
		}
		int[] ret=new int[2]; ret[0]=ind; ret[1]=max;
		return ret;
	}
	
	public static int[] getMax_ind_val (List<Integer> array) { 
		int ind=-1; int max=0;
		for (int i = 0; i < array.size(); i++) {
			if(max<array.get(i)){
				ind=i;
				max=array.get(i);
			}
		}
		int[] ret=new int[2]; ret[0]=ind; ret[1]=max;
		return ret;
	}
	
	public static LinkedList<int[]> getMax_ind_vals (int[] array) { 
		LinkedList<int[]> ind_vals=new LinkedList<int[]>(); int max=0;
		for (int i = 0; i < array.length; i++) {
			if(max<array[i]){
				max=array[i];
				ind_vals.clear();
				ind_vals.add(new int[]{i,max});
			}else if(max==array[i]){
				ind_vals.add(new int[]{i,max});
			}
		}
		return ind_vals;
	}
	
	public static int[] getMin_ind_val (int[] array) { 
		int ind=-1; int min=Integer.MAX_VALUE;
		for (int i = 0; i < array.length; i++) {
			if(min>array[i]){
				ind=i;
				min=array[i];
			}
		}
		int[] ret=new int[2]; ret[0]=ind; ret[1]=min;
		return ret;
	}
	
	public static int getMax_ind (float[] array) { 
		int ind=-1; float max=Integer.MIN_VALUE;
		for (int i = 0; i < array.length; i++) {
			if(max<array[i]){
				ind=i;
				max=array[i];
			}
		}
		return ind;
	}
	
	public static int[] getMinMax_ind (float[] array) { 
		int ind_min=-1; int ind_max=-1; 
		float min=Integer.MAX_VALUE; float max=Integer.MIN_VALUE;
		for (int i = 0; i < array.length; i++) {
			if(min>array[i]){
				ind_min=i;
				min=array[i];
			}
			if(max<array[i]){
				ind_max=i;
				max=array[i];
			}
		}
		return new int[]{ind_min,ind_max};
	}
	
	public static int[][] getMinMax_ind (float[][] array) { 
		int ind_min_i=-1; int ind_min_j=-1; float min=Float.MAX_VALUE;
		int ind_max_i=-1; int ind_max_j=-1; float max=Integer.MIN_VALUE;
		for (int i = 0; i < array.length; i++) {
			for (int j = 0; j < array[i].length; j++) {
				//attention, this two can not if,else, for example:2,1 then min=1, max=null!
				if(min>array[i][j]){
					ind_min_i=i; ind_min_j=j;
					min=array[i][j];
				}
				if(max<array[i][j]){
					ind_max_i=i; ind_max_j=j;
					max=array[i][j];
				}
			}
		}
		return new int[][]{{ind_min_i,ind_min_j}, {ind_max_i,ind_max_j}};
	}
	
	public static <K> ArrayList<LinkedList<Entry<K, Integer>>> getMinMax_entry (HashMap<K, Integer> oneHashMap) { 
		//find conflict_min and _max, each may has multiple ones.
		LinkedList<Entry<K, Integer>> conflict_min = new LinkedList<Entry<K,Integer>>(); LinkedList<Entry<K, Integer>> conflict_max = new LinkedList<Entry<K,Integer>>(); 
		int minNum=Integer.MAX_VALUE; int maxNum=Integer.MIN_VALUE; 
		for (Entry<K, Integer> one : oneHashMap.entrySet()) {
			//attention, this two can not if,else, for example:2,1 then min=1, max=null!
			if (minNum>one.getValue()) {
				minNum=one.getValue();
				conflict_min.clear();conflict_min.add(one);
			}else if (minNum==one.getValue()) {
				conflict_min.add(one);
			}
			if (maxNum<one.getValue()) {
				maxNum=one.getValue();
				conflict_max.clear();conflict_max.add(one);
			}else if (maxNum==one.getValue()) {
				conflict_max.add(one);
			}
		}
		//return
		ArrayList<LinkedList<Entry<K, Integer>>> res=new ArrayList<LinkedList<Entry<K, Integer>>>();
		res.add(conflict_min);
		res.add(conflict_max);
		return res;
	}
	
	public static float get_Sum (float[] array) { 
		float sum=0;
		for (float f : array) {
			sum+=f;
		}
		return sum;
	}
	
	public static float get_Sum (float[][] array) { 
		float sum=0;
		for (float[] f : array) {
			sum+=get_Sum(f);
		}
		return sum;
	}
	
	public static void dispInfo (PrintWriter outputStream_Report, String info){
		System.out.println(info);
		if (outputStream_Report!=null) {
			outputStream_Report.println(info);
		}
	}
	
	public static void dispInfo_ifNeed (boolean disp, String spacer, String info){
		if (disp) {
			System.out.println(spacer+info);
		}
	}
	
	public static void dispTopLines_FromFile (String filePath, int top) throws IOException{
		BufferedReader inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"));
		String line1Photo; int ind=0;
		while((line1Photo=inStr_photoMeta.readLine())!=null && ind<top){
			System.out.println(ind+"-th Line:"+line1Photo);
			ind++;
		}
		inStr_photoMeta.close();
	}
	
	public static void jumpLines (BufferedReader inStream, int jumpNum) throws IOException{
		for (int i = 0; i < jumpNum; i++) {
			inStream.readLine();
		}
	}
	
	public static String dispNum (long Num, String Q){//covert number to K,M,G,T
		if (Q==null)
			return Num+"";
		else if (Q.equalsIgnoreCase("K"))
			return new DecimalFormat("0.0").format((double)Num/1000)+"K";
		else if (Q.equalsIgnoreCase("M"))
			return new DecimalFormat("0.0").format((double)Num/1000/1000)+"M";
		else if (Q.equalsIgnoreCase("G"))
			return new DecimalFormat("0.0").format((double)Num/1000/1000)+"G";
		else if (Q.equalsIgnoreCase("T"))
			return new DecimalFormat("0.0").format((double)Num/1000/1000)+"T";
		else
			return "errorNumFormate, should be null, K, M, G or T";
	}
	
	public static String dispTime (long time, String formate){//covert time to s,mins,hurs
		if (formate.equalsIgnoreCase("ms"))
			return time+"ms";
		else if (formate.equalsIgnoreCase("s"))
			return new DecimalFormat("0.0").format((double)(time/1000))+"s";
		else if (formate.equalsIgnoreCase("min"))
			return new DecimalFormat("0.0").format((double)(time/1000)/60)+"mins";
		else if (formate.equalsIgnoreCase("hour"))
			return new DecimalFormat("0.0").format((double)(time/1000)/60/60)+"hours";
		else
			return "errorTimeFormate, should be s, min or hour";
	}
	
	public static String dispTimeDate (long time, String formate){//covert time to yyyy-mm-dd hh:mm:ss, if null, then use local's default 
    	return new SimpleDateFormat(formate).format(time);
	}
	
	public static void sleep (int time, String Q){
		//make sleepTime
		int sleepTime=0;
		if (Q.equalsIgnoreCase("ms"))
			sleepTime=time;
		else if (Q.equalsIgnoreCase("s"))
			sleepTime=time*1000;
		else if (Q.equalsIgnoreCase("min"))
			sleepTime=time*1000*60;
		else if (Q.equalsIgnoreCase("hour"))
			sleepTime=time*1000*60*24;
		else
			throw new IllegalArgumentException("errorTimeFormate, it should be ms, s, min or hour");
		//sleep...
		try {
            Thread.sleep(sleepTime); //sleep in ms
		} catch (Exception ex) {}  
	}
	
	public static <K, V> void Map_to_List(Map<K,V> map, List<K> keylist, List<V> valuelist){
		for(Entry<K, V> one:map.entrySet()){
			keylist.add(one.getKey());
			valuelist.add(one.getValue());
		}
	}
	
	public static <K, V> HashMap<K,V> Arr_to_HashMap(K[] keylist,V[] valuelist){
		HashMap<K,V> hashMap=new HashMap<K, V>();
		Assert(keylist.length==valuelist.length, "keylist and valuelist should be equal length!");
		for (int i = 0; i < keylist.length; i++) {
			hashMap.put(keylist[i], valuelist[i]);
		}
		return hashMap;
	}
	
	@SuppressWarnings("unchecked")
	public static <V extends Number> V add(V a, V b){
		if (a instanceof Integer)
			return (V)(Integer)(a.intValue() + b.intValue());
		else if (a instanceof Double)
			return (V)(Double)(a.doubleValue() + b.doubleValue());
		else if (a instanceof Double)
			return (V)(Float)(a.floatValue() + b.floatValue());
		else 
			return null;
	}
	
	public static <K> void updateMap(Map<K,Integer> map, K oneKey, Integer UpdateValue){
		if (map.containsKey(oneKey)) {
			map.put(oneKey, map.get(oneKey)+UpdateValue);
		}else {
			map.put(oneKey, UpdateValue);
		}
	}
	
	public static <K> void updateMap(Map<K,Double> map, K oneKey, double UpdateValue){
		if (map.containsKey(oneKey)) {
			map.put(oneKey, map.get(oneKey)+UpdateValue);
		}else {
			map.put(oneKey, UpdateValue);
		}
	}
	
	public static <K> void updateMap(Map<K,Float> map, K oneKey, float UpdateValue){
		if (map.containsKey(oneKey)) {
			map.put(oneKey, map.get(oneKey)+UpdateValue);
		}else {
			map.put(oneKey, UpdateValue);
		}
	}
		
	public static <K,V> void updateMap(Map<K,ArrayList<V>> map, K oneKey, V addValue){
		ArrayList<V> oneValue=map.get(oneKey);
		if (oneValue!=null) {
			oneValue.add(addValue);
		}else {
			oneValue=new ArrayList<V>();
			oneValue.add(addValue);
			map.put(oneKey, oneValue);
		}
	}
	
	public static double[] floorMatrix(double[] aa, double thr){//only keep value that > thr
		double[] res=new double[aa.length];
		for (int i = 0; i < res.length; i++) {
			if (aa[i]>thr) {
				res[i]=aa[i];
			}
		}
		return res;
	}
	
	public static int[][] thresholdingMatrix(float[][] aa, float thr){
		int[][] res=new int[aa.length][aa[0].length];
		for (int i = 0; i < res.length; i++) {
			for (int j = 0; j < res[0].length; j++) {
				if (aa[i][j]>thr) {
					res[i][j]=1;
				}else {
					res[i][j]=0;
				}
			}
		}
		return res;
	}
	
	public static int[][] thresholdingMatrix_Inverse(float[][] aa, float thr){
		int[][] res=new int[aa.length][aa[0].length];
		for (int i = 0; i < res.length; i++) {
			for (int j = 0; j < res[0].length; j++) {
				if (aa[i][j]>thr) {
					res[i][j]=0;
				}else {
					res[i][j]=1;
				}
			}
		}
		return res;
	}
	
	public static BufferedImage getScaledInstance_onlyDownScale(BufferedImage oriImage, int maxPixelSize){
		int w=oriImage.getHeight();
		int h=oriImage.getWidth();
		double factor=(double)w*h/maxPixelSize;
		if (factor>1.1) {
			double scale=Math.pow(factor, 0.5);
			return getScaledInstance(oriImage, new int[]{(int) (w/scale), (int) (h/scale)});
		}else {
			return oriImage;
		}
	}
	
	public static BufferedImage getScaledInstance(BufferedImage oriImage, int[] targerSize){
		//scale image size
		if(oriImage.getHeight()!=targerSize[0] && oriImage.getWidth()!=targerSize[1]){
			if (oriImage.getHeight()>targerSize[0] && oriImage.getWidth()>targerSize[1]) {//height and weight all downScaling
				return General.getScaledInstance(oriImage, targerSize[0], targerSize[1], RenderingHints.VALUE_INTERPOLATION_BILINEAR, true);//only for downScaling! target sizes(height and weight) are all smaller than ori size. 
			}else {//other case
				return General.getScaledInstance(oriImage, targerSize[0], targerSize[1], RenderingHints.VALUE_INTERPOLATION_BICUBIC, false);//can be used for all!
			}
		}else {
			return oriImage;
		}
	}
	
	/**
     * Convenience method that returns a scaled instance of the provided {@code BufferedImage}.
     * http://today.java.net/pub/a/today/2007/04/03/perils-of-image-getscaledinstance.html
     *
     * @param img the original image to be scaled
     * @param targetWidth the desired width of the scaled instance,
     *    in pixels
     * @param targetHeight the desired height of the scaled instance,
     *    in pixels
     * @param hint one of the rendering hints that corresponds to
     *    {@code RenderingHints.KEY_INTERPOLATION} (e.g.
     *    {@code RenderingHints.VALUE_INTERPOLATION_NEAREST_NEIGHBOR}, 0.225ms but poor image
     *    {@code RenderingHints.VALUE_INTERPOLATION_BILINEAR},  0.75ms 
     *    {@code RenderingHints.VALUE_INTERPOLATION_BICUBIC}), 1.925ms
     * @param higherQuality if true, this method will use a multi-step
     *    scaling technique that provides higher quality than the usual
     *    one-step technique (only useful in downscaling cases, where
     *    {@code targetWidth} or {@code targetHeight} is
     *    smaller than the original dimensions, and generally only when
     *    the {@code BILINEAR} hint is specified)
     * @return a scaled version of the original {@code BufferedImage}
     * 
     * getScaledInstance(BufferedImage img, int targetHeight, int targetWidth, RenderingHints.VALUE_INTERPOLATION_BILINEAR, true)
     */
    public static BufferedImage getScaledInstance(BufferedImage img, int targetHeight, int targetWidth, Object hint, boolean higherQuality){
        int type = (img.getTransparency() == Transparency.OPAQUE) ?
            BufferedImage.TYPE_INT_RGB : BufferedImage.TYPE_INT_ARGB;
        BufferedImage ret = (BufferedImage)img;
        int w, h;
        if (higherQuality) {
            // Use multi-step technique: start with original size, then
            // scale down in multiple passes with drawImage()
            // until the target size is reached
            w = img.getWidth();
            h = img.getHeight();
        } else {
            // Use one-step technique: scale directly from original
            // size to target size with a single drawImage() call
            w = targetWidth;
            h = targetHeight;
        }
        do {
            if (higherQuality && w > targetWidth) {
                w /= 2;
                if (w < targetWidth) {
                    w = targetWidth;
                }
            }
            if (higherQuality && h > targetHeight) {
                h /= 2;
                if (h < targetHeight) {
                    h = targetHeight;
                }
            }
            BufferedImage tmp = new BufferedImage(w, h, type);
            Graphics2D g2 = tmp.createGraphics();
            g2.setRenderingHint(RenderingHints.KEY_INTERPOLATION, hint);
            g2.drawImage(ret, 0, 0, w, h, null);
            g2.dispose();
            ret = tmp;
        } while (w != targetWidth || h != targetHeight);

        return ret;
    }
	
    /** Returns an ori ImageIcon, or null if the path was invalid. */
	public static ImageIcon createImageIcon_ori(String photoPath, String description) {
		if (new File(photoPath).exists()) {
	        return new ImageIcon(photoPath, description);
	    } else {
	        System.err.println("Couldn't find file: " + photoPath);
	        return null;
	    }
	}
	
	/** Returns an scaled ImageIcon, or null if the path was invalid. */
	public static ImageIcon createImageIcon_scaled(String photoPath, String description, int targetWidth, int targetHeight, Object hint, boolean higherQuality) throws IOException {
		if (new File(photoPath).exists()) {
	        return new ImageIcon(General.getScaledInstance(ImageIO.read(new File(photoPath)) , targetWidth, targetHeight, hint, higherQuality), description);
	    } else {
	        System.err.println("Couldn't find file: " + photoPath);
	        return null;
	    }
	}
	
    public static void dispPhotos ( ArrayList<String[]> photoPaths_list, ArrayList<String[]> photoDiscrptions_list, int Size_w, int Size_h) throws IOException{
    	assert photoPaths_list.size()==photoDiscrptions_list.size();
    	assert photoPaths_list.get(0).length==photoDiscrptions_list.get(0).length;
    	//1. Create the frame.
		JFrame frame = new JFrame("FrameDemo");
		//2. Optional: What happens when the frame closes?
		frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE );
		//3. Create components and put them in the frame.
		//set contentPane
		JPanel contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		contentPane.setLayout(new GridBagLayout());
		Insets inset= new Insets(0, 0, 0, 5); //intervel between each photo
		for(int line_i=0;line_i<photoPaths_list.size();line_i++){
			String[] photoPaths=photoPaths_list.get(line_i);
			String[] photoDiscrptions=photoDiscrptions_list.get(line_i);
			for(int i=0;i<photoPaths.length;i++){
				//creat imageIcon
				ImageIcon imageIcon=createImageIcon_scaled( photoPaths[i],  photoDiscrptions[i], Size_w, Size_h, RenderingHints.VALUE_INTERPOLATION_NEAREST_NEIGHBOR, true);
				//set JLabel
				JLabel oneJLable= new JLabel();
				oneJLable.setHorizontalAlignment(JLabel.CENTER);
				oneJLable.setVerticalTextPosition(JLabel.BOTTOM);
				oneJLable.setHorizontalTextPosition(JLabel.CENTER);
				oneJLable.setText(photoDiscrptions[i]);
				oneJLable.setIcon(imageIcon);
				//set GridBagConstraints
				GridBagConstraints gbc_lblNewLabel = new GridBagConstraints();
				gbc_lblNewLabel.gridy = line_i;
				gbc_lblNewLabel.gridx = i;
				gbc_lblNewLabel.insets=inset;
				//add oneJLable to contentPane
				contentPane.add(oneJLable, gbc_lblNewLabel);
			}
		}
		//set scrollPane
		JScrollPane scrollPane = new JScrollPane(contentPane);
		scrollPane.getVerticalScrollBar().setUnitIncrement(20);
		frame.setContentPane(scrollPane);
		//4. Size the frame.
		frame.pack();
		//5. Show it.
		frame.setVisible(true);
	}
    
    public static void dispPhoto ( BufferedImage img) throws IOException{
    	//1. Create the frame.
		JFrame frame = new JFrame("FrameDemo");
		//2. Optional: What happens when the frame closes?
		frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE );
		//3. Create components and put them in the frame.
		//set contentPane
		JPanel contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		contentPane.setLayout(new GridBagLayout());
		Insets inset= new Insets(0, 0, 0, 5); //intervel between each photo
		//creat imageIcon
		ImageIcon imageIcon=new ImageIcon(img);
		//set JLabel
		JLabel oneJLable= new JLabel();
		oneJLable.setHorizontalAlignment(JLabel.CENTER);
		oneJLable.setVerticalTextPosition(JLabel.BOTTOM);
		oneJLable.setHorizontalTextPosition(JLabel.CENTER);
		oneJLable.setText("Height:"+img.getHeight()+", Width:"+img.getWidth());
		oneJLable.setIcon(imageIcon);
		//set GridBagConstraints
		GridBagConstraints gbc_lblNewLabel = new GridBagConstraints();
		gbc_lblNewLabel.gridy = 0;
		gbc_lblNewLabel.gridx = 0;
		gbc_lblNewLabel.insets=inset;
		//add oneJLable to contentPane
		contentPane.add(oneJLable, gbc_lblNewLabel);

		//set scrollPane
		JScrollPane scrollPane = new JScrollPane(contentPane);
		scrollPane.getVerticalScrollBar().setUnitIncrement(20);
		frame.setContentPane(scrollPane);
		//4. Size the frame.
		frame.pack();
		//5. Show it.
		frame.setVisible(true);
	}
    
    public static Color getStrengthColor(int RGBInd, float strength, double[] scalingInfo_matchStrength){//get color according to strength, RGBInd indicate red, green or blue
    	float[] rgb=new float[3]; 
		rgb[RGBInd]=(float) General.scaleValue(strength, scalingInfo_matchStrength, 0, 1); //normalize match strength to 0~1.0
		Color color=new Color(rgb[0],rgb[1],rgb[2]);
		return color;
    }
    
    public static int[][] mkConfusionMatrix( ArrayList<Short> tureLabel, ArrayList<Short> predLabel, int classNum) throws Exception {// 
		// make Confusion Matrix, row: ture   column: pred, class label from 0 to classNum-1!!
    	assert tureLabel.size()==predLabel.size():"tureLabel and predLabel should be equal length!";
		int[][] confuMatrix=new int[classNum][classNum];
		for(int currentClass=0;currentClass<classNum;currentClass++){
			//find this class's member's index in tureLabel
			ArrayList<Integer> trueMembers=new ArrayList<Integer>();
			for(int ti=0;ti<tureLabel.size();ti++){
				if(tureLabel.get(ti)==currentClass){
					trueMembers.add(ti);
				}
			}
			if(trueMembers.size()==0){
				System.out.println("error in makeConfusionMatrix, 0 object in class="+(currentClass));
			}else{
				// find member's predicted class
				for(int oneTM:trueMembers){
					int predClass=predLabel.get(oneTM);
					confuMatrix[currentClass][predClass]++;
				}
			}
		}
		return confuMatrix;
    }
    
    public static String gtInfo_from_ConfusionMatrix(int[][] confuMatrix, String[] className) throws Exception {// 
		// get info from Confusion Matrix, row: ture   column: pred, class label from 0 to classNum-1!!
    	int classNum=className.length;
    	assert classNum==confuMatrix.length:"className.length and confuMatrix.length should be equal length!";
    	//set info
    	StringBuffer info=new StringBuffer();
    	info.append("confuMatrix data: \n");
    	for(int row=0;row<classNum;row++){
    		info.append("class-"+row+": \t");
    		for(int col=0;col<classNum;col++){
    			info.append(confuMatrix[row][col]+"\t");
        	}
    		info.append("\n");
    	}
    	int[][] sumRowCols=sumRowCol(confuMatrix);
    	int[] rowSums=sumRowCols[0];
    	int[] colSums=sumRowCols[1];
    	info.append("confuMatrix precision and recall: \n");
		for(int currentClass=0;currentClass<classNum;currentClass++){
			//get this class's precision and recall
			float P=(float)confuMatrix[currentClass][currentClass]/colSums[currentClass];
			float R=(float)confuMatrix[currentClass][currentClass]/rowSums[currentClass];
			info.append(className[currentClass]+":\t"+"P_"+new DecimalFormat("00%").format(P)+"\t R_"+new DecimalFormat("00%").format(R)+"\n");
		}
		return info.toString();
    }
    
    public static String gtInfo_from_ConfusionMatrix(double[][] confuMatrix, String[] className) throws Exception {// 
		// get info from Confusion Matrix, row: ture   column: pred, class label from 0 to classNum-1!!
    	int classNum=className.length;
    	assert classNum==confuMatrix.length:"className.length and confuMatrix.length should be equal length!";
    	//set info
    	StringBuffer info=new StringBuffer();
    	info.append("confuMatrix data: \n");
    	for(int row=0;row<classNum;row++){
    		info.append("class-"+row+": \t");
    		for(int col=0;col<classNum;col++){
    			info.append(confuMatrix[row][col]+"\t");
        	}
    		info.append("\n");
    	}
    	double[][] sumRowCols=sumRowCol(confuMatrix,-1);
    	double[] rowSums=sumRowCols[0];
    	double[] colSums=sumRowCols[1];
    	info.append("confuMatrix precision and recall: \n");
		for(int currentClass=0;currentClass<classNum;currentClass++){
			//get this class's precision and recall
			double P=confuMatrix[currentClass][currentClass]/colSums[currentClass];
			double R=confuMatrix[currentClass][currentClass]/rowSums[currentClass];
			info.append(className[currentClass]+":\t"+"P_"+new DecimalFormat("00%").format(P)+"\t R_"+new DecimalFormat("00%").format(R)+"\n");
		}
		return info.toString();
    }
    
    public static float[] gtPR_from_ConfusionMatrix(int[][] confuMatrix, int targetClass) throws Exception {// 
		// get precision and recall for targetClass from Confusion Matrix, row: ture   column: pred, class label from 0 to classNum-1!!
    	int[][] sumRowCols=sumRowCol(confuMatrix);
    	int[] rowSums=sumRowCols[0];
    	int[] colSums=sumRowCols[1];
		//get targetClass's precision and recall
    	float P=(float)confuMatrix[targetClass][targetClass]/colSums[targetClass];
    	float R=(float)confuMatrix[targetClass][targetClass]/rowSums[targetClass];
		//save to PR
		float[] PR={P,R};
		return PR;
    }
    
    public static float[] gt_TPR_FPR_from_ConfusionMatrix(int[][] confuMatrix, int targetClass) throws Exception {// 
		// get precision and recall for targetClass from Confusion Matrix, row: ture   column: pred, class label from 0 to classNum-1!!
    	int[][] sumRowCols=sumRowCol(confuMatrix);
    	int[] rowSums=sumRowCols[0];
    	float TPR,FPR;
		//get targetClass's TPR and FPR
    	if(targetClass==0){
    		TPR=(float)confuMatrix[0][0]/rowSums[0];
        	FPR=(float)confuMatrix[1][0]/rowSums[1];
    	}else if(targetClass==1){
    		TPR=(float)confuMatrix[1][1]/rowSums[1];
        	FPR=(float)confuMatrix[0][1]/rowSums[0];
    	}else{
    		System.out.println("targetClass should be either 0 or 1 !!");
    		return null;
    	}
    	//save to PR
		float[] TPR_FPR={TPR,FPR};
		return TPR_FPR;
    }
    
    public static float[] gtPR_from_ConfusionMatrix(double[][] confuMatrix, int targetClass) throws Exception {// 
		// get precision and recall for targetClass from Confusion Matrix, row: ture   column: pred, class label from 0 to classNum-1!!
    	double[][] sumRowCols=sumRowCol(confuMatrix,-1);
    	double[] rowSums=sumRowCols[0];
    	double[] colSums=sumRowCols[1];
		//get targetClass's precision and recall
		double P=confuMatrix[targetClass][targetClass]/colSums[targetClass];
		double R=confuMatrix[targetClass][targetClass]/rowSums[targetClass];
		//save to PR
		float[] PR=new float[2];
		PR[0]=(float) P; PR[1]=(float) R;
		return PR;
    }
    
    public static int[][] sumRowCol (int[][] Matrix) {// 
		// get info from Confusion Matrix, row: ture   column: pred, class label from 0 to classNum-1!!
    	int rowNum=Matrix.length;
    	int colNum=Matrix[0].length;
    	int[] rowSums=new int[rowNum];
    	int[] colSums=new int[colNum];
    	
		for(int i_row=0;i_row<rowNum;i_row++){
			for(int i_col=0;i_col<colNum;i_col++){
				rowSums[i_row]+=Matrix[i_row][i_col];
				colSums[i_col]+=Matrix[i_row][i_col];
			}
		}
		int[][] sumRowCols=new int[2][];
		sumRowCols[0]=rowSums; sumRowCols[1]=colSums;
		return sumRowCols;
    }
    
    public static double[][] sumRowCol (double[][] Matrix, int type) throws InterruptedException {// 
		// get info from Confusion Matrix, row: ture   column: pred, class label from 0 to classNum-1!!
    	int rowNum=Matrix.length;
    	int colNum=Matrix[0].length;
    	double[] rowSums=new double[rowNum];
    	double[] colSums=new double[colNum];
    	
    	if (type==-1) {
    		for(int i_row=0;i_row<rowNum;i_row++){
    			for(int i_col=0;i_col<colNum;i_col++){
    				rowSums[i_row]+=Matrix[i_row][i_col];
    				colSums[i_col]+=Matrix[i_row][i_col];
    			}
    		}
		}else if (type==0) {
			for(int i_row=0;i_row<rowNum;i_row++){
    			for(int i_col=0;i_col<colNum;i_col++){
    				rowSums[i_row]+=Matrix[i_row][i_col];
    			}
    		}
		}else if (type==1)  {
			for(int i_row=0;i_row<rowNum;i_row++){
    			for(int i_col=0;i_col<colNum;i_col++){
    				colSums[i_col]+=Matrix[i_row][i_col];
    			}
    		}
		}else {
			throw new InterruptedException("error in General.sumRowCol, type should be -1-for all, 0-for row, 1-for colum, here type="+type);
		}
		
		double[][] sumRowCols=new double[2][];
		sumRowCols[0]=rowSums; sumRowCols[1]=colSums;
		return sumRowCols;
    }
    
    public static float[][] normBySum_IntArrArr (int[][] Matrix, int RowCol) throws Exception {//no
    	int rowNum=Matrix.length;
    	int colNum=Matrix[0].length;
    	float[][] norm=new float[rowNum][colNum];
		int[] sum= sumRowCol ( Matrix)[RowCol];//0:rowSum, 1:colSum
		if (RowCol==0) {//normlise by row
			for (int i = 0; i < rowNum; i++) {
				for (int j = 0; j < colNum; j++) {
					norm[i][j]=((float)Matrix[i][j])/sum[i];
				}
			}
		}else if (RowCol==1) {//normlise by col
			for (int i = 0; i < rowNum; i++) {
				for (int j = 0; j < colNum; j++) {
					norm[i][j]=((float)Matrix[i][j])/sum[j];
				}
			}
		}else {
			throw new Exception("error para-RowCol in normlise_IntArrArr! RowCol should be 0 for normlise by row, 1 for normlise by column!");
		}
		
		return norm;
    }
        
    public static boolean isEqual_float (float A, float B, float tollerance) {//no
    	if (Math.abs(A-B)<tollerance) {
			return true;
		}else {
			return false;
		}
    }
    
    public static void main(String[] args) throws Exception {//for debug!
		//binary --> big number --> Hex  (can save data space)
//		int bitNum=64; 
//		StringBuffer HESig=new StringBuffer(); 
//		for(int i=0;i<bitNum;i++){
//			if(i%5==0)
//				HESig.append("0");
//			else
//				HESig.append("1");
//		}
//		BigInteger integ=new BigInteger(HESig.toString(),2); //transfer binary to BigInteger
//		byte[] integ_byte=integ.toByteArray(); //str.getBytes();//integ_1.toByteArray();
//		System.out.println("integ_byte.length:"+integ_byte.length+", "+integ_byte[0]);//if integ_byte.length==9, then integ_byte[0] always 0000!
//		BigInteger integ_br=new BigInteger(integ_byte);
//		System.out.println(integ_br+": \t"+StrleftPad(integ_br.toString(2),0,64,"0"));
//		System.out.println("Character.MAX_RADIX:"+Character.MAX_RADIX);
//		int RADIX=36;
//		String hex=integ.toString(RADIX);//transfer BigInteger to hex value, can save data space
//		System.out.println(integ);
//		System.out.println(HESig);
//		System.out.println(hex);
//		BigInteger integ_recover=new BigInteger(hex,RADIX);// read from hex value
//		System.out.println("integ_recover:"+integ_recover);
//		//hamming distance for binary
//		BigInteger integ_1=new BigInteger("1011101",2);
//		BigInteger integ_2=new BigInteger("0100100",2);
//		int HM_dis=integ_1.xor(integ_2).bitCount();
//		System.out.println("HM_dis: "+HM_dis);
//		//hamming distance 
//		integ_1=new BigInteger("3c8tqzeqgbrwc",RADIX);
////		String str="3c8tqzeqgbrwc";
//		byte[] integ_1_byte=integ_1.toByteArray(); //str.getBytes();//integ_1.toByteArray();
//		System.out.println("integ_1_byte.length:"+integ_1_byte.length+", "+integ_1_byte[8]);
//		BigInteger integ_1_br=new BigInteger(integ_1_byte);
//		System.out.println(integ_1_br+": \t"+StrleftPad(integ_1_br.toString(2),0,65,"0"));
//
//		System.out.println(integ_1+": \t"+StrleftPad(integ_1.toString(2),0,65,"0"));
//		integ_2=new BigInteger("1d3bhd21q7rtr",RADIX);
//		System.out.println(integ_2+": \t"+StrleftPad(integ_2.toString(2),0,65,"0"));
//		HM_dis=integ_1.xor(integ_2).bitCount();
//		System.out.println("HM_dis: "+HM_dis);
//		//String to binary
//		String git="foo?/n";
//		byte[] bytes=git.getBytes("UTF-8");
//		for (byte b : bytes){
//			int b_int=b;
//			System.out.println(Integer.toBinaryString(b_int));
//		}
		
//		BitSet HESig1=new BitSet(bitNum); 
//		System.out.println("HESig1.size():"+HESig1.size());
//		for(int i=0;i<bitNum;i++){
//			HESig1.set(i);
//		}
//		System.out.println(HESig1.toString());
//		BitSet HESig2=new BitSet(bitNum); 
//		System.out.println("HESig2.size():"+HESig2.size());
//		for(int i=0;i<bitNum;i=i+25){
//			HESig2.set(i);
//		}
//		System.out.println("HESig2.length():"+HESig2.length());
//		System.out.println("HESig2.toString(): "+HESig2.toString());
//		byte[] kk=BitSettoByteArray(HESig2);
//		System.out.println("byte[] kk=BitSettoByteArray(HESig2); kk.length: "+kk.length);
//		BitSet jj=ByteArraytoBitSet(kk);
//		System.out.println("jj=ByteArraytoBitSet(kk), jj.size(): "+jj.size());
//		System.out.println("jj.length(): "+jj.length());
//		System.out.println("jj.toString(): "+jj.toString());
//		
//		
		
//		System.out.println(General.calculateGeoDistance(37.109, 100.108, 37.1, 100.1, "GreatCircle"));
//		double dou=37.08;
//		int kk=(int) (dou*10);
//		System.out.println("dou:"+dou+", kk:"+kk);
//		
//		int[] binsForGroupMAP={0,2,4};// grounTSize bins
//		int obj=5;
//		int binIndex=General.getBinIndex(binsForGroupMAP,obj);
//		System.out.println("obj:"+obj+", binIndex:"+binIndex);
//		
//		double[] latlon={37.10925825, 100.10835636};
//		double[] cartesian=latlonToCartesian (latlon[0], latlon[1]);
//		float[] latlon_re=CartesianTolatlon (cartesian);
//		System.out.println("latlon:"+latlon[0]+", "+latlon[1]+", recovered:"+latlon_re[0]+", "+latlon_re[1]);
		
//		System.out.println(new DecimalFormat("0.00").format((double)((1112339999-102123888)/1000/60)/60)+"hrs");
//		System.out.println((double)3185259/1000/1000);
//		System.out.println(2.0002);
//		System.out.println(0.0002);
		
//		HashMap<Integer,Float> hashMap=new HashMap<Integer,Float>();
//		hashMap.put(1, (float) 0.1);hashMap.put(2, (float) 0.2);
//		ArrayList<Integer> keylist=new ArrayList<Integer>(hashMap.size());
//		ArrayList<Float> valuelist=new ArrayList<Float>(hashMap.size());
//		HashMap_to_ArrayList( hashMap, keylist, valuelist);
//		System.out.println(hashMap);
//		System.out.println(keylist);
//		System.out.println(valuelist);
		
//		//** test dispPhotos**//
//		String PhotoOriPath_3MFlickr="O:\\MediaEval_3185258Images\\trainImages_1-3185258\\";
//		int saveInterval=100*1000; int total_photos=3185258;  
//		int photoNum=10; 
//		ArrayList<String[]> photoPaths_list=new ArrayList<String[]>(); ArrayList<String[]> photoDiscrptions_list=new ArrayList<String[]>();
//		String[] photoPaths=new String[photoNum]; String[] photoDiscrptions=new String[photoNum];
//		for(int i=0;i<photoNum;i++){
//			int PhotoIndex=i; 
//			String folder=(PhotoIndex/saveInterval*saveInterval+1)+"-"+(PhotoIndex/saveInterval+1)*saveInterval;
//			String filename=PhotoIndex+"_"+total_photos+".jpg";
//			String photoPath=PhotoOriPath_3MFlickr+folder+"\\"+filename;
//			photoPaths[i]=photoPath;
//			photoDiscrptions[i]="rank-"+i;
//		}
//		for(int i=0;i<10;i++){
//			photoPaths_list.add(photoPaths); photoDiscrptions_list.add(photoDiscrptions);
//			photoPaths_list.add(photoPaths); photoDiscrptions_list.add(photoDiscrptions);
//		}
//		General.dispPhotos(photoPaths_list, photoDiscrptions_list, 100, 100);
//		System.out.println("done!");
    	
//    	//test random
//    	int seed1=123456789; int seed2=1234567;  int maxValue=100;
//    	Random rand1=new Random(); Random rand2=new Random();
//    	rand1.setSeed(seed1);
//    	System.out.println(rand1.nextInt(maxValue));
//    	rand1.setSeed(seed2);
//    	System.out.println(rand1.nextInt(maxValue));
//    	rand2.setSeed(seed1);
//    	System.out.println(rand2.nextInt(maxValue));
//    	
//    	ArrayList<Integer> rankListLength=new ArrayList<Integer>();
//		rankListLength.add(1);rankListLength.add(2);
//		System.out.println(rankListLength);
 	
//    	System.out.println(createArrayList(0,10));
		
//    	double[] tt=new double[10*13 * 1000*1000];
//    	System.out.println(10);
    	
//    	//test boolean
//    	boolean[] temp=new boolean[2];
//    	for (int i = 0; i < temp.length; i++) {
//			System.out.println(temp[i]);
//		}
//    	//test rand
//    	Random rand=new Random();
//    	System.out.println(randSelect(rand, 10, 5)); 
//    	
//    	//test HashSet<int[]>
//    	HashSet<int[]> hashSet=new HashSet<int[]>();
//    	int[] a1={0,1,2,3}; int[] a2={2,3,5,6}; int[] a3={0,1,2,3};
//    	hashSet.add(a1); hashSet.add(a2); hashSet.add(a3);
//    	System.out.println(hashSet);
//    	
//    	//test .split("\\.")
//    	String tt="class_0_music.trainFeat";
//    	String[] kk=tt.split("\\.");
//    	System.out.println(kk[0]);
//    	
//    	//dispTimeDate
//    	System.out.println(dispTimeDate(System.currentTimeMillis(),"yyyy.MM.dd G 'at' HH:mm:ss z"));
//    	
//    	int[][] mm={{1,2},{3,4},{1,4}};
//    	float[][] norm=General.normBySum_IntArrArr(mm, 1);
//    	System.out.println(General.floatArrToString(norm[2], ",","0.00"));
    	
//    	System.out.println(listFilesInfo(new File("O:/ICMR2013/GVR/3M/GVR_VisConceptThr0.9_D3M_Q100K_HD12_topDoc200_ori/"),-1));
    	
//    	int[] ratio={333,23,25};
//    	System.out.println(General.IntArrToString(assign_by_ratio(ratio,10000),"_"));
    	
//    	boolean[] nonzero = new boolean[10];
//    	System.out.println(nonzero[0]);
    	
//    	//test makeRange
//    	int[] stup={-10,2,5};
//    	System.out.println(General.IntArrToString(makeRange(stup),"_"));
//    	float[] stp={0,1,(float) 0.1};
//    	System.out.println(General.floatArrToString(makeRange(stp),"_","0.0"));
    	
    	//showPhoto_inHTML
//    	PrintWriter html = new PrintWriter(new OutputStreamWriter(new FileOutputStream("O:/MediaEval_3185258Images/test_showPhoto_inHTML.html",false), "UTF-8"),true);
//
//    	String HtmlTitle="for test showPhoto_inHTML";
//    	int htmlSentenceType=-1;
//    	String imageBasePath="O:/MediaEval_3185258Images/trainImages_1-3185258/";
//		int saveInterval=100*1000; int total_photos=3185258;  
//		int photoNum=20; 
//		ArrayList<String> photoPaths_list=new ArrayList<String>(); ArrayList<String> photoDiscrptions_list=new ArrayList<String>(); ArrayList<String> photoColor_lists=new ArrayList<String>();
//		ArrayList<ArrayList<String>> photoNeigh_list=new ArrayList<ArrayList<String>>(); ArrayList<ArrayList<String>> phoNeighDiscrptions_list=new ArrayList<ArrayList<String>>(); ArrayList<ArrayList<String>> phoNeighColor_list=new ArrayList<ArrayList<String>>();
//		for(int i=10;i<photoNum;i++){
//			int PhotoIndex=i; 
//			String folder=(PhotoIndex/saveInterval*saveInterval+1)+"-"+(PhotoIndex/saveInterval+1)*saveInterval;
//			String filename=PhotoIndex+"_"+total_photos+".jpg";
//			photoPaths_list.add(folder+"/"+filename);
//			photoDiscrptions_list.add("rank-"+i);
//			photoColor_lists.add("rgb("+General.floatArrToString_nolastDelimiter(General.mul(Color.red.getRGBColorComponents(null), 255), ",", "0")+")");
//		}
//		for(int i=0;i<photoPaths_list.size();i++){
//			photoNeigh_list.add(photoPaths_list);
//			phoNeighDiscrptions_list.add(photoDiscrptions_list);
//			phoNeighColor_list.add(photoColor_lists);
//		}
//    	showPhoto_inHTML( html,  HtmlTitle, HtmlTitle, htmlSentenceType,  imageBasePath,  
//    			photoPaths_list.get(0), "query", photoColor_lists.get(0), 
//    			photoPaths_list, photoDiscrptions_list, photoColor_lists, 3, 
//    			photoPaths_list, photoDiscrptions_list, photoColor_lists,
//    			photoNeigh_list, phoNeighDiscrptions_list, phoNeighColor_list, 4);
    	
//    	//test IntArrArrListToString
//    	ArrayList<int[]> intArrArrList=new ArrayList<int[]>();
//    	intArrArrList.add(new int[]{0,0});
//    	intArrArrList.add(new int[]{1,1});
//    	System.out.println( IntArrArrListToString(intArrArrList, ",", "_"));
    	
//    	//test int_to_byteArr
//    	byte[] byteArr=int_to_byteArr(886456);
//    	System.out.println(byteArr_to_int(byteArr,0,byteArr.length));
//    	System.out.println(byteArr_to_short(short_to_byteArr((short) 0), 0,byteArr.length));
    	
//    	//test random
//    	int numPartitions=10;
//    	Random rand= new Random();	
//    	int randNum=100; int[] randV=new int[randNum];
//    	for (int i = 0; i < randNum; i++) {
//    		randV[i]=rand.nextInt(numPartitions);
//		}
//    	System.out.println("randV:"+General.IntArrToString(randV, "_"));
//    	randV=new int[randNum];
//    	for (int i = 0; i < randNum; i++) {
//    		rand.setSeed(i);
//    		randV[i]=rand.nextInt(numPartitions);
//		}
//    	System.out.println("randV:"+General.IntArrToString(randV, "_"));
    	
//    	//test intArr
//    	int[] t=new int[]{0,1,2};
//    	int[][] tt=new int[][]{t};
//    	System.out.println(tt.length+", tt[0]:"+IntArrToString(tt[0], "_"));
    	
//    	//test memoryInfo
//    	System.out.println(memoryInfo());
//    	int[][] intArrArr=new int[1][10*1000*1000];
//    	System.out.println(memoryInfo());
    	
    	
//    	System.out.println(new HashSet<>(randSelMSamples(new Random(2), 10000, 10000000)).size());
    	
//    	System.out.println(isOrdered(new Integer[]{6,2,6}, false));
    	
    	Integer[] t0=new Integer[]{0,1,2,6,}; 
    	Integer[] t1=new Integer[]{6,2,6};
    	System.out.println(isSameArr(t0,t1));
    	
	}

}
