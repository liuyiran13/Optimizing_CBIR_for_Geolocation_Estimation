package MyCustomedHaoop.MapRedFunction;


import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.Obj.KMean_InMemory;
import MyAPI.Obj.Statistics;
import MyAPI.SystemCommand.RenewKerberos;
import MyCustomedHaoop.MapRedFunction.MapRed_indexData.Reducer_index;
import MyCustomedHaoop.Mapper.SelectSamples;
import MyCustomedHaoop.Partitioner.Partitioner_random;
import MyCustomedHaoop.ValueClass.FloatArr;
import MyCustomedHaoop.ValueClass.IntArr_FloatArr;

public class MapRed_KMean{

	/**
	 * job1:  	KMean clustering on input data, data format: IntWritable.class, FloatArr.class
	 * 
	 * support from any loop point to continue!
	 */

	public static void preparData() throws Exception {
		
	}
	
	public static String runHadoop_Single(Configuration conf, Path[] input, String outPut, int k_clusterNum, int maxInterNum) throws Exception {						
		conf.set("centerNum", k_clusterNum+"");
		conf.set("kMean_maxInterNum", maxInterNum+"");
		//*********************** job1: only 1 reduecer, do in memory kmean  ********************
		General_Hadoop.Job(conf, input, outPut, "kmean_Single", 1, 8, 2, true, 
				MapRed_KMean.class, null, null, null, null, Reducer_InMemoryKmeans.class,
				IntWritable.class, FloatArr.class, IntWritable.class, FloatArr.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024*1024L, 0,
				null,null);
		return outPut+"/part-r-00000";	
	}
	
	public static String runHadoop_Parallel(RenewKerberos renewTicket, Configuration conf, Path[] input, String outPut, int k, int maxInterNum, int startLoop, int reducerNum_KMean) throws Exception {		
		FileSystem hdfs=FileSystem.get(conf); 
		ArrayList<String> cacheFilePaths=new ArrayList<String>();
				
		conf.set("centerNum", k+"");
		
		String randomSeedPath=outPut+"random-seed" ;
		if (startLoop==-1) {//mark to start from very beginning: random generate centers
			General.Assert(!General_Hadoop.isExistFile(conf, new Path(outPut)), "err! outPut already exist, please check, outPut:"+outPut);
			hdfs.mkdirs(new Path(outPut));
			//******* job1_1: index data (for random seed) and count dataNum ******
			String indexedData=outPut+"indexedData";
			int dataNum=MapRed_indexData.runHadoop(conf, input, indexedData, IntWritable.class, FloatArr.class);
			conf.set("dataNum", dataNum+"");
			System.out.println("tot dataNum:"+dataNum);
			//******* job1_2: rand select k center ******
			boolean isDuplicated=true;
			while (isDuplicated) {
				String selHashPath=outPut+"temp_randomSeed.hashSet";
				General_Hadoop.deleteIfExist(selHashPath, hdfs);
				General_Hadoop.deleteIfExist(randomSeedPath, hdfs);
				//make random ind
				HashSet<Integer> sellected=new HashSet<Integer>(k*2);
				int[] randInd=null;
				if (dataNum<=100000) {
					System.out.println("use General.randIndex to make randInd");
					randInd=General.randIndex(dataNum);					
					for (int i = 0; i < k; i++) {
						sellected.add(randInd[i]);
					}
				}else {
					System.out.println("dataNum is too large: "+dataNum+", do not use General.randIndex to make randInd, use partition and rand");
					int inter=(dataNum-1)/k; Random rand=new Random();
					for (int i = 0; i < k; i++) {
						sellected.add(i*inter+rand.nextInt(inter));
					}
				}
				General.Assert(sellected.size()==k, "err! number of random selected samples should == "+k+", but: "+sellected.size());
				General_Hadoop.writeObject_HDFS(hdfs, selHashPath, sellected);
				//run
				SelectSamples selectSamples=new SelectSamples(selHashPath,false);
				selectSamples.addDistriCache_SelectSamples(cacheFilePaths); //SelSamples path with symLink
				General_Hadoop.Job(conf, new Path[]{new Path(indexedData)}, randomSeedPath, "randomSeed", 1, 8, 2, true, 
						MapRed_KMean.class, selectSamples.getMapper(), null, null, null,Reducer_index.class,
						IntWritable.class, FloatArr.class, IntWritable.class,FloatArr.class,
						SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 100*1024*1024L, 0,
						cacheFilePaths.toArray(new String[0]),null);
				//check duplicated centers
				isDuplicated=checkRandomSeed_isDuplicated(k, conf, randomSeedPath+"/part-r-00000",randInd,(float) 0.001);
				System.out.println("isDuplicated:"+isDuplicated);
			}
			startLoop=0;
		}else {
			//******* job1_1: count dataNum ******
			int dataNum=MapRed_countDataNum.runHadoop(conf, input, outPut);
			conf.set("dataNum", dataNum+"");
		}
		
		
		for (int loop_i = startLoop; loop_i < maxInterNum; loop_i++) {
			renewTicket.renewTicket(true);
			String centerPath=loop_i==0?randomSeedPath+"/part-r-00000":outPut+"loop-"+(loop_i-1)+"/part-r-00000";
			//************ job2_1  ********************
			String out_job_loop_temp=outPut+"loop-"+loop_i+"_temp";
			hdfs.delete(new Path(out_job_loop_temp), true);//delete old job folder, for continue job
			//Distributed cache, add VWPath, pMatrixPath, HEThresholdPath, middleNode, nodeLink_learned, selectPhotosPath
			cacheFilePaths.clear();
			cacheFilePaths.add(centerPath+"#centers.file"); //centers path with symLink
			General_Hadoop.Job(conf, input, out_job_loop_temp, "findCenter-"+loop_i, reducerNum_KMean, 8, 2, true, 
					MapRed_KMean.class, null, Partitioner_random.class, null, null,Reducer_findCenter.class,
					IntWritable.class, FloatArr.class, IntWritable.class,IntArr_FloatArr.class,
					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024*1024L, 0,
					cacheFilePaths.toArray(new String[0]),null);
			//************ job2_2  ********************
			String out_job_loop=outPut+"loop-"+loop_i;
			hdfs.delete(new Path(out_job_loop), true);//delete old job folder, for continue job
			//Distributed cache, add VWPath, pMatrixPath, HEThresholdPath, middleNode, nodeLink_learned, selectPhotosPath
			cacheFilePaths.clear();
			cacheFilePaths.add(centerPath+"#centers.file"); //centers path with symLink
			General_Hadoop.Job(conf, new Path[]{new Path(out_job_loop_temp)}, out_job_loop, "getCenter-"+loop_i, 1, 8, 2, true, 
					MapRed_KMean.class, null, null, null, null,Reducer_combineCenters.class,
					IntWritable.class, IntArr_FloatArr.class, IntWritable.class,FloatArr.class,
					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024*1024L, 0,
					cacheFilePaths.toArray(new String[0]),null);
			//clean-up
			hdfs.delete(new Path(out_job_loop_temp), true);
		}

		return outPut+"loop-"+(maxInterNum-1)+"/part-r-00000";	
	}

	private static boolean checkRandomSeed_isDuplicated(int k, Configuration conf, String randomSeedPath, int[] randInd, float tollerance) throws IOException, InterruptedException {
		//check whether have duplicated centers 
		float[][] centers=General_Hadoop.readMatrixFromSeq_floatMatrix(conf, new Path(randomSeedPath));
		System.out.println("randomSeedPath: "+randomSeedPath+", centers==null: "+centers==null);
		General.Assert(centers.length==k, "err! random generated center num should be: "+k+", but "+centers.length);
		boolean isDuplicated=General.isDuplicated_Rows(centers, randInd, tollerance, true);
		return isDuplicated;
	}
	
	//******** job2_1 **************	
	public static class Reducer_findCenter extends Reducer<IntWritable,FloatArr,IntWritable,IntArr_FloatArr>{
		
		private float[][] centers;
		private HashMap<Integer, float[]> clusters;
		private int[] cluster_size;
		
		private double dataNumPerReducer;
		private int procSamples;
		private long startTime;
		private int dispInter;
		private boolean disp;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf=context.getConfiguration();
			//read center into memory
			System.out.println("before read center, memory:"+General.memoryInfo());
			centers=General_Hadoop.readMatrixFromSeq_floatMatrix(conf, General_Hadoop.getLocalPath("centers.file", conf));
			System.out.println("center-number: "+centers.length+", after read centers, memory:"+General.memoryInfo());
			//initial clusters
			clusters=new HashMap<Integer, float[]>();
			cluster_size=new int[centers.length];
			//get dataNumPerReducer
			int dataNum=Integer.valueOf(conf.get("dataNum"));
			System.out.println("total dataNum:"+dataNum);
			int ReducerNum=Integer.valueOf(conf.get("mapreduce.job.reduces"));
			dataNumPerReducer=dataNum/ReducerNum;
			System.out.println("ReducerNum:"+ReducerNum+", on average, data num per reducer:"+new DecimalFormat("0.0").format(dataNumPerReducer));
			// ***** setup finished ***//
			dispInter=1000; 
			disp=false;
			startTime=System.currentTimeMillis();
			procSamples=0;
			System.out.println("Reducer_findCenter setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		protected void reduce(IntWritable key, Iterable<FloatArr> values, Context context) throws IOException, InterruptedException {
			//key: featInd
			//value: feat
			//******** key-featInd maybe duplicated, not care the ID ************		
			for(Iterator<FloatArr> it=values.iterator();it.hasNext();){// loop over all HashMaps		
				//update statistics
				procSamples++;
				if (procSamples%dispInter==0){ //debug disp info
					disp=true;
				}
				//get Feat
				FloatArr oneSample=it.next();
				//key: notcare, value: featArr
				float[] oneFeat=oneSample.getFloatArr();
				//find center
				int centerInd=General.assignFeatToCenter(oneFeat, centers);
				//update center
				float[] oneCl=clusters.get(centerInd);
				if (oneCl==null) {
					clusters.put(centerInd, oneFeat);
					General.dispInfo_ifNeed(disp, "\n","current sample is assign to cluster:"+centerInd+", sampleFeat:"+General.floatArrToString(General.selectArrFloat(oneFeat, null, 10), ", ", "0.0")+"\n"
							+"this cluster's old featSum:null, current feat is the first one assigned to this cluster! \n"
							+"this cluster's new featSum:"+General.floatArrToString(General.selectArrFloat(clusters.get(centerInd), null, 10), ", ", "0.0"));
				}else {
					General.dispInfo_ifNeed(disp, "\n","current sample is assign to cluster:"+centerInd+", sampleFeat:"+General.floatArrToString(General.selectArrFloat(oneFeat, null, 10), ", ", "0.0")+"\n"
							+"this cluster's old featSum:"+General.floatArrToString(General.selectArrFloat(clusters.get(centerInd), null, 10), ", ", "0.0"));
					General.addFloatArr(oneCl, oneFeat);
					General.dispInfo_ifNeed(disp, "", "this cluster's new featSum:"+General.floatArrToString(General.selectArrFloat(clusters.get(centerInd), null, 10), ", ", "0.0"));
				}
				cluster_size[centerInd]++;
				if (disp) {
					System.out.println(procSamples+" procSamples finsihed!..."+General.dispTime(System.currentTimeMillis()-startTime, "min"));
					int estimatedComputeTime_min=(int) (dataNumPerReducer/procSamples*(System.currentTimeMillis()-startTime)/1000/60);
					System.out.println("estimatedComputeTime_min:"+estimatedComputeTime_min);
					System.out.println("clusters number:"+clusters.size());
					disp=false;
				}
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one Reducer finished! total Samples in this Reducer: "+procSamples);
			System.out.println("clusters num:"+clusters.size());
			//** output, set key, value **//
			for (Entry<Integer, float[]> one:clusters.entrySet()) {
				context.write(new IntWritable(one.getKey()), new IntArr_FloatArr(new int[]{cluster_size[one.getKey()]},one.getValue()));
			}
			super.setup(context);
	 	}
	}

	public static class Reducer_combineCenters extends Reducer<IntWritable,IntArr_FloatArr,IntWritable,FloatArr>{

		int reduceNum;
		int centerNum;
		private Statistics<Integer> stat;
		private long startTime;
		private int dispInter;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf=context.getConfiguration();
			centerNum=Integer.valueOf(conf.get("centerNum"));
			reduceNum=0;
			stat=new Statistics<>(100);
			System.out.println("setup finsihed!");
			dispInter=10*1000; 
			startTime=System.currentTimeMillis();
			super.setup(context);
	 	}
		
		@Override
		protected void reduce(IntWritable key, Iterable<IntArr_FloatArr> value, Context context) throws IOException, InterruptedException {
			//key: cluserInd
			//value: file content
			//******** only one in value! ************	
			int loopNum=0; float[] newCenter=null; int clusterSize=0;
			for(Iterator<IntArr_FloatArr> it=value.iterator();it.hasNext();){// loop over all BufferedImage_jpg				
				IntArr_FloatArr one=it.next();
				if (loopNum==0) {
					newCenter=one.getFloatArr();
				}else {
					General.addFloatArr(newCenter, one.getFloatArr());
				}
				clusterSize+=one.getIntArr()[0];
				loopNum++;
			}
			General.elementDiv(newCenter, clusterSize);
			context.write(key, new FloatArr(newCenter));
			stat.addSample(clusterSize, key.get());
			
			if (reduceNum%dispInter==0){ //debug disp info
				System.out.println();
				System.out.println(reduceNum+" reduce finsihed!..."+General.dispTime(System.currentTimeMillis()-startTime, "min"));
				System.out.println("current cluster is "+key+", loopNum: "+loopNum+", this cluster size:"+clusterSize);
			}
			reduceNum++;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			General.Assert(reduceNum==centerNum, "err in Reducer_combineCenters: reduceNum should ==centerNum:"+centerNum+", but "+reduceNum+"\n"
							+"if this happens in the first loop: loop-0, this maybe because some centers are the same when random select centers, if this run random seed again.");
			System.out.println("one reducer finished! reduceNum: "+reduceNum+", should ==centerNum:"+centerNum);
			System.out.println("statistics of vw-cluster size: "+stat.getFullStatistics("0", true));
			super.setup(context);
	 	}
	}
	
	public static class Reducer_InMemoryKmeans extends Reducer<IntWritable,FloatArr,IntWritable,FloatArr>{

		KMean_InMemory kmean;
		int k_clusterNum;
		int maxInterNum;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf=context.getConfiguration();
			kmean=new KMean_InMemory(null, true);
			k_clusterNum=Integer.valueOf(conf.get("centerNum"));
			maxInterNum=Integer.valueOf(conf.get("kMean_maxInterNum"));
			System.out.println("k_clusterNum:"+k_clusterNum+", maxInterNum:"+maxInterNum);
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		protected void reduce(IntWritable key, Iterable<FloatArr> values, Context context) throws IOException, InterruptedException {
			//******** key-featInd maybe duplicated, not care the ID ************		
			for(Iterator<FloatArr> it=values.iterator();it.hasNext();){// loop over all HashMaps				
				FloatArr oneSample=it.next();
				//key: notcare, value: featArr
				float[] oneFeat=oneSample.getFloatArr();
				kmean.addDataSample(oneFeat);
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("add all Data finsihed! now start kmean!");
			float[][] centers= kmean.makeRes(k_clusterNum, maxInterNum, null, context);
			for (int i = 0; i < centers.length; i++) {
				context.write(new IntWritable(i), new FloatArr(centers[i]));
			}
			System.out.println("done! center num: "+centers.length);
			super.setup(context);
	 	}
	}
}
