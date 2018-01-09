package BuildRank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import net.semanticmetadata.lire.imageanalysis.LireFeature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyAPI.General.General;
import MyAPI.General.General_EJML;
import MyAPI.General.General_Hadoop;
import MyAPI.General.General_IR;
import MyAPI.General.General_Lire;
import MyCustomedHaoop.ValueClass.FloatArr;
import MyCustomedHaoop.ValueClass.IntList_FloatList;
import MyCustomedHaoop.Mapper.SelectSamples;
import MyCustomedHaoop.Partitioner.Partitioner_random;
import MyCustomedHaoop.Reducer.Reducer_InOut.Reducer_InOut_1key_1value;


public class MapRed_BuildRank_linearSearch_loadTrainInReducer extends Configured implements Tool{

	/**  loadTrainInReducer, use more memory than loadTestInReducer, but score doc and rank doc in one process, save space
	 * 	this is suitable for training set can fit into memory!
	 * 
	 * rank candidate photos for test samples
	 * 
	 * job1_1: group train sample feat, make into one seqfile
	 * mapper: 	read, out
	 * reducer: read ,out
	 * 
	 * job1_2: group test sample feat, score docs, and make Rank
	 * mapper: 	read, select and out
	 * reducer: load all training samples, score them, and build rank for each query, save to SeqFile, [testID]_[docIDs]
	 * @param  "mapred.SelQuerys" "mapred.topRank" "mapred.trainSamNum"
	 * 
	 * job2: combine all feat rank together, save into mapfile
	 * mapper: 	read,  and out
	 * reducer: save to mapfile, [testID]_list[docIDs]
	 * @param  "mapred.featNum" 
	 * 
	 * @throws Exception 
	 * @command_example: 
	 * 
	 * global feats: 	hadoop jar MapRed_BuildRank_linearSearch.jar BuildRank.MapRed_BuildRank_linearSearch -libjars mahout-core-0.8-SNAPSHOT-job.jar,lire136.jar,EJML_boof0.9.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.SelQuerys=ICMR2013/Querys_100K_LtoS_from_D3M_ICMR2013.hashMap -Dmapred.topRank=10000 -Dmapred.trainSamNum=3000000 3M_PhoFeats_SeqFiles/ ImageR/GlobalFeatBased/ 1000 
	 * 					hadoop jar MapRed_BuildRank_linearSearch.jar BuildRank.MapRed_BuildRank_linearSearch -libjars mahout-core-0.8-SNAPSHOT-job.jar,lire136.jar,EJML_boof0.9.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.SelQuerys=MediaEval13/MEval13_S_to_S_test.hashMap -Dmapred.topRank=10000 -Dmapred.trainSamNum=9000000 MediaEval13/Global_PhoFeats_Seqs/ MediaEval13/ranks/ 4000 
	 * concept feat:	hadoop jar MapRed_BuildRank_linearSearch.jar BuildRank.MapRed_BuildRank_linearSearch -libjars mahout-core-0.8-SNAPSHOT-job.jar,lire136.jar,EJML_boof0.9.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.SelQuerys=ICMR2013/Querys_100K_LtoS_from_D3M_ICMR2013.hashMap -Dmapred.topRank=10000 -Dmapred.trainSamNum=3000000 ConceptDetection/conceptScores/3M_conceptFeat_MapFile/part-r-00000/data ImageR/GlobalFeatBased/ 180 
	 */
	
	public static void main(String[] args) throws Exception {
		//run hadoop
		int ret = ToolRunner.run(new MapRed_BuildRank_linearSearch_loadTrainInReducer(), args);
		System.exit(ret);	 
		//check data
//		checkDataMemory();
	}

	public static void checkDataMemory() throws IOException, InterruptedException{
		Configuration conf = new Configuration();
		FileSystem hdfs=FileSystem.get(conf);
		//**** load trainFeats from distributed cache ************//
		long startTime=System.currentTimeMillis(); //startTime
		SequenceFile.Reader SeqFileReader = new SequenceFile.Reader(hdfs,  new Path("Q:/Lire/temp_trainSampFeat_SeqFile_CEDD/part-r-00000"), conf);
		IntWritable key=new IntWritable();
		BytesWritable value=new BytesWritable();
		int trainSamNum=3000*1000;
		ArrayList<Integer> train_IDs=new ArrayList<Integer>(trainSamNum); ArrayList<LireFeature> train_feats=new ArrayList<LireFeature>(trainSamNum);
		while (SeqFileReader.next(key, value)) {
			train_IDs.add(key.get());
			byte[] content_byteArr_r=new byte[value.getLength()];
	        System.arraycopy(value.getBytes(), 0, content_byteArr_r, 0, value.getLength()); //The data in value is only valid between 0 and getLength() - 1.!! when call getBytes() , it will return the byte[] by 1.5*ori_size,
	        LireFeature feature=null;
			try {
				feature = (LireFeature) Class.forName("net.semanticmetadata.lire.imageanalysis." + "CEDD").newInstance();
			} catch (InstantiationException e) {
				e.printStackTrace();
				throw new InterruptedException("InstantiationException");
			} catch (IllegalAccessException e) {
				e.printStackTrace();
				throw new InterruptedException("IllegalAccessException");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				throw new InterruptedException("ClassNotFoundException");
			}
	        feature.setByteArrayRepresentation(content_byteArr_r);
	        train_feats.add(feature);
		}
		SeqFileReader.close();
		System.out.println("load train feats finished! num:"+train_IDs.size()+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "s"));
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem hdfs=FileSystem.get(conf);
		String homePath="hdfs://head02.hathi.surfsara.nl/user/yliu/"; //"hdfs://p-head03.alley.sara.nl/user/yliu/";
		String[] otherArgs = args; //use this to parse args!
		ArrayList<String> cacheFilePaths=new ArrayList<String>();
		
		//**********  run for lire global feats  ***************
		String[] classArray = {"CEDD", "EdgeHistogram", "FCTH", "ColorLayout", "PHOG", "JCD", "Gabor", "JpegCoefficientHistogram", "Tamura", "LuminanceLayout", "OpponentHistogram", "ScalableColor"};
		for (int i=5; i<6;i++) {
			//loop config
			String targetFeatClassName=classArray[i];
			conf.set("mapred.targetFeatClassName", targetFeatClassName);
			conf.set("mapred.targetFeatClassIndex",i+"");
			String loopLabel="_"+i+"_"+targetFeatClassName;
			
			ArrayList<Path> imagePaths=new ArrayList<Path>();
			imagePaths.add(new Path(otherArgs[0]+targetFeatClassName)); //otherArgs[0]: 3M_PhoFeats_SeqFiles/
			
			//***** job1_1: group train sample feat, save into one sequence file ***//
			String trainFeat=otherArgs[1]+"temp_trainSampFeat_SeqFile_"+i+"_"+targetFeatClassName;
			General_Hadoop.Job(conf, imagePaths.toArray(new Path[0]), trainFeat, "getTrainFeat"+loopLabel, 1, 8, 2, true,
					MapRed_BuildRank_linearSearch_loadTrainInReducer.class, null, null,null, null,Reducer_InOut_1key_1value.class,
					IntWritable.class, BytesWritable.class, IntWritable.class,BytesWritable.class,
					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0, 0,
					null,null);
				
			//***** job1_2: score docs, and make Rank ***//
			SelectSamples selectSamples=new SelectSamples(conf.get("mapred.SelQuerys"), false);
			cacheFilePaths.clear();
			selectSamples.addDistriCache_SelectSamples(cacheFilePaths);; //SelSamples path with symLink
			cacheFilePaths.add(homePath+trainFeat+"/part-r-00000"+"#trainFeat");
			int job1_2RedNum=Integer.valueOf(otherArgs[2]); //reducer number
			String rankPath_temp=otherArgs[1]+"temp_rank_top"+conf.get("mapred.topRank")+"_"+i+"_"+targetFeatClassName;
			General_Hadoop.Job(conf, imagePaths.toArray(new Path[0]), rankPath_temp, "scoreDocs&rank"+loopLabel, job1_2RedNum, 8, 2, true,
					MapRed_BuildRank_linearSearch_loadTrainInReducer.class, selectSamples.getMapper(), Partitioner_random.class,null, null,Reducer_scoreDocs_buildRank_globalFeat.class,
					IntWritable.class, BytesWritable.class, IntWritable.class,IntList_FloatList.class,
					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0, 0,
					cacheFilePaths.toArray(new String[0]),null);
			
			//******* job1_3: combine into one mapfile ******
			String rankPath=otherArgs[1]+"rank_top"+conf.get("mapred.topRank")+"_"+i+"_"+targetFeatClassName;
			ArrayList<Path> rankPaths=new ArrayList<Path>();
			rankPaths.add(new Path(rankPath_temp));
			General_Hadoop.Job(conf, rankPaths.toArray(new Path[0]), rankPath, "combine_ranks_"+loopLabel, 1, 8, 2, true,
					MapRed_BuildRank_linearSearch_loadTrainInReducer.class, null, null,null, null,Reducer_InOut_1key_1value.class,
					IntWritable.class, IntList_FloatList.class, IntWritable.class,IntList_FloatList.class,
					SequenceFileInputFormat.class, MapFileOutputFormat.class, 0, 10,
					null,null);
			
			//********* clean-up ***********//
			hdfs.delete(new Path(trainFeat), true);
			hdfs.delete(new Path(rankPath_temp), true);
		}
		
		
//		//**********  run for concept feats  ***************
//		int featIndForConcept=100;
//		conf.set("mapred.targetFeatClassIndex",featIndForConcept+"");
//		
//		ArrayList<Path> imagePaths=new ArrayList<Path>();
//		imagePaths.add(new Path(otherArgs[0])); //otherArgs[0]: ConceptDetection/conceptScores/3M_conceptFeat_MapFile/part-r-00000/data
//		
//		//***** job1_1: group train sample feat, save into one sequence file ***//
//		cacheFilePaths.clear();
//		cacheFilePaths.add(homePath+conf.get("mapred.selectPhotosPath")+"#SelSamples.file"); //SelSamples path with symLink
//		String trainFeat=otherArgs[1]+"temp_trainSampFeat_SeqFile_"+featIndForConcept+"_conceptFeat";
//		General_Hadoop.Job(conf, imagePaths.toArray(new Path[0]), trainFeat, "getTrainFeat", 1, 8, 2,
//				MapRed_BuildRank_linearSearch.class, null, null,null,Reducer_InOut_1key_1value.class,
//				IntWritable.class, floatArr.class, IntWritable.class,floatArr.class,
//				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0, 0,
//				cacheFilePaths.toArray(new String[0]),null);
//			
//		//***** job1_2: score docs, and make Rank ***//
//		cacheFilePaths.clear();
//		cacheFilePaths.add(homePath+conf.get("mapred.SelQuerys")+"#SelSamples.file"); //SelSamples path with symLink
//		cacheFilePaths.add(homePath+trainFeat+"/part-r-00000"+"#trainFeat");
//		int job1_2RedNum=Integer.valueOf(otherArgs[2]); //reducer number
//		String rankPath_temp=otherArgs[1]+"temp_rank_top"+conf.get("mapred.topRank")+"_"+featIndForConcept+"_conceptFeat";
//		General_Hadoop.Job(conf, imagePaths.toArray(new Path[0]), rankPath_temp, "scoreDocs&rank", job1_2RedNum, 2, 2,
//				MapRed_BuildRank_linearSearch.class, Mapper_selectSamples_hashMap.class, Partitioner_random.class,null,Reducer_scoreDocs_buildRank_conceptFeat.class,
//				IntWritable.class, floatArr.class, IntWritable.class,IntList_FloatList.class,
//				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0, 0,
//				cacheFilePaths.toArray(new String[0]),null);
//		
//		
//		//******* job1_3: combine into one mapfile ******
//		String rankPath=otherArgs[1]+"rank_top"+conf.get("mapred.topRank")+"_"+featIndForConcept+"_conceptFeat";
//		ArrayList<Path> rankPaths=new ArrayList<Path>();
//		rankPaths.add(new Path(rankPath_temp));
//		General_Hadoop.Job(conf, rankPaths.toArray(new Path[0]), rankPath, "combine_ranks", 1, 8, 2,
//				MapRed_BuildRank_linearSearch.class, null, null,null,Reducer_InOut_1key_1value.class,
//				IntWritable.class, IntList_FloatList.class, IntWritable.class,IntList_FloatList.class,
//				SequenceFileInputFormat.class, MapFileOutputFormat.class, 0, 10,
//				null,null);
//		
//		//********* clean-up ***********//
//		hdfs.delete(new Path(trainFeat), true);
//		hdfs.delete(new Path(rankPath_temp), true);
		
		hdfs.close();
		return 0;
	}
		
	//****************** lire global feat: job1_2, score docs, and make Rank ************************
	public static class Reducer_scoreDocs_buildRank_globalFeat_fastInMem extends Reducer<IntWritable,BytesWritable,IntWritable,IntList_FloatList>{
		private Configuration conf;
		private boolean disp;
		private int trainSamNum;
		private ArrayList<Integer> train_IDs;
		private byte[][] train_feats;
		String targetFeatClassName;
		private int targetFeatClassIndex;
		private int topRank;
		private int procPhotos;
		private int dispInter;
		private long startTime, endTime0, endTime1;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			startTime=System.currentTimeMillis(); //startTime
			//***** check memory and taskTracker
			System.out.println("mapred.child.java.opts:  "+conf.get("mapred.child.java.opts"));
			System.out.println("mapred.tasktracker.reduce.tasks.maximum:  "+conf.get("mapred.tasktracker.reduce.tasks.maximum"));
			System.out.println("mapred.reduce.max.attempts:  "+conf.get("mapred.reduce.max.attempts"));
			//**** load featClassName ************//
			targetFeatClassName=conf.get("mapred.targetFeatClassName");
			System.out.println("targetFeatClassName: "+targetFeatClassName);
			targetFeatClassIndex=Integer.valueOf(conf.get("mapred.targetFeatClassIndex"));
			System.out.println("targetFeatClassIndex: "+targetFeatClassIndex);
			//**** set topRank ************//
			topRank=Integer.valueOf(conf.get("mapred.topRank"));
			System.out.println("topRank: "+topRank);
			
			//**** load trainFeats from distributed cache ************//
			SequenceFile.Reader SeqFileReader = new SequenceFile.Reader(FileSystem.getLocal(conf),  new Path("trainFeat"), conf);
			IntWritable key=new IntWritable();
			BytesWritable value=new BytesWritable();
			trainSamNum=0;
			while (SeqFileReader.next(key, value)) {
				trainSamNum++;
			}
			SeqFileReader.close();
			train_IDs=new ArrayList<Integer>(trainSamNum+100); train_feats=new byte[trainSamNum][];
			SeqFileReader = new SequenceFile.Reader(FileSystem.getLocal(conf),  new Path("trainFeat"), conf);
			int samInd=0;
			while (SeqFileReader.next(key, value)) {
				train_IDs.add(key.get());
				byte[] content_byteArr_r=new byte[value.getLength()];
		        System.arraycopy(value.getBytes(), 0, content_byteArr_r, 0, value.getLength()); //The data in value is only valid between 0 and getLength() - 1.!! when call getBytes() , it will return the byte[] by 1.5*ori_size,
//		        LireFeature feature=null;
//				try {
//					feature = (LireFeature) Class.forName("net.semanticmetadata.lire.imageanalysis." + targetFeatClassName).newInstance();
//				} catch (InstantiationException e) {
//					e.printStackTrace();
//					throw new InterruptedException("InstantiationException");
//				} catch (IllegalAccessException e) {
//					e.printStackTrace();
//					throw new InterruptedException("IllegalAccessException");
//				} catch (ClassNotFoundException e) {
//					e.printStackTrace();
//					throw new InterruptedException("ClassNotFoundException");
//				}
//		        feature.setByteArrayRepresentation(content_byteArr_r);
		        train_feats[samInd++]=content_byteArr_r;
			}
			SeqFileReader.close();
			System.out.println("load train feats finished! num:"+train_IDs.size()+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "s"));
			
			//set procPhotos
			procPhotos=0;
			//set dispInter
			dispInter=50;
			startTime=System.currentTimeMillis(); //startTime
			
			System.out.println("mapper setup finsihed! \n---------------------------\n");
			disp=true; 
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable key, Iterable<BytesWritable> Feats, Context context) throws IOException, InterruptedException {
			//key: photoName
			//value: Feats, byte[] format, BytesWritable
			//******** only one in Feats! ************	
			int loopNum=0; BytesWritable feat=null;
			for(Iterator<BytesWritable> it=Feats.iterator();it.hasNext();){// loop over all value			
				feat=it.next();
				loopNum++;
			}
			General.Assert(loopNum==1, "error in Reducer_scoreDocs_buildRank! loopNum should == 1, here loopNum="+loopNum+", photoName:"+key.get());
			
			int queryID=key.get();
			byte[] queryfeat_byteArr_r=new byte[feat.getLength()];
	        System.arraycopy(feat.getBytes(), 0, queryfeat_byteArr_r, 0, feat.getLength()); //The data in value is only valid between 0 and getLength() - 1.!! when call getBytes() , it will return the byte[] by 1.5*ori_size,

			procPhotos++;
			
			//score this query for all train samples 
			ArrayList<Float> dists=new ArrayList<Float>(train_IDs.size()+100);
			for (int i = 0; i<train_IDs.size(); i++) {
				float dist;
				dist = (float) General_Lire.getFeatDistance_lire136(train_feats[i],queryfeat_byteArr_r, targetFeatClassName);
				dists.add(dist);
			}
	        if (disp==true){ //show time
				endTime0=System.currentTimeMillis(); //end time 
				System.out.println( "1st query's scoring time for "+train_IDs.size()+" docs, "+ General.dispTime (endTime0-startTime, "ms"));
			}
			//build rank for this query
			ArrayList<Integer> topDocs=new ArrayList<Integer>(topRank+1); ArrayList<Float> topScores=new ArrayList<Float>(topRank);
			General_IR.rank_get_TopDocScores_treeSet(train_IDs, dists, topRank, topDocs, topScores,"ASC");
	        if (disp==true){ //show time
				endTime1=System.currentTimeMillis(); //end time 
				System.out.println( "1st query's ranking time for topRank:"+topRank+" in "+train_IDs.size()+" docs, "+ General.dispTime (endTime1-endTime0, "ms")
						+", for testSample-"+queryID+", out put topDocs:"+topDocs.size()+", last one in topDocs is targetFeatClassIndex !!");
				disp=false;
				System.out.println("disp:"+disp+"\n");
			}
			topDocs.add(targetFeatClassIndex); //last one in topDocs is targetFeatClassIndex !!
			
			//disp state
    		if((procPhotos)%dispInter==0){ 							
				System.out.println( "scoring and ranking in "+train_IDs.size()+" docs, for "+procPhotos
						+" test samples, finished!! ......"+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
			}
    		
    		//output
			context.write(new IntWritable(queryID), new IntList_FloatList(topDocs,topScores));
			
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one Reducer finished! total test samples in this Reducer: "+procPhotos
					+" .....total time: "+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
			super.setup(context);
	 	}
	}
	
	public static class Reducer_scoreDocs_buildRank_globalFeat extends Reducer<IntWritable,BytesWritable,IntWritable,IntList_FloatList>{
		private Configuration conf;
		private boolean disp;
		private SequenceFile.Reader SeqFileReader;
		private ArrayList<Integer> train_IDs;
		private byte[][] train_feats;
		private int trainSamNum;
		String targetFeatClassName;
		private int targetFeatClassIndex;
		private int topRank;
		private int procPhotos;
		private int dispInter;
		private long startTime, endTime0, endTime1;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			startTime=System.currentTimeMillis(); //startTime
			//***** check memory and taskTracker
			System.out.println("mapred.child.java.opts:  "+conf.get("mapred.child.java.opts"));
			System.out.println("mapred.tasktracker.reduce.tasks.maximum:  "+conf.get("mapred.tasktracker.reduce.tasks.maximum"));
			System.out.println("mapred.reduce.max.attempts:  "+conf.get("mapred.reduce.max.attempts"));
			//**** load featClassName ************//
			targetFeatClassName=conf.get("mapred.targetFeatClassName");
			System.out.println("targetFeatClassName: "+targetFeatClassName);
			targetFeatClassIndex=Integer.valueOf(conf.get("mapred.targetFeatClassIndex"));
			System.out.println("targetFeatClassIndex: "+targetFeatClassIndex);
			//**** set topRank ************//
			topRank=Integer.valueOf(conf.get("mapred.topRank"));
			System.out.println("topRank: "+topRank);
			
			//**** load trainFeats from distributed cache ************//
			try {
				//****try to load trainFeats from distributed cache into memory ************//
				SequenceFile.Reader SeqFileReader = new SequenceFile.Reader(FileSystem.getLocal(conf),  new Path("trainFeat"), conf);
				IntWritable key=new IntWritable();
				BytesWritable value=new BytesWritable();
				trainSamNum=0;
				while (SeqFileReader.next(key, value)) {
					trainSamNum++;
				}
				SeqFileReader.close();
				train_IDs=new ArrayList<Integer>(trainSamNum+100); train_feats=new byte[trainSamNum][];
				SeqFileReader = new SequenceFile.Reader(FileSystem.getLocal(conf),  new Path("trainFeat"), conf);
				int samInd=0;
				while (SeqFileReader.next(key, value)) {
					train_IDs.add(key.get());
					byte[] content_byteArr_r=new byte[value.getLength()];
			        System.arraycopy(value.getBytes(), 0, content_byteArr_r, 0, value.getLength()); //The data in value is only valid between 0 and getLength() - 1.!! when call getBytes() , it will return the byte[] by 1.5*ori_size,
//			        LireFeature feature=null;
//					try {
//						feature = (LireFeature) Class.forName("net.semanticmetadata.lire.imageanalysis." + targetFeatClassName).newInstance();
//					} catch (InstantiationException e) {
//						e.printStackTrace();
//						throw new InterruptedException("InstantiationException");
//					} catch (IllegalAccessException e) {
//						e.printStackTrace();
//						throw new InterruptedException("IllegalAccessException");
//					} catch (ClassNotFoundException e) {
//						e.printStackTrace();
//						throw new InterruptedException("ClassNotFoundException");
//					}
//			        feature.setByteArrayRepresentation(content_byteArr_r);
			        train_feats[samInd++]=content_byteArr_r;
				}
				SeqFileReader.close();
				System.out.println("load train feats into memory sucessed! num:"+train_IDs.size()+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "s"));
			} catch (Exception e) {
				System.out.println("load train feats into memory fail, use seqfile reader instead, error-message: "+e.getMessage());
				e.printStackTrace();
				train_IDs=null; train_feats=null;
				//**** cannot read into memory, set SeqFileReader instead ************//
				SeqFileReader = new SequenceFile.Reader(FileSystem.getLocal(conf),  new Path("trainFeat"), conf);
				IntWritable key=new IntWritable();
				BytesWritable value=new BytesWritable();
				trainSamNum=0;
				while (SeqFileReader.next(key, value)) {
					trainSamNum++;
				}
				SeqFileReader.close();
				System.out.println("1 loop of trainFeat seqFile finished! num:"+trainSamNum+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "s"));
			}
			//set procPhotos
			procPhotos=0;
			//set dispInter
			dispInter=10;
			startTime=System.currentTimeMillis(); //startTime
			
			System.out.println("mapper setup finsihed! \n---------------------------\n");
			disp=true; 
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable key, Iterable<BytesWritable> Feats, Context context) throws IOException, InterruptedException {
			//key: photoName
			//value: Feats, byte[] format, BytesWritable
			//******** only one in Feats! ************	
			int loopNum=0; BytesWritable feat=null;
			for(Iterator<BytesWritable> it=Feats.iterator();it.hasNext();){// loop over all value			
				feat=it.next();
				loopNum++;
			}
			General.Assert(loopNum==1, "error in Reducer_scoreDocs_buildRank! loopNum should == 1, here loopNum="+loopNum+", photoName:"+key.get());
			
			int queryID=key.get();
			byte[] queryfeat_byteArr_r=new byte[feat.getLength()];
	        System.arraycopy(feat.getBytes(), 0, queryfeat_byteArr_r, 0, feat.getLength()); //The data in value is only valid between 0 and getLength() - 1.!! when call getBytes() , it will return the byte[] by 1.5*ori_size,

			procPhotos++;
			
			//score this query for all train samples 
			ArrayList<Integer> train_IDs_loc=new ArrayList<Integer>(trainSamNum+100);
			ArrayList<Float> dists=new ArrayList<Float>(trainSamNum+100);
			if (train_IDs!=null) {//in memory approach
				for (int i = 0; i<train_IDs.size(); i++) {
					float dist;
					dist = (float) General_Lire.getFeatDistance_lire136(train_feats[i],queryfeat_byteArr_r, targetFeatClassName);
					dists.add(dist);
				}
				train_IDs_loc=train_IDs;
			}else {//seq file reader approach
				SeqFileReader = new SequenceFile.Reader(FileSystem.getLocal(conf),  new Path("trainFeat"), conf);
				IntWritable key_feat=new IntWritable();
				BytesWritable value_feat=new BytesWritable();
				while (SeqFileReader.next(key_feat, value_feat)) {
					train_IDs_loc.add(key_feat.get());
					byte[] content_byteArr_r=new byte[value_feat.getLength()];
			        System.arraycopy(value_feat.getBytes(), 0, content_byteArr_r, 0, value_feat.getLength()); //The data in value is only valid between 0 and getLength() - 1.!! when call getBytes() , it will return the byte[] by 1.5*ori_size,
			        float dist;
					dist = (float) General_Lire.getFeatDistance_lire136(content_byteArr_r,queryfeat_byteArr_r, targetFeatClassName);
					dists.add(dist);
				}
				SeqFileReader.close();
			}
			
	        if (disp==true){ //show time
				endTime0=System.currentTimeMillis(); //end time 
				System.out.println( "1st query's scoring time for "+train_IDs_loc.size()+" docs, "+ General.dispTime (endTime0-startTime, "ms"));
			}
	        
			//build rank for this query
			ArrayList<Integer> topDocs=new ArrayList<Integer>(topRank+1); ArrayList<Float> topScores=new ArrayList<Float>(topRank);
			General_IR.rank_get_TopDocScores_treeSet(train_IDs_loc, dists, topRank, topDocs, topScores,"ASC");
	        if (disp==true){ //show time
				endTime1=System.currentTimeMillis(); //end time 
				System.out.println( "1st query's ranking time for topRank:"+topRank+" in "+train_IDs_loc.size()+" docs, "+ General.dispTime (endTime1-endTime0, "ms")
						+", for testSample-"+queryID+", out put topDocs:"+topDocs.size()+", last one in topDocs is targetFeatClassIndex !!");
				disp=false;
				System.out.println("disp:"+disp+"\n");
			}
			topDocs.add(targetFeatClassIndex); //last one in topDocs is targetFeatClassIndex !!
			
			//disp state
    		if((procPhotos)%dispInter==0){ 							
				System.out.println( "scoring and ranking in "+train_IDs_loc.size()+" docs, for "+procPhotos
						+" test samples, finished!! ......"+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
			}
    		
    		//output
			context.write(new IntWritable(queryID), new IntList_FloatList(topDocs,topScores));
			
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one Reducer finished! total test samples in this Reducer: "+procPhotos
					+" .....total time: "+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
			super.setup(context);
	 	}
	}
	
	//****************** concept feat: job1_2, score docs, and make Rank ************************
	public static class Reducer_scoreDocs_buildRank_conceptFeat extends Reducer<IntWritable,FloatArr,IntWritable,IntList_FloatList>{
		private Configuration conf;
		private boolean disp;
		private SequenceFile.Reader SeqFileReader;
		private int trainSamNum;
		private int targetFeatClassIndex;
		private int topRank;
		private int procPhotos;
		private int dispInter;
		private long startTime, endTime0, endTime1;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			startTime=System.currentTimeMillis(); //startTime
			
			//**** set targetFeatClassIndex ************//
			targetFeatClassIndex=Integer.valueOf(conf.get("mapred.targetFeatClassIndex"));
			
			//**** set topRank ************//
			topRank=Integer.valueOf(conf.get("mapred.topRank"));
			System.out.println("topRank: "+topRank);
			
			//**** load trainFeats from distributed cache ************//
			SeqFileReader = new SequenceFile.Reader(FileSystem.getLocal(conf),  new Path("trainFeat"), conf);
			IntWritable key=new IntWritable();
			FloatArr value=new FloatArr();
			trainSamNum=0;
			while (SeqFileReader.next(key, value)) {
				trainSamNum++;
			}
			SeqFileReader.close(); 
			System.out.println("1 loop of trainFeat seqFile finished! num:"+trainSamNum+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "s"));
			//set procPhotos
			procPhotos=0;
			//set dispInter
			dispInter=50;
			startTime=System.currentTimeMillis(); //startTime
			
			System.out.println("mapper setup finsihed! \n---------------------------\n");
			disp=true; 
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable key, Iterable<FloatArr> Feats, Context context) throws IOException, InterruptedException {
			//key: photoName
			//value: Feats, byte[] format, BytesWritable
			//******** only one in Feats! ************	
			int loopNum=0; FloatArr feat=null;
			for(Iterator<FloatArr> it=Feats.iterator();it.hasNext();){// loop over all value			
				feat=it.next();
				loopNum++;
			}
			General.Assert(loopNum==1, "error in Reducer_scoreDocs_buildRank! loopNum should == 1, here loopNum="+loopNum+", photoName:"+key.get());
			
			int queryID=key.get();
			float[] queryFeat=feat.getFloatArr();
			
			procPhotos++;
			
			//score this query for all train samples 
			ArrayList<Integer> train_IDs=new ArrayList<Integer>(trainSamNum+100);
			ArrayList<Float> sims=new ArrayList<Float>(trainSamNum+100);
			IntWritable key_feat=new IntWritable();
			FloatArr value_feat=new FloatArr();
			SeqFileReader = new SequenceFile.Reader(FileSystem.getLocal(conf),  new Path("trainFeat"), conf);
			while (SeqFileReader.next(key_feat, value_feat)) {
				train_IDs.add(key_feat.get());
				float conSim=General_EJML.cosSim(value_feat.getFloatArr(), queryFeat);
				sims.add(conSim);
			}
			SeqFileReader.close();
	        if (disp==true){ //show time
				endTime0=System.currentTimeMillis(); //end time 
				System.out.println( "1st query's scoring time for "+trainSamNum+" docs, "+ General.dispTime (endTime0-startTime, "ms"));
			}
			//build rank for this query
			ArrayList<Integer> topDocs=new ArrayList<Integer>(topRank+1); ArrayList<Float> topScores=new ArrayList<Float>(topRank);
			General_IR.rank_get_TopDocScores_treeSet(train_IDs, sims, topRank, topDocs, topScores,"DES");
	        if (disp==true){ //show time
				endTime1=System.currentTimeMillis(); //end time 
				System.out.println( "1st query's ranking time for topRank:"+topRank+" in "+train_IDs.size()+" docs, "+ General.dispTime (endTime1-endTime0, "ms")
						+", for testSample-"+queryID+", out put topDocs:"+topDocs.size()+", last one in topDocs is targetFeatClassIndex !!");
				disp=false;
				System.out.println("disp:"+disp+"\n");
			}
			topDocs.add(targetFeatClassIndex); //last one in topDocs is targetFeatClassIndex !!
			
			//disp state
    		if((procPhotos)%dispInter==0){ 							
				System.out.println( "scoring and ranking in "+train_IDs.size()+" docs, for "+procPhotos
						+" test samples, finished!! ......"+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
			}
    		
    		//output
			context.write(new IntWritable(queryID), new IntList_FloatList(topDocs,topScores));
			
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one Reducer finished! total test samples in this Reducer: "+procPhotos
					+" .....total time: "+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
			super.setup(context);
	 	}
	}

}
