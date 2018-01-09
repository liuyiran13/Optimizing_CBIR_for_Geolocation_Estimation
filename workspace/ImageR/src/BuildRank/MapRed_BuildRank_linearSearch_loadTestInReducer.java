package BuildRank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.General.General_IR;
import MyAPI.General.ComparableCls.slave_masterFloat_ASC;
import MyCustomedHaoop.ValueClass.FloatArr;
import MyCustomedHaoop.ValueClass.IntList_FloatList;
import MyCustomedHaoop.Combiner.Combiner_combine_IntList_FloatList;
import MyCustomedHaoop.Mapper.SelectSamples;
import MyCustomedHaoop.Partitioner.Partitioner_random;
import MyCustomedHaoop.Partitioner.Partitioner_random_sameKey;


public class MapRed_BuildRank_linearSearch_loadTestInReducer extends Configured implements Tool{

	/**  loadTestInReducer, use less memory than loadTrainInReducer, but need save all doc's score which use very large space!
	 * 
	 * rank candidate photos for test samples
	 * 
	 * job1_1: group test sample feat,
	 * mapper: 	read, select and out
	 * reducer: save test sample feat into SeqFile, 
	 * 
	 * @param  "mapred.SelQuerys"
	 * 
	 * job1_2: score docs
	 * mapper: 	read and out
	 * reducer: load all test samples, score some traing docs
	 * 			save into SeqFile, [docID]_[testIDs,Scores]
	 * 
	 * @param  
	 * 
	 * job1_3: combine all docs together, make final rank, 
	 * mapper: read all sequence file, transfer from [docID]_[testIDs,Scores] to [testID]_[docIDs,Scores] 
	 * reducer: make rank, save to SeqFile, [testID]_[docIDs,scores]
	 * 
	 * @param	"mapred.topRank"
	 * 
	 * 
	 * @throws Exception 
	 * @command_example: 
	 * 
	 * hadoop jar MapRed_BuildRank_linearSearch_loadTestInReducer.jar Gist.MapRed_BuildRank_linearSearch_loadTestInReducer -libjars mahout-core-0.8-SNAPSHOT-job.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.SelQuerys=MediaEval13/Querys_S_to_S/ -Dmapred.SelTrainPhotos=MediaEval13/MEval13_S_to_S_train.hashMap -Dmapred.topRank=1000 MediaEval13/Global_PhoFeats/Gist_MFile/part-r-00000/data TMM_GVR/imagR/ranks/Gist 1000 700 
	 * 
	 */
	
	public static void main(String[] args) throws Exception {
//		prepareData();
		
		//run hadoop
		int ret = ToolRunner.run(new MapRed_BuildRank_linearSearch_loadTestInReducer(), args);
		System.exit(ret);	 
	}
	
	@SuppressWarnings("unchecked")
	public static void prepareData() throws Exception {
		//when extracting gist features, photos alreay in S, so here when build rank, we need S_to_S
		
//		//***** for making sub-query-set for 9M dataset, as when build rank, doc_scores take a lot space if dataset is large, so decrease the query number ************
//		//to make the result comparable with SURF, so use the same queries subset with buildRank!
//		int totQnum=0;	String Q_S_to_S_folder="O:/MediaEval13/Querys_S_to_S/";
//		General.makeORdelectFolder(Q_S_to_S_folder);
//		for (File oneQSet:new File("O:/MediaEval13/Querys/").listFiles()) {
//			HashMap<Integer, Integer> Q_L_to_S= (HashMap<Integer, Integer>) General.readObject(oneQSet.getAbsolutePath());
//			HashMap<Integer, Integer> Q_S_to_S=new HashMap<Integer, Integer>(Q_L_to_S.size());
//			for (Entry<Integer, Integer> one : Q_L_to_S.entrySet()) {
//				Q_S_to_S.put(one.getValue(), one.getValue());
//			}
//			General.writeObject(Q_S_to_S_folder+oneQSet.getName(), Q_S_to_S);
//			System.out.println(oneQSet.getName()+", "+Q_S_to_S.size());
//			totQnum+=Q_S_to_S.size();
//		}	
//		System.out.println("totQnum:"+totQnum);
		
		//**** make train S_to_S **** 
		HashMap<Integer, Integer> train_L_to_S= (HashMap<Integer, Integer>) General.readObject("O:/MediaEval13/MEval13_L_to_S_train.hashMap");
		HashMap<Integer, Integer> train_S_to_S=new HashMap<Integer, Integer>(train_L_to_S.size());
		for (Entry<Integer, Integer> one : train_L_to_S.entrySet()) {
			train_S_to_S.put(one.getValue(), one.getValue());
		}
		General.writeObject("O:/MediaEval13/MEval13_S_to_S_train.hashMap", train_S_to_S);
		System.out.println("train sample num: "+train_S_to_S.size());
		
	}

	@SuppressWarnings("unchecked")
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem hdfs=FileSystem.get(conf);
		String homePath="hdfs://head02.hathi.surfsara.nl/user/yliu/"; //"hdfs://p-head03.alley.sara.nl/user/yliu/";
		String[] otherArgs = args; //use this to parse args!
		ArrayList<String> cacheFilePaths=new ArrayList<String>();

		Path[] featPath=new Path[]{new Path(otherArgs[0])}; //pre-extracted features
		
		//set selected querys set
		ArrayList<String> selQuerys=new ArrayList<String>(); 
		String queryHashMapPath=homePath+conf.get("mapred.SelQuerys");
		if (hdfs.isFile(new Path(queryHashMapPath))) {
			selQuerys.add(queryHashMapPath);
		}else {
			FileStatus[] files= hdfs.listStatus(new Path(queryHashMapPath));
			for (int i = 0; i < files.length; i++) {
				selQuerys.add(files[i].getPath().toString());
			}
		}
		System.out.println("selQuerys:\n"+selQuerys+"\n");
				
		String rankLabel="_rank_top"+Integer.valueOf(conf.get("mapred.topRank"))/1000+"K";
		
		Path[] rankPaths=new Path[selQuerys.size()];
		for (int i = 0; i < selQuerys.size(); i++) {
			String loopLabel="_Q"+i+"_"+(selQuerys.size()-1);		
			HashMap<Integer, Integer> testSamps=(HashMap<Integer, Integer>) General_Hadoop.readObject_HDFS(hdfs, selQuerys.get(i));
			int testSamNum=testSamps.size();
			conf.set("mapred.testSamNum", testSamNum+"");
			//***** job1_1: group test sample feat, save into one sequence file ***//
			SelectSamples selectSamples=new SelectSamples(selQuerys.get(i), false);
			cacheFilePaths.clear();
			selectSamples.addDistriCache_SelectSamples(cacheFilePaths);//SelSamples path with symLink
			String testFeat=otherArgs[1]+"_temp_testSampFeat_SeqFile"+loopLabel;
			General_Hadoop.Job(conf, featPath, testFeat, "getTestFeat", 1, 8, 2, true,
					MapRed_BuildRank_linearSearch_loadTestInReducer.class, selectSamples.getMapper(), null, null, null, null,
					IntWritable.class, FloatArr.class, IntWritable.class,FloatArr.class,
					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0, 0,
					cacheFilePaths.toArray(new String[0]),null);
			//***** job1_2: score docs ***//
			cacheFilePaths.clear();
			cacheFilePaths.add(homePath+conf.get("mapred.SelTrainPhotos")+"#SelSamples.file"); //SelSamples path with symLink
			cacheFilePaths.add(homePath+testFeat+"/part-r-00000"+"#testFeat");
			int job1_2RedNum=Integer.valueOf(otherArgs[2]); //reducer number
			String docScores=otherArgs[1]+"_temp_docScores_SeqFile"+loopLabel;
			General_Hadoop.Job(conf, featPath, docScores, "scoreDocs", job1_2RedNum, 8, 2, true,
					MapRed_BuildRank_linearSearch_loadTestInReducer.class, selectSamples.getMapper(), Partitioner_random.class,null, null,Reducer_scoreDocs.class,
					IntWritable.class, FloatArr.class, IntWritable.class,IntList_FloatList.class,
					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0, 0,
					cacheFilePaths.toArray(new String[0]),null);
			//******* job1_3: combine all doc scores, make rank, save to SeqFile ******
			int job1_3RedNum=Integer.valueOf(otherArgs[3]); //reducer number
			String rankPath=otherArgs[1]+rankLabel+loopLabel;
			General_Hadoop.Job(conf, new Path[]{new Path(docScores)}, rankPath, "combine&rank", job1_3RedNum, 8, 2, true,
					MapRed_BuildRank_linearSearch_loadTestInReducer.class, Mapper_transfer_Job1_3.class, Partitioner_random_sameKey.class,Combiner_combine_IntList_FloatList.class, null,Reducer_makeRank.class,
					IntWritable.class, IntList_FloatList.class, IntWritable.class,IntList_FloatList.class,
					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0, 0,
					null,null);			
			//********* clean-up ***********//
			hdfs.delete(new Path(testFeat), true);
			hdfs.delete(new Path(docScores), true);
			rankPaths[i]=new Path(rankPath);
		}
			
//		//******* job4: save all querys' rank into one MapFile ******
//		General_Hadoop.Job(conf, rankPaths, otherArgs[1]+rankLabel, "combine&save", 1, 8, 2, true,
//				MapRed_BuildRank_linearSearch_loadTestInReducer.class, null, null, null, Reducer_InOut_1key_1value.class,
//				IntWritable.class, IntList_FloatList.class, IntWritable.class,IntList_FloatList.class,
//				SequenceFileInputFormat.class, MapFileOutputFormat.class, 0, 10,
//				null,null);	
//		//clean-up rankPaths
//		for (Path path: rankPaths) {
//			hdfs.delete(path, true);
//		}		
		
		hdfs.close();
		return 0;
	}

	//*********** job1_2, score docs ***************
	public static class Reducer_scoreDocs extends Reducer<IntWritable,FloatArr,IntWritable,IntList_FloatList>{
		private Configuration conf;
		private boolean disp;
		private ArrayList<Integer> test_IDs;
		private ArrayList<float[]> test_feats;
		String targetFeatClassName;
		private int procPhotos;
		private int dispInter;
		private long startTime;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			startTime=System.currentTimeMillis(); //startTime
			
			//**** load featClassName ************//
			targetFeatClassName=conf.get("mapred.targetFeatClassName");
			System.out.println("targetFeatClassName: "+targetFeatClassName);
			
			//**** load testFeats from distributed cache ************//
			SequenceFile.Reader SeqFileReader = new SequenceFile.Reader(FileSystem.getLocal(conf),  new Path("testFeat"), conf);
			IntWritable key=new IntWritable();
			FloatArr value=new FloatArr();
			int testNum=Integer.valueOf(conf.get("mapred.testSamNum"));
			test_IDs=new ArrayList<Integer>(testNum*2); test_feats=new ArrayList<float[]>(testNum*2);
			while (SeqFileReader.next(key, value)) {
				test_IDs.add(key.get());
		        test_feats.add(value.getFloatArr());
			}
			SeqFileReader.close();
			System.out.println("load test feats finished! num:"+test_IDs.size()+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "s"));
			
			//set procPhotos
			procPhotos=0;
			//set dispInter
			dispInter=1000;
			startTime=System.currentTimeMillis(); //startTime
			
			System.out.println("mapper setup finsihed! \n---------------------------\n");
			disp=true; 
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable key, Iterable<FloatArr> Feats, Context context) throws IOException, InterruptedException {
			//key: photoName_classID
			//value: Feats, byte[] format, BytesWritable
			
			procPhotos++;
			//disp
    		if((procPhotos)%dispInter==0){ 	
    			disp=true;
    		}
    		
			//******** only one in Feats! ************	
			int loopNum=0; FloatArr feat=null;
			for(Iterator<FloatArr> it=Feats.iterator();it.hasNext();){// loop over all value			
				feat=it.next();
				loopNum++;
			}
			General.Assert(loopNum==1, "error in Reducer_scoreDocs! loopNum should == 1, here loopNum="+loopNum+", photoName:"+key.get());
			
			int docID=key.get();
			float[] feat_arr=feat.getFloatArr();
			
			//score this doc for all test samples 
			ArrayList<Integer> testSams=new ArrayList<Integer>(); ArrayList<Float> dists=new ArrayList<Float>();
			for (int i = 0; i<test_IDs.size(); i++) {
				float dist=General.suqaredEuclidian(test_feats.get(i), feat_arr);
				int test_ID=test_IDs.get(i);
				testSams.add(test_ID);
				dists.add(dist);
			}
			//output
			context.write(new IntWritable(docID), new IntList_FloatList(testSams, dists));
			
			//debug disp info
	        if (disp==true){ 
	        	System.out.println( "scoring for "+test_IDs.size()+" test samples, "+procPhotos+" photos finished!! ......"+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
				disp=false;
			}

		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one Reducer finished! total test samples in this Reducer: "+test_IDs.size()
					+", processed photos: "+procPhotos+" .....total time: "+ General.dispTime ( System.currentTimeMillis()-startTime, "min"));
			super.setup(context);
	 	}
	}
	
	//*********** job1_3, combine doc_score, make rank ***************
	public static class Mapper_transfer_Job1_3 extends Mapper<IntWritable,IntList_FloatList,IntWritable,IntList_FloatList>{
		protected void map(IntWritable key, IntList_FloatList value, Context context) throws IOException, InterruptedException {
			//key: docName
			//value: tesIDs_Scores
			int docID=key.get();
			for (int i=0; i< value.getIntegers().size();i++) {
				int tesID=value.getIntegers().get(i);
				float score=value.getFloats().get(i);
				ArrayList<Integer> docs=new ArrayList<Integer>(); ArrayList<Float> scores=new ArrayList<Float>();
				docs.add(docID); scores.add(score);
				context.write(new IntWritable(tesID), new IntList_FloatList(docs, scores));
			}
			
		}
	}
		
	public static class Reducer_makeRank extends Reducer<IntWritable,IntList_FloatList,IntWritable,IntList_FloatList>  {	
		private Configuration conf;
		private int topRank;
		private int procPhos;
		private int dispInter;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			//**** set topRank ************//
			topRank=Integer.valueOf(conf.get("mapred.topRank"));
			System.out.println("topRank: "+topRank);
			// ***** setup finsihed ***//
			procPhos=0;
			dispInter=1000;
			System.out.println("setup finsihed! \n---------------------------\n");
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable key, Iterable<IntList_FloatList> value, Context context) throws IOException, InterruptedException {
			//key: testSampleName
			//value_[docID_score]
			
			//******** collect doc scores************	
			PriorityQueue<slave_masterFloat_ASC<Integer>> doc_scores_queue=new PriorityQueue<slave_masterFloat_ASC<Integer>>(topRank);
			float thr_max=-1; int docNum=0;
			for(Iterator<IntList_FloatList> it=value.iterator();it.hasNext();){// loop over all value			
				IntList_FloatList doc_scores_one=it.next();
				for (int i = 0; i < doc_scores_one.getIntegers().size(); i++) {
					int doc=doc_scores_one.getIntegers().get(i);
					float score=doc_scores_one.getFloats().get(i);
					// if the array is not full yet:
			        if (doc_scores_queue.size() < topRank) {
			        	doc_scores_queue.add(new slave_masterFloat_ASC<Integer>(doc,score));
			            if (score>thr_max) //update current thr in doc_scores_order
			            	thr_max = score;
			        } else if (score<thr_max) { // if it is "better" than the least one in the current doc_scores_order
			        	// remove the last one ...
			        	doc_scores_queue.poll();
			            // add the new one ...
			        	doc_scores_queue.offer(new slave_masterFloat_ASC<Integer>(doc,score));
			            // update new thr in doc_scores_order
			        	thr_max = doc_scores_queue.peek().getMaster();
			        }
			        docNum++;
				}
			}
			//make rank
			ArrayList<Integer> topDocs=new ArrayList<Integer>(topRank); ArrayList<Float> topScores=new ArrayList<Float>(topRank);
			ArrayList<slave_masterFloat_ASC<Integer>> order= General_IR.get_topRanked_from_PriorityQueue(doc_scores_queue, doc_scores_queue.size());
			for (int i = 0; i < order.size(); i++) {
				topDocs.add(order.get(i).getSlave());
				topScores.add(order.get(i).getMaster());
			}
			//disp
			if (procPhos%dispInter==0) {
				System.out.println("ranking for "+procPhos+" testSamples done! current testSample-"+key.get()+", ranked Docs:"+docNum+", out put topDocs:"+topDocs.size());
				System.out.println("top-10 docs:"+topDocs.subList(0, Math.min(10, topDocs.size())));
				System.out.println("top-10 scores:"+topScores.subList(0, Math.min(10, topScores.size())));
			}
			//outPut
			context.write(key, new IntList_FloatList(topDocs,topScores));
			procPhos++;
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.print("ranking for "+procPhos+" testSamples done!");
		    super.setup(context);
	 	}
	}
	
}
