package Lire;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.General.General_Lire;
import MyAPI.Obj.Disp;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;


public class MapRed_extractStrFeat_Lire093 extends Configured implements Tool{

	/**
	 * 
	 * job1: select photos from dataSet, and save into one MapFile
	 * mapper: select photos
	 * reducer: extract feat in string representation by lire 0.9.3
	 * 			output photoIndex_ImageFeatStr
	 * @param (Mapper_selectPhotos): "mapred.SelPhotos"  
	 * 
	 * @throws Exception 
	 * @command_example: 
	 * 
	 * hadoop jar MapRed_extractStrFeat_Lire093.jar Lire.MapRed_extractStrFeat_Lire093 -libjars mahout-core-0.8-SNAPSHOT-job.jar,lire0.9.3.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.SelPhotos=MediaEval13/L_to_L_newAddMisBloPho.hashMap 66M_Photos_SeqFiles/ MediaEval13/misBloPho_feat 20
	 */
	
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new MapRed_extractStrFeat_Lire093(), args);
		System.exit(ret);
		 
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs=FileSystem.get(conf);
		String homePath="hdfs://p-head03.alley.sara.nl/user/yliu/";
		String[] otherArgs = args; //use this to parse args!
		//set input/output path
		String In=otherArgs[0]; //input path
		System.out.println("In:"+In);
		String out=otherArgs[1]; //output path
		System.out.println("out:"+out);
		//set reducer number
		int job1RedNum=Integer.valueOf(otherArgs[2]); //reducer number
		System.out.println("job1RedNum:"+job1RedNum);
		
		//a.set image sequence file paths: 3M, missiing blocks, In=otherArgs[0]: 3M_Photos_SeqFiles, 66M_Photos_SeqFiles/missingBlocks_patch_seq
//		List<Path> imageSeqPaths = new ArrayList<Path>();
//		imageSeqPaths.add(new Path(In));
			
		//b.set image sequence file paths, 66M, In=otherArgs[0]: 66M_Photos_SeqFiles/
		List<Path> imageSeqPaths = new ArrayList<Path>();
//		double Sym1M=1000*1000;
//		int saveInterval=1000*1000; 
//		int start_loop=3; //should start from 3
//		int end_loop=66;  //66
//		for(int loop_i=start_loop;loop_i<=end_loop;loop_i++){//one loop, one MapFile
//			//set photo range for one file
//			int[] photoRang=new int[2];
//			if(loop_i==3){
//				photoRang[0]=3185259;
//			}else{
//				photoRang[0]=loop_i*saveInterval;
//			}
//			photoRang[1]=(loop_i+1)*saveInterval-1;
//			imageSeqPaths.add(new Path(In+photoRang[0]/Sym1M+"_"+photoRang[1]/Sym1M+"_seq"));
//		}
//		imageSeqPaths.add(new Path(In+"3_66_patch_seq"));
		imageSeqPaths.add(new Path(In+"missingBlocks_patch_seq"));
		
		
		String[] CatchFilePaths=new String[1];
		
		//******* 1st job: get photos ******
		CatchFilePaths[0]=homePath+conf.get("mapred.SelPhotos")+"#SelPhotos.file"; //latlons path with symLink
		Job1(conf, (Path[]) imageSeqPaths.toArray(new Path[imageSeqPaths.size()]), out, "getPhoFeat",job1RedNum,CatchFilePaths,null);

		fs.close();
		return 0;
	}
	
	public void Job1(Configuration conf, Path[] inPath, String outPath, String JobName, int jobRedNum, String[] CatchFilePaths, String[] CatchArchPaths) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException{
		Job job = new Job(conf, JobName);
		//add files to Distributed catch, 
		General_Hadoop.add_to_DistributedCache(job, CatchFilePaths, CatchArchPaths); //change on jobConf only available for this job, not influence the original conf
		//define which jar to find all class in job config (classes below) 
		job.setJarByClass(MapRed_extractStrFeat_Lire093.class);
		//set mapper, reducer, partitioner
		job.setMapperClass(Mapper_selectPhotos_extractFeat.class);
		job.setReducerClass(Reducer_InOut_job1.class);
		job.setNumReduceTasks(jobRedNum);
		//set mapper out-put Key_Value
		job.setMapOutputKeyClass(IntWritable.class); //if not set, MapOutputKeyClass will be OutputKeyClass
		job.setMapOutputValueClass(Text.class);
		//set job out-put Key_Value
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		//set job in/out FileClass
		job.setInputFormatClass(SequenceFileInputFormat.class); 
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//set job in/out file Path
		FileInputFormat.setInputPaths(job, inPath);//commaSeparatedPaths
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		//wait for completion
		if (!job.waitForCompletion(true)) {
	        throw new InterruptedException("Job Failed! job: "+job.getJobName());
		}
	}

	//****************** job_1, get photos  ************************
	public static class Mapper_selectPhotos_extractFeat extends Mapper<IntWritable,BufferedImage_jpg,IntWritable,Text>{
		private HashMap<Integer,Integer> selectedPhotos;
		private int procPhotos;
		private int dispInter;
		private long startTime, endTime;
		
		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			selectedPhotos= (HashMap<Integer, Integer>)  General.readObject("SelPhotos.file");
			System.out.println("total selected Photos:"+selectedPhotos.size());
			//set procPhotos
			procPhotos=0;
			//set dispInter
			dispInter=5000;
			startTime=System.currentTimeMillis(); //startTime
			
			System.out.println("mapper setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		protected void map(IntWritable key, BufferedImage_jpg value, Context context) throws IOException, InterruptedException {
			//key: photoName in L-index
			//value: file content
			
			int photoName=key.get();// photoName
			if(selectedPhotos.containsKey(photoName)){
				try {
					//feat extract in str
					String feat = General_Lire.extractFeat_inStr_lire093(value.getBufferedImage("photoName:"+photoName, Disp.getNotDisp()));
					//output
					procPhotos++;
					IntWritable photoNameInS=new IntWritable(selectedPhotos.get(photoName));
					context.write(photoNameInS, new Text(feat));
					//disp
		    		if((procPhotos)%dispInter==0){ 							
						endTime=System.currentTimeMillis(); //end time 
						System.out.println( "select photos, "+procPhotos+" photos, feat extraction finished!! ......"+ General.dispTime (endTime-startTime, "min"));
					}
				} catch (Exception e) {
					System.err.println("err for pho-"+key.get()+", e:"+e.getMessage()+", stop the system!!");
					e.printStackTrace();
					throw new InterruptedException(e.getMessage());
				}
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
		    endTime=System.currentTimeMillis(); //end time 
			System.out.println("one mapper finished! total selected photos in this Mapper: "+procPhotos+" ....."+ General.dispTime ( endTime-startTime, "min"));
			super.setup(context);
	 	}
	}

	public static class Reducer_InOut_job1 extends Reducer<IntWritable,Text,IntWritable,Text>  {
		
		private int photoNums;
		private HashMap<Integer,Integer> selectedPhotos;
		
		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			photoNums=0;
			selectedPhotos= (HashMap<Integer, Integer>)  General.readObject("SelPhotos.file");
			System.out.println("total selected Photos:"+selectedPhotos.size());
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable photoName, Iterable<Text> image, Context context) throws IOException, InterruptedException {
			//key: queryName, value: DocName&Scores

			//******** only one list in DocNameScores! ************		
			int loopNum=0;  Text onePhoto=null;
			for(Iterator<Text> it=image.iterator();it.hasNext();){// loop over all HashMaps				
				onePhoto=it.next();
				loopNum++;
			}
			General.Assert(loopNum==1, "error in Reducer_InOut_job1! one photoName, one photo, loopNum should == 1, here loopNum="+loopNum);
			
			photoNums++;
			context.write(photoName, onePhoto);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
//			General.Assert(photoNums==selectedPhotos.size(), "photoNums: "+photoNums+", selectedPhotos.size():"+selectedPhotos.size());
			System.out.println("Reducer done!  total photos:"+photoNums);
			super.setup(context);
	 	}
	}

}
