package Lire;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.General.General_Lire;
import MyAPI.Obj.Disp;
import MyCustomedHaoop.Mapper.SelectSamples;
import MyCustomedHaoop.Partitioner.Partitioner_random;
import MyCustomedHaoop.Reducer.Reducer_InOut.Reducer_InOut_1key_1value;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;


public class MapRed_extractByteFeat_Lire136 extends Configured implements Tool{

	/**
	 * 
	 * job1: select photos from dataSet, and save into one MapFile
	 * mapper: select photos
	 * reducer: extract feat in string representation by lire 0.9.3
	 * 			output photoIndex_ImageFeatStr
	 * @param (Mapper_selectPhotos): "mapred.SelPhotos"  "mapred.targetFeatClassName"
	 * 
	 * @throws Exception 
	 * @command_example: 
	 * 
	 * 3M:	hadoop jar MapRed_extractByteFeat_Lire136.jar Lire.MapRed_extractByteFeat_Lire136 -libjars mahout-core-0.8-SNAPSHOT-job.jar,lire136.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.selectPhotosPath=ICMR2013/3M_transIndex_LtoS.hashMap 3M_Photos_SeqFiles/ 3M_PhoFeats_SeqFiles/ 100
	 * MEval13:	hadoop jar MapRed_extractByteFeat_Lire136.jar Lire.MapRed_extractByteFeat_Lire136 -libjars mahout-core-0.8-SNAPSHOT-job.jar,lire136.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.selectPhotosPath=MediaEval13/MEval13_L_to_S.hashMap 66M_Phos_Seqs/ MediaEval13/Global_PhoFeats/ 2000
	 */
	
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new MapRed_extractByteFeat_Lire136(), args);
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
		//set distributed cache
		ArrayList<String> cacheFilePaths=new ArrayList<String>();
		
		//******* add input images
		ArrayList<Path> imageSeqPaths = new ArrayList<Path>();
//		//a.set image sequence file paths: 3M, missiing blocks, In=otherArgs[0]: 3M_Photos_SeqFiles
//		imageSeqPaths.add(new Path(In));
		//b.set image sequence file paths, 66M, In=otherArgs[0]: 66M_Phos_Seqs/
		double Sym1M=1000*1000;
		int saveInterval=1000*1000; 
		int start_loop=3; //should start from 3
		int end_loop=66;  //66
		for(int loop_i=start_loop;loop_i<=end_loop;loop_i++){//one loop, one MapFile
			//set photo range for one file
			int[] photoRang=new int[2];
			if(loop_i==3){
				photoRang[0]=3185259;
			}else{
				photoRang[0]=loop_i*saveInterval;
			}
			photoRang[1]=(loop_i+1)*saveInterval-1;
			imageSeqPaths.add(new Path(In+photoRang[0]/Sym1M+"_"+photoRang[1]/Sym1M+"_seq"));
		}
		imageSeqPaths.add(new Path(In+"3_66_patch_seq"));
		imageSeqPaths.add(new Path(In+"missingBlocks_patch_seq"));
		
		//******* 1st job: run for each feat ******
		String[] classArray = {"CEDD", "EdgeHistogram", "FCTH", "ColorLayout", "PHOG", "JCD", "Gabor", "JpegCoefficientHistogram", "Tamura", "LuminanceLayout", "OpponentHistogram", "ScalableColor"};
		SelectSamples selectSamples=new SelectSamples(conf.get("mapred.selectPhotosPath"), false);
		for (int i=0; i<classArray.length;i++) {
			String targetFeatClassName=classArray[i];
			conf.set("mapred.targetFeatClassName", targetFeatClassName);
			cacheFilePaths.clear();
			cacheFilePaths.add(homePath+conf.get("mapred.selectPhotosPath")+"#SelSamples.file"); //SelSamples path with symLink
			String trainFeat=otherArgs[1]+targetFeatClassName;
			General_Hadoop.Job(conf, imageSeqPaths.toArray(new Path[0]), trainFeat, "getTrainFeat_"+i+"_"+targetFeatClassName, job1RedNum, 8, 2, true,
					MapRed_extractByteFeat_Lire136.class, selectSamples.getMapper(), Partitioner_random.class, null, null, Reducer_extractFeat_job1.class,
					IntWritable.class, BufferedImage_jpg.class, IntWritable.class,BytesWritable.class,
					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024*1024L, 0,
					cacheFilePaths.toArray(new String[0]),null);
			//combine & save into one mapfile
			ArrayList<Path> featSeqPaths = new ArrayList<Path>(); featSeqPaths.add(new Path(trainFeat));
			General_Hadoop.Job(conf, featSeqPaths.toArray(new Path[0]), trainFeat+"_MFile", "combineTrainFeat_"+i+"_"+targetFeatClassName, 1, 8, 2, true,
					MapRed_extractByteFeat_Lire136.class, null, null,null, null,Reducer_InOut_1key_1value.class,
					IntWritable.class, BytesWritable.class, IntWritable.class,BytesWritable.class,
					SequenceFileInputFormat.class, MapFileOutputFormat.class, 0, 10,
					null,null);
//			fs.delete(new Path(trainFeat), true);
		}
		
		fs.close();
		return 0;
	}

	//****************** job_1, get photo feat  ************************
	public static class Reducer_extractFeat_job1 extends Reducer<IntWritable,BufferedImage_jpg,IntWritable,BytesWritable>  {
		
		private String targetFeatClassName;
		private int procPhotos;
		private int dispInter;
		private long startTime, endTime;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//***** read target class name ***//
			targetFeatClassName= conf.get("mapred.targetFeatClassName");
			General.Assert(targetFeatClassName!=null, "err in Mapper_selectPhotos_extractFeat!  targetFeatClassName cannot be null!");
			System.out.println("targetFeatClassName: "+targetFeatClassName);
			//set procPhotos
			procPhotos=0;
			//set dispInter
			dispInter=500;
			startTime=System.currentTimeMillis(); //startTime
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable photoName, Iterable<BufferedImage_jpg> image, Context context) throws IOException, InterruptedException {
			//key: queryName, value: BufferedImage_jpg

			//******** only one list in BufferedImage_jpg! ************		
			int loopNum=0;  BufferedImage_jpg onePhoto=null;
			for(Iterator<BufferedImage_jpg> it=image.iterator();it.hasNext();){// loop over all HashMaps				
				onePhoto=it.next();
				loopNum++;
			}
			General.Assert(loopNum==1, "error in Reducer_extractFeat_job1! one photoName, one photo, loopNum should == 1, here loopNum="+loopNum);

			try {
				//feat extract in byte[]
				byte[] feat = General_Lire.extractFeat_inByteArr_lire136(onePhoto.getBufferedImage("photoName:"+photoName, Disp.getNotDisp()),targetFeatClassName);
				//output
				context.write(photoName, new BytesWritable(feat));
				//disp
				procPhotos++;
	    		if((procPhotos)%dispInter==0){ 							
					endTime=System.currentTimeMillis(); //end time 
					System.out.println( "select photos, "+procPhotos+" photos, feat extraction finished!! ......"+ General.dispTime (endTime-startTime, "min"));
				}
			} catch (Exception e) {
				System.err.println("err for pho-"+photoName.get()+", e:"+e.getMessage()+", stop the system!!");
				e.printStackTrace();
				throw new InterruptedException(e.getMessage());
			}
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			endTime=System.currentTimeMillis(); //end time 
			System.out.println("one reducer finished! total processed photos in this reducer: "+procPhotos+" ....."+ General.dispTime ( endTime-startTime, "min"));
			super.setup(context);
	 	}
	}

}
