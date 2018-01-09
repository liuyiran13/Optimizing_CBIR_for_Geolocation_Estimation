package MediaEval14;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.Obj.Statistics;
import MyCustomedHaoop.Mapper.SelectSamples;
import MyCustomedHaoop.Partitioner.Partitioner_random;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
import MyCustomedHaoop.ValueClass.VideoBytes;

public class MapRed_MergeVideoAndPhoto extends Configured implements Tool{

	/**
	 * merge train photos + train-video frames: L_to_S_train
	 * merge train_test photos + train-video frames: latlon, userID
	 * 
	 * @param (Mapper_selectPhotoIndex): "mapred.photoIndexRang"
	 * 
	 * @throws Exception 
	 * 
	 * @command_example: 
	 * ME14_videos:	hadoop jar MapRed_MergeVideoAndPhoto.jar MediaEval14.MapRed_MergeVideoAndPhoto -libjars MyAPI.jar,commons-io-2.1.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar -Dmapred.task.timeout=600000 -Dmapred.selectVideosPath=MediaEval14/MEval14_videos_L_to_S_train.hashMap -Dmapred.BinTool_getVideoFrames=MediaEval14/ffmpeg -Dmapred.selectPhotosPath=MediaEval14/MEval14_photos_L_to_S_train.hashMap -Dmapred.latlons_pho=MediaEval14/MEval14_photos_latlons.floatArr -Dmapred.userIDs_0_pho=MediaEval14/MEval14_photos_userIDs_0.long -Dmapred.userIDs_1_pho=MediaEval14/MEval14_photos_userIDs_1.int -Dmapred.latlons_video=MediaEval14/MEval14_videos_latlons.floatArr -Dmapred.userIDs_0_video=MediaEval14/MEval14_photos_userIDs_0.long -Dmapred.userIDs_1_video=MediaEval14/MEval14_photos_userIDs_1.int MediaEval14/ Webscope100M/ME14_Crawl/Videos 700
	 */
	
	public static void main(String[] args) throws Exception {
		//run hadoop
		int ret = ToolRunner.run(new MapRed_MergeVideoAndPhoto(), args);
		System.exit(ret);
	}

	@Override
	public int run(String[] args) throws Exception {
		//set common
		Configuration conf = getConf();
		FileSystem fs=FileSystem.get(conf);
		String homePath="hdfs://p-head03.alley.sara.nl/user/yliu/";
		String[] otherArgs = args; //use this to parse args!
		ArrayList<String> cacheFilePaths=new ArrayList<String>();
		String workPath=otherArgs[0];
		String videoDataPath=otherArgs[1];
		int job1RedNum=Integer.valueOf(otherArgs[2]);
		//******* job1: extract video Frames ******
		String job1_out=workPath+"temp_MapRed_MergeVideoAndPhoto_job1";
		SelectSamples selectSamples=new SelectSamples(conf.get("mapred.selectVideosPath"), false);
		//Distributed cache
		cacheFilePaths.clear();
		selectSamples.addDistriCache_SelectSamples(cacheFilePaths);; //SelSamples path with symLink
		cacheFilePaths.add(homePath+conf.get("mapred.BinTool_getVideoFrames")+"#BinTool_getVideoFrames.exe"); //BinTool_getVideoFrames path with symLink
		General_Hadoop.Job(conf, new Path[]{new Path(videoDataPath)}, job1_out, "getFrames", job1RedNum, 2, 10, true,
				MapRed_MergeVideoAndPhoto.class, selectSamples.getMapper(), Partitioner_random.class,null, null,Reducer_extractVideoFrames.class,
				IntWritable.class, VideoBytes.class, IntWritable.class,BufferedImage_jpg.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
				cacheFilePaths.toArray(new String[0]),null);
		//******* job2: group video Frames into one index, f->videoIndex_s, only 1 reducer ******
		String fInd_vIndinS_path=homePath+workPath+"ME14Train_fInd_vIndinS.IntegerArr";
		String vIndInS_1stFrameInd_path=homePath+workPath+"vIndInS_1stFrameInd.hashMap";
		conf.set("mapred.fInd_vIndinS_path", fInd_vIndinS_path);
		conf.set("mapred.vIndInS_1stFrameInd_path", vIndInS_1stFrameInd_path);
		General_Hadoop.Job(conf, new Path[]{new Path(job1_out)}, null, "groupFrames", 1, 4, 10, false,
				MapRed_MergeVideoAndPhoto.class, Mapper_transferContentToNum.class, null,null, null,Reducer_groupVideoFrames.class,
				IntWritable.class, IntWritable.class, IntWritable.class,IntWritable.class,
				SequenceFileInputFormat.class, NullOutputFormat.class, 1*1024*1024*1024L, 0,
				null,null);
		//******* job3: index video Frames into one index, f->videoIndex_s ******
		String job3_out=workPath+"ME14Train_VidFrames_infInd";
		//Distributed cache
		cacheFilePaths.clear();
		cacheFilePaths.add(vIndInS_1stFrameInd_path+"#vIndInS_1stFrameInd.file"); //SelSamples path with symLink
		General_Hadoop.Job(conf, new Path[]{new Path(job1_out)}, job3_out, "indexFrames", job1RedNum, 8, 10, true,
				MapRed_MergeVideoAndPhoto.class, null, null,null, null,Reducer_indexVideoFrames.class,
				IntWritable.class, BufferedImage_jpg.class, IntWritable.class,BufferedImage_jpg.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 1*1024*1024*1024L, 0,
				cacheFilePaths.toArray(new String[0]),null);
		General_Hadoop.deleteIfExist(job1_out, fs);
		General_Hadoop.deleteIfExist(vIndInS_1stFrameInd_path, fs);
		//******* job4: run local, combine all photoDB with framesDB trainData ******
		combinePhotoDB_FramesDB(homePath, conf, workPath);
		//clean-up
		fs.close();

		return 0;
	}
	
	private void combinePhotoDB_FramesDB(String homePath, Configuration conf, String workPath) throws InterruptedException, FileNotFoundException, IOException, ClassNotFoundException {
		System.out.println("start combine all PhotoDB with FramesDB_trainData");
		//read photo info
		FileSystem hdfs=FileSystem.get(conf);
		@SuppressWarnings("unchecked")
		HashMap<Integer, Integer> L_to_S_pho_train=(HashMap<Integer, Integer>) General_Hadoop.readObject_HDFS(hdfs, homePath+conf.get("mapred.selectPhotosPath"));
		float[][] latlons_pho=(float[][]) General_Hadoop.readObject_HDFS(hdfs, homePath+conf.get("mapred.latlons_pho"));
		long[] userIDs_0_pho=(long[]) General_Hadoop.readObject_HDFS(hdfs, homePath+conf.get("mapred.userIDs_0_pho")); 
		int[] userIDs_1_pho=(int[]) General_Hadoop.readObject_HDFS(hdfs, homePath+conf.get("mapred.userIDs_1_pho")); 
		//read video info
		Integer[] fInd_vIndinS=(Integer[]) General_Hadoop.readObject_HDFS(hdfs, conf.get("mapred.fInd_vIndinS_path")); //frameInd start from -1
		float[][] latlons_video=(float[][]) General_Hadoop.readObject_HDFS(hdfs, homePath+conf.get("mapred.latlons_video"));
		long[] userIDs_0_video=(long[]) General_Hadoop.readObject_HDFS(hdfs, homePath+conf.get("mapred.userIDs_0_video")); 
		int[] userIDs_1_video=(int[]) General_Hadoop.readObject_HDFS(hdfs, homePath+conf.get("mapred.userIDs_1_video")); 
		//combine: photo+video
		int photoNum=userIDs_0_pho.length; 
		int frameNum=fInd_vIndinS.length;
		int totPics=photoNum+frameNum;
		HashMap<Integer, Integer> L_to_S_train=new HashMap<Integer, Integer>();
		float[][] latlons=new float[2][totPics];
		long[] userIDs_0=new long[totPics];
		int[] userIDs_1=new int[totPics];
		//add photos
		L_to_S_train.putAll(L_to_S_pho_train);
		for (int i = 0; i < photoNum; i++) {
			latlons[0][i]=latlons_pho[0][i];
			latlons[1][i]=latlons_pho[1][i];
			userIDs_0[i]=userIDs_0_pho[i];
			userIDs_1[i]=userIDs_1_pho[i];
		}
		//add video frames
		int current_S_forFrame=photoNum;
		for (int fInd = 0; fInd < frameNum; fInd++) {
			L_to_S_train.put(-fInd-1, current_S_forFrame);
			int videoInd_S=fInd_vIndinS[fInd];
			latlons[0][current_S_forFrame]=latlons_video[0][videoInd_S];
			latlons[1][current_S_forFrame]=latlons_video[1][videoInd_S];
			userIDs_0[current_S_forFrame]=userIDs_0_video[videoInd_S];
			userIDs_1[current_S_forFrame]=userIDs_1_video[videoInd_S];
			current_S_forFrame++;
		}
		General_Hadoop.writeObject_HDFS(hdfs, workPath+"ME14_Pho-TrTe_VidFrame-Tr_latlons.floatArr", latlons);
		General_Hadoop.writeObject_HDFS(hdfs, workPath+"ME14_Pho-TrTe_VidFrame-Tr_userIDs_0.long", userIDs_0);
		General_Hadoop.writeObject_HDFS(hdfs, workPath+"ME14_Pho-TrTe_VidFrame-Tr_userIDs_1.int", userIDs_1);
		General_Hadoop.writeObject_HDFS(hdfs, workPath+"ME14_PhoVidFrame_L_to_S_train.hashMap", L_to_S_train);
		System.out.println("done for combinePhotoDB with FramesDB, phoNum:"+photoNum+", frameNum:"+frameNum+", current_S_forFrame:"+current_S_forFrame);
	}
	
	public static class Reducer_extractVideoFrames extends Reducer<IntWritable,VideoBytes,IntWritable,BufferedImage_jpg>  {

		String tempFilesPath;
		String binaryPath_extactVideoFrame;
		private Statistics<String> stat_videoSize;
		private Statistics<String> stat_farmNum;
		private int procSamples;
		private int totSkipped;
		private int dispInter;
		private long startTime;
		private boolean disp;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			stat_videoSize=new Statistics<String>(1);
			stat_farmNum=new Statistics<String>(1);
			tempFilesPath="./";//current attempt's distributed cache path
			binaryPath_extactVideoFrame="./BinTool_getVideoFrames.exe";
			//set dispInter
			procSamples=0;
			totSkipped=0;
			dispInter=1;
			startTime=System.currentTimeMillis(); //startTime
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			disp=true;
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable VideoIndex_in_s, Iterable<VideoBytes> VideoContent, Context context) throws IOException, InterruptedException {
			procSamples++;
			int videoIndex_in_s=VideoIndex_in_s.get(); 
			//get video conten
			int loopNum=0; VideoBytes videoContent=null;
			for(Iterator<VideoBytes> it=VideoContent.iterator();it.hasNext();){//only one element
				videoContent=it.next();
				loopNum++;
			}
			General.Assert(loopNum==1, "error in Reducer_extractVideoFrames! one VideoIndex_in_s, one VideoContent, loopNum should == 1, here loopNum="+loopNum);
			int contentSizeInM=videoContent.getBytes().length/1000/1000;
			//show progress
			if((procSamples)%dispInter==0){ 							
				System.out.println( "extractVideoFrames: "+procSamples+" samples finished!! totSkipped:"+totSkipped
						+", current Memory:"+General.memoryInfo()
						+" ...... "+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
				System.out.println( "\t"+"current videoIndex_in_s for this reduce: "+videoIndex_in_s+", its size:"+General.dispNum(videoContent.getBytes().length, "M"));
				disp=true;
			}
//			//debug--for videoIndex_in_s==8829
//			if (videoIndex_in_s==8829) {
//				Configuration conf=context.getConfiguration();
//				String homePath="hdfs://p-head03.alley.sara.nl/user/yliu/";
//				General_Hadoop.writeObject_HDFS(FileSystem.get(conf), homePath+"MediaEval14/VideoBytes_"+videoIndex_in_s, videoContent.getBytes());
//			}
			//extract frames
			String framesRes=tempFilesPath+videoIndex_in_s+"/";
			videoContent.getVideoFrams(tempFilesPath+"video_"+videoIndex_in_s, framesRes, binaryPath_extactVideoFrame, false, "\t");
			videoContent=null;//nullify this obj, let GC know this one is not used anymore.
			General.dispInfo_ifNeed(disp, "\t", "video_"+videoIndex_in_s+" call binary done! .... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			//read frames and output each as BufferedImage_jpg
			int framNum=0;
			for (File oneFrame : new File(framesRes).listFiles()) {
				context.write(VideoIndex_in_s, new BufferedImage_jpg(oneFrame, (framNum<5) && disp, "\t\t"));
				framNum++;
			}
			General.dispInfo_ifNeed(disp, "\t", "video_"+videoIndex_in_s+" extract frames done! framNum:"+framNum+" ...... "+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
			stat_farmNum.addSample(framNum, "video_"+videoIndex_in_s);
			stat_videoSize.addSample(contentSizeInM, "video_"+videoIndex_in_s);
			General.deleteAll(new File(framesRes));
			disp=false;
			System.gc();
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one reducer finished! total processed samples: "+procSamples+", totSkipped:"+totSkipped+", ....."+ General.dispTime ( System.currentTimeMillis()-startTime, "min"));
			System.out.println("stat_videoSize in Mb: "+stat_videoSize.getFullStatistics("0"));
			System.out.println("stat_farmNum: "+stat_farmNum.getFullStatistics("0"));
			super.setup(context);
	 	}
	}

	public static class Mapper_transferContentToNum extends Mapper<IntWritable,BufferedImage_jpg,IntWritable,IntWritable>{
		//to save tranfere load, convert BufferedImage_jpg to one num
		private int procSamples;
		private HashSet<Integer> uniKeys;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			
			procSamples=0;
			uniKeys=new HashSet<Integer>();
			// ***** setup finished ***//
			System.out.println("Mapper_transferContentToNum setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		protected void map(IntWritable key, BufferedImage_jpg value, Context context) throws IOException, InterruptedException {
			
			//** output, set key, value **//
			context.write(new IntWritable(key.get()),new IntWritable(1));
			procSamples++;
			uniKeys.add(key.get());
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one mapper finished! total Samples in this Mapper: "+procSamples+", uniKey num:"+uniKeys.size()+", uniKeys:"+uniKeys);
			super.setup(context);
	 	}
	}

	public static class Reducer_groupVideoFrames extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>  {
		
		LinkedList<Integer> fInd_vIndInS;
		HashMap<Integer, Integer> vIndInS_1stFrameInd;
		int fInd;
		private Statistics<String> stat_frameNum;
		private int procSamples;
		private int dispInter;
		private long startTime;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			fInd_vIndInS=new LinkedList<Integer>();
			vIndInS_1stFrameInd=new HashMap<Integer, Integer>();
			fInd=-1; //frame index start from -1
			stat_frameNum=new Statistics<String>(1);
			//set dispInter
			procSamples=0;
			dispInter=1000;
			startTime=System.currentTimeMillis(); //startTime
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable VideoIndex_in_s, Iterable<IntWritable> VideoFrames, Context context) throws IOException, InterruptedException {
			procSamples++;
			int videoIndex_in_s=VideoIndex_in_s.get(); 
			vIndInS_1stFrameInd.put(videoIndex_in_s, fInd);//save 1st frame's global fInd
			//get frames
			int framNum=0; 
			for(Iterator<IntWritable> it=VideoFrames.iterator();it.hasNext();){//only one element
				it.next();//although do not need value, but still call it.next, otherwise it does not loop!!
				fInd_vIndInS.add(videoIndex_in_s);//all this video's frames are mapped to this video's videoIndex_in_s
				fInd--;
				framNum++;
			}
			//show progress
			if((procSamples)%dispInter==0){ 							
				System.out.println( "groupVideoFrames: "+procSamples+" samples finished!! current Memory:"+General.memoryInfo()
						+" ...... "+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
				System.out.println( "\t"+"current videoIndex_in_s for this reduce: "+videoIndex_in_s+", its framNum:"+framNum+", current global fInd:"+fInd);
			}
			stat_frameNum.addSample(framNum, "video_"+videoIndex_in_s);
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** save fInd_vIndinS into HDFS ***//
			Configuration conf=context.getConfiguration();
			String fInd_vIndinS_path=conf.get("mapred.fInd_vIndinS_path");
			String vIndInS_1stFrameInd_path=conf.get("mapred.vIndInS_1stFrameInd_path");
			General_Hadoop.writeObject_HDFS(FileSystem.get(conf), fInd_vIndinS_path, fInd_vIndInS.toArray(new Integer[0]));
			General_Hadoop.writeObject_HDFS(FileSystem.get(conf), vIndInS_1stFrameInd_path, vIndInS_1stFrameInd);
			System.out.println("one reducer finished! total processed samples: "+procSamples+", current global fInd:"+fInd+", fInd_vIndInS is saved to: "+fInd_vIndinS_path+" ........"+ General.dispTime ( System.currentTimeMillis()-startTime, "min"));
			System.out.println("stat_frameNum: "+stat_frameNum.getFullStatistics("0"));
			super.setup(context);
	 	}
	}
	
	public static class Reducer_indexVideoFrames extends Reducer<IntWritable,BufferedImage_jpg,IntWritable,BufferedImage_jpg>  {
		//change video frame id to global fInd
		HashMap<Integer, Integer> vIndInS_1stFrameInd;
		private Statistics<String> stat_farmNum;
		private int procSamples;
		private int dispInter;
		private long startTime;
		
		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			vIndInS_1stFrameInd=(HashMap<Integer, Integer>) General.readObject("vIndInS_1stFrameInd.file");
			stat_farmNum=new Statistics<String>(1);
			//set dispInter
			procSamples=0;
			dispInter=10;
			startTime=System.currentTimeMillis(); //startTime
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable VideoIndex_in_s, Iterable<BufferedImage_jpg> VideoFrames, Context context) throws IOException, InterruptedException {
			procSamples++;
			int videoIndex_in_s=VideoIndex_in_s.get(); 
			//get frames
			int framNum=0; int currentFInd=vIndInS_1stFrameInd.get(videoIndex_in_s);
			for(Iterator<BufferedImage_jpg> it=VideoFrames.iterator();it.hasNext();){//only one element
				BufferedImage_jpg oneFrame=it.next();
				context.write(new IntWritable(currentFInd), oneFrame);
				currentFInd--;
				framNum++;
			}
			//show progress
			if((procSamples)%dispInter==0){ 							
				System.out.println( "indexVideoFrames: "+procSamples+" samples finished!! current Memory:"+General.memoryInfo()
						+" ...... "+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
				System.out.println( "\t"+"current videoIndex_in_s for this reduce: "+videoIndex_in_s+", its framNum:"+framNum+", current global currentFInd:"+currentFInd);
			}
			stat_farmNum.addSample(framNum, "video_"+videoIndex_in_s);
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** save fInd_vIndinS into HDFS ***//
			System.out.println("one reducer finished! total processed samples: "+procSamples+" ........"+ General.dispTime ( System.currentTimeMillis()-startTime, "min"));
			System.out.println("stat_farmNum: "+stat_farmNum.getFullStatistics("0"));
			super.setup(context);
	 	}
	}
}
