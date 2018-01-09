package Gist;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;
import MyAPI.General.General_Hadoop;
import MyAPI.General.General_IR;
import MyAPI.Obj.Disp;
import MyAPI.Obj.GistParam;
import MyCustomedHaoop.Mapper.SelectSamples;
import MyCustomedHaoop.Partitioner.Partitioner_random;
import MyCustomedHaoop.Reducer.Reducer_InOut.Reducer_InOut_1key_1value;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
import MyCustomedHaoop.ValueClass.FloatArr;


public class MapRed_extractGistFeat extends Configured implements Tool{

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
	 * MEval13:	hadoop jar MapRed_extractGistFeat.jar Gist.MapRed_extractGistFeat -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar,commons-math3-3.2.jar,jtransforms-2.4.jar,MyAPI.jar -Dmapred.task.timeout=60000000000 -Dmapred.selectPhotosPath=MediaEval13/MEval13_L_to_S.hashMap -Dmapred.selectPhotosPath_grouped=MediaEval13/MEval13_L_to_S 66M_Phos_Seqs MediaEval13/Global_PhoFeats/Gist 3000 0
	 */
	
	public static void main(String[] args) throws Exception {		
//		prepareData();
		
		runHadoop(args);
	}

	public static void runHadoop(String[] args) throws Exception {
		int ret = ToolRunner.run(new MapRed_extractGistFeat(), args);
		System.exit(ret);
	}
	
	@SuppressWarnings("unchecked")
	public static void prepareData() throws Exception {
		
		//***** for making sub-photo-set for 9M dataset ************
		int subSetSize=1000*1000;
		HashMap<Integer, Integer> totQ=(HashMap<Integer, Integer>) General.readObject("O:/MediaEval13/MEval13_L_to_S.hashMap");
		String folderPath="O:/MediaEval13/MEval13_L_to_S/";
		General.makeORdelectFolder(folderPath);
		Random rand=new Random();
		ArrayList<HashMap<Integer, Integer>> Qsets =General.randSplitHashMap(rand, totQ, 0, subSetSize);
		int totQnum=0;
		for (int i = 0; i < Qsets.size(); i++) {
			General.writeObject(folderPath+i, Qsets.get(i));
			System.out.println(i+", "+Qsets.get(i).size());
			totQnum+=Qsets.get(i).size();
		}
		General.Assert(totQnum==totQ.size(), "err, totQnum:"+totQnum+", should =="+totQ.size());
		System.out.println("taget subSetSize:"+subSetSize+", totQnum:"+totQnum+", should =="+totQ.size());
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem hdfs=FileSystem.get(conf);
		String homePath="hdfs://p-head03.alley.sara.nl/user/yliu/";
		String[] otherArgs = args; //use this to parse args!
		
		//set imagesPath
		String imagesPath=otherArgs[0];
		System.out.println("imagesPath:"+imagesPath);
		ArrayList<Path> imageSeqPaths = General_Hadoop.addImgPathsFromMyDataSet(imagesPath);
		System.out.println("imageSeqPaths:"+imageSeqPaths);
		//set selected querys set
		ArrayList<String> selPhotos=new ArrayList<String>(); 
		String photoHashMapPath=homePath+conf.get("mapred.selectPhotosPath_grouped");
		if (hdfs.isFile(new Path(photoHashMapPath))) {
			selPhotos.add(photoHashMapPath);
		}else {
			FileStatus[] files= hdfs.listStatus(new Path(photoHashMapPath));
			for (int i = 0; i < files.length; i++) {
				selPhotos.add(files[i].getPath().toString());
			}
		}
		System.out.println("selPhotos:\n"+selPhotos+"\n");
				
		//set input/output path
		String out=otherArgs[1]; //output path
		System.out.println("out:"+out);
		//set reducer number
		int job1RedNum=Integer.valueOf(otherArgs[2]); //reducer number
		System.out.println("job1RedNum:"+job1RedNum);
		//set start loop
		int startLoop=Integer.valueOf(otherArgs[3]); //reducer number
		System.out.println("startLoop:"+startLoop);
		//set distributed cache
		ArrayList<String> cacheFilePaths=new ArrayList<String>();
		
		//******* 0st job: get most common size ******
		cacheFilePaths.clear();
		cacheFilePaths.add(homePath+conf.get("mapred.selectPhotosPath")+"#SelSamples.file"); //SelSamples path with symLink
		String commonSizePath=homePath+out+"_temp_commonSize.InfoStr";
		conf.set("mapred.commonSizePath", commonSizePath);
		if (startLoop==0) {
			General_Hadoop.Job(conf, imageSeqPaths.toArray(new Path[0]), null, "getCommonSize", 1, 8, 2, true,
					MapRed_extractGistFeat.class, Mapper_getImageSize.class, null, null, null, Reducer_getImageSize.class,
					IntWritable.class, IntWritable.class, IntWritable.class, IntWritable.class,
					SequenceFileInputFormat.class, NullOutputFormat.class, 10*1024*1024*1024L, 0,
					cacheFilePaths.toArray(new String[0]),null);
		}
		String Info=(String) General_Hadoop.readObject_HDFS(hdfs, commonSizePath);
		conf.set("mapred.imgSize",Info);
		
		//******* 1st job: run for each feat ******
		ArrayList<Path> res=new ArrayList<Path>();
		for (int i = 0; i < selPhotos.size(); i++) {
			String loopLabel="_"+i+"_"+(selPhotos.size()-1);	
			String tempFeat=out+"_Seq"+loopLabel;
			if (i>=startLoop) {
				SelectSamples selectSamples=new SelectSamples(selPhotos.get(i), false);
				cacheFilePaths.clear();
				selectSamples.addDistriCache_SelectSamples(cacheFilePaths); //SelSamples path with symLink
				General_Hadoop.deleteIfExist(tempFeat, hdfs);
				General_Hadoop.Job(conf, imageSeqPaths.toArray(new Path[0]), tempFeat, "getTrainFeat"+loopLabel, job1RedNum, 8, 2, true,
						MapRed_extractGistFeat.class, selectSamples.getMapper(), Partitioner_random.class,null, null,Reducer_extractFeat_job1.class,
						IntWritable.class, BufferedImage_jpg.class, IntWritable.class,FloatArr.class,
						SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 10*1024*1024*1024L, 0,
						cacheFilePaths.toArray(new String[0]),null);
			}
			res.add(new Path(tempFeat)); 
		}
		//combine & save into one mapfile
		String finialFeat=out+"_MFile";
		General_Hadoop.Job(conf, res.toArray(new Path[0]), finialFeat, "combineTrainFeat", 1, 8, 2, true,
				MapRed_extractGistFeat.class, null, null,null, null,Reducer_InOut_1key_1value.class,
				IntWritable.class, FloatArr.class, IntWritable.class,FloatArr.class,
				SequenceFileInputFormat.class, MapFileOutputFormat.class, 0, 10,
				null,null);
		
		//clean-up
		hdfs.delete(new Path(commonSizePath), true);
		for (Path path: res) {
			hdfs.delete(path, true);
		}		
		hdfs.close();
		return 0;
	}

	//****************** job_0, get photo common size  ************************
	public static class Mapper_getImageSize extends Mapper<IntWritable,BufferedImage_jpg,IntWritable,IntWritable>{
		
		private HashMap<Integer,Integer> selectedSamples;
		private int procSamples;
		private int procSelSamples;
		private int dispInter;
		private long startTime, endTime;
		
		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			//***** read selected Samples ***//
			selectedSamples= (HashMap<Integer, Integer>)  General.readObject("SelSamples.file");
			System.out.println("total selected Samples:"+selectedSamples.size());
			//set procSamples
			procSamples=0;
			procSelSamples=0;
			//set dispInter
			dispInter=5000;
			startTime=System.currentTimeMillis(); //startTime
			
			System.out.println("mapper setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		protected void map(IntWritable key, BufferedImage_jpg value, Context context) throws IOException, InterruptedException {
			//key: SampleName in L-index
			//value: file content
			procSamples++;
			int SampleName=key.get();// SampleName
			if(selectedSamples.containsKey(SampleName)){
				procSelSamples++;
				context.write(new IntWritable(value.getBufferedImage("SampleName:"+SampleName, Disp.getNotDisp()).getHeight()), new IntWritable(value.getBufferedImage("SampleName:"+SampleName, Disp.getNotDisp()).getWidth()));
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
			super.setup(context);
	 	}
	}

	public static class Reducer_getImageSize extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>  {
		
		private int maxNum, commonHeight, commonWidth;
		private int phoNum;
		private int reduceNum;
		private ArrayList<int[]> sizes;
		private ArrayList<Float> nums;
		private int dispInter;
		private long startTime, endTime;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			
			maxNum=0; commonHeight=0; commonWidth=0;
			
			//set procPhotos
			phoNum=0;
			reduceNum=0;
			sizes=new ArrayList<int[]>(1000);
			nums=new ArrayList<Float>(1000);
			//set dispInter
			dispInter=10;
			startTime=System.currentTimeMillis(); //startTime
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		public void reduce(IntWritable height, Iterable<IntWritable> widths, Context context) throws IOException, InterruptedException {
			//key: height, value: widths

			//******** widthNum ************		
			HashMap<Integer, Integer> widthNum=new HashMap<Integer, Integer>();
			for(Iterator<IntWritable> it=widths.iterator();it.hasNext();){// loop over all HashMaps				
				IntWritable one=it.next();
				General.updateMap(widthNum, one.get(), 1);
				phoNum++;
			}

			int this_maxNum=0; int this_commonWidth=-1;
			for (Entry<Integer, Integer> one : widthNum.entrySet()) {
				if (one.getValue()>this_maxNum) {
					this_maxNum=one.getValue();
					this_commonWidth=one.getKey();
				}
				sizes.add(new int[]{height.get(),one.getKey()});
				nums.add((float)one.getValue());
			}
			
			//updata common size
			if (this_maxNum>maxNum) {
				maxNum=this_maxNum;
				commonHeight=height.get();
				commonWidth=this_commonWidth;
			}

			//disp
			reduceNum++;
    		if((reduceNum)%dispInter==0){ 							
				endTime=System.currentTimeMillis(); //end time 
				System.out.println(reduceNum+ " reduce finished!! ......"+ General.dispTime (endTime-startTime, "min"));
			}
			
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			endTime=System.currentTimeMillis(); //end time 
			System.out.println("one reducer finished! total phoNum: "+phoNum+", different size num:"+sizes.size()+" ....."+ General.dispTime ( endTime-startTime, "min"));
			ArrayList<int[]> sizes_top=new ArrayList<int[]>(1000);
			ArrayList<Float> nums_top=new ArrayList<Float>(1000);
			General_IR.rank_get_TopDocScores_treeSet(sizes, nums, Math.min(10, sizes.size()), sizes_top, nums_top, "DES");
			System.out.println("top sizeInfo:");
			for (int i = 0; i < sizes_top.size(); i++) {
				System.out.print(General.IntArrToString(sizes_top.get(i), "-")+nums_top.get(i)+", ");
			}
			System.out.println();
			System.out.println("most commonHeight:"+commonHeight+", commonWidth:"+commonWidth+", number:"+maxNum);
			Configuration conf = context.getConfiguration();
			General_Hadoop.writeObject_HDFS(FileSystem.get(conf), conf.get("mapred.commonSizePath"), commonHeight+"_"+commonWidth);
			super.setup(context);
	 	}
	}
	
	//****************** job_1, get photo feat  ************************
	public static class Reducer_extractFeat_job1 extends Reducer<IntWritable,BufferedImage_jpg,IntWritable,FloatArr>  {
		
		private GistParam gistParam;
		private int procPhotos;
		private int dispInter;
		private long startTime, endTime;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//***** read target imageSize ***//
			int[] imageSize = General.StrArrToIntArr(conf.get("mapred.imgSize").split("_"));
			System.out.println("target imageSize: "+conf.get("mapred.imgSize"));
			
			gistParam=new GistParam(imageSize);
			System.out.println("gistParam: "+gistParam.toString());
			
			//set procPhotos
			procPhotos=0;
			//set dispInter
			dispInter=200;
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
				//feat extract in float[]
				float[] gist=General_BoofCV.LMGist(onePhoto.getBufferedImage("photoName:"+photoName, Disp.getNotDisp()), gistParam);
				//output
				context.write(photoName, new FloatArr(gist));
				//disp
				procPhotos++;
	    		if((procPhotos)%dispInter==0){ 							
					endTime=System.currentTimeMillis(); //end time 
					System.out.println( "select photos, "+procPhotos+" photos, feat extraction finished!! ......"+ General.dispTime (endTime-startTime, "min"));
					System.out.println( "current photo: "+photoName+", gist:"+ General.floatArrToString(gist, ",", "0.0000"));
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
