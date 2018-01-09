package SelectPhoto;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyAPI.General.General;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
import MyCustomedHaoop.ValueClass.ZipFileInputFormat;

public class MapRed_saveZipPhotoToSeqFile extends Configured implements Tool{

	/**
	 * job1: save photos from zip file to MapFile
	 * mapper: read photo 
	 * partitioner: divide photos to multiple reducers
	 * reducer: save photos to Sequence File
	 * 	@param 
	 * 
	 * 
	 * hadoop jar MapRed_saveZipPhotoToMapFile.jar SelectPhoto.MapRed_saveZipPhotoToMapFile -libjars mahout-core-0.8-SNAPSHOT-job.jar,BoofCV.jar,EJML.jar,GeoRegression.jar,libpja.jar,MyAPI.jar 30 imageZips 3M_Photos_SeqFile
	 */
	
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new MapRed_saveZipPhotoToSeqFile(), args);
		System.exit(ret);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String[] otherArgs = args;
		
		int job1RedNum=Integer.valueOf(otherArgs[0]);
		System.out.println("job1RedNum:"+job1RedNum);
		String In=otherArgs[1];
		System.out.println("In:"+In);
		String out=otherArgs[2];
		System.out.println("out:"+out);
		//******* 1st job: save photos ******
		Job1(conf, In, out, "saveSeq",job1RedNum);
		
		return 0;
		
		
	}
	
	public void Job1(Configuration conf, String inPath, String outPath, String JobName, int jobRedNum) throws IOException, InterruptedException, ClassNotFoundException{
		Job job = new Job(conf, JobName); 
		//define which jar to find all class in job config (classes below) 
		job.setJarByClass(MapRed_saveZipPhotoToSeqFile.class);
		//set mapper, reducer
		job.setMapperClass(Mapper_readZipPhotos.class);  
		job.setReducerClass(Reducer_savePhotosToSeq.class);
		job.setNumReduceTasks(jobRedNum);
		//set mapper out-put Key_Value
		job.setMapOutputKeyClass(IntWritable.class); //if not set, MapOutputKeyClass will be OutputKeyClass
		job.setMapOutputValueClass(BufferedImage_jpg.class);
		//set job out-put Key_Value
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(BufferedImage_jpg.class);
		//set job in/out FileClass
		job.setInputFormatClass(ZipFileInputFormat.class); //KeyValueTextInputFormat, ZipFileInputFormat
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//set job in/out file Path
		FileInputFormat.setInputPaths(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		if (!job.waitForCompletion(true)) {
	        throw new InterruptedException("Job Failed! job: "+job.getJobName());
		}
	}
	
	public static class Mapper_readZipPhotos extends Mapper<Text,BytesWritable,IntWritable,BufferedImage_jpg>{

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			//key: file path in zip, NOTE: the filename is the *full* path within the ZIP file, e.g. "subdir1/subsubdir2/Ulysses-18.txt"
			//value: file content
			
			IntWritable mapout_Key=new IntWritable(-1);//"clusterId":0~19999
			BufferedImage_jpg mapout_Value=new BufferedImage_jpg(); //"photoName&HESing"
		
	        String filename = key.toString();//Photo_1.jpg
//			        // We only want to process .txt files
//			        if ( filename.endsWith(".txt") == false )
//			            return;
	        // Prepare the content 
	        BufferedImage img= ImageIO.read(new ByteArrayInputStream(value.getBytes()));
	    	// extract visual word
			if( img == null ){
				System.err.println("no image for "+filename);
				return;
			}else{
				int photoName=Integer.valueOf(filename.split("_")[1].split("\\.")[0]);//only keep photoName, Photo_1.jpg --> 1
				mapout_Key.set(photoName);
				mapout_Value.setBufferedImage(img,"jpg");
		        context.write(mapout_Key, mapout_Value);
			}
				
		}
	}

	public static class Reducer_savePhotosToSeq extends Reducer<IntWritable,BufferedImage_jpg,IntWritable,BufferedImage_jpg>  {

		private int processedPhotos;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			processedPhotos=0;
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		protected void reduce(IntWritable imgName, Iterable<BufferedImage_jpg> imgs, Context context) throws IOException, InterruptedException {
			//NameSigs: Name-Integer, Sig-ByteArr
			BufferedImage_jpg img=new BufferedImage_jpg(); int loopNum=0;
			for(Iterator<BufferedImage_jpg> it=imgs.iterator();it.hasNext();){
				img=it.next();
				loopNum++;
	        }		
			General.Assert(loopNum==1, "error in Reducer_savePhotosToSeq! loopNum should == 1, here loopNum="+loopNum);
			processedPhotos++;
			context.write(imgName, img); //outputfile: SeqFile;	outputFormat: key_IntWritable-->(imgName)  value_BufferedImage_jpg-->(img)
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("one reducer finsihed! processedPhotos:"+processedPhotos);
			super.setup(context);
	 	}

	}

	
}
