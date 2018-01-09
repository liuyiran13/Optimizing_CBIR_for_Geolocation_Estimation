package MyCustomedHaoop.Mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import MyAPI.Obj.Disp;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
import MyCustomedHaoop.ValueClass.IntList_FloatList;

public class Mapper_read_out{
	
	public static class Mapper_readOut <K,V extends Writable> extends Mapper<K,V,K,V>{
		//do nothing, just read, out
		private boolean disp;
		private int procSamples;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			disp=true; 
			procSamples=0;
			// ***** setup finished ***//
			System.out.println("Mapper_readRank setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		protected void map(K key, V value, Context context) throws IOException, InterruptedException {
			//key: photoName, value: (ranked) docNames_scores	
			if (disp==true){ //debug disp info
				System.out.println("Mapper: do nothing, just read and out");
				System.out.println("key: "+key+", value: "+value);
				disp=false;
			}
			context.write(key, value);
			procSamples++;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one mapper finished! total Samples in this Mapper: "+procSamples);
			super.setup(context);
	 	}
	}
	
	public static class Mapper_readRank extends Mapper<IntWritable,IntList_FloatList,IntWritable,IntList_FloatList>{

		private boolean disp;
		private int procSamples;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			disp=true; 
			procSamples=0;
			// ***** setup finished ***//
			System.out.println("Mapper_readRank setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		protected void map(IntWritable key, IntList_FloatList value, Context context) throws IOException, InterruptedException {
			//key: photoName, value: (ranked) docNames_scores	
			if (disp==true){ //debug disp info
				System.out.println("Mapper: read and out");
				System.out.println("mapIn_Key, queryName: "+key.get());
				System.out.println("mapIn_Value, ranked doc_scores, length: "+value.getIntegers().size());
				System.out.println("mapIn_Value, ranked doc_scores, sample, 1st doc&scores: "+value.getIntegers().get(0)+"_"+value.getFloats().get(0));
				System.out.println("mapIn_Value, ranked doc_scores, sample, 2nd doc&scores: "+value.getIntegers().get(1)+"_"+value.getFloats().get(1));
				disp=false;
			}
			//** output, set key, value **//
			context.write(key, value);
			procSamples++;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one mapper finished! total Samples in this Mapper: "+procSamples);
			super.setup(context);
	 	}
	}
	
	public static class Mapper_read_BufferedImage_jpg extends Mapper<IntWritable,BufferedImage_jpg,IntWritable,BufferedImage_jpg>{

		private boolean disp;
		private int procSamples;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			disp=true; 
			procSamples=0;
			// ***** setup finished ***//
			System.out.println("Mapper_readRank setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		protected void map(IntWritable key, BufferedImage_jpg value, Context context) throws IOException, InterruptedException {
			//key: photoName, value: (ranked) docNames_scores	
			if (disp==true){ //debug disp info
				System.out.println("Mapper: read and out");
				System.out.println("mapIn_Key, SampleName: "+key.get());
				disp=false;
			}
			//** output, set key, value **//
			if (value.getBufferedImage("photo-"+key.get(), Disp.getNotDisp())==null) {
				System.out.println("photo-"+key.get()+"'s image is null! ignor!");
			}else {
				context.write(key, value);
			}
			procSamples++;
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("one mapper finished! total Samples in this Mapper: "+procSamples);
			super.setup(context);
	 	}
	}
}
