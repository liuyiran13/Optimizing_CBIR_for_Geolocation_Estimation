package MyCustomedHaoop.Reducer;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import MyAPI.General.General_Hadoop;
public class Reducer_InOut {
	
	public static class Reducer_InOut_normal <K extends Writable, V extends Writable> extends Reducer<K,V,K,V>  {
		
		private int sampleNums;
		private int reduceNum;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			sampleNums=0;
			reduceNum=0;
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		public void reduce(K sampleName, Iterable<V> values, Context context) throws IOException, InterruptedException {
			//key: sampleName, value: content
	
			for(Iterator<V> it=values.iterator();it.hasNext();){// loop over		
				V oneSample=it.next();
				context.write(sampleName, oneSample);
				sampleNums++;
			}
			reduceNum++;

		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("read and out finished! total sampleNums:"+sampleNums+", reduceNum:"+reduceNum);
			super.setup(context);
	 	}
	}

	public static class Reducer_InOut_1key_1value <K extends Writable, V extends Writable> extends Reducer<K,V,K,V>  {
		
		private int sampleNums;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			sampleNums=0;
			// ***** setup finsihed ***//
			System.out.println("setup finsihed!");
			super.setup(context);
	 	}
		
		@Override
		public void reduce(K sampleName, Iterable<V> values, Context context) throws IOException, InterruptedException {
			//key: sampleName, value: content

			//******** only one list in values! ************		
			V oneSample=General_Hadoop.readOnlyOneElement(values, sampleName+""); 
			sampleNums++;
			context.write(sampleName, oneSample);
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ***** setup finsihed ***//
			System.out.println("read and out finished! total sampleNums:"+sampleNums);
			super.setup(context);
	 	}
	}

}