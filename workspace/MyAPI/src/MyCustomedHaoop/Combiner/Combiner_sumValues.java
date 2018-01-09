package MyCustomedHaoop.Combiner;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Combiner_sumValues extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>  {
	/**
	 * when use combiner, combiner's input/output class should be the same and equal to mapper's output class!! 
	 * combiner is performed in the mapping phase, mappper-partitioner-combiner, each combiner maybe called multiple time!
	 * combiner is also performed in the beginning of reducing phase to combine same key's values from different mapper.
	 */
//	private int keySampNum;
//	private int valueNum;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// ***** setup finsihed ***//
//		System.out.println("Combiner_combine_docs_scores setup finished!!");
//		keySampNum=0;
//		valueNum=0;
		super.setup(context);
 	}
	
	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		//*** sum all values ******//
		int sum=0;
		for(IntWritable one : values){
			sum+=one.get();
//			valueNum++;
        }
//		keySampNum++;
		context.write(key, new IntWritable(sum)); 
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// ***** setup finsihed ***//
//		System.out.println("one combiner finished! testSampNum: "+keySampNum);
//		System.out.println("one combiner finished! total docNum: "+valueNum);
		super.setup(context);
 	}
}