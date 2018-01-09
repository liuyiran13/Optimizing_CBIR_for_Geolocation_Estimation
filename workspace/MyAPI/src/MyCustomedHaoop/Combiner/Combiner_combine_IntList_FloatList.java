package MyCustomedHaoop.Combiner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import MyCustomedHaoop.ValueClass.IntList_FloatList;

public class Combiner_combine_IntList_FloatList extends Reducer<IntWritable,IntList_FloatList,IntWritable,IntList_FloatList>  {
	/**
	 * when use combiner, combiner's input/output class should be the same and equal to mapper's output class!! 
	 * combiner is performed in the mapping phase, mappper-partitioner-combiner, each combiner maybe called multiple time!
	 * combiner is also performed in the beginning of reducing phase to combine same key's values from different mapper.
	 */
//	private int testSampNum;
//	private int docNum;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		// ***** setup finsihed ***//
//		System.out.println("Combiner_combine_docs_scores setup finished!!");
//		testSampNum=0;
//		docNum=0;
		super.setup(context);
 	}
	
	public void reduce(IntWritable testID, Iterable<IntList_FloatList> doc_scores, Context context) throws IOException, InterruptedException {
		
		//*** built docs, scores ******//
		ArrayList<Integer> docs=new ArrayList<Integer>(); ArrayList<Float> scores=new ArrayList<Float>();
		for(Iterator<IntList_FloatList> it=doc_scores.iterator();it.hasNext();){
			IntList_FloatList one=it.next(); //can not: it.next().getInt() !!
			for (int i = 0; i < one.getIntegers().size(); i++) {
				docs.add(one.getIntegers().get(i));
				scores.add(one.getFloats().get(i));
//				docNum++;
			}
        }
//		testSampNum++;
		context.write(testID, new IntList_FloatList(docs, scores)); 
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// ***** setup finsihed ***//
//		System.out.println("one combiner finished! testSampNum: "+testSampNum);
//		System.out.println("one combiner finished! total docNum: "+docNum);
		super.setup(context);
 	}
}