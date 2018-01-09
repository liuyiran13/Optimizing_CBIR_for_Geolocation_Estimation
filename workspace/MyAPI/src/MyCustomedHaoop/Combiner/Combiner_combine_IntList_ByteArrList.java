package MyCustomedHaoop.Combiner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import MyCustomedHaoop.ValueClass.IntList_ByteArrList;

public class Combiner_combine_IntList_ByteArrList extends Reducer<IntWritable,IntList_ByteArrList,IntWritable,IntList_ByteArrList>  {
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
	
	public void reduce(IntWritable key, Iterable<IntList_ByteArrList> value_list, Context context) throws IOException, InterruptedException {
		
		//*** built vw, sig_ ******//
		ArrayList<Integer> intList=new ArrayList<Integer>(); ArrayList<byte[]> byteArrList=new ArrayList<byte[]>();
		for(Iterator<IntList_ByteArrList> it=value_list.iterator();it.hasNext();){
			IntList_ByteArrList one=it.next(); //can not: it.next().getInt() !!
			for (int i = 0; i < one.getInts().size(); i++) {
				intList.add(one.getInts().get(i));
				byteArrList.add(one.getbyteArrs().get(i));
//				docNum++;
			}
        }
//		testSampNum++;
		context.write(key, new IntList_ByteArrList(intList, byteArrList)); 
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// ***** setup finsihed ***//
//		System.out.println("one combiner finished! testSampNum: "+testSampNum);
//		System.out.println("one combiner finished! total docNum: "+docNum);
		super.setup(context);
 	}
}