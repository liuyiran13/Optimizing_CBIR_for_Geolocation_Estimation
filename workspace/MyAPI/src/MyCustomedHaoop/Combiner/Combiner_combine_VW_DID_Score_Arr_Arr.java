package MyCustomedHaoop.Combiner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import MyCustomedHaoop.ValueClass.VW_DID_Score_Arr;
import MyCustomedHaoop.ValueClass.VW_DID_Score_Arr_Arr;

public class Combiner_combine_VW_DID_Score_Arr_Arr extends Reducer<IntWritable,VW_DID_Score_Arr_Arr,IntWritable,VW_DID_Score_Arr_Arr>  {
	/**
	 * when use combiner, combiner's input/output class should be the same and equal to mapper's output class!! 
	 * combiner is performed in the mapping phase, mappper-partitioner-combiner, each combiner maybe called multiple time!
	 * combiner is also performed in the beginning of reducing phase to combine same key's values from different mapper.
	 */
//	private boolean disp;
//	private int sampleNum;
//	private int groupNum;
//	private int keyNum;
//	private StringBuffer keys;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		// ***** setup finsihed ***//
//		disp=true;
//		sampleNum=0;
//		groupNum=0;
//		keyNum=0;
//		keys=new StringBuffer();
//		System.out.println("Combiner_combine_IntArrArr_SURFfeat_ShortArr_Arr setup finished!!");
		super.setup(context);
 	}
	
	public void reduce(IntWritable queryID, Iterable<VW_DID_Score_Arr_Arr> values, Context context) throws IOException, InterruptedException {
		/**
		 * attention: the combiner may be called mutiple times, and may combine two already combined list! so photo number in "one" is not necessarily 1 !!
		 */

		ArrayList<VW_DID_Score_Arr> List=new ArrayList<VW_DID_Score_Arr>(5000);
		for(Iterator<VW_DID_Score_Arr_Arr> it=values.iterator();it.hasNext();){
			VW_DID_Score_Arr_Arr one=it.next(); //can not: it.next().getInt() !!
			VW_DID_Score_Arr[] one_arrs =one.getArr();
			for (int i = 0; i < one_arrs.length; i++) {
				//add this sample to list
				List.add(one_arrs[i]);
//				//for debug
//				sampleNum++;
			}
//			//for debug
//			groupNum++;
        }
		context.write(queryID, new VW_DID_Score_Arr_Arr(List));
//		//for debug
//		keys.append(queryID.get()+",");
//		keyNum++;
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// ***** setup finsihed ***//
//		System.out.println("one combiner finished! sampleNum: "+sampleNum+", groupNum: "+groupNum+", keyNum: "+keyNum);
//		System.out.println("keys: "+keys.toString());
		super.setup(context);
 	}
}