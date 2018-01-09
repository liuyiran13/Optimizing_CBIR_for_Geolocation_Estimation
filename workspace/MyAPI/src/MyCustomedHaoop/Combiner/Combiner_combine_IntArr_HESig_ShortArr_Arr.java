package MyCustomedHaoop.Combiner;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import MyAPI.General.General;
import MyCustomedHaoop.ValueClass.ArrWritableClassCollection.HESig_ShortArr_Arr;
import MyCustomedHaoop.ValueClass.ComposeWritableClassCollection.IntArr_HESig_ShortArr_Arr;
import MyCustomedHaoop.ValueClass.HESig_ShortArr_AggSig;
import MyCustomedHaoop.ValueClass.IntArr;


public class Combiner_combine_IntArr_HESig_ShortArr_Arr extends Reducer<IntWritable,IntArr_HESig_ShortArr_Arr,IntWritable,IntArr_HESig_ShortArr_Arr>  {
	/**
	 * when use combiner, combiner's input/output class should be the same and equal to mapper's output class!! 
	 * combiner is performed in the mapping phase, mappper-partitioner-combiner, each combiner maybe called multiple time!
	 * combiner is also performed in the beginning of reducing phase to combine same key's values from different mapper.
	 * @throws IOException 
	 */
//	private boolean disp;
//	private int sampleNum;
//	private int groupNum;
//	private int keyNum;
//	private StringBuffer keys;
	
	protected void setup(Context context) throws InterruptedException, IOException {
		// ***** setup finsihed ***//
//		disp=true;
//		sampleNum=0;
//		groupNum=0;
//		keyNum=0;
//		keys=new StringBuffer();
//		System.out.println("Combiner_combine_IntArrArr_SURFfeat_ShortArr_Arr setup finished!!");
		super.setup(context);
 	}
	
	public void reduce(IntWritable VW, Iterable<IntArr_HESig_ShortArr_Arr> values, Context context) throws IOException, InterruptedException {
		/**
		 * attention: the combiner may be called mutiple times, and may combine two already combined list! so photo number in "one" is not necessarily 1 !!
		 */
		//QueryName_Sigs: QueryName-Integer, Sigs:-ByteArrList

		//*** built vw's vextor: photoNames, sigs  ******//
		LinkedList<Integer> intList=new LinkedList<Integer>(); LinkedList<HESig_ShortArr_AggSig> featArrList=new LinkedList<HESig_ShortArr_AggSig>();
		for(Iterator<IntArr_HESig_ShortArr_Arr> it=values.iterator();it.hasNext();){
			IntArr_HESig_ShortArr_Arr one=it.next(); //can not: it.next().getInt() !!
			General.Assert(one.obj_1.getIntArr().length==one.obj_2.getArr().length
					, "err in Combiner_combine_IntArr_HESig_ShortArr_Arr: not equal! one.obj_1.getIntArr().length:"+one.obj_1.getIntArr().length
					+", one.obj_2.getArr().length:"+one.obj_2.getArr().length);
			int featNum=one.obj_1.getIntArr().length;
			for (int i = 0; i < featNum; i++) {
				int photoName=one.obj_1.getIntArr()[i];//photoName, 
				HESig_ShortArr_AggSig feats=one.obj_2.getArr()[i]; //sigs, 
				//add this photoName-sigs list
				intList.add(photoName);
				featArrList.add(feats);
//				//for debug
//				sampleNum++;
			}
//			//for debug
//			groupNum++;
        }
		General.Assert(intList.size()==featArrList.size(), "not equal!  intList.size():"+intList.size()+", featArrList.size():"+featArrList.size());
		IntArr_HESig_ShortArr_Arr value=new IntArr_HESig_ShortArr_Arr(new IntArr(intList),new HESig_ShortArr_Arr(featArrList));
		context.write(VW, value); //key_IntWritable-->(vw)  value_IntArr_SURFfeat_ShortArr_Arr-->(photoNames,sigs)
//		//for debug
//		keys.append(VW.get()+",");
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