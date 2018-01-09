package MyCustomedHaoop.ValueClass;

public class IntArr_SURFfeat_ShortArr_Arr0 {
//	//use out.writeShort!! ,so SURFfeat_ShortArr_Arr[i]==SURFfeat_ShortArr its length need to be 0~32767
//	IntArr integers;
//	SURFfeat_ShortArr_Arr feats;
//
//	public IntArr_SURFfeat_ShortArr_Arr0(int[] integers, SURFfeat[][] feats) {
//		super();
//		this.integers = new IntArr(integers);
//		this.feats = new SURFfeat_ShortArr_Arr(feats);
//	}
//	
//	public IntArr_SURFfeat_ShortArr_Arr0(ArrayList<Integer> integersList, ArrayList<SURFfeat_ShortArr> feats) {
//		super();
//		set(integersList, feats);
//	}
//
//	public IntArr_SURFfeat_ShortArr_Arr0() {
//		// do nothing
//	}
//	
//	public void set(ArrayList<Integer> integersList, ArrayList<SURFfeat_ShortArr> feats) {
//		this.integers = new IntArr(integersList);
//		this.feats = new SURFfeat_ShortArr_Arr(feats);
//		
//	}
//	
//	public int[] getIntegers() {
//		return integers.getIntArr();
//	}
//	
//	public SURFfeat_ShortArr[] getFeats() {
//		return feats.getArrArr();
//	}
//
//
//	/* (non-Javadoc)
//	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
//	 */
//	@Override
//	public void write(DataOutput out) throws IOException {
//		//write out IntArr
//		integers.write(out);
//		//write out SURFfeat_ShortArr_Arr
//		feats.write(out);
//	}
//
//	/* (non-Javadoc)
//	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
//	 */
//	@Override
//	public void readFields(DataInput in) throws IOException {
//		//read IntArr
//		integers=new IntArr();
//		integers.readFields(in);
//		//read SURFfeat_ShortArr_Arr
//		feats=new SURFfeat_ShortArr_Arr();
//		feats.readFields(in);
//	}
	
}
