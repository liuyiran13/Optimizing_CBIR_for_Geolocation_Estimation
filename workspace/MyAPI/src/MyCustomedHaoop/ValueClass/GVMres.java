package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import MyAPI.Obj.DataInOutput_Functions;


public class GVMres implements Writable {
	public int groundTrueSize;
	public int topVisRankedGTruthNum;
	public int[] firstTrueRank;
	
	public GVMres(int groundTrueSize, int topVisRankedGTruthNum, int[] firstTrueRank) {
		super();
		this.groundTrueSize =groundTrueSize;
		this.topVisRankedGTruthNum = topVisRankedGTruthNum;
		this.firstTrueRank = firstTrueRank;
	}
	
	public GVMres() {
		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeInt(groundTrueSize);
		out.writeInt(topVisRankedGTruthNum);
		DataInOutput_Functions.writeIntArr(firstTrueRank, out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		groundTrueSize = in.readInt();
		topVisRankedGTruthNum = in.readInt();
		firstTrueRank=DataInOutput_Functions.readIntArr(in);
	}

}
