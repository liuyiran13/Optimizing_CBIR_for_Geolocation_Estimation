package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class VW_DID_Score_Arr implements Writable{
	public int vw;
	public DID_Score_Arr docs_scores;
	
	public VW_DID_Score_Arr(int vw, DID_Score_Arr docs_scores) {
		super();
		this.vw = vw;
		this.docs_scores = docs_scores;
	}

	public VW_DID_Score_Arr() {
		// do nothing
	}
	
	public void set(int vw, DID_Score_Arr docs_scores) {
		this.vw = vw;
		this.docs_scores = docs_scores;
	}
	
	public int[] getDocs(){
		DID_Score[] did_scores=docs_scores.getArr();
		int[] docs=new int[did_scores.length];
		for (int i = 0; i < docs.length; i++) {
			docs[i]=did_scores[i].docID;
		}
		return docs;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		//write out vw
		out.writeInt(vw);
		//write out DID_Score_Arr
		docs_scores.write(out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		//read vw
		vw=in.readInt();
		//read DID_Score_Arr
		docs_scores=new DID_Score_Arr();
		docs_scores.readFields(in);
	}
	
}
