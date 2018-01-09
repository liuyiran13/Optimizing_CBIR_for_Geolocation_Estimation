package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class GeoExpansionData_2 implements Writable{
	int groudTSize;
	float ap;
	int[] topDocs;

	public GeoExpansionData_2(int groudTSize, float ap, int[] topDocs) {
		super();
		this.groudTSize=groudTSize;
		this.ap=ap;
		this.topDocs=topDocs;
	}
	
	public GeoExpansionData_2() {
		// do nothing
	}

	public int get_groudTSize() {
		return groudTSize;
	}
	
	public float get_ap() {
		return ap;
	}
	
	public int[] get_topDocs() {
		return topDocs;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(groudTSize);
		out.writeFloat(ap);
		out.writeInt(topDocs.length);
		
		for(int i=0;i<topDocs.length;i++){
			out.writeInt(topDocs[i]);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		
		groudTSize=in.readInt();
		ap=in.readFloat();
		int topDocs_L=in.readInt();
		topDocs=new int[topDocs_L];
		
		for(int i=0;i<topDocs_L;i++){
			topDocs[i]=in.readInt();
		}

	}	
	
}
