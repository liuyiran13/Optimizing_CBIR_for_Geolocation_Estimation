package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.io.Writable;


/**
 * Indexes of two associated features.
 *
 */
public class ImageRegionMatch implements Writable {

	// index of the feature in the source image
	public int src;
	// index of the feature in the destination image
	public int dst;
	// The association score.  Meaning will very depending on implementation
	public float matchScore;

	public ImageRegionMatch( int src, int dst, float matchScore) {
		this.src = src;
		this.dst = dst;
		this.matchScore = matchScore;
	}
	
	public ImageRegionMatch() {
		// do nothing
	}
	
	public void setAssociation(int src , int dst , float matchScore ) {
		this.src = src;
		this.dst = dst;
		this.matchScore = matchScore;
	}

	public void set( ImageRegionMatch a ) {
		src = a.src;
		dst = a.dst;
		matchScore = a.matchScore;
	}
	
	public String toString() {
		return "src:"+src+", dst:"+dst+", matchScore:"+matchScore;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	
	public static LinkedList<Integer> getSrcList(LinkedList<ImageRegionMatch> finalMatches){
		LinkedList<Integer> res=new LinkedList<Integer>();
		for (ImageRegionMatch one : finalMatches) {
			res.add(one.src);
		}
		return res;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(src);
		out.writeInt(dst);
		out.writeFloat(matchScore);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		src = in.readInt();
		dst = in.readInt();
		matchScore = in.readFloat();
	}
}