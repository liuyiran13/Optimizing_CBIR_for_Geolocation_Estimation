package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;

import MyAPI.Obj.DataInOutput_Functions;

public class FeatIDFs implements Writable{

	Integer[] featInds;
	Float[] IDFs;
			
	public FeatIDFs(Integer[] featInds, Float[] IDFs) {
		this.featInds=featInds;
		this.IDFs=IDFs;
	}
	
	public FeatIDFs(List<Integer> featInds, List<Float> IDFs) {
		this.featInds=featInds.toArray(new Integer[0]);
		this.IDFs=IDFs.toArray(new Float[0]);;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		featInds = DataInOutput_Functions.readINTArr(in);
		IDFs = DataInOutput_Functions.readFLOATArr(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		DataInOutput_Functions.writeINTArr(featInds, out);
		DataInOutput_Functions.writeFLOATArr(IDFs, out);
	}
	
	

}
