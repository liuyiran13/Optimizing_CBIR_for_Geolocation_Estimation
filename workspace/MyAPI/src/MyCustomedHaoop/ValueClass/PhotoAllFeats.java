package MyCustomedHaoop.ValueClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

import MyAPI.General.General_BoofCV;
import MyAPI.Obj.DataInOutput_Functions;

public class PhotoAllFeats implements Writable{
	public int width;
	public int height;
	public SURFpointVWs[] feats;
	public VW_AggSig[] vw_aggSig;
	
	public PhotoAllFeats(int width, int height, SURFpointVWs[] feats, VW_AggSig[] vw_aggSig) {
		super();
		this.width = width;
		this.height = height;
		this.feats = feats;
		this.vw_aggSig = vw_aggSig;
	}
	
	public PhotoAllFeats(int width, int height, List<SURFpointVWs> feats, List<VW_AggSig> vw_aggSig) {
		super();
		this.width = width;
		this.height = height;
		this.feats = feats.toArray(new SURFpointVWs[0]);
		this.vw_aggSig = vw_aggSig.toArray(new VW_AggSig[0]);
	}

	public PhotoAllFeats() {
		// do nothing
	}
	
	public void set(int width, int height, SURFpointVWs[] feats, VW_AggSig[] vw_aggSig) {
		this.width = width;
		this.height = height;
		this.feats = feats;
		this.vw_aggSig = vw_aggSig;
	}
	
	@Override
	public String toString() {
		return "width:"+width+", height:"+height+", feat's num: "+feats.length+", assigned vwNum:"+getTotVWNum()+", unque vw num:"+vw_aggSig.length;
	}
	
	public HashMap<Integer,HESig_ShortArr_AggSig> group_VW_HESigAggSig(){
		HashMap<Integer, ArrayList<HESig>> vw_HESigs=new HashMap<>();
		General_BoofCV.group_VW_HESig(vw_HESigs, feats);
		HashMap<Integer, byte[]> vw_aggSig=groupVW_AggSig();
		//combine
		HashMap<Integer, HESig_ShortArr_AggSig> res=new HashMap<>();
		for (Entry<Integer, ArrayList<HESig>> oneVW : vw_HESigs.entrySet()) {
			res.put(oneVW.getKey(), new HESig_ShortArr_AggSig(oneVW.getValue(), vw_aggSig.get(oneVW.getKey())));
		}
		return res;
	}
	
	public HashMap<Integer,SURFfeat_ShortArr_AggSig> group_VW_SURFfeatAggSig(){
		HashMap<Integer,ArrayList<SURFfeat>> vw_feats=new HashMap<>();
		General_BoofCV.group_VW_SURFfeat(vw_feats, feats);//feat num, vw num, mutiAssNum, uniqueVW num
		HashMap<Integer,byte[]> vw_aggSig=groupVW_AggSig();
		//combine
		HashMap<Integer,SURFfeat_ShortArr_AggSig> res=new HashMap<>();
		for (Entry<Integer, ArrayList<SURFfeat>> oneVW : vw_feats.entrySet()) {
			res.put(oneVW.getKey(), new SURFfeat_ShortArr_AggSig(oneVW.getValue(), vw_aggSig.get(oneVW.getKey())));
		}
		return res;
	}
	
	public HashMap<Integer,byte[]> groupVW_AggSig(){
		HashMap<Integer, byte[]> res=new HashMap<>();
		for (VW_AggSig one : vw_aggSig) {
			res.put(one.vw, one.aggSig);
		}
		return res;
	}
	
	public ArrayList<SURFpoint> group_InterestPoints(){
		return General_BoofCV.group_InteresPoints(feats);
	}
	
	public float computeBagOfVWVectorNorm(){
		return General_BoofCV.computeBagOfVWVectorNorm(group_VW_SURFfeatAggSig());
	}
	
	public float computeIDFBagOfVWVectorNorm(float[] idf_square){
		return General_BoofCV.computeIDFBagOfVWVectorNorm(group_VW_SURFfeatAggSig(), idf_square);
	}
	
	public float computeIDF1VW1FeatVectorNorm(float[] idf_square){
		return General_BoofCV.computeIDF1VW1FeatVectorNorm(group_VW_SURFfeatAggSig(), idf_square);
	}
	
	public DocInfo getDocInfo(float[] idf_square){
		return new DocInfo((short)feats.length, computeBagOfVWVectorNorm(), computeIDFBagOfVWVectorNorm(idf_square), computeIDF1VW1FeatVectorNorm(idf_square), (short)width, (short)height);
	}
	
	public PhotoSize getPhotoSize(){
		return new PhotoSize(width, height);
	}
	
	public int getTotVWNum(){
		int vwNum=0;
		for (SURFpointVWs one : feats) {
			vwNum+=one.vws.ObjArr.length;
		}
		return vwNum;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		//write out width and height
		out.writeInt(width);
		out.writeInt(height);
		//write out feats
		DataInOutput_Functions.writeArr(feats, out);
		//write out vw_aggSig
		DataInOutput_Functions.writeArr(vw_aggSig, out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		//read width
		width=in.readInt();
		height=in.readInt();
		//read feats
		feats=DataInOutput_Functions.readArr(in, SURFpointVWs.class);
		//read feats
		vw_aggSig=DataInOutput_Functions.readArr(in, VW_AggSig.class);
	}
	
}
