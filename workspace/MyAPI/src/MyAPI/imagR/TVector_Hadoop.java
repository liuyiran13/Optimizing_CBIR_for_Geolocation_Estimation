package MyAPI.imagR;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;
import MyAPI.General.General_Hadoop;
import MyCustomedHaoop.ValueClass.Int_SURFfeat_ShortArr;
import MyCustomedHaoop.ValueClass.SURFfeat_ShortArr_AggSig;
import MyCustomedHaoop.ValueClass.TVector;

public class TVector_Hadoop{
	
	Configuration conf;
	MulitLevelFileSys mulitLevelFileSys;
	public TVector tVector;
	
	public TVector_Hadoop(Configuration conf, String TVectorPath, int VWFileInter, boolean disp) {
		tVector=new TVector();
		this.conf=conf;
		mulitLevelFileSys=new MulitLevelFileSys(TVectorPath,VWFileInter,".seq", disp);
	}
	
	public void makeTVector(int vw, boolean disp) throws IllegalArgumentException, IOException, InterruptedException{
		tVector.sortByDocID(disp);
		//out-put TVector
		SequenceFile.Writer TVector_Writer=General_Hadoop.createSeqFileWriter(conf, new Path(mulitLevelFileSys.getOneFilePath(vw)), IntWritable.class, SURFfeat_ShortArr_AggSig.class);
		for (Int_SURFfeat_ShortArr one : tVector.docID_feats) {
			TVector_Writer.append(new IntWritable(one.integer), one.feats);
		}
		TVector_Writer.close();
	}
	
	public int readTVectorIntoMemory(int vw) throws IllegalArgumentException, IOException {
		Path thisVWFilePath=new Path(mulitLevelFileSys.getOneFilePath(vw));
		if (FileSystem.get(conf).exists(thisVWFilePath)) {
			SequenceFile.Reader TVector_Reader=new SequenceFile.Reader(conf, SequenceFile.Reader.file(thisVWFilePath));
			IntWritable TVector_key = new IntWritable(vw) ; //docIDs
			SURFfeat_ShortArr_AggSig TVector_value = new SURFfeat_ShortArr_AggSig();//SURFfeat[], aggSig
			//******* read TVector into memory **************
			int TVectorFeatNum=0;
			while (TVector_Reader.next(TVector_key, TVector_value)) {
				tVector.addOneDoc(TVector_key.get(), TVector_value.feats, TVector_value.aggSig);//bug: cannot tVector.addOneDoc(TVector_key.get(), TVector_value); because all docFeats in tVector will save the address of TVector_value!
				TVectorFeatNum+=TVector_value.feats.length;
			}
			TVector_Reader.close();
			return TVectorFeatNum;
		}else{
			System.out.println("\n -Warning!!  for VW-"+vw+", query exist this vw, but no photo in dataset exist this vw, TVector not exist for this vw! "+thisVWFilePath);
			return -1;
		}
	}	
	
	public static void main(String[] args) throws Exception {		
		TVector_Hadoop tVector=new TVector_Hadoop(new Configuration(),"F:/Experiments/SanFrancisco/index/TVector_SanFran_DPCI_QDPCIVW_SIFTUPRightINRIA2_VW20K", 20, true);
		int TVectorFeatNum=tVector.readTVectorIntoMemory(0);
		System.out.println("TVectorFeatNum: "+TVectorFeatNum);
		byte[] aggSig_0=tVector.tVector.docID_feats.get(0).feats.aggSig;
		byte[] aggSig_1=tVector.tVector.docID_feats.get(1).feats.aggSig;
		double[] ASMK_weight=General.makeRange(new double[]{1,-1,-2d/(aggSig_0.length*8)});
		for (int i = 0; i < ASMK_weight.length; i++) {
			ASMK_weight[i]=Math.signum(ASMK_weight[i])*Math.pow(Math.abs(ASMK_weight[i]), 2);
		}
		
		System.out.println(General_BoofCV.computeHESim_fromHESig(aggSig_0, aggSig_1,ASMK_weight));
		System.out.println(General_BoofCV.computeHESim_fromHESig(aggSig_0, aggSig_0,ASMK_weight));
	}

}
