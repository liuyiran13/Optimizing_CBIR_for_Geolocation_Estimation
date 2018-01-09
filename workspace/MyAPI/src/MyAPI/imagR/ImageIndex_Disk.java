package MyAPI.imagR;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import MyCustomedHaoop.ValueClass.TVector;

public class ImageIndex_Disk extends ImageIndex{//TVectors are not in-memory, everytime read from disk
	
	//this needs a lot of IO, so is very slow!!
	
	//********************** data ******************
	private String TVectorPath;
	private int VWFileInter;
		
	public ImageIndex_Disk(String iniR_Scheme, Configuration conf, String docInfoPath, String TVectorInfoPath, String TVectorPath, int VWFileInter) throws InterruptedException, IOException {
		this.TVectorPath=TVectorPath;
		this.VWFileInter=VWFileInter;
		indexInfo=new ImageIndexInfo(iniR_Scheme, conf, docInfoPath, TVectorInfoPath);
	}

	@Override
	public TVector getOneTVector(int vw) throws IllegalArgumentException, IOException {
		TVector_Hadoop tVector=new TVector_Hadoop(new Configuration(),TVectorPath, VWFileInter, false);
		int TVectorFeatNum=tVector.readTVectorIntoMemory(vw);	
		if (TVectorFeatNum>0) {
			return tVector.tVector;
		}else {
			return null;
		}
		
	}
	
}
