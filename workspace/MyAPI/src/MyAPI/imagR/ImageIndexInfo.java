package MyAPI.imagR;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;
import MyAPI.General.General_Hadoop;
import MyAPI.Obj.DataInOutput_Functions;
import MyAPI.Obj.Statistics;
import MyCustomedHaoop.ValueClass.DocInfo;
import MyCustomedHaoop.ValueClass.ArrWritableClassCollection.TVector_Arr;

public class ImageIndexInfo{//docID should in S
	
	//********************** para ******************
	private boolean isIDF1VW1FeatVectorNorm;
	
	//********************** data ******************
	//docInfo
	public int totDocNum;
	public int maxDocID;
	public float[] docNorms;
	public short[] doc_maxDim; //for HPM
	//TVector
	public float[] idf_squre;
	//statistics of the whole index 
	private StringBuffer stat_index;
		
	public ImageIndexInfo(String iniR_scheme){
		stat_index=new StringBuffer();
		isIDF1VW1FeatVectorNorm=isIDF1VW1FeatVectorNorm(iniR_scheme);
		stat_index.append("isIDF1VW1FeatVectorNorm:"+isIDF1VW1FeatVectorNorm);
	}
			
	public ImageIndexInfo(Conf_ImageR conf_ImageR) throws InterruptedException, IOException{
		this(conf_ImageR.sd_iniR_scheme, conf_ImageR.conf, Conf_ImageR.sd_docInfo, Conf_ImageR.sd_TVectorInfo);
	}
			
	public ImageIndexInfo(String iniR_scheme, Configuration conf, String docInfoPath, String TVectorInfoPath) throws IOException, InterruptedException {
		//make imgIndexInfo from disk-index
		this(iniR_scheme);
		readIndexInfosFromFile(conf, docInfoPath, TVectorInfoPath);
	}
	
	public boolean isIDF1VW1FeatVectorNorm(String iniR_scheme){
		return iniR_scheme.split("@")[0].equalsIgnoreCase("_iniR-ASMK") || iniR_scheme.split("@")[0].equalsIgnoreCase("_iniR-1vw1match");
	}
	
	protected void makeIDF_Squre(TVector_Arr tVectors, int totDocNum){
		idf_squre=tVectors.makeIDFsquare(totDocNum);
	}
	
	protected void readIndexInfosFromFile(Configuration conf, String docInfoPath, String TVectorInfoPath) throws IOException, InterruptedException{
		LinkedList<DID_DocInfo> did_docInfos=null; int[][] TVectorInfo=null;
		//get totDocNum, maxDocID
		if (docInfoPath!=null && new File(docInfoPath).exists()) {
			did_docInfos=new LinkedList<>();
			SequenceFile.Reader docInfoReader=General_Hadoop.openSeqFileInNode(docInfoPath, conf, true);
			IntWritable docID=new IntWritable(); DocInfo docInfo=new DocInfo();
			while (docInfoReader.next(docID, docInfo)) {
				did_docInfos.add(new DID_DocInfo(docID.get(), new DocInfo(docInfo)));
			}docInfoReader.close();
			stat_index.append(", read LinkedList<DID_DocInfo> did_docInfos finished, total doc number:"+did_docInfos.size());
			getDocSideInfo(did_docInfos);
		}
		//get TVector IDF
		if (TVectorInfoPath!=null && new File(TVectorInfoPath).exists()) {
			TVectorInfo = (int[][]) General.readObject(TVectorInfoPath); //photoNum,featNum
			stat_index.append(", read int[][] TVectorInfo finished, total vw number:"+TVectorInfo.length);
			idf_squre=General_BoofCV.make_idf_squre(TVectorInfo, totDocNum);
		}		
		System.out.println("stat_index: "+stat_index);
	}
	
	protected void getDocSideInfo(LinkedList<DID_DocInfo> did_docInfos) throws InterruptedException{
		if (did_docInfos!=null) {
			//get totDocNum, maxDocID
			totDocNum=0; maxDocID=0; Statistics<Integer> docFeatNumStat=new Statistics<Integer>(3);
			for(DID_DocInfo did_docInfo:did_docInfos){
				totDocNum++;
				maxDocID=Math.max(maxDocID, did_docInfo.DID);
				docFeatNumStat.addSample(did_docInfo.docInfo.pointNum, did_docInfo.DID);
			}
			stat_index.append(", docFeatPointNumInfo: "+docFeatNumStat.getFullStatistics("0", false));
			//get doc_maxDim, doc_FeatVectorNormSqure
			doc_maxDim=new short[maxDocID+1]; docNorms=new float[maxDocID+1];
			if (isIDF1VW1FeatVectorNorm) {
				for(DID_DocInfo did_docInfo:did_docInfos){
					doc_maxDim[did_docInfo.DID]=(short) Math.max(did_docInfo.docInfo.height, did_docInfo.docInfo.width);
					docNorms[did_docInfo.DID]=did_docInfo.docInfo.IDF1VW1FeatVectorNorm;
				}
			}else {
				for(DID_DocInfo did_docInfo:did_docInfos){
					doc_maxDim[did_docInfo.DID]=(short) Math.max(did_docInfo.docInfo.height, did_docInfo.docInfo.width);
					docNorms[did_docInfo.DID]=did_docInfo.docInfo.IDFBoVWVectorNorm;
				}
			}
		}
	}
	
	public void saveIndexInfoOnDisk(DataOutputStream outPut) throws FileNotFoundException, IOException{
		outPut.writeInt(totDocNum);
		outPut.writeInt(maxDocID);
		DataInOutput_Functions.writeFloatArr(docNorms, outPut);
		DataInOutput_Functions.writeShortArr(doc_maxDim, outPut);
		DataInOutput_Functions.writeFloatArr(idf_squre, outPut);
		DataInOutput_Functions.writeString(getIndexStat(), outPut);
	}
	
	public void loadIndexInfoFromDisk(DataInputStream inPut) throws IOException {
		totDocNum=inPut.readInt();
		maxDocID=inPut.readInt();
		docNorms=DataInOutput_Functions.readFloatArr(inPut);
		doc_maxDim=DataInOutput_Functions.readShortArr(inPut);
		idf_squre=DataInOutput_Functions.readFloatArr(inPut);
		stat_index=new StringBuffer(DataInOutput_Functions.readString(inPut));
	}
	
	public String getIndexStat(){
		return  stat_index.toString();
	}
}
