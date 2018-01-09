package MediaEval15;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import MyAPI.General.General;
import MyAPI.General.General_geoRank;
import MyAPI.Geo.groupDocs.LatLon;
import MyCustomedHaoop.KeyClass.Key_RankFlagID_QID;
import MyCustomedHaoop.ValueClass.ComposeWritableClassCollection.QID_IntList_FloatList;
import MyCustomedHaoop.ValueClass.IntList_FloatList;

public class makeTaskRes {

	public static void main(String[] args) throws Exception {
		makeRunRes();
	}

	@SuppressWarnings("unchecked")
	public static void makeRunRes() throws IOException, InterruptedException{
		String saveBasePath="F:/Experiments/MediaEval15/";
		String testSetYear="2015";
        long startTime=System.currentTimeMillis();
		//read s_to_PhotoID in memory
		String[] s_to_MD5=new String[5603955];
		HashMap<Integer, Integer> test_s_to_s=(HashMap<Integer, Integer>) General.readObject(saveBasePath+"DataSet/ME15_photos_S_to_S_test"+testSetYear+".hashMap");
		BufferedReader inStr_photoMeta = new BufferedReader(new InputStreamReader(new FileInputStream(saveBasePath+"DataSet/ME15_photos_s_to_photoID_md5_phoIndInL.txt"), "UTF-8"));
		String line1Photo; int ind=0;
		while((line1Photo=inStr_photoMeta.readLine())!=null){
			if (test_s_to_s.containsKey(ind)) {//this s is test photo
				s_to_MD5[ind]=line1Photo.split("\t")[1];
			}
			ind++;
		}
		inStr_photoMeta.close();
		System.out.println("done for read s_to_MD5 in memory! "+General.memoryInfo()+" ... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
		//read latlons
		float[][] latlons=(float[][]) General.readObject(saveBasePath+"DataSet/ME15_photos_latlons.floatArr");
		//read rank, save result to txt
		PrintWriter outputStream_run=new PrintWriter(new OutputStreamWriter(new FileOutputStream(saveBasePath+"ranks/rankScores/ME15pt_Locale_TUDMMC_2_"+testSetYear+".txt",false), "UTF-8"),true); 
		int dispInter=1000; int queryNum=0; int queryNum_noRes=0; int queryNum_R=0; float isSameLocScale=0.01f; DecimalFormat percent=new DecimalFormat("0.0%");
		if (testSetYear.equalsIgnoreCase("2014")) {
			for (File one : new File(saveBasePath+"ranks/rankScores").listFiles()) {
				if (one.getName().startsWith("Q2014_HR1000_ME15_SURF_VW20K")) {//this is target folder
					for (File onePartFile : one.listFiles()) {
						if (onePartFile.getName().startsWith("part")) {
							SequenceFile.Reader seqFile=new SequenceFile.Reader(new Configuration(), SequenceFile.Reader.file(new Path(onePartFile.getAbsolutePath())));
							Key_RankFlagID_QID key=new Key_RankFlagID_QID();
							QID_IntList_FloatList value=new QID_IntList_FloatList();
							while (seqFile.next(key, value)) {
								int qID_s=value.obj_1.get();
								if (value.obj_2.getIntegers().size()==0) {
									queryNum_noRes++;
								}else {
									int topDocID=value.obj_2.getIntegers().get(0);
									outputStream_run.println(s_to_MD5[qID_s]+";"+LatLon.getLatLon(latlons, topDocID, ";"));
									if (General_geoRank.isOneLocation(latlons[0][qID_s],latlons[1][qID_s],latlons[0][topDocID],latlons[1][topDocID],isSameLocScale))
						    			queryNum_R++;
								}
								queryNum++;
								General.dispInfo_ifNeed(queryNum%dispInter==0, "\t--", queryNum+" photos finished! queryNum_noRes: "+queryNum_noRes+", queryNum_R:"+queryNum_R+", "+percent.format((float)queryNum_R/queryNum)+" ... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
							}
							seqFile.close();
						}
					}
				}
			}
		}else{
			SequenceFile.Reader seqFile=new SequenceFile.Reader(new Configuration(), SequenceFile.Reader.file(new Path(saveBasePath+"ranks/rankScores/Q2015_HR1000_ME15_SURF_VW20K_iniR-BurstIntraInter@18@12_ReR1K_reRHE@18@12_Top1K_1vs1AndHistAndAngle@true@true@false@0.52@0.2@1@0@0@0@0@0@0@0_rankDocScore/data")));
			IntWritable qID_s=new IntWritable();
			IntList_FloatList value=new IntList_FloatList();
			while (seqFile.next(qID_s, value)) {
				if (value.getIntegers().size()==0) {
					queryNum_noRes++;
				}else {
					int topDocID=value.getIntegers().get(0);
					outputStream_run.println(s_to_MD5[qID_s.get()]+";"+LatLon.getLatLon(latlons, topDocID, ";"));
					if (General_geoRank.isOneLocation(latlons[0][qID_s.get()],latlons[1][qID_s.get()],latlons[0][topDocID],latlons[1][topDocID],isSameLocScale))
		    			queryNum_R++;
				}
				queryNum++;
				General.dispInfo_ifNeed(queryNum%dispInter==0, "\t--", queryNum+" photos finished! queryNum_noRes: "+queryNum_noRes+", queryNum_R:"+queryNum_R+", "+percent.format((float)queryNum_R/queryNum)+" ... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			}
			seqFile.close();
		}
		outputStream_run.close();
		General.dispInfo_ifNeed(true, "done! ", queryNum+" photos finished! queryNum_noRes: "+queryNum_noRes+", queryNum_R:"+queryNum_R+", "+percent.format((float)queryNum_R/queryNum)+" ... "+General.dispTime(System.currentTimeMillis()-startTime, "min"));
	}
	
}
