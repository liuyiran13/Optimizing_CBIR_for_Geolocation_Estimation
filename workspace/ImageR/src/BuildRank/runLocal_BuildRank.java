package BuildRank;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import MyAPI.General.General;
import MyAPI.General.myComparator.ValueComparator_Float_DES;
import MyCustomedHaoop.ValueClass.HashMapClass.HashMap_Int_ByteArrList;
import MyCustomedHaoop.ValueClass.IntegerByteArrList;

public class runLocal_BuildRank {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {//for debug
		DecimalFormat percformat= (DecimalFormat)NumberFormat.getPercentInstance();
		percformat.applyPattern("00.000%");//设置百分率的输出形式，形如00.*,根据需要设定。
		double percentage;
		long startTime,endTime;
		int intervel=10;  //disp, file number
		Configuration conf = new Configuration();
        FileSystem hdfs  = FileSystem.getLocal(conf);
        
        int HMDistThr=22;//hamming dist threthod
        int topRank=10;// list top rank
        int ShowTopN=10; // show top rank
        
        String SPath="/tudelft.net/staff-bulk/ewi/mm/D-MIRLab/XinchaoLi/HerveImages/";
        
		//***** read phoNamFeatNum  ***//
//		String photoFeatNum_Path="Q:\\HerveImages\\photoFeatNum_Holiday";
		String photoFeatNum_Path=SPath+"photoFeatNum_Holiday";
		HashMap<Integer, Integer> phoNamFeatNum=new HashMap<Integer, Integer>();
		phoNamFeatNum = (HashMap<Integer, Integer>) General.readObject(photoFeatNum_Path);
		
		//***** set total indexed doc number in dataTVector***//
		int totDocNum=phoNamFeatNum.size();
		System.out.println("totDocNum: "+totDocNum);
		
		//***** read all TVectors into memory and  caculate each vw's term IDF***//
//		String TVector_path="Q:\\HerveImages\\TVector_Holiday_MapFile\\part-r-00000";
		String TVector_path=SPath+"TVector_Holiday_MapFile/part-r-00000";
		MapFile.Reader MapFileReader=new MapFile.Reader(hdfs, TVector_path, conf);
		System.out.println("TVector MapFile Key-Class: "+MapFileReader.getKeyClass().getName());
		System.out.println("TVector MapFile Value-Class: "+MapFileReader.getValueClass().getName());
		HashMap<Integer,IntegerByteArrList> TVectors=new HashMap<Integer,IntegerByteArrList>();
		IntWritable Key= new IntWritable();
		IntegerByteArrList Value= new IntegerByteArrList();
		HashMap<Integer,Float> termIDFs=new HashMap<Integer,Float>(); //save term IDFs
		while(MapFileReader.next(Key, Value)){ //loop over all TVectors, key-value(vw-docName&Sig)
			int vw=Key.get();
			TVectors.put(vw, new IntegerByteArrList(Value.getIntegers(),Value.getbyteArrs()));
			//caculate this vw's term IDF
			ArrayList<Integer> TNames=Value.getIntegers();
        	HashSet<Integer> temp=new HashSet<Integer>();
        	int df=0;
        	for(int PhotoName:TNames){ //find unique photo-names in TNames
        		if(temp.add(PhotoName)){
        			df++;
        		}
        	}
        	float idf=(float) Math.log((double)totDocNum/df);
        	termIDFs.put(vw, idf);
        	System.out.println("vw:"+vw);
		}
		System.out.println("read TVectors finished, total vws: "+TVectors.size());
		
		//***** make selected docs for output (if groundTruth is offered for evaluation like MAP) ***//
//		String queryGroTruPath="O:\\HerveImages\\eval_holidays\\queryGroTrue.hashmap_int_hashset";
		String queryGroTruPath=SPath+"queryGroTrue.hashmap_int_hashset";
		HashMap<Integer, HashSet<Integer>> queryGroundTurth=(HashMap<Integer, HashSet<Integer>>) General.readObject(queryGroTruPath);
		System.out.println("read queryGroundTurth finished, queryNum:"+queryGroundTurth.size());
		//***** set MAP data path ***//
//		String path_outStr_dataForMAP="O:\\HerveImages\\eval_holidays\\my_HE_idf_Noraml_HD"+HMDistThr+".dat";
		String path_outStr_dataForMAP=SPath+"my_HE_idf_Noraml_HD"+HMDistThr+".dat";
		PrintWriter outStr_dataForMAP = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path_outStr_dataForMAP,false), "UTF-8"));
		
		//*****loop over querys, seach ***********//
//		String queryFeat_path="Q:\\HerveImages\\Query_VW_Sig\\part-r-00000";
		String queryFeat_path=SPath+"Query_VW_Sig/part-r-00000";
		MapFileReader=new MapFile.Reader(hdfs, queryFeat_path, conf);
		System.out.println("queryFeat MapFile Key-Class: "+MapFileReader.getKeyClass().getName());
		System.out.println("queryFeat MapFile Value-Class: "+MapFileReader.getValueClass().getName());
		IntWritable queryName= new IntWritable();
		HashMap_Int_ByteArrList VW_Sigs= new HashMap_Int_ByteArrList();
		//calculate total query number
		int queryNum=0; 
		while(MapFileReader.next(queryName, VW_Sigs)){
			queryNum++;
		}
		MapFileReader.close();
		System.out.println("total query-number:"+queryNum);
		//search
		int queryIndex=0;
		MapFileReader=new MapFile.Reader(hdfs, queryFeat_path, conf);
		startTime=System.currentTimeMillis(); //end time 
		while(MapFileReader.next(queryName, VW_Sigs)){ //loop over all querys, key-value(queryName-HashMap<VW&Sig>)
			//******search MapFile, make doc scores, only use IDF, no tf******//
			HashMap<Integer,Float> totDocScores=new HashMap<Integer,Float>();
			for(int vw:VW_Sigs.getHashMap().keySet()){//loop over each vw
				System.out.println(queryName+","+vw);
				ArrayList<byte[]> QuerySigs=VW_Sigs.getHashMap().get(vw);
				ArrayList<byte[]> TSigs=TVectors.get(vw).getbyteArrs();
				ArrayList<Integer> TNames=TVectors.get(vw).getIntegers();
				if(TNames.contains(queryName.get())){
					System.out.println(queryName+","+vw);
				}
				float idf=termIDFs.get(vw);
				for(int ei=0;ei<QuerySigs.size();ei++){ //loop over sigs of this vw
					byte[] querySig=QuerySigs.get(ei);
					for(int i=0;i<TNames.size();i++){
						int hammingDist=(new BigInteger(querySig)).xor(new BigInteger(TSigs.get(i))).bitCount();
						if(hammingDist<=HMDistThr){
							int docName=TNames.get(i);
							if(totDocScores.containsKey(docName)){
								totDocScores.put(docName, new Float(totDocScores.get(docName)+idf));
							}else{
								totDocScores.put(docName,new Float(idf));
							}
						}
					}
				}
			}
			//******** normlize doc-Score  ************
			for(Integer doc:totDocScores.keySet()){
				int featNum=phoNamFeatNum.get(doc);
				float normal=(float) Math.sqrt(featNum); //normlize by sqrt of doc-featNum
				totDocScores.put(doc, totDocScores.get(doc)/normal);
			}
			//*****sort doc scores***********//
			ValueComparator_Float_DES mvCompartor = new ValueComparator_Float_DES(totDocScores);
			TreeMap<Integer,Float> totDocScores_Des = new TreeMap<Integer,Float>(mvCompartor);
			totDocScores_Des.putAll(totDocScores);
			System.out.println("query-"+queryName+": sort finished, listed doc number:"+totDocScores_Des.size());
			//********* output seleted docRanks *****//
			ArrayList<Integer> DocNames=new ArrayList<Integer>();
			ArrayList<Integer> Ranks=new ArrayList<Integer>();
			ArrayList<Float> Scores=new ArrayList<Float>();
			int index=0;
			for (int key:totDocScores_Des.keySet()){//save top topRank photos
				if(index<topRank){
					DocNames.add(key);
					Ranks.add(index);
					Scores.add(totDocScores_Des.get(key));
					assert totDocScores_Des.get(key)==totDocScores.get(key): "Key:"+key+" misMatch!";
				}else{
					break;
				}
				index++;
			}
			ArrayList<Integer> DocNamesAll=new ArrayList<Integer>();
			DocNamesAll.addAll(totDocScores_Des.keySet()); //to get the rank of one key
			assert DocNamesAll.indexOf(totDocScores_Des.firstKey())==0 : "error in DocNamesAll.addAll(totDocScores_Des.keySet()):"
					+DocNamesAll.addAll(totDocScores_Des.keySet());
			HashSet<Integer> reldocs = queryGroundTurth.get(queryName.get());
			for(Integer reldoc:reldocs){ //save ground true photos not in top topRank photos
				if(!DocNames.contains(reldoc)){
					if(totDocScores_Des.containsKey(reldoc)){
						DocNames.add(reldoc);
						Ranks.add(DocNamesAll.indexOf(reldoc));
						Scores.add(totDocScores_Des.get(reldoc));
					}else{
						System.out.println("reldoc:"+reldoc+"not exist for query-"+queryName);
					}
				}
			}
			//********* make MAP data *****//
			int listedNum=DocNames.size();
			System.out.println("Query:"+queryName);
			System.out.println("Rank Size:"+listedNum);
			StringBuffer oneLine=new StringBuffer();
			oneLine.append(queryName+".jpg ");
			for(int i=0;i<listedNum;i++){
				if(i<ShowTopN){
					int docName=DocNames.get(i);
					int rank=Ranks.get(i);
					float score=Scores.get(i);
					System.out.println("rank-"+rank+", doc:"+docName+", Score:"+score);
					if (docName<150000){
						// make MAP data
						oneLine.append(rank+" "+docName+".jpg "+" ");
					}
				}else{
					break;
				}
			}
			outStr_dataForMAP.println(oneLine.toString());
			
			if ((queryIndex)%intervel==0){
				//disp info
				endTime=System.currentTimeMillis(); //end time 
				percentage=(double)queryIndex/queryNum;
				System.out.println("query searching, "+queryNum+" photos, now photo-"+queryIndex+" finished ......"
				+percformat.format(percentage)+", running time:"+(endTime-startTime)/1000/60+"mins");
			}
			queryIndex++;
		}
		outStr_dataForMAP.close();
		MapFileReader.close();
	}
}
