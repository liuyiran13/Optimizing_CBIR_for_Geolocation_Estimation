//package QDP;
//
//import java.io.FileOutputStream;
//import java.io.OutputStreamWriter;
//import java.io.PrintWriter;
//import java.util.ArrayList;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.MapFile;
//
//import MyAPI.General.General;
//import MyAPI.General.General_geoRank;
//import MyCustomedHaoop.ValueClass.GeoExpansionData.fistMatch_GTruth_Docs_GVSizes_docScores;
//import MyCustomedHaoop.ValueClass.IntArr_FloatArr;
//
//public class QDP_feat {
//
//	public static void main(String[] args) throws Exception {
//
//		
//		int random=100*1000; String label;MapFile.Reader MapFile_R_Rank;
//		
//		String servePath="D:/xinchaoli/Desktop/My research/My Code/DataSaved/";
//		
//		String basePath=servePath+"ICMR2013/";	
//		
//		String Q_rank_Path=basePath+"GVR/10M/";
//		String feat_Path=basePath+"QDP/Feat/";
//		
//		int num_topLocationDocs=10;  float isSameLocScale=(float) 0.01; float isOneLocScale=(float) (isSameLocScale+isSameLocScale*0.2);
//		int showTopScore=10;
//		//set FileSystem
//		Configuration conf = new Configuration();
//        FileSystem hdfs  = FileSystem.get(conf);
//        //******* read latlons **************
////      	float[][] latlons=(float[][]) General.readObject("D:/xinchaoli/Desktop/My research/Database/FlickrPhotos/image-meta/3M_latlon.float2");
//      	float[][] latlons=(float[][]) General.readObject(servePath+"ICMR2013/"+"10M_selectedPhotos_LatLon.float2");
//      	
//      	//*******  load  rank  ********//
//      	int visScale=500; double geoExpanScale=0.01;
// 		label="_VisScal_"+visScale+"_expScal_"+geoExpanScale;  //"_ori"  "_VisScal_"+visScale+"_expScal_"+geoExpanScale
//      	// read rank from MapFile 
//// 		MapFile_R_Rank=new MapFile.Reader(hdfs,Q_rank_Path+"GVR_random"+random+label+"/part-r-00000", conf);
// 		MapFile_R_Rank=new MapFile.Reader(hdfs,Q_rank_Path+"GVR_D10M_Q100K"+label+"/part-r-00000", conf);
// 		System.out.println("MapFile_R_Rank Key-Class: "+MapFile_R_Rank.getKeyClass().getName());
// 		System.out.println("MapFile_R_Rank Value-Class: "+MapFile_R_Rank.getValueClass().getName());
// 		IntWritable Key_queryName= new IntWritable();
// 		fistMatch_GTruth_Docs_GVSizes_docScores Value_Rank= new fistMatch_GTruth_Docs_GVSizes_docScores();
// 		//set save MapFile
// 		IntArr_FloatArr Value_feat=new IntArr_FloatArr();
// 		MapFile.Writer MapFileWriter = new MapFile.Writer(conf, hdfs, feat_Path+"Feat_GVR_D10M_Q100K"+label, Key_queryName.getClass(), Value_feat.getClass());
// 		int MapFileindInter=10; MapFileWriter.setIndexInterval(MapFileindInter);
// 		//set save text
// 		PrintWriter feat_text = new PrintWriter(new OutputStreamWriter(new FileOutputStream(feat_Path+"Feat_GVR_D10M_Q100K"+label+".txt", false), "UTF-8"),true); 
// 		while(MapFile_R_Rank.next(Key_queryName, Value_Rank)){ //loop over all queries, key-value(query-rank)
// 			int queryName=Key_queryName.get();
// 			int[] topDocs = Value_Rank.get_Docs(); 
// 			float[] docScores = Value_Rank.get_docScores(); 
// 			int[] docGVSizes=Value_Rank.get_GVSizes(); 
// 			int GTSize=Value_Rank.get_GTruth().size();
// 			int showTopScore_act=Math.min(showTopScore, docScores.length);
// 			if (showTopScore_act!=0){ //some query do not have any match in the whole dataSet
//	 			//get top Location Doc
// 				ArrayList<Integer> topLocationIndex=new ArrayList<Integer>(); //topLocationDocs's index in topDocs
//				ArrayList<Integer> topLocationDocs=General_geoRank.get_topLocationDocs(num_topLocationDocs, topDocs, isOneLocScale, latlons,topLocationIndex);
//				//get True-Location rank
//				int trueLocRank=General_geoRank.get_trueLocRank(queryName, topLocationDocs, isSameLocScale, latlons);
//				//set top locations' score, GVSize
////				float[] topLocScore_accumPerc=General_geoRank.get_topLocScoreAccumPerc(docScores, topLocationIndex);
//				float[] topLocScore_Norm=General_geoRank.get_topLocScoreNormalized(docScores, topLocationIndex,num_topLocationDocs);
//				float[] topLocGVSizes_Norm=General_geoRank.get_topLocGVSizeNormalized( docGVSizes, topLocationIndex,  num_topLocationDocs);
//				//*************** extract feat ****************//
//				//make classLabel
//				int[] classLabel=new int[2]; //only one element, save class-label
//				if(trueLocRank==1)//correct query
//					classLabel[0]=0;
//				else
//					classLabel[0]=1;
//				classLabel[1]=GTSize;
//				//make feat
//				float[] feat=new float[3];
//				feat[0]=topLocScore_Norm[0]; //top1 loc score (Normalized)
//				feat[1]=topLocScore_Norm[0]-topLocScore_Norm[1]; //jump between top1 and top2 loc score (Normalized)
//				feat[2]=topLocGVSizes_Norm[0]; //top1 loc GVSize (Normalized)
//				//write-out to MapFile
//				Value_feat.setIntArr_FloatArr(classLabel, feat);
//				MapFileWriter.append(Key_queryName, Value_feat);
//				feat_text.println(classLabel[0]+","+General.floatArrToString(feat, ",", "0.0000"));
// 			}else{
// 				System.out.println("query:"+queryName+", do not have matches!!");
// 			}
// 		}
// 		MapFileWriter.close();
// 		MapFile_R_Rank.close();
// 		feat_text.close();
//	}
//
//}
