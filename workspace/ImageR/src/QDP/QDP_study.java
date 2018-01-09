//package QDP;
//
//import java.io.FileOutputStream;
//import java.io.OutputStreamWriter;
//import java.io.PrintWriter;
//import java.text.DecimalFormat;
//import java.text.NumberFormat;
//import java.util.ArrayList;
//import java.util.HashSet;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.MapFile;
//
//import MyAPI.General.General;
//import MyAPI.General.General_geoRank;
//import MyCustomedHaoop.ValueClass.GeoExpansionData.fistMatch_GTruth_Docs_GVSizes_docScores;
//
//public class QDP_study {
//
//	public static void main(String[] args) throws Exception {
//		DecimalFormat percformat = (DecimalFormat)NumberFormat.getPercentInstance();
//	    percformat.applyPattern("00.0%");
//	    double percentage;
//	    
//	    String PhotoOriPath_3MFlickr="O:/MediaEval_3185258Images/trainImages_1-3185258/";
//		int saveInterval=100*1000; int total_photos=3185258;
//		
//		String label;	MapFile.Reader MapFileR_oriRank, MapFileR_gvrRank;
//		
//		String servePath="D:/xinchaoli/Desktop/My research/My Code/DataSaved/";
//		String dataLabel="10M";
//		
//		String basePath=servePath+"ICMR2013/QDP/"+dataLabel+"/";	
//		
//		String Q_rank_Path=servePath+"ICMR2013/GVR/"+dataLabel+"/";	
//
//		int num_topLocationDocs=10;  float isSameLocScale=(float) 0.01; float isOneLocScale=(float) (0.1);  //(isSameLocScale+isSameLocScale*0.2)
//		int totalQueryNum=0;  int showTopScore=10;
//		
//		String showPhotoPath="Q:/ICMR2013/PhotoShow_QDPStudy_10M/isSameLocScale"+isSameLocScale+"/";
//		
//		//set FileSystem
//		Configuration conf = new Configuration();
//        FileSystem hdfs  = FileSystem.get(conf);
//        //******* read latlons **************
//      	float[][] latlons=(float[][]) General.readObject("D:\\xinchaoli\\Desktop\\My research\\Database\\FlickrPhotos\\image-meta\\3M_latlon.float2");
////      	float[][] latlons=(float[][]) General.readObject(servePath+"ICMR2013/"+"10M_selectedPhotos_LatLon.float2");
//      	//******* load ori-rank  ********//
//      	label="_ori";
//      	//set score-out put
//  		PrintWriter locScore_oriRight = new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"topLocScore"+label+".oriRight"), "UTF-8")); 
//  		PrintWriter locScore_oriWrong = new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"topLocScore"+label+".oriWrong"), "UTF-8")); 
//  		PrintWriter locScoreNormalized_oriRight = new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"topLocScoreNormalized"+label+".oriRight"), "UTF-8")); 
//  		PrintWriter locScoreNormalized_oriWrong = new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"topLocScoreNormalized"+label+".oriWrong"), "UTF-8")); 
//  		PrintWriter locGVSize_oriRight = new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"topLocGVSize"+label+".oriRight"), "UTF-8")); 
//  		PrintWriter locGVSize_oriWrong = new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"topLocGVSize"+label+".oriWrong"), "UTF-8")); 
//  		// read rank from MapFile 
//  		MapFileR_oriRank=new MapFile.Reader(hdfs,Q_rank_Path+"GVR_D"+dataLabel+"_Q100K"+label+"/part-r-00000", conf);
// 		System.out.println("MapFileR_oriRank Key-Class: "+MapFileR_oriRank.getKeyClass().getName());
// 		System.out.println("MapFileR_oriRank Value-Class: "+MapFileR_oriRank.getValueClass().getName());
// 		IntWritable Key_queryName= new IntWritable();
// 		fistMatch_GTruth_Docs_GVSizes_docScores Value_Rank= new fistMatch_GTruth_Docs_GVSizes_docScores();
// 		int totGoodQueryNum_ori=0; HashSet<Integer> goodPhos_ori=new HashSet<Integer>();
// 		float tureScore_min=9999; float wrongScore_Max=0;
//// 		photoPaths_list=new ArrayList<String[]>(); photoDiscrptions_list=new ArrayList<String[]>();
// 		ArrayList<Short> QDP_trueClass=new ArrayList<Short>(); ArrayList<Float> top1LocScoreNorm_ori= new ArrayList<Float>();
// 		while(MapFileR_oriRank.next(Key_queryName, Value_Rank)){ //loop over all queries, key-value(query-rank)
// 			int queryName=Key_queryName.get();
// 			int[] topDocs = Value_Rank.get_Docs(); 
// 			int[] docGVSizes=Value_Rank.get_GVSizes(); 
// 			float[] docScores = Value_Rank.get_docScores(); 
// 			int GTSize=Value_Rank.get_GTruth().size();
// 			int showTopScore_act=Math.min(showTopScore, docScores.length);
// 			if (showTopScore_act!=0){ //some query do not have any match in the whole dataSet
//	 			//get top Location Doc
// 				ArrayList<Integer> topLocationIndex=new ArrayList<Integer>(); //topLocationDocs's index in topDocs
//				ArrayList<Integer> topLocationDocs=General_geoRank.get_topLocationDocs(num_topLocationDocs, topDocs, isOneLocScale, latlons,topLocationIndex);
//				showTopScore_act=Math.min(showTopScore, topLocationDocs.size()); 
//				//set top location scores, GVSizes
////				float[] topLocScore_accumPerc=General_geoRank.get_topLocScoreAccumPerc(docScores, topLocationIndex);
//				float[] topLocScore_Norm=General_geoRank.get_topLocScoreNormalized(docScores, topLocationIndex,num_topLocationDocs);
//				StringBuffer outText_locScore=new StringBuffer(); StringBuffer outText_locScoreNormalized=new StringBuffer(); StringBuffer outText_locGVSizes=new StringBuffer(); 
//				for (int i=0;i<showTopScore_act;i++){
//					outText_locScore.append(docScores[topLocationIndex.get(i)]+"\t");
//					outText_locScoreNormalized.append(topLocScore_Norm[i]+"\t");
//					outText_locGVSizes.append(docGVSizes[topLocationIndex.get(i)]+"\t");
//				}
//				//get True-Location rank
//				int trueLocRank=General_geoRank.get_trueLocRank(queryName, topLocationDocs, isSameLocScale, latlons);
//				//set dispPhoto info
//				String[] photoPaths=new String[showTopScore_act+1]; String[] photoDiscrptions=new String[showTopScore_act+1];
//				photoPaths[0]=PhotoOriPath_3MFlickr+(queryName/saveInterval*saveInterval+1)+"-"+(queryName/saveInterval+1)*saveInterval+"\\"+queryName+"_"+total_photos+".jpg";
//				photoDiscrptions[0]="Q"+queryName+"_G"+GTSize+"_R"+trueLocRank;
//				for(int i=1;i<=showTopScore_act;i++){
//					int photoI=i-1;
//					int photoIndex=topDocs[topLocationIndex.get(photoI)]; 
//					photoPaths[i]=PhotoOriPath_3MFlickr+(photoIndex/saveInterval*saveInterval+1)+"-"+(photoIndex/saveInterval+1)*saveInterval+"\\"+photoIndex+"_"+total_photos+".jpg";
//					photoDiscrptions[i]=photoIndex+"_S"+topLocScore_Norm[photoI];
//				}
//				//set feature
//				if(trueLocRank==1){// top1 is right
//					goodPhos_ori.add(queryName);
//					QDP_trueClass.add((short) 0); //add this query's original true0 or wrong1 label
//					locScore_oriRight.println(photoDiscrptions[0]+":\t"+outText_locScore.toString());
//					locScoreNormalized_oriRight.println(photoDiscrptions[0]+":\t"+outText_locScoreNormalized.toString());
//					locGVSize_oriRight.println(photoDiscrptions[0]+":\t"+outText_locGVSizes.toString());
//					tureScore_min=Math.min(tureScore_min, topLocScore_Norm[0]);
//					totGoodQueryNum_ori++;
//				}else{// top1 is not right
//					QDP_trueClass.add((short) 1); //add this query's original true0 or wrong1 label
//					locScore_oriWrong.println(photoDiscrptions[0]+":\t"+outText_locScore.toString());
//					locScoreNormalized_oriWrong.println(photoDiscrptions[0]+":\t"+outText_locScoreNormalized.toString());
//					locGVSize_oriWrong.println(photoDiscrptions[0]+":\t"+outText_locGVSizes.toString());
//					wrongScore_Max=Math.max(wrongScore_Max, topLocScore_Norm[0]);
//				}
//				//for test,  one sample predictor
//				top1LocScoreNorm_ori.add((float) topLocScore_Norm[0]); //topLocScore_Norm[0]-topLocScore_Norm[1]
//				//updata totalQueryNum
//				totalQueryNum++;
// 			}else{
// 				System.out.println("query:"+queryName+", do not have matches!!");
// 			}
// 		}
// 		percentage=(double)totGoodQueryNum_ori/totalQueryNum;
// 		System.out.println("totalQueryNum: "+totalQueryNum+", totGoodPhoNum_ori: "+totGoodQueryNum_ori+", "+new DecimalFormat("00.0%").format(percentage));
// 		System.out.println("tureScore_min: "+tureScore_min+", wrongScore_Max: "+wrongScore_Max);
// 		//for test,  make QDP confusion matrix
// 		float[] QDP_Thres={(float) 0.1,(float) 0.15, (float) 0.2,(float) 0.3,(float) 0.4,(float) 0.5,(float) 0.6 ,(float) 0.7};
//// 		float[] QDP_Thres={(float) 2,(float) 4, (float) 6,(float) 8,(float) 10,(float) 20,(float) 30 ,(float) 40};
// 		String[] className={"GeoCorrect","GeoInCorrect"}; StringBuffer PR_info=new StringBuffer(); int targetClass=0;
// 		for(float QDP_Thre:QDP_Thres){
// 			ArrayList<Short> QDP_predClass=new ArrayList<Short>();
// 			for(float top1score:top1LocScoreNorm_ori){
// 	 			if(top1score>QDP_Thre)
// 	 				QDP_predClass.add((short) 0);
// 	 			else
// 	 				QDP_predClass.add((short) 1);
// 	 		}
// 	 		System.out.println("one Simple Predictor: threthoding normolized locScores.. Threthod="+QDP_Thre);
// 	 		int[][] QDP_ConfMatrix=General.mkConfusionMatrix(QDP_trueClass, QDP_predClass, 2);
// 	 		System.out.println(General.gtInfo_from_ConfusionMatrix(QDP_ConfMatrix, className));
// 	 		float[] TPR_FPR=General.gt_TPR_FPR_from_ConfusionMatrix(QDP_ConfMatrix, targetClass);
// 	 		PR_info.append(TPR_FPR[0]+"\t"+TPR_FPR[1]+"\n");
// 		}
// 		System.out.println("Precision and Recall of target class-"+targetClass+":\n"+PR_info.toString());
// 		//clean-up
// 		MapFileR_oriRank.close();
// 		locScore_oriRight.close();locScoreNormalized_oriRight.close();locGVSize_oriRight.close();
// 		locScore_oriWrong.close();locScoreNormalized_oriWrong.close();locGVSize_oriWrong.close();
// 		
// 		
// 		//******* load geoVisual-rank  ********//
// 		int visScale=200; double geoExpanScale=0.01;
// 		label="_VisScal_"+visScale+"_expScal_"+geoExpanScale;
// 		int[] bins_GVSize={0,10,20,40,60,100}; int[] showNum=new int[bins_GVSize.length+1]; int max_show=40; 
//      	//set score-out put
//		PrintWriter locScore_gvrRight = new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"topLocScore"+label+".gvrRight"), "UTF-8")); 
//		PrintWriter locScore_gvrWrong = new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"topLocScore"+label+".gvrWrong"), "UTF-8")); 
//		PrintWriter locScoreNormalized_gvrRight = new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"topLocScoreNormalized"+label+".gvrRight"), "UTF-8")); 
//		PrintWriter locScoreNormalized_gvrWrong = new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"topLocScoreNormalized"+label+".gvrWrong"), "UTF-8")); 
//		PrintWriter locGVSize_gvrRight = new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"topLocGVSize"+label+".gvrRight"), "UTF-8")); 
//  		PrintWriter locGVSize_gvrWrong = new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"topLocGVSize"+label+".gvrWrong"), "UTF-8")); 
//  		PrintWriter locGVSizeNormalized_gvrRight = new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"toplocGVSizeNormalized"+label+".gvrRight"), "UTF-8")); 
//  		PrintWriter locGVSizeNormalized_gvrWrong = new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"toplocGVSizeNormalized"+label+".gvrWrong"), "UTF-8")); 
//		// read rank from MapFile 
//  		MapFileR_gvrRank=new MapFile.Reader(hdfs,Q_rank_Path+"GVR_D"+dataLabel+"_Q100K"+label+"/part-r-00000", conf);
// 		System.out.println("MapFileR_gvrRank Key-Class: "+MapFileR_gvrRank.getKeyClass().getName());
// 		System.out.println("MapFileR_gvrRank Value-Class: "+MapFileR_gvrRank.getValueClass().getName());
// 		int totGoodQueryNum_gvr=0; 
// 		tureScore_min=9999;  wrongScore_Max=0; totalQueryNum=0;   
// 		ArrayList<Float> top1LocScoreNorm_gvr= new ArrayList<Float>();
// 		QDP_trueClass=new ArrayList<Short>(); 
// 		HashSet<Integer> goodPhos_gvr=new HashSet<Integer>();
// 		while(MapFileR_gvrRank.next(Key_queryName, Value_Rank)){ //loop over all queries, key-value(query-rank)
// 			int queryName=Key_queryName.get();
// 			int[] topDocs = Value_Rank.get_Docs(); 
// 			int[] docGVSizes=Value_Rank.get_GVSizes(); 
// 			float[] docScores = Value_Rank.get_docScores(); 
// 			int GTSize=Value_Rank.get_GTruth().size();
// 			int showTopScore_act=Math.min(showTopScore, docScores.length);
// 			if (showTopScore_act!=0){ //some query do not have any match in the whole dataSet
//	 			//get top Location Doc
// 				ArrayList<Integer> topLocationIndex=new ArrayList<Integer>(); //topLocationDocs's index in topDocs
//				ArrayList<Integer> topLocationDocs=General_geoRank.get_topLocationDocs(num_topLocationDocs, topDocs, isOneLocScale, latlons,topLocationIndex);
//				showTopScore_act=Math.min(showTopScore, topLocationDocs.size()); 
//				//set top location scores
////				float[] topLocScore_accumPerc=General_geoRank.get_topLocScoreAccumPerc(docScores, topLocationIndex);
//				float[] topLocScore_Norm=General_geoRank.get_topLocScoreNormalized(docScores, topLocationIndex,num_topLocationDocs);
//				float[] topLocGVSizes_Norm=General_geoRank.get_topLocGVSizeNormalized( docGVSizes, topLocationIndex,  num_topLocationDocs);
//				StringBuffer outText_locScore=new StringBuffer(); StringBuffer outText_locScoreNormalized=new StringBuffer(); StringBuffer outText_locGVSizes=new StringBuffer();StringBuffer outText_locGVSizesNormalized=new StringBuffer();
//				for (int i=0;i<showTopScore_act;i++){
//					outText_locScore.append(docScores[topLocationIndex.get(i)]+"\t");
//					outText_locScoreNormalized.append(topLocScore_Norm[i]+"\t");
//					outText_locGVSizes.append(docGVSizes[topLocationIndex.get(i)]+"\t");
//					outText_locGVSizesNormalized.append(topLocGVSizes_Norm[i]+"\t");
//				}
//				//get True-Location rank
//				int trueLocRank=General_geoRank.get_trueLocRank(queryName, topLocationDocs, isSameLocScale, latlons);
//				//set dispPhoto info
//				String[] photoPaths=new String[showTopScore_act+1]; String[] photoDiscrptions=new String[showTopScore_act+1];
//				photoPaths[0]=PhotoOriPath_3MFlickr+(queryName/saveInterval*saveInterval+1)+"-"+(queryName/saveInterval+1)*saveInterval+"\\"+queryName+"_"+total_photos+".jpg";
//				photoDiscrptions[0]="Q"+queryName+"_r0_G"+GTSize+"_TR"+trueLocRank;
//				for(int i=1;i<=showTopScore_act;i++){
//					int photoI=i-1;
//					int photoIndex=topDocs[topLocationIndex.get(photoI)]; 
////					int photoIndex=topDocs[photoI]; 
//					photoPaths[i]=PhotoOriPath_3MFlickr+(photoIndex/saveInterval*saveInterval+1)+"-"+(photoIndex/saveInterval+1)*saveInterval+"\\"+photoIndex+"_"+total_photos+".jpg";
//					photoDiscrptions[i]="Q"+queryName+"_r"+i+"_S"+topLocScore_Norm[photoI]+"_GVSize"+docGVSizes[photoI]+"_p"+photoIndex;
//				}
//				//set feature
//				if(trueLocRank==1){// top1 is right
//					goodPhos_gvr.add(queryName);
//					QDP_trueClass.add((short) 0); //add this query's original true0 or wrong1 label
//					locScore_gvrRight.println(photoDiscrptions[0]+":\t"+outText_locScore.toString());
//					locScoreNormalized_gvrRight.println(photoDiscrptions[0]+":\t"+outText_locScoreNormalized.toString());
//					locGVSize_gvrRight.println(photoDiscrptions[0]+":\t"+outText_locGVSizes.toString());
//					locGVSizeNormalized_gvrRight.println(photoDiscrptions[0]+":\t"+outText_locGVSizesNormalized.toString());
//					tureScore_min=Math.min(tureScore_min, topLocScore_Norm[0]);
//					totGoodQueryNum_gvr++;
//				}else{// top1 is not right
//					QDP_trueClass.add((short) 1); //add this query's original true0 or wrong1 label
//					locScore_gvrWrong.println(photoDiscrptions[0]+":\t"+outText_locScore.toString());
//					locScoreNormalized_gvrWrong.println(photoDiscrptions[0]+":\t"+outText_locScoreNormalized.toString());
//					locGVSize_gvrWrong.println(photoDiscrptions[0]+":\t"+outText_locGVSizes.toString());
//					locGVSizeNormalized_gvrWrong.println(photoDiscrptions[0]+":\t"+outText_locGVSizesNormalized.toString());
//					wrongScore_Max=Math.max(wrongScore_Max, topLocScore_Norm[0]);
////					//show photos
////					int binIndex=General.getBinIndex(bins_GVSize,docGVSizes[0]);
////					String folderName=showPhotoPath+"top1DocGVSize_gvrWrong/";
////					General.showPhoto_BinIndex(bins_GVSize, showNum, binIndex, max_show,photoPaths,photoDiscrptions,folderName);
////					// updata showNum
////					if (binIndex==-1){
////						showNum[showNum.length-1]++;
////					}else{
////						showNum[binIndex]++;
////					}
//				}
//				//for test,  one sample predictor
//				top1LocScoreNorm_gvr.add((float) topLocScore_Norm[0]); //topLocScore_Norm[0]-topLocScore_Norm[1], topLocGVSizes_Norm[0]
//				//updata totalQueryNum
//				totalQueryNum++;
// 			}else{
// 				System.out.println("query:"+queryName+", do not have matches!!");
// 			}
// 		}
// 		// make location accuracy
// 		percentage=(double)totGoodQueryNum_gvr/totalQueryNum;
// 		System.out.println("totalQueryNum: "+totalQueryNum+", totGoodQueryNum_gvr: "+totGoodQueryNum_gvr+", "+new DecimalFormat("00.0%").format(percentage));
// 		System.out.println("tureScore_min: "+tureScore_min+", wrongScore_Max: "+wrongScore_Max);
// 		//for test,  make QDP confusion matrix
// 		PR_info=new StringBuffer();
// 		for(float QDP_Thre:QDP_Thres){
// 			ArrayList<Short> QDP_predClass=new ArrayList<Short>();
// 			for(float top1score:top1LocScoreNorm_gvr){
// 	 			if(top1score>QDP_Thre)
// 	 				QDP_predClass.add((short) 0);
// 	 			else
// 	 				QDP_predClass.add((short) 1);
// 	 		}
// 	 		System.out.println("one Simple Predictor: threthoding normolized locScores.. Threthod="+QDP_Thre);
// 	 		int[][] QDP_ConfMatrix=General.mkConfusionMatrix(QDP_trueClass, QDP_predClass, 2);
// 	 		System.out.println(General.gtInfo_from_ConfusionMatrix(QDP_ConfMatrix, className));
// 	 		float[] TPR_FPR=General.gt_TPR_FPR_from_ConfusionMatrix(QDP_ConfMatrix, targetClass);
// 	 		PR_info.append(TPR_FPR[0]+"\t"+TPR_FPR[1]+"\n");
// 		}
// 		System.out.println("Precision and Recall of target class-"+targetClass+":\n"+PR_info.toString());
// 		//clean-up
// 		MapFileR_gvrRank.close();
// 		locScore_gvrRight.close();locScoreNormalized_gvrRight.close(); locGVSize_gvrRight.close();locGVSizeNormalized_gvrRight.close();
// 		locScore_gvrWrong.close();locScoreNormalized_gvrWrong.close(); locGVSize_gvrWrong.close();locGVSizeNormalized_gvrWrong.close();
//// 		
//// 		//** compare 1-NN with GVR
//// 		HashSet<Integer> goodPhos_ori_gvr=new HashSet<Integer>(goodPhos_ori); goodPhos_ori_gvr.retainAll(goodPhos_gvr);
//// 		HashSet<Integer> goodOri_NoGvr=new HashSet<Integer>(goodPhos_ori); goodOri_NoGvr.removeAll(goodPhos_gvr);
//// 		HashSet<Integer> goodGvr_NoOri=new HashSet<Integer>(goodPhos_gvr); goodGvr_NoOri.removeAll(goodPhos_ori);
//// 		System.out.println("goodPhos_ori_gvr:"+goodPhos_ori_gvr.size()+", goodOri_NoGvr:"+goodOri_NoGvr.size()+", goodGvr_NoOri:"+goodGvr_NoOri.size());
//// 		//for test, choose between 1-NN and GVR
//// 		MapFileR_oriRank=new MapFile.Reader(hdfs,Q_rank_Path+"GVR_random"+random+label+"/part-r-00000", conf);
//// 		int queryIndex=0; float choose_thre_ori=(float) 0.6; float choose_thre_gvr=(float) 1.0; int correctQuery=0; int correctByori=0;
//// 		StringBuffer goodOri_NoGvr_scores=new StringBuffer();
//// 		photoPaths_list=new ArrayList<String[]>(); photoDiscrptions_list=new ArrayList<String[]>();  
//// 		while(MapFileR_oriRank.next(Key_queryName, Value_Rank)){ //loop over all queries, key-value(query-rank)
//// 			int queryName=Key_queryName.get();
//// 			int showTopScore_act=Math.min(showTopScore, Value_Rank.get_Docs().length);
//// 			if (showTopScore_act!=0){ //some query do not have any match in the whole dataSet
////	 			float score_ori=top1LocScoreNorm_ori.get(queryIndex);
////	 			float score_gvr=top1LocScoreNorm_gvr.get(queryIndex);
////	 			//** one simple judge, 1NN or GVR
////	 			if (score_ori>choose_thre_ori){
////	 				if(score_gvr<choose_thre_gvr){
////	 					if(goodPhos_ori.contains(queryName)) //use 1-NN to predict
////	 						correctQuery++;
////	 					if(goodOri_NoGvr.contains(queryName))
////	 						correctByori++;
////	 				}else{
////	 					if(goodPhos_gvr.contains(queryName)) //use GVR to predict
////	 						correctQuery++;
////	 				}
////	 			}else{
////	 				if(goodPhos_gvr.contains(queryName)) //use GVR to predict
////	 					correctQuery++;
////	 			}
////	 			if(goodOri_NoGvr.contains(queryName))
////	 				goodOri_NoGvr_scores.append(score_ori+"_"+score_gvr+", ");
////	 			//** show query own high confidence to be right!
////	 			if(score_gvr>0.6){
////	 				//set dispPhoto info
////	 				int[] topDocs = Value_Rank.get_Docs(); 
////	 	 			Value_Rank.get_GVSizes(); 
////	 	 			float[] docScores = Value_Rank.get_docScores(); 
////	 	 			int GTSize=Value_Rank.get_groudTSize();
////	 	 			//get top Location Doc
////	 				ArrayList<Integer> topLocationIndex=new ArrayList<Integer>(); //topLocationDocs's index in topDocs
////					ArrayList<Integer> topLocationDocs=General_geoRank.get_topLocationDocs(num_topLocationDocs, topDocs, isOneLocScale, latlons,topLocationIndex);
////					showTopScore_act=Math.min(showTopScore, topLocationDocs.size()); 
////					//set top location scores
//////					float[] topLocScore_accumPerc=General_geoRank.get_topLocScoreAccumPerc(docScores, topLocationIndex);
////					float[] topLocScore_Norm=General_geoRank.get_topLocScoreNormalized(docScores, topLocationIndex);
////					StringBuffer outText_locScore=new StringBuffer(); StringBuffer outText_locScoreNormalized=new StringBuffer();
////					for (int i=0;i<showTopScore_act;i++){
////						outText_locScore.append(docScores[topLocationIndex.get(i)]+"\t");
////						outText_locScoreNormalized.append(topLocScore_Norm[i]+"\t");
////					}
////					//get True-Location rank
////					int trueLocRank=General_geoRank.get_trueLocRank(queryName, topLocationDocs, isSameLocScale, latlons);
////					String[] photoPaths=new String[showTopScore_act+1]; String[] photoDiscrptions=new String[showTopScore_act+1];
////					photoPaths[0]=PhotoOriPath_3MFlickr+(queryName/saveInterval*saveInterval+1)+"-"+(queryName/saveInterval+1)*saveInterval+"\\"+queryName+"_"+total_photos+".jpg";
////					photoDiscrptions[0]="Q"+queryName+"_G"+GTSize+"_R"+trueLocRank;
////					for(int i=1;i<=showTopScore_act;i++){
////						int photoI=i-1;
////						int photoIndex=topDocs[topLocationIndex.get(photoI)]; 
////						photoPaths[i]=PhotoOriPath_3MFlickr+(photoIndex/saveInterval*saveInterval+1)+"-"+(photoIndex/saveInterval+1)*saveInterval+"\\"+photoIndex+"_"+total_photos+".jpg";
////						photoDiscrptions[i]=photoIndex+"_S"+topLocScore_Norm[photoI];
////					}
////					photoPaths_list.add(photoPaths);photoDiscrptions_list.add(photoDiscrptions);
////	 			}
////	 			queryIndex++;
//// 			}else{
//// 				System.out.println("query:"+queryName+", do not have matches!!");
//// 			}
//// 		}
//// 		//disp photos
//// 		General.dispPhotos(photoPaths_list, photoDiscrptions_list, 100, 100);
//// 		System.out.println("choose with choose_thre_ori="+choose_thre_ori+", choose_thre_gvr="+choose_thre_gvr
//// 				+", correctQuery: "+correctQuery+", correctByori:"+correctByori);
//// 		System.out.println("goodOri_NoGvr_scores: "+goodOri_NoGvr_scores.toString());
//// 		MapFileR_oriRank.close();
//	}
//
//}
