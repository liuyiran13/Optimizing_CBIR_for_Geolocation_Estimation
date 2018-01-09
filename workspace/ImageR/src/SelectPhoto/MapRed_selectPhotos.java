package SelectPhoto;

import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyAPI.General.General_Hadoop;
import MyCustomedHaoop.Mapper.SelectSamples;
import MyCustomedHaoop.Partitioner.Partitioner_equalAssign;
import MyCustomedHaoop.Reducer.Reducer_InOut.Reducer_InOut_1key_1value;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;

public class MapRed_selectPhotos extends Configured implements Tool{

	/**
	 * 
	 * job1:  	select photos and save into mapFiles
	 * mapper: 
	 * partitioner: 
	 * reducer: 
	 * @param 		"reducerInter"  "selectPhotosPath"
	 * 
	 * @throws Exception 
	 * 
	 * MEva13:	yarn jar MapRed_selectPhotos.jar SelectPhoto.MapRed_selectPhotos -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar  -DreducerInter=100000 -DselectPhotosPath=MediaEval13/MEval13_L_to_S.hashMap -DselectSampleMaxS=8801049 66M_Phos_Seqs/ MediaEval13/Photos_MEva13_9M_MapFiles
	 * MEva14:	yarn jar MapRed_selectPhotos.jar SelectPhoto.MapRed_selectPhotos -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar  -DreducerInter=100000 -DselectPhotosPath=MediaEval14/MEval14_photos_L_to_S.hashMap Webscope100M/ME14_Crawl/Photos MediaEval14/Photos_MEva14_5M_MapFiles
	 * MEva15:	yarn jar MapRed_selectPhotos.jar SelectPhoto.MapRed_selectPhotos -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar  -DreducerInter=100000 -DselectPhotosPath=MediaEval15/ME15_photos_L_to_S_train.hashMap Webscope100M/ME15,Webscope100M/ME14_Crawl/Photos MediaEval15/Photos_MEva15_train_inSInd_MapFiles
	 * MEva15:	yarn jar MapRed_selectPhotos.jar SelectPhoto.MapRed_selectPhotos -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar  -DreducerInter=100000 -DselectPhotosPath=MediaEval15/ME15_photos_L_to_S_test.hashMap Webscope100M/ME15,Webscope100M/ME14_Crawl/Photos MediaEval15/Photos_MEva15_test_inSInd_MapFiles
	 * Herve:	yarn jar MapRed_selectPhotos.jar SelectPhoto.MapRed_selectPhotos ImageR/BenchMark/Herve/HerverImage.seq ImageR/BenchMark/Herve/HerverImage.mapFile
	 * Oxford:	yarn jar MapRed_selectPhotos.jar SelectPhoto.MapRed_selectPhotos -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar  ImageR/BenchMark/Oxford/OxfordBuilding.seq ImageR/BenchMark/Oxford/OxfordBuilding.mapFile
	 * Barcel:	yarn jar MapRed_selectPhotos.jar SelectPhoto.MapRed_selectPhotos -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar  ImageR/BenchMark/Barcelona/Barcelona1K.seq ImageR/BenchMark/Barcelona/Barcelona1K.mapFile
	 * CVPR15:	yarn jar MapRed_selectPhotos.jar SelectPhoto.MapRed_selectPhotos -libjars BoofCV0.9.jar,EJML_boof0.9.jar,GeoRegression_boof0.9.jar,libpja_boof0.9.jar -DreducerInter=100000 -DselectPhotosPath=ImageR/BenchMark/UniDistractors_10M_fromFlickr66MWithPatch.hashSet 66M_Phos_Seqs ImageR/BenchMark/Pho_UniDistra_10M_Flickr66M_Inter100K
	 */
	
	public static void main(String[] args) throws Exception {
		runHadoop(args);
	}
	
	public static void runHadoop(String[] args) throws Exception {
		int ret = ToolRunner.run(new MapRed_selectPhotos(), args);
		System.exit(ret);
	}
	
	@SuppressWarnings({ "rawtypes" })
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem hdfs=FileSystem.get(conf); 
		ArrayList<String> cacheFilePaths=new ArrayList<String>();

		//set imagesPath
		String imagesPath=args[0];
		System.out.println("imagesPath:"+imagesPath);
		ArrayList<Path> imageSeqPaths = General_Hadoop.addImgPathsFromMyDataSet(imagesPath);
		System.out.println("imageSeqPaths:"+imageSeqPaths);
		//set out path
		String out_Job1_1=args[1];
		System.out.println("save mapFile to:"+out_Job1_1);
				
		//******* 1st job: save photos into MapFile ******
		String selectPhoPath=conf.get("selectPhotosPath");
		SelectSamples selectSamples=new SelectSamples(selectPhoPath, false);
		cacheFilePaths.clear();
		selectSamples.addDistriCache_SelectSamples(cacheFilePaths);//SelSamples path with symLink
		//set job1RedNum_saveMapFile
		int job1RedNum_saveMapFile=1; Class partitioner=null; //noSelection, no equalAssign
		if (selectSamples.isSelection) {
			Partitioner_equalAssign partitioner_equalAssign=new Partitioner_equalAssign(conf, false);
			job1RedNum_saveMapFile=partitioner_equalAssign.getReducerNum(selectSamples.getMaxS(conf.get("selectSampleMaxS"), hdfs, true));
			partitioner=partitioner_equalAssign.getPartitioner();
		}
		General_Hadoop.Job(conf, imageSeqPaths.toArray(new Path[0]), out_Job1_1, "savePhotos", job1RedNum_saveMapFile, 8, 2, true,
				MapRed_selectPhotos.class, selectSamples.getMapper(), partitioner, null,null, Reducer_InOut_1key_1value.class,
				IntWritable.class, BufferedImage_jpg.class, IntWritable.class, BufferedImage_jpg.class,
				SequenceFileInputFormat.class, MapFileOutputFormat.class, 1*1024*1024*1024L, 10,
				cacheFilePaths.toArray(new String[0]),null);

		hdfs.close();
		return 0;

	}
}
