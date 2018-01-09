package MyCustomedHaoop.MapRedFunction;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MyAPI.General.General_Hadoop;
import MyCustomedHaoop.Mapper.Mapper_read_out;
import MyCustomedHaoop.Mapper.SelectSamples;

public class MapRed_SelectSample extends Configured implements Tool{

	/**
	 * select data samples
	 * @command_example: 
	 * hadoop jar MapRed_SelectSample.jar MyCustomedHaoop.MapRedFunction.MapRed_SelectSample  Webscope100M/ME14_Crawl/Photos Webscope100M/ME14_Crawl/	 
	 */

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new MapRed_SelectSample(), args);
		System.exit(ret);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		String input=args[0];//input paths to process, seprated with ","
		String output=args[1];
		String selSamples=args[2];
		String dataKeyClass=args[3];
		String dataValueClass=args[4];
		runHadoop(getConf(), General_Hadoop.strArr_to_PathArr(input.split(",")), output, selSamples, Class.forName(dataKeyClass), Class.forName(dataValueClass));
		return 0;
	}
	
	@SuppressWarnings("rawtypes")
	public static void runHadoop(Configuration conf, Path[] input, String output, String selSamples, Class dataKeyClass, Class dataValueClass) throws Exception {		
		SelectSamples selectSamples=new SelectSamples(selSamples, false);
		if (selectSamples.isSelection) {
			ArrayList<String> cacheFilePaths=new ArrayList<>();
			selectSamples.addDistriCache_SelectSamples(cacheFilePaths);//SelSamples path with symLink
			//run 
			General_Hadoop.Job(conf, input, output, "selSample", 0, 8, 10, true,
					MapRed_makeRawFeat_VW.class, selectSamples.getMapper(), null, null, null, null,
					dataKeyClass, dataValueClass, dataKeyClass, dataValueClass,
					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 100*1024*1024L, 0,
					cacheFilePaths.toArray(new String[0]),null);
		}else{
			//run 
			General_Hadoop.Job(conf, input, output, "selSample", 0, 8, 10, true,
					MapRed_makeRawFeat_VW.class, Mapper_read_out.Mapper_readOut.class, null, null, null, null,
					dataKeyClass, dataValueClass, dataKeyClass, dataValueClass,
					SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 100*1024*1024L, 0,
					null,null);
		}
	}

}
