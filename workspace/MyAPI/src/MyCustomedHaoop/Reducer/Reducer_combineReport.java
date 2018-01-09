package MyCustomedHaoop.Reducer;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyCustomedHaoop.Reducer.Reducer_InOut.Reducer_InOut_1key_1value;

public class Reducer_combineReport extends Reducer<IntWritable,Text,IntWritable,Text>  {
	
	private Configuration conf;
	private FileSystem hdfs;
	private StringBuffer outInfo;
	private String InfoStrPath;
	private int report_num;
	private int dispInter;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		hdfs=FileSystem.get(conf);
		
		outInfo=new StringBuffer();
		
		//**** set InfoStrPath ************//
		InfoStrPath=conf.get("InfoStrPath");
		
		report_num=0;
		dispInter=100;
		// ***** setup finsihed ***//
		System.out.println("only 1 reducer, combine analysized report from all reRankFlags, save String obj to InfoStrPath: "+InfoStrPath);
		System.out.println("setup finsihed!");
		super.setup(context);
 	}
	
	@Override
	public void reduce(IntWritable ReportID, Iterable<Text> reports, Context context) throws IOException, InterruptedException {
		//key: queryName, value: rank result
		
		//******** only one list in rank result! ************		
		int reportID=ReportID.get(); Text oneReport = General_Hadoop.readOnlyOneElement(reports, ReportID+"");
		
		outInfo.append(oneReport.toString()+"\n");
		
		report_num++;	
		
		General.dispInfo_ifNeed(report_num%dispInter==0, "", "current reportID:"+reportID+", Report:"+oneReport);

	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		//outPut
		General_Hadoop.writeObject_HDFS(hdfs, InfoStrPath, outInfo.toString());
		
		// ***** setup finsihed ***//
		System.out.println("\n Reducer finished! total report_num:"+report_num);
		super.setup(context);
 	}

	@SuppressWarnings("rawtypes")
	public static String Job_combineReports(Configuration conf, FileSystem hdfs, String reportPath, String InfoStrPath, Class JarByClass, String jobName, boolean deleteInfoStrPath) throws ClassNotFoundException, IllegalArgumentException, IOException, InterruptedException, URISyntaxException{		
		//set info
		conf.set("InfoStrPath",InfoStrPath); //save Info as String object to InfoStrPath
		General_Hadoop.Job(conf, new Path[]{new Path(reportPath)}, null, jobName, 1, 8, 10, true,
				JarByClass, null, null, null, null, Reducer_combineReport.class,
				IntWritable.class, Text.class, IntWritable.class,Text.class,
				SequenceFileInputFormat.class, NullOutputFormat.class, 0, 10,
				null,null);
		String res=(String) General_Hadoop.readObject_HDFS(hdfs, InfoStrPath);
		if (deleteInfoStrPath) {
			hdfs.delete(new Path(InfoStrPath), true);
		}
		return res;
	}
	
	@SuppressWarnings("rawtypes")
	public static void Job_combineReports(Configuration conf, String reportPath, PrintWriter repWriter, Class JarByClass, String jobName) throws ClassNotFoundException, IllegalArgumentException, IOException, InterruptedException, URISyntaxException{		
		//set info
		String tmpPath=reportPath+"_tmp";
		General_Hadoop.Job(conf, new Path[]{new Path(reportPath)}, tmpPath, jobName, 1, 8, 10, true,
				JarByClass, null, null, null, null, Reducer_InOut_1key_1value.class,
				IntWritable.class, Text.class, IntWritable.class,Text.class,
				SequenceFileInputFormat.class, SequenceFileOutputFormat.class, 0, 10,
				null,null);
		SequenceFile.Reader seqReader=new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(tmpPath+"/part-r-00000")));
		IntWritable key=new IntWritable();
		Text value=new Text();
		int repNum=0;
		while (seqReader.next(key, value)) {
			repWriter.println(value.toString());
			repNum++;
		}
		repWriter.println("\n"+"combine reports done! total reports: "+repNum);
		seqReader.close();
		FileSystem.get(conf).delete(new Path(tmpPath), true);
	}
}
