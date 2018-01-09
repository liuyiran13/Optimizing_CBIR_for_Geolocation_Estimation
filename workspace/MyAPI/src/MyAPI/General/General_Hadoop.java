
package MyAPI.General;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import MyAPI.Obj.Disp;
import MyCustomedHaoop.ValueClass.FloatArr;
import MyCustomedHaoop.ValueClass.IntArr_byteArrArrArr_Short;

public class General_Hadoop {
	
	public static Object readObject_HDFS(FileSystem fileSystem, String objectPath) throws IOException, ClassNotFoundException {
		// deserialize model
		 ObjectInputStream ois = new ObjectInputStream( fileSystem.open(new Path(objectPath)));
		 Object obj = ois.readObject();
		 ois.close();
		return obj;
	}
	
	public static void writeObject_HDFS(FileSystem fileSystem, String objectPath, Object object) throws FileNotFoundException, IOException {
		// serialize model
		Path path_outModel=new Path(objectPath);
		if (fileSystem.exists(path_outModel))
			  System.err.println("Output already exists: "+path_outModel.toString());
		FSDataOutputStream fsdOut = fileSystem.create(path_outModel);
		ObjectOutputStream oos = new ObjectOutputStream(fsdOut);
		oos.writeObject(object);
		oos.flush(); oos.close();
	}
	
	public static void add_to_DistributedCache(Job job, String[] CatchFilePaths, String[] CatchArchPaths) throws URISyntaxException, InterruptedException {
		//add files to Distributed catch, //change on jobConf only available for this job, not influence the original conf
		if (CatchFilePaths!=null) {
			for (int i = 0; i < CatchFilePaths.length; i++) {
				job.addCacheFile(new URI(CatchFilePaths[i]));//use job's conf to make this cache setting only available for this job! 
			}
		}
		if (CatchArchPaths!=null) {
			for (int i = 0; i < CatchArchPaths.length; i++) {
				job.addCacheArchive(new URI(CatchArchPaths[i]));//use job's conf to make this cache setting only available for this job! 
			}
		}
	}
	
	public static void addToCacheListWithOriNameAsSymLink(List<String> list, String toAdd, String delimeter, String preFix) {
		if (toAdd!=null) {
			for (String one:toAdd.split(delimeter)) {
				String[] infos=one.split("/");
				list.add(preFix+one+"#"+infos[infos.length-1]); 
			}
		}
	}
	
	@SuppressWarnings({ "rawtypes" })
	public static void Job(boolean isRun, Configuration conf, Path[] inPath, String outPath, String JobName, int jobRedNum, int reduNumPerNode, int maxRedAttemp, boolean speculativeExecution,
			Class JarByClass, Class mapperClass, Class partitionerClass, Class combinerClass, Class keyGrouperClass, Class reducerClass, 
			Class mapOutkeyCls, Class mapOutvalueCls, Class redOutkeyCls, Class redOutvalueCls,  
			Class inputFormateCls, Class outputFormatCls, long MinInputSplitSize, int MapFileIndexInter, 
			String[] CatchFilePaths, String[] CatchArchPaths) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException{
		if (isRun) {
			if (outPath!=null) {//delete old outPath 
				FileSystem.get(conf).delete(new Path(outPath), true);
			}
			Job(conf, inPath, outPath, JobName, jobRedNum, reduNumPerNode, maxRedAttemp, speculativeExecution, JarByClass, mapperClass, partitionerClass, combinerClass, keyGrouperClass, reducerClass, mapOutkeyCls, mapOutvalueCls, redOutkeyCls, redOutvalueCls, inputFormateCls, outputFormatCls, MinInputSplitSize, MapFileIndexInter, CatchFilePaths, CatchArchPaths);
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void Job(Configuration conf, Path[] inPath, String outPath, String JobName, int jobRedNum, int reduNumPerNode, int maxRedAttemp, boolean speculativeExecution,
			Class JarByClass, Class mapperClass, Class partitionerClass, Class combinerClass, Class keyGrouperClass, Class reducerClass, 
			Class mapOutkeyCls, Class mapOutvalueCls, Class redOutkeyCls, Class redOutvalueCls,  
			Class inputFormateCls, Class outputFormatCls, long MinInputSplitSize, int MapFileIndexInter, 
			String[] CatchFilePaths, String[] CatchArchPaths) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException{
		Job job = Job.getInstance(conf, JobName);
		Configuration jobConf=job.getConfiguration();
		//add files to Distributed catch, 
		General_Hadoop.add_to_DistributedCache(job, CatchFilePaths, CatchArchPaths); //change on jobConf only available for this job, not influence the original conf
		//define which jar to find all class in job config (classes below) 
		job.setJarByClass(JarByClass);
		//set max reducer number per node
		jobConf.set("mapreduce.tasktracker.reduce.tasks.maximum", reduNumPerNode+"");
		//set max reducer attempt per node
		jobConf.set("mapreduce.reduce.maxattempts", maxRedAttemp+"");
		//set speculativeExecution for reducer (enable or disable backup reducer for one attempt)
		jobConf.set("mapreduce.reduce.speculative", speculativeExecution+"");
		//set task timeout
//		jobConf.set("mapreduce.task.timeout", "600000");//in ms, default, 10min:600000. do not set here, because this is for all jobs! if want a higher one, please use -Dmapreduce.task.timeout=  to set, 
		//set when reducer start
		jobConf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.9");
		//set buffer memory used while sorting files to prevent error when one mapper needs to output a lot of data: Secure IO is not possible without native code extensions... MapOutputBuffer.mergeParts...
		jobConf.set("mapreduce.task.io.sort.mb", "600");
		//set memory opt
		int memory_mb=1024*56/reduNumPerNode;//in total 56GB per node
		jobConf.set("mapreduce.map.memory.mb",""+memory_mb);
		jobConf.set("mapreduce.map.java.opts","-Xmx"+memory_mb+"m");
		jobConf.set("mapreduce.reduce.memory.mb",""+memory_mb);
		jobConf.set("mapreduce.reduce.java.opts","-Xmx"+memory_mb+"m");
		//set mapper, reducer, partitioner
		if (mapperClass!=null) {
			job.setMapperClass(mapperClass);
		}
		if (partitionerClass!=null) {
			job.setPartitionerClass(partitionerClass);
		}
		if (combinerClass!=null) {
			job.setCombinerClass(combinerClass);
		}
		if (keyGrouperClass!=null) {
			job.setGroupingComparatorClass(keyGrouperClass);
		}
		if (reducerClass!=null) {
			job.setReducerClass(reducerClass);
		}
		job.setNumReduceTasks(jobRedNum);
		//set mapper out-put Key_Value
		job.setMapOutputKeyClass(mapOutkeyCls); //IntWritable.class,  if not set, MapOutputKeyClass will be OutputKeyClass
		job.setMapOutputValueClass(mapOutvalueCls);//BufferedImage_jpg.class
		//set job out-put Key_Value
		job.setOutputKeyClass(redOutkeyCls); //IntWritable.class
		job.setOutputValueClass(redOutvalueCls);//BufferedImage_jpg.class
		//set job in/out FileClass
		if (MapFileIndexInter>0) {
			MapFile.Writer.setIndexInterval(jobConf, MapFileIndexInter); //set MapFileOutputFormat index intervel
		}
		job.setInputFormatClass(inputFormateCls);
		job.setOutputFormatClass(outputFormatCls);
		//set FileInputFormat SplitSize,  SplitSize=Math.max(minSize, Math.min(maxSize, blockSize)), maxSize default is Long.Max
		if (MinInputSplitSize>0) {
			FileInputFormat.setMinInputSplitSize(job, MinInputSplitSize);//set MinInputSplitSize to a large value prevent split one seqFile to mutilple mappers!
		}
		//set job in/out file Path
		FileInputFormat.setInputPaths(job, inPath);//commaSeparatedPaths
		if (outPath!=null) {
			FileOutputFormat.setOutputPath(job, new Path(outPath));
		}
		//wait for completion
		if (!job.waitForCompletion(true)) {
	        throw new InterruptedException("Job Failed! job: "+job.getJobName());
		}
	}

	public static int[] make_vwPartitionIDs_HESig(int[][] TVectorInfo, int[] vw_queryFeatNum, int minRedueceNum){
		General.Assert(TVectorInfo.length==vw_queryFeatNum.length, "err in make_vePartitionIDs: TVectorInfo and vw_queryFeatNum should be equal length! TVectorInfo:"
				+TVectorInfo.length+", vw_queryFeatNum:"+vw_queryFeatNum.length);
		int[] PaIDs=new int[TVectorInfo.length]; //each element save vw-group's last Partition Position(from 0)!
		double kk=1000000; double k10=10000; double reducerNum=0;
		for(int vw=0;vw<TVectorInfo.length;vw++){
			if (TVectorInfo[vw]!=null) {//this vw has photos
				int TVector_featNum=TVectorInfo[vw][1];//TVectorInfo: photoNum, featNum
				int Query_featNum=vw_queryFeatNum[vw];
				reducerNum+=(TVector_featNum/kk)* (Query_featNum/k10);//TVector_featNum*Query_featNum every 2*(10^10):30min
			}
			PaIDs[vw] = (int) (reducerNum*10); //each element in PaIDs is mutipled by 10!
		}
		//check whether small scale data
		double scale=(minRedueceNum-1)/reducerNum;//we want the last reducerNum should be minRedueceNum-1
		if (scale>1) {//for small scale
			reducerNum=0;
			for(int vw=0;vw<TVectorInfo.length;vw++){
				if (TVectorInfo[vw]!=null) {//this vw has photos
					int TVector_featNum=TVectorInfo[vw][1];//TVectorInfo: photoNum, featNum
					int Query_featNum=vw_queryFeatNum[vw];
					reducerNum+=(TVector_featNum/kk)* (Query_featNum/k10)*scale;//TVector_featNum*Query_featNum every 2*(10^10):30min
				}
				PaIDs[vw] = (int) (reducerNum*10); //each element in PaIDs is mutipled by 10!
			}
		}
		return PaIDs;
	}
	
	public static int[] make_vwPartitionIDs_TFIDF(int[][] TVectorInfo, int[] vw_queryNum, int minRedueceNum){
		General.Assert(TVectorInfo.length==vw_queryNum.length, "err in make_vePartitionIDs: TVectorInfo and vw_queryNum should be equal length! TVectorInfo:"
				+TVectorInfo.length+", vw_queryNum:"+vw_queryNum.length);
		int[] PaIDs=new int[TVectorInfo.length]; //each element save vw-group's last Partition Position(from 0)!
		double kk=1000000; double k5=5000; double reducerNum=0;
		for(int vw=0;vw<TVectorInfo.length;vw++){
			if (TVectorInfo[vw]!=null) {//this vw has photos
				int TVector_photoNum=TVectorInfo[vw][0];//TVectorInfo: photoNum, featNum
				int Query_photoNum=vw_queryNum[vw];
				reducerNum+=(TVector_photoNum/kk)* (Query_photoNum/k5);//TVector_photoNum*Query_photoNum every 1*(10^9):15min
			}
			PaIDs[vw] = (int) (reducerNum*10); //each element in PaIDs is mutipled by 10!
		}
		//check whether small scale data
		double scale=(minRedueceNum-1)/reducerNum;//we want the last reducerNum should be minRedueceNum-1
		if (scale>1) {//for small scale
			reducerNum=0;
			for(int vw=0;vw<TVectorInfo.length;vw++){
				if (TVectorInfo[vw]!=null) {//this vw has photos
					int TVector_photoNum=TVectorInfo[vw][0];//TVectorInfo: photoNum, featNum
					int Query_photoNum=vw_queryNum[vw];
					reducerNum+=(TVector_photoNum/kk)* (Query_photoNum/k5)*scale;//TVector_featNum*Query_featNum every 2*(10^10):30min
				}
				PaIDs[vw] = (int) (reducerNum*10); //each element in PaIDs is mutipled by 10!
			}
		}
		return PaIDs;
	}
		
	public static void makeTextSeq_indIsKey(Path filePath, ArrayList<String> Info, Configuration conf) throws IOException {
		SequenceFile.Writer seq=SequenceFile.createWriter(conf, SequenceFile.Writer.file(filePath),SequenceFile.Writer.keyClass(IntWritable.class),SequenceFile.Writer.valueClass(Text.class));
		for (int i = 0; i < Info.size(); i++) {
			seq.append(new IntWritable(i), new Text(Info.get(i)));
		}
		seq.close();
	}
	
	public static int getTotReducerNum_from_vwPartitionIDs(int[] PaIDs) {
		//each element in PaIDs is index from 0, and is mutipled by 10!
		return (int) (Math.ceil(PaIDs[PaIDs.length-1]/10.0)+1);
	}
	
	public static int getVWReducerNum_from_vwPartitionIDs(int[] PaIDs, int vw) {
		//each element in PaIDs is index from 0, and is mutipled by 10!
		int previous=0;
		if (vw!=0) {
			previous=PaIDs[vw-1];
		}
		return PaIDs[vw]/10-previous/10+1;
	}
	
	public static int getVWPartitionID_from_vwPartitionIDs(int vw, int[] PaIDs, int numPartitions, Random rand) {
		//each element in PaIDs is index from 0, and is mutipled by 10!
		int groNum; //group number for this vw
		if(vw==0){
			groNum=PaIDs[vw]+1; //PaIDs value from 0!
		}else{
			groNum=PaIDs[vw]-PaIDs[vw-1]+1;
		}
		int PartitionID=(PaIDs[vw]-rand.nextInt(groNum))/10;//groNum at least == 1, two vw's PartitionID maybe the same, so multiple vws share one reduecer.
		PartitionID=PartitionID%numPartitions;
		return PartitionID;
	}
	
	public static Path getLocalPath(String path, Configuration conf) throws IOException{
		FileSystem fs_local=FileSystem.getLocal(conf);
		Path path_local=new Path(path).makeQualified(fs_local.getUri(),fs_local.getWorkingDirectory());
		return path_local;
	}
	
	public static MapFile.Reader openMapFileInNode(String dataSysLink, Configuration conf, Boolean showPathInfo) throws IOException{
		if (new File(dataSysLink).exists()) {
			Path path_local=getLocalPath(dataSysLink, conf);
			General.dispInfo_ifNeed(showPathInfo, "\t", "path_local:"+path_local);
			return new MapFile.Reader(path_local, conf);
		}else {
			return null;
		}
	}
	
	public static SequenceFile.Reader openSeqFileInNode(String dataSysLink, Configuration conf, Boolean showPathInfo) throws IOException{
		if (new File(dataSysLink).exists()) {
			Path path_local=getLocalPath(dataSysLink, conf);
//			Path path_local=new Path(dataSysLink);
			General.dispInfo_ifNeed(showPathInfo, "\t", "openSeqFileInNode, path_local:"+path_local);
			return new SequenceFile.Reader(conf, SequenceFile.Reader.file(path_local));
		}else{
			return null;
		}
	}
	
	public static MapFile.Reader openOneMapFile(String mapFilePath) throws IOException{
		return openAllMapFiles(new String[]{mapFilePath})[0];
	}
	
	public static MapFile.Reader[] openAllMapFiles(String[] MapFilePaths) throws IOException{
		//set FileSystem
      	Configuration conf = new Configuration();
		//load image mapFiles
        LinkedList<MapFile.Reader> mapFiles=new LinkedList<MapFile.Reader>();
		for (int i = 0; i < MapFilePaths.length; i++) {
			if (new File(MapFilePaths[i]).exists()) {
				if (isOneMapFileFolder(MapFilePaths[i])) {//this one is a single mapFileFolder
					mapFiles.add(new MapFile.Reader(new Path(MapFilePaths[i]), conf));
				}else {//this one is a folder, and contains many mapFileFolders with the name part* 
					int mapfileNum=0;
					for(File oneFile: new File(MapFilePaths[i]).listFiles()){
						if (oneFile.getName().startsWith("part")) {
							mapfileNum++;
						}
					}
					for (int ind = 0; ind < mapfileNum; ind++) {
						mapFiles.add(new MapFile.Reader(new Path(MapFilePaths[i]+"part-r-"+General.StrleftPad(ind+"", 0, 5, "0")), conf));
					}
				}
			}
		}
		if (mapFiles.size()>0) {
			return mapFiles.toArray(new MapFile.Reader[0]);
		}else {
			return null;
		}
	}
	
	public static boolean isOneMapFileFolder(String MapFilePath) throws IOException{
		File thisPath=new File(MapFilePath);
		boolean containData=false;
		if (thisPath.isDirectory()) {
			File[] files=thisPath.listFiles();
			for (File file : files) {
				if (file.getName().equalsIgnoreCase("data")) {
					containData=true;
					break;
				}
			}
		}
		return containData;
	}
		
	public static Path[] strArr_to_PathArr(String[] paths) {
		Path[] resPaths=new Path[paths.length];
		for (int i = 0; i < paths.length; i++) {
			resPaths[i]=new Path(paths[i]);
		}
		return resPaths;
	}
	
	public static void closeAllMapFiles(MapFile.Reader[] mapFiles) throws IOException{
		//close image mapFiles
		if (mapFiles!=null) {
			for (int i = 0; i < mapFiles.length; i++) {
				mapFiles[i].close();
			}
		}
	}
	
	public static void deleteIfExist(String path, FileSystem hdfs) throws IOException{
		if (hdfs.exists(new Path(path))) {
			hdfs.delete(new Path(path), true);
		}
	}

	public static <T extends Writable> T readValueFromMFiles(int photoIndex, int saveInterval, MapFile.Reader[] mapFiles, T value, Disp disp) throws IOException{
		int mapFileIndex=-1;
		if (mapFiles.length==1) {
			mapFileIndex=0;
		}else if (photoIndex<0) {//negative photos are ImageR dataset photos, e.g., Oxford, Holidays or Barcelona..   
			//these photos are save into one mapFile, and put in the last entry of mapFiles
			mapFileIndex=mapFiles.length-1;
		}else {
			mapFileIndex=photoIndex/saveInterval;
		}		
		if (mapFiles[mapFileIndex].get(new IntWritable(photoIndex), value)==null) {//no exist
			disp.disp("warning in readValueFromMFiles!  "+photoIndex+" key not exist in mapFiles-"+mapFileIndex);
			return null;
		}else {
			return value;
		}
	}
	
	public static <T extends Writable> T readValueFromSeqFile(int targetPho, String seqFilePath, T value) throws IOException{
		SequenceFile.Reader seqFile=new SequenceFile.Reader(new Configuration(), SequenceFile.Reader.file(new Path(seqFilePath)));
		IntWritable phoID=new IntWritable();
		T res=null;
		while (seqFile.next(phoID, value)) {
			if (phoID.get()==targetPho) {
				res=value;
				break;
			}
		}
		if(res==null){
			System.out.println("warning!  "+targetPho+" key not exist in seqFile: "+seqFilePath);
		}
		seqFile.close();
		return res;
	}

	public static <T extends Writable> T readOnlyOneElement(Iterable<T> value, String keyLabel){
		//******** only one in value! ************	
		int loopNum=0; T res=null; LinkedList<T> allSamples=new LinkedList<T>();
		for(Iterator<T> it=value.iterator();it.hasNext();){// loop over all T				
			res=it.next();
			allSamples.add(res);
			loopNum++;
		}
		General.Assert(loopNum==1, "err! for key:"+keyLabel+", it should be only 1 value in the Iterable, loopNum should == 1, here loopNum="+loopNum
				+", allSamples: "+allSamples);
		return res;
	}
	
	public static float[][] readMatrixFromSeq_floatMatrix(Configuration conf, Path fileName) throws IOException{
		//**** read matrix from seq file in local node, key is the index, value is one row in Matrix.
		if (fileName!=null && isExistFile(conf, fileName)) {
			SequenceFile.Reader seqReader=new SequenceFile.Reader(conf, SequenceFile.Reader.file(fileName));
			IntWritable key=new IntWritable(); FloatArr value=new FloatArr(); int dataNum=0;
			while (seqReader.next(key, value)) {
				dataNum++;
			}
			seqReader.close();
			//add data
			float[][] dataMatrix=new float[dataNum][];
			seqReader=new SequenceFile.Reader(conf, SequenceFile.Reader.file(fileName));
			while (seqReader.next(key, value)) {
				dataMatrix[key.get()]=value.getFloatArr();
			}
			seqReader.close();
			return dataMatrix;
		}else {
			return null;
		}
	}
	
	public static boolean isExistFile(Configuration conf, Path fileName) throws IOException{
		//get correct system for data in HDFS or local node
		if (fileName.toString().startsWith("file")) {
			return FileSystem.getLocal(conf).exists(fileName);
		}else{
			return FileSystem.get(conf).exists(fileName) || FileSystem.get(conf).exists(new Path("hdfs://hathi-surfsara/"+fileName.toString()));
		}
	}
	
	public static SequenceFile.Writer createSeqFileWriter(Configuration conf, Path pathToFile, Class<?> keyClass, Class<?> valueClass) throws IOException {
        return SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(pathToFile),
//                SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE),
                SequenceFile.Writer.keyClass(keyClass),
                SequenceFile.Writer.valueClass(valueClass)
        );
    }
	
	@SuppressWarnings("deprecation")
	public static HashMap<Integer, ArrayList<byte[]>> extractFeatFromTVector (int targetDoc, String TVectorPath, int fileNum, int dispInter, long startTime) throws IOException, ClassNotFoundException {
		//set FileSystem
      	Configuration conf = new Configuration();
        FileSystem hdfs  = FileSystem.get(conf);
        IntWritable MapFile_key=new IntWritable();
        IntArr_byteArrArrArr_Short MapFile_value = new IntArr_byteArrArrArr_Short();//<Names&Sigs>
		//extract feat from TVector
        HashMap<Integer,ArrayList<byte[]>> VW_Sigs_query=new HashMap<Integer, ArrayList<byte[]>>();
        
		for(int file_i=0; file_i<fileNum; file_i++){
			MapFile.Reader mapFile=new MapFile.Reader(hdfs,TVectorPath+"part-r-"+General.StrleftPad(file_i+"", 0, 5, "0"), conf);
			while(mapFile.next(MapFile_key, MapFile_value)){//loop over all vws_sigs 
				if (MapFile_value!=null) {
					int targetDocInd=-1;
					int[] docs=MapFile_value.getIntegers();
					for (int doc_i=0; doc_i<docs.length; doc_i++) {
						if (docs[doc_i]==targetDoc) {
							targetDocInd=doc_i;
						}
					}
					if (targetDocInd>-1) {//targetDoc exist this vw
						VW_Sigs_query.put(MapFile_key.get(), General.ByteArrArrToListByteArr(MapFile_value.getbyteArrArrArr()[targetDocInd]));
					}
				}
			}
			mapFile.close();
			if (dispInter!=0 && file_i%dispInter==0) {
				System.out.println("read TVector Mapfile-"+file_i+" finished!"+General.dispTime(System.currentTimeMillis()-startTime, "min"));
			}
		}
		return VW_Sigs_query;
	}

	public static ArrayList<Path> addImgPathsFromMyDataSet(String imagesPath){
		ArrayList<Path> imageSeqPaths=new ArrayList<Path>();
		String[] imgFolders=imagesPath.split(",");
		for (String oneFolder: imgFolders) {
			if (oneFolder.equalsIgnoreCase("66M_Phos_Seqs")) {//b.set image sequence file paths, 66M, imagesPath=otherArgs[0]: 66M_Phos_Seqs/
				double Sym1M=1000*1000;
				int saveInterval=1000*1000; 
				int start_loop=3; //should start from 3
				int end_loop=66;  //66
				for(int loop_i=start_loop;loop_i<=end_loop;loop_i++){//one loop, one MapFile
					//set photo range for one file
					int[] photoRang=new int[2];
					if(loop_i==3){
						photoRang[0]=3185259;
					}else{
						photoRang[0]=loop_i*saveInterval;
					}
					photoRang[1]=(loop_i+1)*saveInterval-1;
					imageSeqPaths.add(new Path(oneFolder+"/"+photoRang[0]/Sym1M+"_"+photoRang[1]/Sym1M+"_seq"));
				}
				imageSeqPaths.add(new Path(oneFolder+"/"+"3_66_patch_seq"));
				imageSeqPaths.add(new Path(oneFolder+"/"+"missingBlocks_patch_seq"));
			}else {//a.set image sequence file paths_3M and Herve, imagesPath=otherArgs[0]: 3M_Photos_SeqFiles,ImageR/BenchMark/HerveImage/HerverImage_relImg.seq
				imageSeqPaths.add(new Path(oneFolder));
			}
		}
		
		return imageSeqPaths;
	}
	
	public static Integer[] intWritableArr_to_IntegerrArr(IntWritable[] aa){
		Integer[] res=new Integer[aa.length];
		for (int i = 0; i < aa.length; i++) {
			res[i]=aa[i].get();
		}
		return res;
	}
}
