package MyCustomedHaoop.ValueClass;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;
import MyAPI.General.General_http;

/**
 *	writable class for saving buffered image in jpg image 
 *  Attention: every time, read and write BufferedImage_jpg will cause Jpg compression! so img is not exactly the same!!
 *
 */
public class VideoBytes extends BytesValue{

	public VideoBytes(byte[] byteContent) {
		super(byteContent);
	}
	
	public VideoBytes(File file, boolean disp, String spacer) throws InterruptedException {
		super(file, disp, spacer);
	}
	
	public VideoBytes(String filePath, boolean disp, String spacer) throws InterruptedException {
		super(filePath, disp, spacer);
	}
	
	public VideoBytes() {
		// do nothing
	}
	
	public int getVideoFrams(String videoPath, String frameFolder, String binaryPath, boolean disp, String spacer) throws IOException, InterruptedException {
		//write bytes to local file
		General.writeBinaryFile(byteContent, videoPath, disp, spacer);
		//call ffmpeg to extract frames
		int frameNum=General_BoofCV.getVideoFrames_binaryFFMPEG(videoPath, binaryPath, frameFolder, "-vf fps=fps=1 %d.png", disp, false);
		//delete video file
		General.deleteAll(new File(videoPath));
		return frameNum;
	}
	
	public static void main(String[] args) throws IOException, InterruptedException{ 
//		testGeneralWR();	
		
//		test_downloadFlickrVideo();
		
		checkOneVideo();
	}
	
	@SuppressWarnings("deprecation")
	public static void testGeneralWR() throws IOException, InterruptedException{ 
		//****** test general read and write
		String basePath="D:/xinchaoli/Desktop/My research/My Code/test/";
		//set save class
		IntWritable MapFile_key = new IntWritable();
		VideoBytes MapFile_value = new VideoBytes();
		//set MapFile.Writer
		Configuration conf = new Configuration();
		FileSystem hdfs  = FileSystem.get(conf);
		int MapFileindInter=10;
		String MapFile_path=basePath+"video_MapFile_test/";
		
		// ******* write from video file to MapFile *************
		MapFile.Writer MapFileWriter = new MapFile.Writer(conf, hdfs, MapFile_path, MapFile_key.getClass(), MapFile_value.getClass());
		MapFileWriter.setIndexInterval(MapFileindInter);
		File[] videos=new File(basePath+"oriVideos").listFiles();
		for(int i=0;i<videos.length;i++){
			MapFile_key.set(i);
			MapFile_value.setBytesValue(videos[i].getAbsolutePath(), true, "");
			MapFileWriter.append(MapFile_key, MapFile_value);
		}
		MapFileWriter.close();
		
		// ******* read from MapFile to image *************
		String binaryPath="D:/xinchaoli/Desktop/My research/Code_Tools/FFmepeg/ffmpeg-20140727-git-ad91bf8-win64-static/bin/ffmpeg.exe";
		MapFile.Reader MapFileReader=new MapFile.Reader(hdfs, MapFile_path, conf);
		for(int i=0;i<videos.length;i++){
			MapFile_key.set(i); 
			MapFileReader.get(MapFile_key, MapFile_value);
			MapFile_value.getVideoFrams(basePath+MapFile_key.get()+"_recVideo", basePath+MapFile_key.get()+"/", binaryPath, true, "");
		}
		MapFileReader.close();
	}

	public static void test_downloadFlickrVideo() throws IOException, InterruptedException {
		String url="http://www.flickr.com/videos/12289718@N00/3915809543/play/orig/f826b40bea";
		String binaryPath="D:/xinchaoli/Desktop/My research/Code_Tools/FFmepeg/ffmpeg-20140727-git-ad91bf8-win64-static/bin/ffmpeg.exe";
		String basePath="D:/xinchaoli/Desktop/My research/My Code/test/";
		int maxRetry=5; int sleepSec=5; int[] skipped_statics=new int[3]; boolean disp=true;
		byte[] video_bytes=General_http.readByteFromURL(true, url, maxRetry, "VideoTest", sleepSec, skipped_statics, disp, "\t");
		VideoBytes video = new VideoBytes(video_bytes);
		video.getVideoFrams(basePath+"downloadFlickrVideo_recVideo", basePath+"downloadFlickrVideo/", binaryPath, true, "");
	}
	
	public static void checkOneVideo() throws IOException, InterruptedException{ 
		//****** checkOneVideo
		String basePath="O:/MediaEval14/";
		String binaryPath="D:/xinchaoli/Desktop/My research/Code_Tools/FFmepeg/ffmpeg-20140727-git-ad91bf8-win64-static/bin/ffmpeg.exe";
		//set save class
		VideoBytes MapFile_value = new VideoBytes((byte[]) General.readObject(basePath+"VideoBytes_8829"));
		MapFile_value.getVideoFrams(basePath+"Video_8829.video", basePath+"Video_8829/", binaryPath, true, "\t");
	}
}
