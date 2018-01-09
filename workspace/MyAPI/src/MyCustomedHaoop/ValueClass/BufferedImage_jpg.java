package MyCustomedHaoop.ValueClass;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;

import MyAPI.General.General_BoofCV;
import MyAPI.Obj.Disp;

/**
 *	writable class for saving buffered image in jpg image 
 *  Attention: every time, read and write BufferedImage_jpg will cause Jpg compression! so img is not exactly the same!!
 *
 */
public class BufferedImage_jpg extends BytesValue{

	public BufferedImage_jpg(byte[] byteContent) {
		super(byteContent);
	}
	
	public BufferedImage_jpg(BufferedImage img, String format) throws IOException {
		super(General_BoofCV.BufferedImageToBytes(img, format));
	}
	
	public BufferedImage_jpg(String filePath, boolean disp, String spacer) throws IOException, InterruptedException {
		super(filePath, disp, spacer);
	}
	
	public BufferedImage_jpg(File filePath, boolean disp, String spacer) throws IOException, InterruptedException {
		super(filePath, disp, spacer);
	}
	
	public BufferedImage_jpg() {
		// do nothing
	}

	public void setBufferedImage(BufferedImage img, String format) throws IOException {
		this.byteContent = General_BoofCV.BufferedImageToBytes(img, format);
	}

	public BufferedImage getBufferedImage(String photoInfo, Disp disp) {//photoInfo: "photoName:253662"
		if (byteContent!=null) {
			return General_BoofCV.BytesToBufferedImage(byteContent, photoInfo, disp);
		}else {
			disp.disp("in getBufferedImage, no byteContent, return null for "+photoInfo);
			return null;
		}
	}
	
	@Override
	public String toString(){
		return "BufferedImage_jpg: byteNum:"+byteContent.length;
	}
	
	public static void main(String[] args) throws IOException, InterruptedException{ 
		testGeneralWR();
		
//		testImgToByteArr();
	}
	
	@SuppressWarnings("deprecation")
	public static void testGeneralWR() throws IOException, InterruptedException{ 
		//****** test general read and write
		String basePath="D:/xinchaoli/Desktop/My research/My Code/test/";
		//set save class
		IntWritable MapFile_key = new IntWritable();
		BufferedImage_jpg MapFile_value = new BufferedImage_jpg();
		//set MapFile.Writer
		Configuration conf = new Configuration();
		FileSystem hdfs  = FileSystem.get(conf);
		int MapFileindInter=10;
		String MapFile_path=basePath+"image_MapFile_test";
		
		// ******* write from image to MapFile *************
		MapFile.Writer MapFileWriter = new MapFile.Writer(conf, hdfs, MapFile_path, MapFile_key.getClass(), MapFile_value.getClass());
		MapFileWriter.setIndexInterval(MapFileindInter);
		for(int i=1;i<=10;i++){
			MapFile_key.set(i);
//			MapFile_value.setBufferedImage(ImageIO.read(new File(basePath+"out_"+i+".png")),"jpg");
			MapFile_value.setBytesValue(basePath+"out_"+i+".png", true, "\t");
			MapFileWriter.append(MapFile_key, MapFile_value);
		}
		MapFileWriter.close();
		
		// ******* read from MapFile to image *************
		MapFile.Reader MapFileReader=new MapFile.Reader(hdfs, MapFile_path, conf);
		for(int i=1;i<=10;i++){
			MapFile_key.set(i); 
			MapFileReader.get(MapFile_key, MapFile_value);
			ImageIO.write(MapFile_value.getBufferedImage("phoID:"+MapFile_key, Disp.getNotDisp()), "jpg", new File(basePath+"out_"+i+"_r.jpg"));
		}
		MapFileReader.close();
	}
	
	public static BufferedImage mimicReadFromBufferedImage_jpg(BufferedImage originalImage) throws IOException{
		/*
		 * mimic the real situation that save a photo to BufferedImage_jpg and then read it back to BufferedImage
		 */
		//transfer BufferedImage to byte[]
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ImageIO.write(originalImage, "jpg", baos);
		baos.flush();
		byte[] imageInByte = baos.toByteArray();
		baos.close();
		// convert byte array back to BufferedImage		
		BufferedImage reImage = ImageIO.read(new ByteArrayInputStream(imageInByte));
		return reImage;
	}
	
	public static void testImgToByteArr() throws IOException{ 
		//****** test BufferedImage to Byte[]
		String imgsPath="O:/MediaEval_3185258Images/trainImages_1-3185258/1-100000/";
		int i=1;
		BufferedImage originalImage = ImageIO.read(new File(imgsPath+i+"_3185258.jpg"));
		BufferedImage reImage=mimicReadFromBufferedImage_jpg(originalImage);
		int diff=0;
		for (int h = 0; h < originalImage.getHeight(); h++) {
			for (int w = 0; w < originalImage.getWidth(); w++) {
				int pix_ori=originalImage.getRGB(w, h);
				int pix_re=reImage.getRGB(w, h);
				if (pix_ori!=pix_re) {
					diff+=Math.abs(pix_ori-pix_re);
				}
			}
		}
		System.out.println("diff: "+diff);
	}

}
