package MyAPI.General;

import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import javax.imageio.ImageIO;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Decompressor;
import net.jpountz.lz4.LZ4Factory;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.math3.complex.Complex;
import org.apache.hadoop.io.MapFile;

import boofcv.alg.misc.PixelMath;
import boofcv.core.image.ConvertBufferedImage;
import boofcv.gui.image.ShowImages;
import boofcv.struct.image.ImageFloat32;
import edu.emory.mathcs.jtransforms.fft.FloatFFT_2D;
import MyAPI.Obj.Disp;
import MyAPI.Obj.GistParam;
import MyAPI.Obj.PowerSet;
import MyAPI.imagR.ImageDataManager;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
import MyCustomedHaoop.ValueClass.SURFpoint;

public class Test {

	public static void findCommonElement_twoSorted_ASC(){
		Random random=new Random(); long startTime; ArrayList<Integer> commons=new ArrayList<Integer>(); ArrayList<int[]> commonInds=null;
    	int docNum=10*1000*1000; int rep=100;
    	//make aa
    	int[] aa=new int[1000]; Integer[] aaI=new Integer[1000]; 
    	for (int i = 0; i < 1000; i++) {
    		aa[i]=random.nextInt(docNum);
    		aaI[i]=aa[i];
//    		aa[i]=i;
		}
    	Arrays.sort(aa);
    	Arrays.sort(aaI);
    	ArrayList<Integer> aaList=new ArrayList<>(Arrays.asList(aaI));
    	LinkedList<Integer> aaList0=new LinkedList<>(Arrays.asList(aaI));
    	//make bb
    	int[] bb=new int[docNum]; Integer[] bbI=new Integer[docNum];
    	for (int i = 0; i < docNum; i++) {
    		bb[i]=i;
    		bbI[i]=bb[i];
		}
    	ArrayList<Integer> bbList=new ArrayList<>(Arrays.asList(bbI));
    	LinkedList<Integer> bbList0=new LinkedList<>(Arrays.asList(bbI));
    	
    	//run 1
    	startTime=System.currentTimeMillis(); 
    	for (int i = 0; i < rep; i++) {
    		commonInds=General.findCommonElementInds_twoSorted_ASC_loopShotArr(aa, bb);
		}
		System.out.println("Run"+1+", "+rep+" rep finished: "+General.dispTime((System.currentTimeMillis()-startTime), "ms")+", memory:"+General.memoryInfo());
		for (int[] is : commonInds) {
			commons.add(is[0]);
		}
		System.out.println("commons size:"+commonInds.size()+", top:"+commons.subList(0, Math.min(commons.size(), 10))
    			+", last:"+commons.subList( Math.max(commons.size()-10, 0), commons.size()));
		//run 1
    	startTime=System.currentTimeMillis(); 
    	for (int i = 0; i < rep; i++) {
    		commons=General.findCommonElement_twoSorted_ASC_loopShotArr(aaI, bbI);
		}
		System.out.println("Run"+1+", "+rep+" rep finished: "+General.dispTime((System.currentTimeMillis()-startTime), "ms")+", memory:"+General.memoryInfo());
    	System.out.println("commons size:"+commons.size()+", top:"+commons.subList(0, Math.min(commons.size(), 10))
    			+", last:"+commons.subList( Math.max(commons.size()-10, 0), commons.size()));
    	//run 1
    	startTime=System.currentTimeMillis(); 
    	for (int i = 0; i < rep; i++) {
    		commons=General.findCommonElement_twoSorted_ASC_loopShotArr(new ArrayList<>(aaList0), new ArrayList<>(bbList0));
		}
		System.out.println("Run"+1+", "+rep+" rep finished: "+General.dispTime((System.currentTimeMillis()-startTime), "ms")+", memory:"+General.memoryInfo());
    	System.out.println("commons size:"+commons.size()+", top:"+commons.subList(0, Math.min(commons.size(), 10))
    			+", last:"+commons.subList( Math.max(commons.size()-10, 0), commons.size()));
    	//run 1
    	startTime=System.currentTimeMillis(); 
    	for (int i = 0; i < rep; i++) {
    		commons=General.findCommonElement_twoSorted_ASC_loopShotArr(aaList.toArray(new Integer[0]), bbList.toArray(new Integer[0]));
		}
		System.out.println("Run"+1+", "+rep+" rep finished: "+General.dispTime((System.currentTimeMillis()-startTime), "ms")+", memory:"+General.memoryInfo());
    	System.out.println("commons size:"+commons.size()+", top:"+commons.subList(0, Math.min(commons.size(), 10))
    			+", last:"+commons.subList( Math.max(commons.size()-10, 0), commons.size()));
		
//    	//run 2
//    	startTime=System.currentTimeMillis(); 
//    	for (int i = 0; i < rep; i++) {
//    		commons=General.findCommonElement_twoSorted_ASC_onebyone(aa, bb);
//		}
//		System.out.println("Run"+2+", "+rep+" rep finished: "+General.dispTime((System.currentTimeMillis()-startTime), "ms")+", memory:"+General.memoryInfo());
//    	System.out.println("commons size:"+commons.size()+", top:"+commons.subList(0, Math.min(commons.size(), 10))
//    			+", last:"+commons.subList( Math.max(commons.size()-10, 0), commons.size()));
//    	
//    	//run 0
//    	startTime=System.currentTimeMillis();  
//    	for (int i = 0; i < rep; i++) {
//    		commonInds=General.findCommonElementInds_twoSorted_ASC_booleanInd(aa, bb);
//		}
//		System.out.println("Run"+0+", "+rep+" rep finished: "+General.dispTime((System.currentTimeMillis()-startTime), "ms")+", memory:"+General.memoryInfo());
//    	commons.clear();
//		for (int[] is : commonInds) {
//			commons.add(is[0]);
//		}
//		System.out.println("commons size:"+commonInds.size()+", top:"+commons.subList(0, Math.min(commons.size(), 10))
//    			+", last:"+commons.subList( Math.max(commons.size()-10, 0), commons.size()));
//		//run 0
//    	startTime=System.currentTimeMillis(); 
//    	for (int i = 0; i < rep; i++) {
//    		commons=General.findCommonElement_twoSorted_ASC_booleanInd(aa, bb);
//		}
//		System.out.println("Run"+0+", "+rep+" rep finished: "+General.dispTime((System.currentTimeMillis()-startTime), "ms")+", memory:"+General.memoryInfo());
//    	System.out.println("commons size:"+commons.size()+", top:"+commons.subList(0, Math.min(commons.size(), 10))
//    			+", last:"+commons.subList( Math.max(commons.size()-10, 0), commons.size()));
//    	//run 0
//    	startTime=System.currentTimeMillis(); 
//    	for (int i = 0; i < rep; i++) {
//    		commons=General.findCommonElement_twoSorted_ASC_booleanInd(aaI, bbI);
//		}
//		System.out.println("Run"+0+", "+rep+" rep finished: "+General.dispTime((System.currentTimeMillis()-startTime), "ms")+", memory:"+General.memoryInfo());
//    	System.out.println("commons size:"+commons.size()+", top:"+commons.subList(0, Math.min(commons.size(), 10))
//    			+", last:"+commons.subList( Math.max(commons.size()-10, 0), commons.size()));
//    	//run 0
//    	startTime=System.currentTimeMillis(); 
//    	for (int i = 0; i < rep; i++) {
//    		commons=General.findCommonElement_twoSorted_ASC_booleanInd(aaList, bbList);
//		}
//		System.out.println("Run"+0+", "+rep+" rep finished: "+General.dispTime((System.currentTimeMillis()-startTime), "ms")+", memory:"+General.memoryInfo());
//    	System.out.println("commons size:"+commons.size()+", top:"+commons.subList(0, Math.min(commons.size(), 10))
//    			+", last:"+commons.subList( Math.max(commons.size()-10, 0), commons.size()));
//    	//run 0
//    	startTime=System.currentTimeMillis(); 
//    	for (int i = 0; i < rep; i++) {
//    		commons=General.findCommonElement_twoSorted_ASC_booleanInd(aaList.toArray(new Integer[0]), bbList.toArray(new Integer[0]));
//		}
//		System.out.println("Run"+0+", "+rep+" rep finished: "+General.dispTime((System.currentTimeMillis()-startTime), "ms")+", memory:"+General.memoryInfo());
//    	System.out.println("commons size:"+commons.size()+", top:"+commons.subList(0, Math.min(commons.size(), 10))
//    			+", last:"+commons.subList( Math.max(commons.size()-10, 0), commons.size()));
	}
	
	public static void readImage() throws InterruptedException, IOException{
		ImageDataManager imageDataManager=new ImageDataManager("IndexIsName", 0, "O:/ImageRetrieval/HerveImage/ori_Data/Jpeg_organised/", null, 0, null, null, null);
		BufferedImage img=imageDataManager.getImage(-10006, new Disp(true,"",null));
		System.out.println(img.getHeight()+"_"+img.getWidth());
	}
	
	public static void computeSIFT_binaryOxford2() throws InterruptedException, IOException{
		ImageDataManager imageDataManager=new ImageDataManager("IndexIsName", 0, "O:/ImageRetrieval/Herve1.5K/ori_Data/Jpeg_organised/", null, 0, null, null, null);
		int[] S_to_L=(int[]) General.readObject("O:/ImageRetrieval/Herve1.5K/Herve_ori1.5K_SelPhos_S_to_L.intArr");
		int ind_inL=S_to_L[409];
		BufferedImage img=imageDataManager.getImage(ind_inL, new Disp(true,"",null));
		String binaryPath_DetDes="D:/xinchaoli/Desktop/My research/Code_Tools/SIFT_binary/Oxford_Version/improved_extract_features2/Oxford2_extract_features_32bit.exe";
		String tempFilesPath="D:/xinchaoli/Desktop/My research/My Code/Sara/"; String tmpFileMarker="testOxfordSIFT_windows";
		System.out.println(img.getHeight()+"_"+img.getWidth());
		img=General.getScaledInstance_onlyDownScale(img, 1024*768);
		System.out.println(img.getHeight()+"_"+img.getWidth());
		ArrayList<SURFpoint> interestPoints=new ArrayList<SURFpoint>();
		double[][] feats= General_BoofCV.computeSIFT_binaryOxford1(ind_inL+"", img, binaryPath_DetDes, tempFilesPath, tmpFileMarker, "-sift -hesaff -hesThres", 300, 30000, true,true,false,interestPoints);
		General_BoofCV.showFeaturePoint(interestPoints, img, ind_inL+"", 1);
		System.out.println("feats:"+feats.length);
		System.out.println("feats-0:"+General.douArrToString(feats[0], "_", "0.0"));
	}
	
	public static void computeSIFT_binaryVLFeat() throws InterruptedException, IOException{
		ImageDataManager imageDataManager=new ImageDataManager("IndexIsName", 0, "O:/ImageRetrieval/Herve1.5K/ori_Data/Jpeg_organised/", null, 0, null, null, null);
		int[] S_to_L=(int[]) General.readObject("O:/ImageRetrieval/Herve1.5K/Herve_ori1.5K_SelPhos_S_to_L.intArr");
		int ind_inL=S_to_L[409];
		BufferedImage img=imageDataManager.getImage(ind_inL, new Disp(true,"",null));
		String binaryPath_DetDes="D:/xinchaoli/Desktop/My research/Code_Tools/SIFT_binary/VLFeat/vlfeat-0.9.18-bin/vlfeat-0.9.18/bin/win64/sift.exe";
		String tempFilesPath="D:/xinchaoli/Desktop/My research/My Code/Sara/"; String tmpFileMarker="testVLFeat_windows";
		System.out.println(img.getHeight()+"_"+img.getWidth());
		img=General.getScaledInstance_onlyDownScale(img, 1024*768);
		System.out.println(img.getHeight()+"_"+img.getWidth());
		ArrayList<SURFpoint> interestPoints=new ArrayList<SURFpoint>();
		double[][] feats= General_BoofCV.computeSIFT_binaryVLFeat09(ind_inL+"", img, binaryPath_DetDes, tempFilesPath, tmpFileMarker, 3, 5, 7000, true,false,interestPoints);
		General_BoofCV.showFeaturePoint(interestPoints, img, ind_inL+"", 1);
		System.out.println("feats:"+feats.length);
		System.out.println("feats-0:"+General.douArrToString(feats[0], "_", "0.0"));
	}
	
	public static void computeSURF() throws InterruptedException, IOException{
		ImageDataManager imageDataManager=new ImageDataManager("IndexIsName", 0, "O:/ImageRetrieval/Herve1.5K/ori_Data/Jpeg_organised/", null, 0, null, null, null);
		int[] S_to_L=(int[]) General.readObject("O:/ImageRetrieval/Herve1.5K/Herve_ori1.5K_SelPhos_S_to_L.intArr");
		int ind_inL=S_to_L[409];
		BufferedImage img=imageDataManager.getImage(ind_inL, new Disp(true,"",null));
		System.out.println(img.getHeight()+"_"+img.getWidth());
		img=General.getScaledInstance_onlyDownScale(img, 1024*768);
		System.out.println(img.getHeight()+"_"+img.getWidth());
		ArrayList<SURFpoint> interestPoints=new ArrayList<SURFpoint>();
		double[][] feats= General_BoofCV.computeSURF_boofCV_09(ind_inL+"", General_BoofCV.BoofCV_loadImage(img, ImageFloat32.class),"2,1,5,true","2000,1,9,4,4", interestPoints);
		General_BoofCV.showFeaturePoint(interestPoints, img, ind_inL+"", 1);
		System.out.println("feats:"+feats.length);
		System.out.println("feats-0:"+General.douArrToString(feats[0], "_", "0.0"));
	}
	
	public static void rgbToGray() throws IOException{
//		String photoPath="D:/xinchaoli/Desktop/My research/Code_Tools/Gist/MyGist/test.jpg";
		String photoPath="D:/xinchaoli/Desktop/My research/Code_Tools/Gist/MyGist/12_3185258.jpg";
		long startTime=System.currentTimeMillis();
		BufferedImage colorImage=ImageIO.read(new File(photoPath));
		General.dispPhoto(colorImage);
		//1
		float[][] YCompont= General_BoofCV.rgbToGray(colorImage);
		System.out.println("time:"+General.dispTime(System.currentTimeMillis()-startTime, "ms")+", YCompont[0]: "+General.floatArrToString(YCompont[0], ",", "0.00"));
		//2
		ImageFloat32 grayImg_ave=General_BoofCV.BoofCV_loadImage(colorImage, ImageFloat32.class); 
		ImageFloat32 grayImg_y=General_BoofCV.rgbToGray_BoofCV_ImageFloat32(colorImage);
		ShowImages.showWindow(grayImg_ave,"grayImg_ave",true);
		ShowImages.showWindow(grayImg_y,"grayImg_y",true);
		System.out.println(PixelMath.maxAbs(grayImg_y));
		System.out.println(PixelMath.min(grayImg_y));
		//3
		BufferedImage grayImg_y_buff=ConvertBufferedImage.convertTo(grayImg_y, null);
		General.dispPhoto(grayImg_y_buff);
		//4
		BufferedImage grayImg_y_IO=General_BoofCV.rgbToGray_ImageIO(colorImage);
		General.dispPhoto(grayImg_y_IO);
	}
	
	public static void getScaledInstance() throws IOException{
//		String photoPath="D:/xinchaoli/Desktop/My research/Code_Tools/Gist/MyGist/test.jpg";
		String photoPath="D:/xinchaoli/Desktop/My research/Code_Tools/Gist/MyGist/12_3185258.jpg";
		long startTime=System.currentTimeMillis();
		int[] targerSize=new int[]{480, 720};
		BufferedImage oriImage =ImageIO.read(new File(photoPath));
		//scale image size
		if(oriImage.getHeight()!=targerSize[0] && oriImage.getWidth()!=targerSize[1]){
			if (oriImage.getHeight()>targerSize[0] && oriImage.getWidth()>targerSize[1]) {//height and weight all downScaling
				oriImage=General.getScaledInstance(oriImage, targerSize[0], targerSize[1], RenderingHints.VALUE_INTERPOLATION_BILINEAR, true);//only for downScaling! target sizes(height and weight) are all smaller than ori size. 
			}else {//other case
				oriImage=General.getScaledInstance(oriImage, targerSize[0], targerSize[1], RenderingHints.VALUE_INTERPOLATION_BICUBIC, false);//can be used for all!
			}
		}
		System.out.println("time:"+General.dispTime(System.currentTimeMillis()-startTime, "ms"));
		General.dispPhoto(oriImage);
	}
	
	public static void creatGabor() {
		GistParam gistParam=new GistParam(new int[]{500,370});
		System.out.println("gistParam:"+gistParam.toString());
		System.out.println("Gabor:");
		System.out.println(General.floatArrToString(gistParam.Gabor[0][0], ",", "0.000000"));
		System.out.println(General.floatArrToString(gistParam.Gabor[0][1], ",", "0.000000"));
		System.out.println(General.floatArrToString(gistParam.Gabor[0][2], ",", "0.000000"));
		System.out.println(General.floatArrToString(gistParam.Gabor[0][3], ",", "0.000000"));
		System.out.println(General.floatArrToString(gistParam.Gabor[0][4], ",", "0.000000"));
	}
	
	public static void LMGist() throws IOException, InterruptedException {
		GistParam gistParam=new GistParam(new int[]{375,500});
		System.out.println("gistParam:"+gistParam.toString());
//		String photoPath="D:/xinchaoli/Desktop/My research/Code_Tools/Gist/MyGist/test.jpg";
		String photoPath="D:/xinchaoli/Desktop/My research/Code_Tools/Gist/MyGist/12_3185258.jpg";
		long startTime=System.currentTimeMillis();
		float[] gist=General_BoofCV.LMGist(ImageIO.read(new File(photoPath)), gistParam);
		System.out.println("Gist finished! time:"+General.dispTime(System.currentTimeMillis()-startTime, "s"));
		System.out.println("Gist: "+gist.length+": "+General.floatArrToString(gist, "\t", "0.0000"));
	}
	
	public static void FFT2() throws InterruptedException, IOException {
//		//******* use data  **************
////		float[][] data=new float[][]{{(float) 24.2,(float) 32.1,(float) 132.1},{(float)33.2, (float) 67.9, (float) 167.9},{(float)133.2, (float) 167.9, (float) 1167.9}};
//		float[][] data=new float[][]{{(float) 24.2,(float) 32.1},{(float)33.2, (float) 67.9},{(float)133.2, (float) 167.9}};
		//******* use img **************
//		String photoPath="D:/xinchaoli/Desktop/My research/Code_Tools/Gist/MyGist/test.jpg";
		String photoPath="D:/xinchaoli/Desktop/My research/Code_Tools/Gist/MyGist/12_3185258.jpg";
		float[][] data= General_BoofCV.rgbToGray(ImageIO.read(new File(photoPath)));
		long startTime=System.currentTimeMillis();
//		//test FFT2_inSciTool
//		for (int i = 0; i < 1; i++) {
//			General_BoofCV.FFT2_inSciTool(data);
//		}
//		System.out.println("FFT2_inSciTool:"+General.dispTime(System.currentTimeMillis()-startTime, "ms"));
//		ComplexArray b=General_BoofCV.FFT2_inSciTool(data);
//		float[][] real=General.arrToArrArr(General.DouArrToFloatArr(b.torRe().values()), "rowFirst", data.length, data[0].length);
//		float[][] imagenery=General.arrToArrArr(General.DouArrToFloatArr(b.torIm().values()), "rowFirst", data.length, data[0].length);
//		System.out.println(General.floatArrToString(real[0],",","0.00000"));
//		System.out.println(General.floatArrToString(imagenery[0],",","0.00000"));
        //test FFT2_JTansform
        startTime=System.currentTimeMillis();
        FloatFFT_2D fft2 = new FloatFFT_2D(data.length, data[0].length);
  		for (int i = 0; i < 1; i++) {
  			General_BoofCV.FFT2_JTansform(data,fft2);
  		}
  		System.out.println("FFT2_JTansform:"+General.dispTime(System.currentTimeMillis()-startTime, "ms"));
  		Complex[][] res1=General_BoofCV.FFT2_JTansform(data,fft2);
  		Complex[][] res1_inverse=General_BoofCV.FFT2_Inverse_JTansform(res1,fft2);
  		System.out.println(General.ObjArrToString(res1[0],","));
  		General_ImageJ.dispMatrixAsImg(General_BoofCV.ComplexArrToDataArr_JTransform(res1_inverse, "real"),"res1_inverse",true);
	}
	
	public static void elementPower() {
		double[] aa=new double[]{4,25,100};
		General.elementPower(aa, 0.5);
		System.out.println(General.douArrToString(aa, ",", "0.00"));
	}
	
	public static void elementAdd() {
		double[] aa=new double[]{4,25,100};
		General.elementAdd(aa, 0.5);
		System.out.println(General.douArrToString(aa, ",", "0.00"));
	}
	
	public static void makeLinearSpace() {
		System.out.println(General.floatArrToString(General.makeLinearSpace(0, 5, 4), ",", "0.00"));
	}
	
	public static void randomIndex() {
		int repNum=17695546; int selNum=1000*1000;
		int maxV=repNum/selNum;
		Random rand=new Random();
		HashMap<Integer, Integer> value_num=new HashMap<Integer, Integer>(maxV);
		for (int i = 0; i < repNum; i++) {
			General.updateMap(value_num, rand.nextInt(maxV), 1);
		}
		int maxNum=0; int minNum=Integer.MAX_VALUE;
		for (Entry<Integer, Integer> one : value_num.entrySet()) {
			if (maxNum<one.getValue()) {
				maxNum=one.getValue();
			}else if (minNum>one.getValue()) {
				minNum=one.getValue();
			}
		}
		System.out.println(value_num.size());
		System.out.println("maxNum:"+maxNum);
		System.out.println("minNum:"+minNum);
		
		System.out.println(General.IntArrToString(General.randIndex(10),","));
	}

	public static void make_HMWeigthts(){
		int HMDistThr=32; double HMWeight_deta=20;
		float[] hammingW= General_BoofCV.make_HMWeigthts(HMWeight_deta, HMDistThr);
		System.out.println("HMDistThr:"+HMDistThr+", HMWeight_deta:"+HMWeight_deta+", hammingW:"+General.floatArrToString(hammingW, "_", "0.00"));
		
		HMWeight_deta=30;
		hammingW= General_BoofCV.make_HMWeigthts(HMWeight_deta, HMDistThr);
		System.out.println("HMDistThr:"+HMDistThr+", HMWeight_deta:"+HMWeight_deta+", hammingW:"+General.floatArrToString(hammingW, "_", "0.00"));
	}
	
	public static void obj_to_byteArr() throws IOException{
		Random rand=new Random();
		int num=20;
		ArrayList<Integer> list=new ArrayList<Integer>();
		for (int i = 0; i < num; i++) {
			list.add(rand.nextInt(10));
		}
		System.out.println("list:"+list);
		byte[] byteArr=General.intArr_to_byteArr(list);
		int[] intArr=General.byteArr_to_intArr(byteArr);
		System.out.println("list:"+General.IntArrToString(intArr, ", "));
	}
	
	public static void compressData() throws IOException{
		Random rand=new Random();
		int num=100;
		LZ4Factory factory = LZ4Factory.fastestInstance();
		LZ4Compressor compressor = factory.highCompressor();
		LZ4Decompressor decompressor = factory.decompressor();
		//make data
		ArrayList<Integer> list=new ArrayList<Integer>();
		for (int i = 0; i < num; i++) {
			list.add(rand.nextInt(10));
		}
		System.out.println("list:"+list);
		//convert to byte[]
		byte[] byteArr=General.intArr_to_byteArr(list);
		int oriByteNum=byteArr.length;
		System.out.println("before compress, byte num:"+oriByteNum);
		//compress
		byte[] byteArr_compressed=General.compress_LZ4_intList(byteArr, compressor);
		System.out.println("after compress, byte num:"+byteArr_compressed.length);
		//decompress
		byte[] byteArr_decompressed=General.decompress_LZ4_intList(byteArr_compressed, oriByteNum, decompressor);
		//convert to int[]
		int[] intArr=General.byteArr_to_intArr(byteArr_decompressed);
		System.out.println("list:"+General.IntArrToString(intArr, ", "));
	}
	
	public static void readImgMetaData() throws Exception{
//		File jpegFile = new File("O:/MediaEval_3185258Images/trainImages_1-3185258/Random_500_in_out_Door/outdoor_Normal/12808_3185258.jpg");//O:/ImageRetrieval/HerveImage/ori_Data/Jpeg_organised/-134100.jpg
//		Metadata metadata = ImageMetadataReader.readMetadata(jpegFile);
//		for (Directory directory : metadata.getDirectories()) {
//		    for (Tag tag : directory.getTags()) {
//		        System.out.println(tag);
//		    }
//		}
//		int totFileNum=0; int nullMetaNum=0; int notNormOrientation=0; int noOrientation=0;
//		String workPath="O:/ImageRetrieval/Herve1.5K/ori_Data/";
////		String normOrientation_Images=workPath+"normOrientationImages/";
////		String nullMeta_Images=workPath+"nullMetaImages/"; 
//		String notNormOrientation_Images=workPath+"notNormOrientationImages/"; String modified_notNormOrientation_Images=notNormOrientation_Images+"modifiedNotNormOrientationImages/";
////		String noOrientation_Images=workPath+"noOrientationImages/";
////		General.makeORdelectFolder(normOrientation_Images); 
////		General.makeORdelectFolder(modified_notNormOrientation_Images);
////		General.makeORdelectFolder(nullMeta_Images); General.makeORdelectFolder(notNormOrientation_Images); General.makeORdelectFolder(noOrientation_Images);
//		//for Herve and Oxiford
//		for (File oneImg : new File(workPath+"Jpeg_organised").listFiles()) {//O:/ImageRetrieval/HerveImage/ori_Data/Jpeg_organised/, O:/ImageRetrieval/Oxford5K/ori_Data/oxbuild_images_organised, O:/MediaEval_3185258Images/trainImages_1-3185258/
//			try{
//				ImageInformation imgInfo=General_imgMeta.readImageInformation(oneImg, null);
//				if(imgInfo.orientation!=1 && imgInfo.orientation!=0){
//					notNormOrientation++;
////					General.forTransfer(oneImg, new File(notNormOrientation_Images+oneImg.getName()));
////					ImageIO.write(General_BoofCV.getUniOrintaionImg(ImageIO.read(oneImg), oneImg, null, new Disp(true, "", null)), "JPEG", new File(modified_notNormOrientation_Images+oneImg.getName()));
////					//show image
////					BufferedImage img=ImageIO.read(oneImg);
////					General_BoofCV.showImage(img, "oriImage");
////					General_BoofCV.showImage(General_BoofCV.getUniOrintaionImg(img, oneImg, null, new Disp(true, "", null)), "normOrienatedImg");
//					System.out.println(imgInfo.orientation+", "+oneImg);
//				}else {
////					General.forTransfer(oneImg, new File(normOrientation_Images+oneImg.getName()));
//				}
//			}catch(NullPointerException e){
////				General.forTransfer(oneImg, new File(nullMeta_Images+oneImg.getName()));
//				System.out.println(e+", "+oneImg);
//				nullMetaNum++;
//			}catch (InterruptedException e) {
////				General.forTransfer(oneImg, new File(noOrientation_Images+oneImg.getName()));
//				noOrientation++;
//			}
//			totFileNum++;
//		}
//		System.out.println("done! totFileNum:"+totFileNum+", nullMetaNum:"+nullMetaNum+", notNormOrientation: "+notNormOrientation+", noOrientation:"+noOrientation);

		//test 
		String[] imageMapFilesPaths=new String[]{"F:/Experiments/MediaEval15/DataSet/Photos_MEva15_train_inSInd_MapFiles/"};
		MapFile.Reader[] imgMapFiles = General_Hadoop.openAllMapFiles(imageMapFilesPaths);
		for (int i = 0; i < 10000; i++) {
			BufferedImage_jpg temp = General_Hadoop.readValueFromMFiles( i,  100000, imgMapFiles, new BufferedImage_jpg(), Disp.getNotDisp());
			if (temp!=null) {
				temp.getBufferedImage("photo"+i, new Disp(true, "photo"+i+": ", null));
			}
		}
		General_Hadoop.closeAllMapFiles(imgMapFiles);
		
	}
	
	public static void getOneDimInd_forMutiDimArr(){
		System.out.println(General.IntArrToString(General.mod_withInt(-5, -2),","));

		int[] size_eachDim=new int[]{3,2,5};
		int oneDimInd=General.getOneDimInd_forMutiDimArr(new int[]{2,1,2}, size_eachDim);
		System.out.println("oneDimInd: "+oneDimInd);
		System.out.println(General.IntArrToString(General.getMutiDimArr_FromOneDimInd(oneDimInd, new int[]{3,2,5}),","));
	}
	
	public static void getAllCombinations(){
		int[][] valueEachDim=new int[][]{new int[]{1}, new int[]{10,20,90}, new int[]{-4,-2}};
		int[][] comb=General.getAllCombinations(valueEachDim);
		for (int[] is : comb) {
			System.out.println(General.IntArrToString(is, ","));
		}
	}
	
	public static void testInteger (Integer tag) {
		//in java, some object, when assign a value, it creat a new obj!, the change in function do not affect ori obj in main
		tag=1; //the same to tag=new Integer(1);
		
		//to test 
//		Integer tag=2;
//		testInteger(tag);
//		System.out.println(tag);
	}
	
	public static void showImageByBoofCV() throws InterruptedException, IOException{
		ImageDataManager imageDataManager=new ImageDataManager("IndexIsName", 0, "O:/ImageRetrieval/Oxford5K/ori_Data/oxbuild_images_organised/", null, 0, null, null, null);
		BufferedImage img=imageDataManager.getImage(-1000, new Disp(true,"",null));
		ShowImages.showWindow(img, img.getHeight()+"_"+img.getWidth());
		BufferedImage img_t=img.getSubimage(68, 51, 989-68, 682-51);
		ShowImages.showWindow(img_t, img_t.getHeight()+"_"+img_t.getWidth());
	}
	
	public static void testExtarctTarFiles() throws FileNotFoundException, IOException {
		String tarFilePath="N:/ewi/insy/D-MIRLab/XinchaoLi/MediaEval14/ME14Data/GoogleDrive/Placing2014_images_videos/images/0/001.tar.gz";
		//read images in .tar.gz file
		TarArchiveInputStream tarInput = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(tarFilePath)));
		TarArchiveEntry currentEntry = null; int fileInd=0;
		while((currentEntry = tarInput.getNextTarEntry()) != null) {//TarArchiveEntry can be instantiated by three different ways!
			File oneImage = currentEntry.getFile();
			if (oneImage!=null) {
				if (oneImage.isDirectory()) {
					System.out.println("Dir: "+oneImage.getName());
				}else {
					fileInd++;
					System.out.println("file: "+fileInd+", "+oneImage.getName());
				}
			}else {
				System.out.println("byteEntriy:"+currentEntry.getName()+", isNormalFile:"+currentEntry.isFile());
			}
		}
		System.out.println(fileInd);
		tarInput.close();
	}
	
	public static void sqlite_JDBC() throws SQLException, ClassNotFoundException{
		String dbPath=":memory:";//  O:/temp.sqliteDB, :memory:
		// load the sqlite-JDBC driver using the current class loader
		Class.forName("org.sqlite.JDBC");
	    // create a database connection
	    Connection db_connection = DriverManager.getConnection("jdbc:sqlite:"+dbPath);
	    Statement db_stmt = db_connection.createStatement();
	    db_stmt.setQueryTimeout(0);  // set timeout to some sec, 0 is for no limitation
	    // query db
	    db_stmt.executeUpdate("drop table if exists person");
	    db_stmt.executeUpdate("create table person (id integer, name string)");
	    db_stmt.executeUpdate("insert into person values(1, 'leo')");
	    db_stmt.executeUpdate("insert into person values(2, 'yui')");
	    ResultSet rs = db_stmt.executeQuery("select * from person");
	    while(rs.next()){
	        // read the result set
	        System.out.println("name = " + rs.getString("name"));
	        System.out.println("id = " + rs.getInt("id"));
	    }
	    db_stmt.close();
	    db_connection.close();
	}
	
	public static void getPermutation(){
		HashSet<Integer> aa= new HashSet<>();
		aa.add(1);aa.add(2);aa.add(3);
		ArrayList<LinkedList<Set<Integer>>> res = PowerSet.getPermutation_forSmallElemNum(aa);
		System.out.println(res);
		System.out.println(res.get(0).get(0).isEmpty());
	}
	
	public static void testTreeSet(){
		TreeSet<Integer> set=new TreeSet<>();
		ArrayList<Integer> allv=new ArrayList<Integer>();
		Random rand=new Random(1);
		for (int i = 0; i < 20; i++) {
			int one=rand.nextInt(10);
			set.add(one);
			allv.add(one);
		}
		System.out.println("allv: "+allv);
		System.out.println("set: "+set);
		System.out.println("set list: "+new ArrayList<>(set));
	}
	
	public static void main(String[] args) throws Exception {
		for (double matchingScoreSmooth: new double[]{1,2,3,5,10,15,20}) {
			float[] matchingScoreSmooths=new float[50];//if ini GVM matching score > 100, then all smoothed to 1
			for (int oriScore = 0; oriScore < matchingScoreSmooths.length; oriScore++) {
				matchingScoreSmooths[oriScore]=(float) (1-Math.exp(-Math.pow(oriScore/matchingScoreSmooth, 2)));
			}
			System.out.println(matchingScoreSmooth+"\t"+General.floatArrToString(matchingScoreSmooths, "\t", "0.000"));
		}
		
	}

}
