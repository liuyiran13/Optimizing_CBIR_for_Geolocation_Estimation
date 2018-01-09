package MyAPI.imagR;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import javax.imageio.ImageIO;

import MyAPI.General.General_BoofCV;
import MyAPI.Obj.Disp;
import MyCustomedHaoop.ValueClass.SURFpoint;
import boofcv.struct.image.ImageFloat32;

public class ExtractRawFeat {

	public int targetFeature; //0-SURF, 1-SIFT-binTool-Oxford1, 2-SIFT-binTool-INRIA2, 3-SIFT-binTool-VLFeat, 3-SIFT-binTool-UPRightINRIA2
	public String binaryPath_Detector, tempFilesPath;
	public String configInfo;
	
	public ExtractRawFeat(Conf_ImageR confImageR) throws InterruptedException{
		this(confImageR.ef_targetFeat,"./"+Conf_ImageR.ef_detector_localPoint, Conf_ImageR.ef_tempFilesPath);
	}
	
	public ExtractRawFeat(String targetFeat, String binaryPath_Detector, String tempFilesPath) throws InterruptedException {
		//***************** set targetFeat ***//
		if (targetFeat.equalsIgnoreCase("SURF")) {
			targetFeature=0;
			configInfo=("targetFeat:"+targetFeat);
		}else if (targetFeat.equalsIgnoreCase("SIFT-binTool-Oxford1")) {//SIFT-binTool-Oxford1
			targetFeature=1;
			this.binaryPath_Detector=binaryPath_Detector; this.tempFilesPath=tempFilesPath;
			configInfo=("targetFeat:"+targetFeat+", binaryPath_Detector: "+binaryPath_Detector+", tempFilesPath:"+tempFilesPath);
		}else if (targetFeat.equalsIgnoreCase("SIFT-binTool-INRIA2")) {//SIFT-binTool-INRIA2
			targetFeature=2;
			this.binaryPath_Detector=binaryPath_Detector; this.tempFilesPath=tempFilesPath;
			configInfo=("targetFeat:"+targetFeat+", binaryPath_Detector: "+binaryPath_Detector+", tempFilesPath:"+tempFilesPath);
		}else if (targetFeat.equalsIgnoreCase("SIFT-binTool-VLFeat")) {//SIFT-binTool-VLFeat
			targetFeature=3;
			this.binaryPath_Detector=binaryPath_Detector; this.tempFilesPath=tempFilesPath;
			configInfo=("targetFeat:"+targetFeat+", binaryPath_Detector: "+binaryPath_Detector+", tempFilesPath:"+tempFilesPath);
		}else if (targetFeat.equalsIgnoreCase("SIFT-binTool-UPRightINRIA2")) {//SIFT-binTool-UPRightINRIA2
			targetFeature=4;
			this.binaryPath_Detector=binaryPath_Detector; this.tempFilesPath=tempFilesPath;
			configInfo=("targetFeat:"+targetFeat+", binaryPath_Detector: "+binaryPath_Detector+", tempFilesPath:"+tempFilesPath);
		}else if (targetFeat.equalsIgnoreCase("SIFT-binTool-UPRightOxford1")) {//SIFT-binTool-UPRightOxford1
			targetFeature=5;
			this.binaryPath_Detector=binaryPath_Detector; this.tempFilesPath=tempFilesPath;
			configInfo=("targetFeat:"+targetFeat+", binaryPath_Detector: "+binaryPath_Detector+", tempFilesPath:"+tempFilesPath);
		}else if (targetFeat.equalsIgnoreCase("SIFT-binTool-UPRightOxford1NoRoot")) {//SIFT-binTool-UPRightOxford1NoRoot
			targetFeature=6;
			this.binaryPath_Detector=binaryPath_Detector; this.tempFilesPath=tempFilesPath;
			configInfo=("targetFeat:"+targetFeat+", binaryPath_Detector: "+binaryPath_Detector+", tempFilesPath:"+tempFilesPath);
		}else if(targetFeat.equalsIgnoreCase("noMatter")) {
			configInfo=("targetFeat:"+targetFeat);
		}else {
			throw new InterruptedException("err! targetFeat should be SURF, SIFT-binTool-Oxford2, SIFT-binTool-INRIA2, SIFT-binTool-UPRightINRIA2, SIFT-binTool-UPRightOxford2 or noMatter! here:"+targetFeat);
		}
	}
	
	public double[][] extractRawFeature(String imgMark, BufferedImage photoImg, ArrayList<SURFpoint> interestPoints, boolean disp) throws IOException, InterruptedException{
		double[][] photoFeat=null; 
		if (targetFeature==0) {//SURF
			ImageFloat32 targetImage=General_BoofCV.BoofCV_loadImage(photoImg, ImageFloat32.class); 
//			ShowImages.showWindow(targetImage, imgMark, true);
			photoFeat=General_BoofCV.computeSURF_boofCV_09(imgMark, targetImage, "2,1,5,true", "2000,1,9,4,4", interestPoints);
		}else if (targetFeature==1) {//SIFT-binTool-Oxford1
			photoFeat=General_BoofCV.computeSIFT_binaryOxford1(imgMark, photoImg, binaryPath_Detector, tempFilesPath, "SIFT", "-sift -hesaff", 200, 5000, false, disp, true, interestPoints);
			General_BoofCV.computeRootSIFT_fromSIFT(photoFeat, false);
		}else if (targetFeature==2) {//SIFT-binTool-INRIA2
			photoFeat=General_BoofCV.computeSIFT_binaryINRIA2(imgMark, photoImg, binaryPath_Detector, tempFilesPath, "SIFT", "-sift -hesaff", 400, 5000, false, disp, true, interestPoints);
			General_BoofCV.computeRootSIFT_fromSIFT(photoFeat, false);
		}else if (targetFeature==3) {//SIFT-binTool-VLFeat
			photoFeat=General_BoofCV.computeSIFT_binaryVLFeat09(imgMark, photoImg, binaryPath_Detector, tempFilesPath, "SIFT", 1, 5, 7000, disp, true, interestPoints);
			General_BoofCV.computeRootSIFT_fromSIFT(photoFeat, false);
		}else if (targetFeature==4) {//SIFT-binTool-UPRightINRIA2
			photoFeat=General_BoofCV.computeSIFT_binaryINRIA2(imgMark, photoImg, binaryPath_Detector, tempFilesPath, "SIFT", "-sift -hesaff -noangle", 100, 5000, false, disp, true, interestPoints);
			General_BoofCV.computeRootSIFT_fromSIFT(photoFeat, false);
		}else if (targetFeature==5) {//SIFT-binTool-UPRightOxford1
			photoFeat=General_BoofCV.computeSIFT_binaryOxford1(imgMark, photoImg, binaryPath_Detector, tempFilesPath, "SIFT", "-sift -hesaff -noangle", 100, 5000, false, disp, true, interestPoints);
			General_BoofCV.computeRootSIFT_fromSIFT(photoFeat, false);
		}else if (targetFeature==6) {//SIFT-binTool-UPRightOxford1NoRoot
			photoFeat=General_BoofCV.computeSIFT_binaryOxford1(imgMark, photoImg, binaryPath_Detector, tempFilesPath, "SIFT", "-sift -hesaff -noangle", 100, 5000, false, disp, true, interestPoints);
		}
		return photoFeat;
	}
	
	public void extractShowRawFeat(String imgMark, BufferedImage photoImg) throws IOException, InterruptedException{
		ArrayList<SURFpoint> interestPoints=new ArrayList<>();
		extractRawFeature(imgMark, photoImg, interestPoints, true);
		General_BoofCV.showFeaturePoint(interestPoints, photoImg, imgMark+"_point"+interestPoints.size()+"_"+configInfo, 1);
	}
	
	public static void main(String[] args) throws IOException, InterruptedException{
		//test
		String photoName="D:/xinchaoli/Desktop/My research/Code_Tools/SIFT_binary/Oxford_Version/extract_features/examp_SanFranQ/SanFranQ.png";
//		String photoName="F:/Experiments/SanFrancisco/dataSet/GTruth_inSind/46_558/253162.jpg";
		BufferedImage img=ImageIO.read(new File(photoName));
		Disp disp=new Disp(true, "", null); 
		PreProcessImage preProcessImage=new PreProcessImage(disp, null,1024*768,true);
		img=preProcessImage.preProcImage(img, 0, disp);
		ExtractRawFeat extRaw;
		//SURF
		extRaw=new ExtractRawFeat("SURF", null, null);
		extRaw.extractShowRawFeat("", img);
		//SIFT
		extRaw=new ExtractRawFeat("SIFT-binTool-Oxford1", "D:/xinchaoli/Desktop/My research/Code_Tools/SIFT_binary/Oxford_Version/extract_features/Oxford_extract_features.exe", "D:/xinchaoli/Desktop/My research/Code_Tools/SIFT_binary/Oxford_Version/extract_features/");
		extRaw.extractShowRawFeat("", img);
		//SIFT
		extRaw=new ExtractRawFeat("SIFT-binTool-UPRightOxford1", "D:/xinchaoli/Desktop/My research/Code_Tools/SIFT_binary/Oxford_Version/extract_features/Oxford_extract_features.exe", "D:/xinchaoli/Desktop/My research/Code_Tools/SIFT_binary/Oxford_Version/extract_features/");
		extRaw.extractShowRawFeat("", img);
	}

}
