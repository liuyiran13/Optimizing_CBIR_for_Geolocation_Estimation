package MyAPI.imagR;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.HashMap;

import boofcv.alg.misc.PixelMath;
import boofcv.core.image.ConvertBufferedImage;
import boofcv.struct.image.ImageFloat32;
import MyAPI.General.General;
import MyAPI.General.General_BoofCV;
import MyAPI.Obj.Disp;

public class PreProcessImage {
	
	public boolean isCutImage;
	public HashMap<Integer, float[]> photoID_Postions;
	public boolean isResize;
	public int targetImgSize;
	public boolean isIntensityNorm;
	
	public PreProcessImage(Disp disp, Conf_ImageR confImageR) throws InterruptedException {
		this(disp, Conf_ImageR.ef_PhotoPos_HashMap, confImageR.ef_targetImgSize, confImageR.ef_isIntensityNorm);
	}
	
	@SuppressWarnings("unchecked")
	public PreProcessImage(Disp disp, String imageBoundingBoxPath, int targetImgSize, boolean isIntensityNorm) throws InterruptedException {
		//***************** set preproc image ***//
		photoID_Postions=(HashMap<Integer, float[]>) General.readObject(imageBoundingBoxPath);
		isCutImage=(photoID_Postions!=null);
		this.targetImgSize=targetImgSize; //1024*768=786432 pixels
		isResize=targetImgSize>0;//if no need resize, then set targetImgSize=0;
		this.isIntensityNorm=isIntensityNorm;
		disp.disp("PreProcessImage setup: "+toString());
	}
	
	@Override
	public String toString(){
		return "isCutImage:"+isCutImage+", isResize:"+isResize+", targetImgSize:"+targetImgSize+", isIntensityNorm:"+isIntensityNorm;
	}

	public BufferedImage preProcImage(BufferedImage photoImg, int photoID, Disp disp) throws IOException, InterruptedException {
		BufferedImage res=photoImg;
		if (isCutImage) {
			float[] pos=photoID_Postions.get(photoID);
			if (pos!=null) {
				res=General_BoofCV.getSubImage(photoImg, (int)pos[0], (int)pos[1], (int)(pos[2]), (int)(pos[3]), Disp.makeHardCopyAddSpacer(disp, "photoID:"+photoID+", "));
			}
		}
		//resize image not bigger than targetImgSize, e.g., 1024*768=786432 pixels
		if (isResize) {
			int ori_w=photoImg.getWidth(); int ori_h=photoImg.getHeight();
			res=General.getScaledInstance_onlyDownScale(res, targetImgSize);
			disp.disp( "reSizeImage, ori_w:"+ori_w+", ori_h:"+ori_h+", resized_w:"+res.getWidth()+", resized_h:"+res.getHeight());
		}else{
			disp.disp( "no-reSizeImage, w:"+res.getWidth()+", h:"+res.getHeight());
		}
		//transfer color image to gray image, and do a light whiten
		if (isIntensityNorm) {
			ImageFloat32 grayImg_y=General_BoofCV.rgbToGray_BoofCV_ImageFloat32(res);
			disp.disp("before rgbToGray_BoofCV_ImageFloat32, min max pixel value: "+PixelMath.min(grayImg_y)+", "+PixelMath.maxAbs(grayImg_y));
			//** note! this normliseIntensity do affect the darkness photos like some streetview photos in SanFran, e.g., GTruth_inSind/46_558/253162.jpg, feature points are doubled when use this step, so please do this step!
			General_BoofCV.normliseIntensity(grayImg_y,1,1);
			disp.disp("after rgbToGray_BoofCV_ImageFloat32, min max pixel value: "+PixelMath.min(grayImg_y)+", "+PixelMath.maxAbs(grayImg_y));
			res=ConvertBufferedImage.convertTo(grayImg_y, null);
		}
		return res;
	}
}
