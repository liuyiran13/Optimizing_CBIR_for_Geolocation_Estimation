package MyAPI.Obj;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;

public class GistParam {
	public int[] imgSize;
	public int[] orientationsPerScale;
	public int numberBlocks;
	public int fc_prefilt;
	public int boundaryExtension;
	public float[][][] Gabor;
	
	public GistParam(int[] imgSize, int[] orientationsPerScale, int numberBlocks, int fc_prefilt, int boundaryExtension) {
		super();
		this.imgSize = imgSize;
		this.orientationsPerScale = orientationsPerScale;
		this.numberBlocks = numberBlocks;
		this.fc_prefilt = fc_prefilt;
		this.boundaryExtension = boundaryExtension;
		//creat Gabor
		this.Gabor=General_BoofCV.creatGabor(this.orientationsPerScale, General.elementAdd(this.imgSize, 2*this.boundaryExtension));
	}
	
	public GistParam(int[] imgSize) {//use default parameters
		super();
		this.imgSize = imgSize;
		this.orientationsPerScale = new int[]{6,6,6,6}; //default in matlab code: 8,8,8,8, in IM2GPS: 6,6,6,6
		this.numberBlocks = 5; //spatial resolution, default in matlab code: 4, in IM2GPS: 5
		this.fc_prefilt = 4;
		this.boundaryExtension = 32;
		//creat Gabor
		this.Gabor=General_BoofCV.creatGabor(this.orientationsPerScale, General.elementAdd(this.imgSize, 2*this.boundaryExtension));
	}
	
	public String toString() {
		return "imgSize:"+General.IntArrToString(imgSize, "_")+", orientationsPerScale:"+General.IntArrToString(orientationsPerScale, "_")
				+", numberBlocks:"+numberBlocks+", fc_prefilt:"+fc_prefilt+", boundaryExtension:"+boundaryExtension+", Gabor filter size:"+Gabor.length+"_"+Gabor[0].length+"_"+Gabor[0][0].length;
	}
}
