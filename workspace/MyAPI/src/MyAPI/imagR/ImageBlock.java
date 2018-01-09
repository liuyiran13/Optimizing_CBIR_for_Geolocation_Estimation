package MyAPI.imagR;

import java.io.IOException;
import java.util.LinkedList;

import MyAPI.General.General;
import MyAPI.Obj.Disp;
import MyCustomedHaoop.ValueClass.PhotoPointsLoc;
import MyCustomedHaoop.ValueClass.SURFpoint_onlyLoc;

public class ImageBlock {

	boolean isDivide;//if this is true then divide image into blocks, otherwise, do not divide, use the original feats.
	//para
	int portion; //2~...
	ImageDataManager imageDataManager;
	//data
	PhotoPointsLoc photoInfo;
	double blockSize_w;
	double blockSize_h;
	int blockNum_w;
	int blockNum_h;
	//MutAss
	double blockSize_half_w;
	double blockSize_half_h;
	int maxBInd_x;
	int maxBInd_y;
//	float[] startEndStep_w;
//	float[] startEndStep_h;
//	float[] middleBins_w;
//	float[] middleBins_h;
	
	/**
	 * use query size info from rank matches
	 */
	public ImageBlock(int portion) {
		this(null, portion);
	}
	
	/**
	 * use query size info from feat file
	 */
	public ImageBlock(ImageDataManager imageDataManager, int portion) {
		this.imageDataManager=imageDataManager;
		this.portion=portion;
		this.isDivide=(portion>0);//portion==0 means no divide
	}

	/**
	 * use query size info from rank matches
	 */
	public void iniForOneImage(PhotoPointsLoc photoInfo) throws InterruptedException, IOException{
		this.photoInfo=photoInfo;
		if (isDivide) {
			//blockSize
			double minBlockSize=Math.min(Math.max((double)photoInfo.width/portion,1), Math.max((double)photoInfo.height/portion,1));
			blockSize_w=minBlockSize;
			blockSize_h=minBlockSize;
			//blockNum
			blockNum_w=(int) ((photoInfo.width-1)/blockSize_w)+1;
			blockNum_h=(int) ((photoInfo.height-1)/blockSize_h)+1;
			//MutAss
			blockSize_half_w=blockSize_w/2;
			blockSize_half_h=blockSize_h/2;
			maxBInd_x=blockNum_w-1;
			maxBInd_y=blockNum_h-1;
//				startEndStep_w=new float[]{0f, photoInfo.width, (float) blockSize_w};
//				startEndStep_h=new float[]{0f, photoInfo.height, (float) blockSize_h};
//				bins[i]=General.makeRange(begin_end_step[i]);//attention: the last value in initialBins should > all sample's maximum value! 
//				bins_middle[i]=General.makeMiddleBins(bins[i]);
		}
	}
	
	/**
	 * use query size info from feat file
	 */
	public void iniForOneImage(int phoID) throws InterruptedException, IOException{
		photoInfo=imageDataManager.getPhoFeat(phoID, new Disp(false,"",null)).getPhotoPointsLoc();
		iniForOneImage(photoInfo);
	}
	
	public int getBlockID(int featID){
		if (isDivide) {
			SURFpoint_onlyLoc point=photoInfo.feats.getArr()[featID];
			int bInd_x=(int) (point.x/blockSize_w);
			int bInd_y=(int) (point.y/blockSize_h);
			return getBlockID(bInd_x,bInd_y);//row first order
		}else {
			return featID;
		}
	}
	
	public Integer[] getBlockID_MultAss(int featID){//one feat vote for 4 nearby blocks
		if (isDivide) {
			SURFpoint_onlyLoc point=photoInfo.feats.getArr()[featID];
			int bInd_x=(int) (point.x/blockSize_w);
			int bInd_y=(int) (point.y/blockSize_h);
			double xInbin=point.x%blockSize_w;
			int bInd2_x=bInd_x+(xInbin<blockSize_half_w?-1:+1);
			double yInbin=point.y%blockSize_h;
			int bInd2_y=bInd_y+(yInbin<blockSize_half_h?-1:+1);
			LinkedList<Integer> res=new LinkedList<>();
			if (bInd_x==0 || bInd_y==0 || bInd_x==maxBInd_x || bInd_y==maxBInd_y) {//boarder
				for (int bindX: new int[]{bInd_x, bInd2_x}) {
					for (int bindY: new int[]{bInd_y, bInd2_y}) {
						if (bindX!=-1 && bindY!=-1 && bindX!=blockNum_w && bindY!=blockNum_h) {
							res.add(getBlockID(bindX,bindY));
						}
					}
				}
			}else{
				for (int bindX: new int[]{bInd_x, bInd2_x}) {
					for (int bindY: new int[]{bInd_y, bInd2_y}) {
						res.add(getBlockID(bindX,bindY));
					}
				}
			}
			return res.toArray(new Integer[0]);
		}else {
			return new Integer[]{featID};
		}
	}
	
	private int getBlockID(int bInd_x, int bInd_y){//width_hight
		General.Assert(isDivide, "this function is to translate the block coordinate to blockID, and only for isDivide==true.");
		return bInd_y*blockNum_w+bInd_x;//row first order
	}
	
	public int[] getBlockCoordinate(int blockID){//width_hight, the same with matrix
		if (isDivide) {
			return new int[]{blockID%blockNum_w, blockID/blockNum_w};
		}else{//no divide, use the orginal feat
			SURFpoint_onlyLoc point=photoInfo.feats.getArr()[blockID];
			return new int[]{point.x, point.y};
		}
	}
	
	public int[] getBlockCoordinateInImage(int blockID){//width_hight, the same with showImage
		if (isDivide) {//return block center postion
			return new int[]{(int) ((blockID%blockNum_w+0.5)*blockSize_w), (int) ((blockID/blockNum_w+0.5)*blockSize_h)};
		}else{//no divide, use the orginal feat
			SURFpoint_onlyLoc point=photoInfo.feats.getArr()[blockID];
			return new int[]{point.x, point.y};
		}
	}
	
	public int getTotBlockNum(){
		if (isDivide) {
			return blockNum_w*blockNum_h;
		}else {
			return photoInfo.feats.getArr().length;
		}
	}
	
	public int getImgSize_w(){
		return photoInfo.width;
	}
	
	public int getImgSize_h(){
		return photoInfo.height;
	}
	
}
