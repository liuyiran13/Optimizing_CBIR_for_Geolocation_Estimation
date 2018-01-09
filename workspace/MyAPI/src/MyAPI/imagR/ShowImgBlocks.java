package MyAPI.imagR;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import MyAPI.General.General_BoofCV;
import MyAPI.General.BoofCV.ShowPointsPanel.PointLink;
import MyAPI.General.BoofCV.ShowPointsPanel.SURFpoint_Weight;
import MyAPI.Obj.Disp;
import MyAPI.Obj.Statistics;
import MyCustomedHaoop.ValueClass.Int_Float;
import MyCustomedHaoop.ValueClass.SURFpoint;
import MyCustomedHaoop.ValueClass.SURFpoint_onlyLoc;

public class ShowImgBlocks {
	
	public static class BlockLink{
		int src;//in blockInd
		int dsc;
		float score;
		public BlockLink(int src, int dsc, float score){
			this.src=src;
			this.dsc=dsc;
			this.score=score;
		}
		
		@Override
		public String toString(){
			return src+"_"+dsc+"_"+ (new DecimalFormat("0.0").format(score));
		}
		
		public static LinkedList<BlockLink> cutOffLinkScore(List<BlockLink> links, float thr){
			LinkedList<BlockLink> res=new LinkedList<>();
			for (BlockLink one : links) {
				if (one.score>=thr) {
					res.add(one);
				}
			}
			return res;
		}
		
		public static String getStatic(List<BlockLink> links, int topNum) throws InterruptedException{
			Statistics<String> stat=new Statistics<String>(topNum);
			for (BlockLink blockLink : links) {
				stat.addSample(blockLink.score, "");
			}
			return stat.getFullStatistics("0.0", false);
		}
	}
	
	private static class PhoBlockWeights{
		public int phoID;
		public String phoWinTitle;
		public ImageBlock imageBlock;
		public List<Int_Float> blockInd_weight; 
		//BlockLinks
		public List<BlockLink> blockLinks_l;
		public List<BlockLink> blockLinks_r;
		
		public PhoBlockWeights(int phoID, String phoWinTitle, ImageBlock imageBlock, HashMap<Integer,Float> blockInd_weight, List<BlockLink> blockLinks_l, List<BlockLink> blockLinks_r){
			LinkedList<Int_Float> res=new LinkedList<>();
			for (Entry<Integer, Float> one : blockInd_weight.entrySet()) {
				res.add(new Int_Float(one.getKey(), one.getValue()));
			}
			ini(phoID, phoWinTitle, imageBlock, res, blockLinks_l, blockLinks_r);
		}
		
		public PhoBlockWeights(int phoID, String phoWinTitle, ImageBlock imageBlock, List<Int_Float> blockInd_weight, List<BlockLink> blockLinks_l, List<BlockLink> blockLinks_r){
			ini(phoID, phoWinTitle, imageBlock, blockInd_weight, blockLinks_l, blockLinks_r);
		}
		
		private void ini(int phoID, String phoWinTitle, ImageBlock imageBlock, List<Int_Float> blockInd_weight, List<BlockLink> blockLinks_l, List<BlockLink> blockLinks_r){
			this.phoID=phoID;
			this.phoWinTitle=phoWinTitle;
			this.imageBlock=imageBlock;
			this.blockInd_weight=blockInd_weight;
			this.blockLinks_l=blockLinks_l;
			this.blockLinks_r=blockLinks_r;
		}
	}
	
	//data
	LinkedList<PhoBlockWeights> photos;
	//para
	protected ImageDataManager imageDataManager;
	protected int RGBInd;
	protected int pointEnlargeFactor;
	protected boolean isNormWeight;
	
	public ShowImgBlocks(ImageDataManager imageDataManager, int RGBInd, int pointEnlargeFactor, boolean isNormWeight) {
		this.imageDataManager=imageDataManager;
		this.RGBInd=RGBInd; 
		this.pointEnlargeFactor=pointEnlargeFactor;
		this.isNormWeight=isNormWeight;
		photos=new LinkedList<>();
	}
	
	public void addOnePhotoPoints(int phoID, String phoWinTitle, ImageBlock imageBlock, HashMap<Integer,Float> blockInd_weight, List<BlockLink> blockLinks_l, List<BlockLink> blockLinks_r){
		photos.add(new PhoBlockWeights(phoID, phoWinTitle, imageBlock, blockInd_weight, blockLinks_l, blockLinks_r));
	}
	
	public void addOnePhotoPoints(int phoID, String phoWinTitle, ImageBlock imageBlock, List<Int_Float> blockInd_weight, List<BlockLink> blockLinks_l, List<BlockLink> blockLinks_r){
		photos.add(new PhoBlockWeights(phoID, phoWinTitle, imageBlock, blockInd_weight, blockLinks_l, blockLinks_r));
	}
	
	public void clearPhotos(){
		photos.clear();
	}

	public void disp() throws InterruptedException, IOException {
		Disp disp=new Disp(false,"",null);
		double[] min_max_PointWeight=null; double[] min_max_PointLink=null;
		if (isNormWeight) {
			//find min and max for normalize point score
			min_max_PointWeight=new double[]{Double.MAX_VALUE, Integer.MIN_VALUE};
			min_max_PointLink=new double[]{Double.MAX_VALUE, Integer.MIN_VALUE};
			boolean isExist_PointWeight=false; boolean isExist_PointLink=false;
			for (PhoBlockWeights onePhoto : photos) {
				if (onePhoto.blockInd_weight!=null) {
					isExist_PointWeight=true;
					for (Int_Float onePoint : onePhoto.blockInd_weight) {
						min_max_PointWeight[0]=Math.min(min_max_PointWeight[0], onePoint.floatV);
						min_max_PointWeight[1]=Math.max(min_max_PointWeight[1], onePoint.floatV);
					}
				}
				if (onePhoto.blockLinks_l!=null) {
					isExist_PointLink=true;
					for (BlockLink oneLink : onePhoto.blockLinks_l) {
						min_max_PointLink[0]=Math.min(min_max_PointLink[0], oneLink.score);
						min_max_PointLink[1]=Math.max(min_max_PointLink[1], oneLink.score);
					}
				}
				if (onePhoto.blockLinks_r!=null) {
					isExist_PointLink=true;
					for (BlockLink oneLink : onePhoto.blockLinks_r) {
						min_max_PointLink[0]=Math.min(min_max_PointLink[0], oneLink.score);
						min_max_PointLink[1]=Math.max(min_max_PointLink[1], oneLink.score);
					}
				}
			}
			min_max_PointWeight=isExist_PointWeight?min_max_PointWeight:null; 
			min_max_PointLink=isExist_PointLink?min_max_PointLink:null; 
		}
		for (PhoBlockWeights onePhoto : photos) {//show one photo
			BufferedImage img=imageDataManager.getImage(onePhoto.phoID, disp);
			//make selblocks
			ImageBlock imgBlock = onePhoto.imageBlock; LinkedList<SURFpoint_Weight> selBlocks=null; LinkedList<PointLink> links_l=null; LinkedList<PointLink> links_r=null;
			float blockAsPointSize=Math.max(3,(float)imgBlock.blockSize_h/3);
			//blocks
			if (onePhoto.blockInd_weight!=null) {
				selBlocks=new LinkedList<>();
				for (Int_Float oneP : onePhoto.blockInd_weight) {
					int[] x_y=imgBlock.getBlockCoordinateInImage(oneP.integerV);
					selBlocks.add(new SURFpoint_Weight(new SURFpoint((short)x_y[0], (short)x_y[1], blockAsPointSize, 0.2f), oneP.floatV));//make one virtual point that located in block center
				}
			}
			//block links
			if (onePhoto.blockLinks_l!=null) {
				links_l=new LinkedList<>();
				for (BlockLink one : onePhoto.blockLinks_l) {
					int[] x_y_src=imgBlock.getBlockCoordinateInImage(one.src);
					int[] x_y_dsc=imgBlock.getBlockCoordinateInImage(one.dsc);
					links_l.add(new PointLink(new SURFpoint_onlyLoc(x_y_src), new SURFpoint_onlyLoc(x_y_dsc), one.score));
				}
			}
			if (onePhoto.blockLinks_r!=null) {
				links_r=new LinkedList<>();
				for (BlockLink one : onePhoto.blockLinks_r) {
					int[] x_y_src=imgBlock.getBlockCoordinateInImage(one.src);
					int[] x_y_dsc=imgBlock.getBlockCoordinateInImage(one.dsc);
					links_r.add(new PointLink(new SURFpoint_onlyLoc(x_y_src), new SURFpoint_onlyLoc(x_y_dsc), one.score));
				}
			}
			
			General_BoofCV.showFeaturePoint(selBlocks, links_l, selBlocks, links_r, img, img, onePhoto.phoWinTitle, pointEnlargeFactor, RGBInd, min_max_PointWeight, min_max_PointLink, false);
		}
	}

}
