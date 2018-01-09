package MyAPI.imagR;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.hadoop.io.MapFile;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;
import MyAPI.General.General_Hadoop;
import MyAPI.Obj.Disp;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
import MyCustomedHaoop.ValueClass.PhotoAllFeats;

public class ImageDataManager {

	String phoSourceType;
	int saveInter_images;
	String imageBasePath;
	MapFile.Reader[] imgMapFiles;
	int saveInter_feats;
	MapFile.Reader[] featMapFiles;
	PreProcessImage preProcImage;
	ExtractFeat extractFeat;
	HashMap<Integer, PhotoAllFeats_orgVW> inMemoryPhotoFeat;
	
	public ImageDataManager(int saveInter_images, String imageMapFilesPaths, int saveInter_feats, String featMapFilesPaths, PreProcessImage preProcImage) throws IOException, InterruptedException{
		//this is to construct MapFile-based, both images and feats are pre-ready
		this(saveInter_images, new String[]{imageMapFilesPaths}, saveInter_feats, new String[]{featMapFilesPaths}, preProcImage);
	}
	
	public ImageDataManager(int saveInter_images, String[] imageMapFilesPaths, int saveInter_feats, String[] featMapFilesPaths, PreProcessImage preProcImage) throws IOException, InterruptedException{
		//this is to construct MapFile-based, both images and feats are pre-ready
		this("MapFile", saveInter_images, null, imageMapFilesPaths, saveInter_feats, featMapFilesPaths, preProcImage, null);
	}
	
	public ImageDataManager(String phoSourceType, int saveInter_images, String imageBasePath, String[] imageMapFilesPaths, int saveInter_feats, String[] featMapFilesPaths, PreProcessImage preProcImage, ExtractFeat extractFeat) throws IOException, InterruptedException{
		this.phoSourceType=phoSourceType;
		this.saveInter_images=saveInter_images;
		if(imageBasePath!=null){
			this.imageBasePath=imageBasePath;
		}
		if(imageMapFilesPaths!=null){
			this.imgMapFiles=General_Hadoop.openAllMapFiles(imageMapFilesPaths);
		}
		if(featMapFilesPaths!=null){
			this.saveInter_feats=saveInter_feats;
			this.featMapFiles=General_Hadoop.openAllMapFiles(featMapFilesPaths);
		}
		if (preProcImage!=null) {
			this.preProcImage=preProcImage;
		}
		if(extractFeat!=null){
			this.extractFeat=extractFeat;
		}
	}
	
	public void loadPhoFeat_InMemory(List<Integer> inMemoryFeatPhos, Disp disp) throws InterruptedException, IOException{
		if (inMemoryFeatPhos!=null && inMemoryFeatPhos.size()!=0) {
			disp.disp("needs to read PhosFeat into Memory, phoNum:"+inMemoryFeatPhos.size()+", current Memory: "+General.memoryInfo());
			long startTime=System.currentTimeMillis();
			HashMap<Integer, PhotoAllFeats_orgVW> inMemoryPhotoFeat=new HashMap<Integer, PhotoAllFeats_orgVW>(inMemoryFeatPhos.size());
			Disp thisDisp=Disp.makeHardCopyAddSpacer(disp, "\t"); int dispInter=(inMemoryFeatPhos.size()-1)/10+1; int disp_i=0;
			for (Integer phoID : inMemoryFeatPhos) {
				thisDisp.disp=(disp_i%dispInter==0);
				inMemoryPhotoFeat.put(phoID, getPhoFeat_fromImgOrDisk(phoID, thisDisp));//current this.inMemoryPhotoFeat is null, so loadPhoFeat will read feat from mapFile
				disp_i++;
			}
			disp.disp("read PhosFeat into Memory done! "+General.dispTime(System.currentTimeMillis()-startTime, "s")+", "+General.memoryInfo());
			this.inMemoryPhotoFeat=inMemoryPhotoFeat;
		}
	}
	
	public BufferedImage getImage(int photoIndex, Disp disp) throws InterruptedException, IOException{
		General.Assert(imgMapFiles!=null, "err! need image in getImage, but imgMapFiles==null here!");
		BufferedImage img=null;
		if (phoSourceType.equalsIgnoreCase("MapFile")) {//copy photos to rankShowPath_photos folder 
			BufferedImage_jpg temp = General_Hadoop.readValueFromMFiles( photoIndex,  saveInter_images, imgMapFiles, new BufferedImage_jpg(), disp);
			img=(temp==null)?null:temp.getBufferedImage("phoID:"+photoIndex, Disp.getNotDisp());
		}else if (phoSourceType.equalsIgnoreCase("SeqFile")) {//copy photos to rankShowPath_photos folder 
			BufferedImage_jpg temp  = General_Hadoop.readValueFromSeqFile( photoIndex, imageBasePath, new BufferedImage_jpg());
			img=(temp==null)?null:temp.getBufferedImage("phoID:"+photoIndex, Disp.getNotDisp());
		}else if (phoSourceType.equalsIgnoreCase("IndexIsName")) {//copy photos to rankShowPath_photos folder 
			File imgFile=new File(imageBasePath+photoIndex+".jpg");
			img=General_BoofCV.getUniOrintaionImg(ImageIO.read(imgFile), imgFile, null, disp);//General_BoofCV.readUniOrintaionImg_from_file
		}else {
			throw new InterruptedException("phoSourceType in addPhotoPath_MovePhoto should be MapFile, SeqFile or IndexIsName! here it is :"+phoSourceType);
		}
		if (preProcImage!=null) {
			preProcImage.preProcImage(img, photoIndex, disp);
		}
		return img;
	}
	
	public PhotoAllFeats_orgVW getPhoFeat(int photoIndex, Disp disp) throws InterruptedException, IOException{
		PhotoAllFeats_orgVW feat=null;
		if (inMemoryPhotoFeat!=null) {//feat not in memroy yet
			feat = inMemoryPhotoFeat.get(photoIndex);
		}
		if (feat==null) {
//			disp.disp("warn! feat not in memroy yet! please call loadPhoFeat_InMemory first to speed up process! now getPhoFeat_fromImgOrDisk"); 
			feat= getPhoFeat_fromImgOrDisk(photoIndex,disp);
		}
		return feat;
	}
	
	private PhotoAllFeats_orgVW getPhoFeat_fromImgOrDisk(int photoIndex, Disp disp) throws InterruptedException, IOException{
		PhotoAllFeats feats=null;
		if(featMapFiles==null){//extract PhoFeat_fromImg
			feats= extractFeat.extractRawFeat_makePhotoAllFeats("phoID:"+photoIndex, getImage(photoIndex, disp), disp.disp);
		}else{//getPhoFeat_fromDisk
			feats= General_Hadoop.readValueFromMFiles(photoIndex, saveInter_feats, featMapFiles, new PhotoAllFeats(), disp);
		}
		if(feats!=null)
			return new PhotoAllFeats_orgVW(photoIndex, feats, disp);
		else
			return null;
	}
	
	public void cleanUp() throws IOException{
		if(imgMapFiles!=null){
			General_Hadoop.closeAllMapFiles(imgMapFiles);
		}
		if(featMapFiles!=null){
			General_Hadoop.closeAllMapFiles(featMapFiles);
		}
	}
	
	public static void main(String[] args) {

	}

}
