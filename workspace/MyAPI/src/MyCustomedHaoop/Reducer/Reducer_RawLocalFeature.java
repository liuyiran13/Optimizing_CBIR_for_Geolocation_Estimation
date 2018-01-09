package MyCustomedHaoop.Reducer;

import java.awt.image.BufferedImage;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import MyAPI.General.General;
import MyAPI.General.General_Hadoop;
import MyAPI.Obj.Disp;
import MyAPI.imagR.Conf_ImageR;
import MyAPI.imagR.ExtractRawFeat;
import MyAPI.imagR.PreProcessImage;
import MyCustomedHaoop.ValueClass.BufferedImage_jpg;
import MyCustomedHaoop.ValueClass.FloatArr;

public class Reducer_RawLocalFeature extends Reducer<IntWritable,BufferedImage_jpg,IntWritable,FloatArr>{

	private PreProcessImage preProcImage;
	private ExtractRawFeat extractRawFeat;
	private int randomFeatNum;
	private int totSelectedFeatNum;
	private int totFeatNum;
	private int noFeatPhotos;
	private int procPhotos;
	private int noImgPhotos;
	private boolean disp;
	private int dispInter;
	private long startTime;

	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf=context.getConfiguration();		
		Conf_ImageR conf_ImageR=new Conf_ImageR(conf);
		System.out.println("current memory:"+General.memoryInfo());
		Disp toDisp=new Disp(true, "\t", null);
		//setup PreProcessImage
		preProcImage=new PreProcessImage(toDisp, conf_ImageR);
		//setup_extractFeat
		extractRawFeat= new ExtractRawFeat(conf_ImageR);
		System.out.println("current memory:"+General.memoryInfo());
		//featNum
		randomFeatNum=conf.get("randomFeatNum")==null?Integer.MAX_VALUE:Integer.valueOf(conf.get("randomFeatNum"));
		totSelectedFeatNum=0;
		totFeatNum=0;
		//set procPhotos
		noFeatPhotos=0;
		procPhotos=0;
		noImgPhotos=0;
		//set dispInter
		disp=false;
		dispInter=100;
		startTime=System.currentTimeMillis(); //startTime
		
		System.out.println("setup finsihed!");
		super.setup(context);
 	}
	
	protected void reduce(IntWritable key, Iterable<BufferedImage_jpg> value, Context context) throws IOException, InterruptedException {
		//key: photoName
		//value: file content
		//******** only one in value! ************	
		BufferedImage_jpg photo=General_Hadoop.readOnlyOneElement(value, key+"");
				
		int photoName=key.get();//photoName

		procPhotos++;
		if((procPhotos)%dispInter==1){ 							
			disp=true;
		}		
		BufferedImage img=photo.getBufferedImage("photoName:"+photoName, new Disp(true, "getImageMessage: ",null));
		if (img!=null) {
			//resize image
			BufferedImage reSizedImg=preProcImage.preProcImage(img, photoName, new Disp(disp,"",null));
			//*** extract visual feat ***//
			double[][] photoFeat=extractRawFeat.extractRawFeature("photoName:"+photoName, reSizedImg, null, disp);
			if(photoFeat!=null){ // photo has feat(some photos are too small, do not have interest point)
				totFeatNum+=photoFeat.length;
				//random select randNumFeat feats per photo
				if (photoFeat.length<randomFeatNum){
					for(int p=0;p<photoFeat.length;p++){
				        context.write(key, new FloatArr(General.DouArrToFloatArr(photoFeat[p])));
					}
					totSelectedFeatNum+=photoFeat.length;
				}else {
					int[] RandIndexFeat=General.randIndex(photoFeat.length);
					int top=randomFeatNum;
					for(int p=0;p<top;p++){
				        context.write(key, new FloatArr(General.DouArrToFloatArr(photoFeat[RandIndexFeat[p]])));
					}
					totSelectedFeatNum+=top;
				}
				if (disp) {
					System.out.println("photoFeat:"+photoFeat.length);
					System.out.println("photoFeat-0th:"+General.douArrToString(photoFeat[0], "_", "0.00"));
				}
			}else{
				System.err.println("image exist, but no feat for "+photoName+", its size w:"+reSizedImg.getWidth()+" h:"+reSizedImg.getHeight());
				noFeatPhotos++;
			}
		}else {
			noImgPhotos++;
		}
		
		//disp
		if(disp){ 							
			System.out.println( "extracting photo rawFeat, "+procPhotos+" photos finished!! noImgPhotos:"+noImgPhotos+", noFeatPhotos:"+noFeatPhotos+" ......"+ General.dispTime (System.currentTimeMillis()-startTime, "min"));
			disp=false;
		}
		

	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// ***** setup finsihed ***//
		System.out.println("one reducer finished! processed photos in this reducer: "+procPhotos+", noImgPhotos:"+noImgPhotos+", noFeatPhotos:"+noFeatPhotos+" ....."+ General.dispTime ( System.currentTimeMillis()-startTime, "min"));
		System.out.println("randomFeatNum:"+randomFeatNum+", totFeatNum:"+totFeatNum+", totSelectedFeatNum:"+totSelectedFeatNum);
		super.setup(context);
 	}
}