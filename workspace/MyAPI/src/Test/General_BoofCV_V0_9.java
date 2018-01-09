package Test;
//package MyAPI;

//import java.awt.image.BufferedImage;
//import java.io.File;
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.ArrayList;
//import java.util.List;
//
//import javax.imageio.ImageIO;
//
//import boofcv.abst.feature.detect.extract.ConfigExtract;
//import boofcv.abst.feature.detect.extract.NonMaxSuppression;
//import boofcv.abst.feature.orientation.OrientationIntegral;
//import boofcv.alg.feature.describe.DescribePointSurf;
//import boofcv.alg.feature.detect.interest.FastHessianFeatureDetector;
//import boofcv.alg.transform.ii.GIntegralImageOps;
//import boofcv.core.image.ConvertBufferedImage;
//import boofcv.core.image.GeneralizedImageOps;
//import boofcv.factory.feature.describe.FactoryDescribePointAlgs;
//import boofcv.factory.feature.detect.extract.FactoryFeatureExtractor;
//import boofcv.factory.feature.orientation.FactoryOrientationAlgs;
//import boofcv.io.image.UtilImageIO;
//import boofcv.struct.feature.ScalePoint;
//import boofcv.struct.feature.SurfFeature;
//import boofcv.struct.image.ImageFloat32;
//import boofcv.struct.image.ImageSingleBand;
//import boofcv.struct.image.MultiSpectral;
//
//public class General_BoofCV_V0_9 {

//	@SuppressWarnings("rawtypes")
//	public static <T extends ImageSingleBand> T BoofCV_loadImage(InputStream inputStream, Class<T> imageType ) throws IOException {
//		//toUse: ImageFloat32 img=BoofCV_loadImage(new ByteArrayInputStream(value.getBytes()),ImageFloat32.class);
//		BufferedImage img_buffer = ImageIO.read(inputStream);
//		if( img_buffer == null )
//			return null;
//		return ConvertBufferedImage.convertFromSingle(img_buffer, (T) null, imageType);
//	}
//	
//	@SuppressWarnings("rawtypes")
//	public static <T extends ImageSingleBand> T BoofCV_loadImage(BufferedImage img_buffer, Class<T> imageType ) throws IOException {
//		//toUse: ImageFloat32 img=BoofCV_loadImage(img_buffer,ImageFloat32.class);
//		if( img_buffer == null )
//			return null;
//		return ConvertBufferedImage.convertFromSingle(img_buffer, (T) null, imageType);
//	}
//	
//	@SuppressWarnings("rawtypes")
//	public static <T extends ImageSingleBand> T BoofCV_loadImage(File inputStream, Class<T> imageType ) throws IOException {
//		//toUse: ImageFloat32 img=BoofCV_loadImage(new ByteArrayInputStream(value.getBytes()),ImageFloat32.class);
//		BufferedImage img_buffer = ImageIO.read(inputStream);
//		if( img_buffer == null )
//			return null;
//		return ConvertBufferedImage.convertFromSingle(img_buffer, (T) null, imageType);
//	}
//
//	@SuppressWarnings("rawtypes")
//	private   static <II extends ImageSingleBand> double[][]  computeSURF_BOOFCV_0_9(ImageFloat32 image, String paraFE, String paraFD){	
//		//paraFE: "2,1,5,true"  ////defaut: FactoryFeatureExtractor.nonmax(2, 0, 5, true);
//		String[] parFE=paraFE.split(",");
//		//paraDE: "2000,1,9,4,4"  ////defaut: FastHessianFeatureDetector<II>(extractor,200,2, 9,4,4)
//		int[] parDE=General.StrArrToIntArr(paraFD.split(","));
//		// SURF works off of integral images
//		Class<II> integralType = GIntegralImageOps.getIntegralType(ImageFloat32.class);
// 
//		// define the feature detection algorithm
//		FeatureExtractor extractor = FactoryFeatureExtractor.nonmax(Integer.valueOf(parFE[0]), Integer.valueOf(parFE[1]), Integer.valueOf(parFE[2]), true); //defaut: FactoryFeatureExtractor.nonmax(2, 0, 5, true);
//		FastHessianFeatureDetector<II> detector = 
//				new FastHessianFeatureDetector<II>(extractor,parDE[0],parDE[1],parDE[2],parDE[3],parDE[4]); //defaut: FastHessianFeatureDetector<II>(extractor,200,2, 9,4,4)
//
//		// estimate orientation
//		OrientationIntegral<II> orientation = 
//				FactoryOrientationAlgs.sliding_ii(0.65, Math.PI / 3.0, 8, -1, 6, integralType);
// 
//		DescribePointSurf<II> descriptor = FactoryDescribePointAlgs.<II>msurf(integralType);
// 
//		// compute the integral image of 'image'
//		II integral = GeneralizedImageOps.createSingleBand(integralType,image.width,image.height);
//		GIntegralImageOps.transform(image, integral);
// 
//		// detect fast hessian features
//		detector.detect(integral);
//		// tell algorithms which image to process
//		orientation.setImage(integral);
//		descriptor.setImage(integral);
// 
//		List<ScalePoint> points = detector.getFoundPoints();
//		if (points.size()!=0){ // interest point exist!
//			List<SurfFeature> descriptions = new ArrayList<SurfFeature>();
//			 
//			for( ScalePoint p : points ) {
//				// estimate orientation
//				orientation.setScale(p.scale);
//				double angle = orientation.compute(p.x,p.y);
//				
//				// extract the SURF description for this region
//				SurfFeature desc = descriptor.describe(p.x,p.y,p.scale,angle,null);
//	 
//				// save everything for processing later on
//				descriptions.add(desc);
//			}
//			
//			int numPoint=points.size();
//			int FeatLength=descriptions.get(0).value.length;
//			double[][] photoFeat=new double[numPoint][FeatLength];
////			int index=0;
//			for(int i=0;i<numPoint;i++){
//				for(int j=0;j<FeatLength;j++){
//					photoFeat[i][j]=descriptions.get(i).value[j];
////					index++;
//				}
//			}
//			return photoFeat;
//		}else{ // interest point dose not exist!
//			System.err.println("no interest point!!");
//			return null;
//		}
//	}
//	
//	@SuppressWarnings("rawtypes")
//	public static <T extends ImageSingleBand> MultiSpectral<T> BoofCV_loadColorImage(BufferedImage img_buffer, Class<T> imageType ) throws IOException {
//		//toUse: MultiSpectral<ImageFloat32> img=BoofCV_loadImage(img_buffer,ImageFloat32.class);
//		if( img_buffer == null )
//			return null;
//		// Convert input image into a BoofCV RGB image
//		MultiSpectral<T> rgb= ConvertBufferedImage.convertFromMulti(img_buffer, (MultiSpectral<T>) null, imageType);
//		ConvertBufferedImage.orderBandsIntoRGB(rgb,img_buffer);
//		return rgb;
//	}
//	
//	public static void main(String[] args) {
//		
//		String photoBasePath="O:\\MediaEval_3185258Images\\trainImages_1-3185258\\1-100000\\";
//		String photoPath=photoBasePath+"1_3185258.jpg";
//		ImageFloat32 image = UtilImageIO.loadImage(photoPath,ImageFloat32.class);
//		 
//		// run each example
//		double[][] photoFeat=General_BoofCV_V0_9.computeSURF_BOOFCV_0_9(image,"2,1,5,true","2000,1,9,4,4");
// 
//		System.out.println("Done!");
//		
//	}
//}
