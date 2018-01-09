package MyAPI.General;

import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.File;

import javax.imageio.ImageIO;

import net.semanticmetadata.lire.imageanalysis.LireFeature;

public class General_Lire {

	public static String extractFeat_inStr_lire093(BufferedImage bim) throws Exception{
		
//		//for color hit bug photo, transfer RGB photo
////		if (bim.getType() != ColorSpace.TYPE_RGB || bim.getSampleModel().getSampleSize(0) != 8) {
////            BufferedImage img = new BufferedImage(bim.getWidth(), bim.getHeight(), BufferedImage.TYPE_INT_RGB);
////            img.getGraphics().drawImage(bim, 0, 0, null);
////            bim = img;
////        }
////		
////		//for gabor bug photo, resize photo if originalWidth, originalHeight ratio is too high, like 500*30, this will cause NegativeArraySize in precomputeGaborWavelet
////		double originalWidth = bim.getWidth();
////	    double originalHeight = bim.getHeight();
////	    double scaleFactor = 0.0; 
////		if (originalWidth > originalHeight) {
////			scaleFactor = ((double) originalWidth / originalHeight);
////			if (scaleFactor>5) {
////				// create smaller image
////		        BufferedImage img = new BufferedImage((int) (originalHeight*5), (int) (originalHeight), BufferedImage.TYPE_INT_RGB);
////		        // fast scale (Java 1.4 & 1.5)
////		        Graphics g = img.getGraphics();
////		        ((Graphics2D) g).setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BICUBIC);
////		        g.drawImage(bim, 0, 0, img.getWidth(), img.getHeight(), null);
////		        bim=img;
////			}
////		} else {
////			scaleFactor = ((double) originalHeight / originalWidth);
////			if (scaleFactor>5) {
////				// create smaller image
////		        BufferedImage img = new BufferedImage((int) (originalWidth), (int) (originalWidth*5), BufferedImage.TYPE_INT_RGB);
////		        // fast scale (Java 1.4 & 1.5)
////		        Graphics g = img.getGraphics();
////		        ((Graphics2D) g).setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BICUBIC);
////		        g.drawImage(bim, 0, 0, img.getWidth(), img.getHeight(), null);
////		        bim=img;
////			}
////		}
//		
//	        
//		
//		AutoColorCorrelogram acc = new AutoColorCorrelogram();
//		BasicFeatures bf = new BasicFeatures();
//		CEDD cedd = new CEDD();
//		ColorLayout col = new ColorLayout();
//		EdgeHistogram edge = new EdgeHistogram();
//		FCTH fcth = new FCTH();
//		FuzzyOpponentHistogram fop = new FuzzyOpponentHistogram();
//		Gabor gab = new Gabor();
//		JointHistogram jh = new JointHistogram();
//		JointOpponentHistogram jop = new JointOpponentHistogram();
//		ScalableColor sc = new ScalableColor();
//		SimpleColorHistogram sch = new SimpleColorHistogram();
//		Tamura tam = new Tamura();
//
		StringBuffer sb = new StringBuffer();
//
//		//extract feat in string
//		acc.extract(bim);
//		sb.append(" acc " + acc.getStringRepresentation());
//
//		bf.extract(bim);
//		sb.append(" bf " + bf.getStringRepresentation());
//
//		cedd.extract(bim);
//		sb.append(" " + cedd.getStringRepresentation());
//
//		col.extract(bim);
//		sb.append(" col " + col.getStringRepresentation());
//
//		edge.extract(bim);
//		sb.append(" " + edge.getStringRepresentation().replace(";", " "));
//
//		fcth.extract(bim);
//		sb.append(" " + fcth.getStringRepresentation());
//
//		fop.extract(bim);
//		sb.append(" " + fop.getStringRepresentation());
//
//		gab.extract(bim);
//		sb.append(" " + gab.getStringRepresentation());
//
//		jh.extract(bim);
//		sb.append(" " + jh.getStringRepresentation());
//
//		jop.extract(bim);
//		sb.append(" " + jop.getStringRepresentation());
//
//		sc.extract(bim);
//		sb.append(" " + sc.getStringRepresentation().replace(";", " "));
//
//		sch.extract(bim);
//		sb.append(" " + sch.getStringRepresentation());
//
//		tam.extract(bim);
//		sb.append(" " + tam.getStringRepresentation());
//
		return sb.toString();
	}
	
	public static byte[] extractFeat_inByteArr_lire136(BufferedImage bim, String featClassName) throws InstantiationException, IllegalAccessException, ClassNotFoundException{
		//String[] classArray = {"CEDD", "EdgeHistogram", "FCTH", "ColorLayout", "PHOG", "JCD", "Gabor", "JpegCoefficientHistogram", "Tamura", "LuminanceLayout", "OpponentHistogram", "ScalableColor"};
		
		//for gabor bug photo, resize photo if originalWidth, originalHeight ratio is too high, like 500*30, this will cause NegativeArraySize in precomputeGaborWavelet
		if (featClassName.equalsIgnoreCase("Gabor")) {
			double originalWidth = bim.getWidth();
		    double originalHeight = bim.getHeight();
		    double scaleFactor = 0.0; 
			if (originalWidth > originalHeight) {
				scaleFactor = ((double) originalWidth / originalHeight);
				if (scaleFactor>5) {
					// create smaller image
			        BufferedImage img = new BufferedImage((int) (originalWidth), (int) (originalWidth/5), BufferedImage.TYPE_INT_RGB);
			        // fast scale (Java 1.4 & 1.5)
			        Graphics g = img.getGraphics();
			        ((Graphics2D) g).setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BICUBIC);
			        g.drawImage(bim, 0, 0, img.getWidth(), img.getHeight(), null);
			        bim=img;
//			        ImageIO.write(img, "jpg", new File("D:/xinchaoli/Desktop/My research/Code_Tools/Lire/Lire-0.9.3/testBugPhos/resized.jpg"));
				}
			} else {
				scaleFactor = ((double) originalHeight / originalWidth);
				if (scaleFactor>5) {
					// create smaller image
			        BufferedImage img = new BufferedImage((int) (originalHeight/5), (int) (originalHeight), BufferedImage.TYPE_INT_RGB);
			        // fast scale (Java 1.4 & 1.5)
			        Graphics g = img.getGraphics();
			        ((Graphics2D) g).setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BICUBIC);
			        g.drawImage(bim, 0, 0, img.getWidth(), img.getHeight(), null);
			        bim=img;
//			        ImageIO.write(img, "jpg", new File("D:/xinchaoli/Desktop/My research/Code_Tools/Lire/Lire-0.9.3/testBugPhos/resized.jpg"));
				}
			}
		}
		
		LireFeature feature = (LireFeature) Class.forName("net.semanticmetadata.lire.imageanalysis." + featClassName).newInstance();
		feature.extract(bim);
		
//		//test byte array representation
//		double[] feature_data=feature.getDoubleHistogram();
//		LireFeature feature_clone= (LireFeature) Class.forName("net.semanticmetadata.lire.imageanalysis." + featClassName).newInstance();
//		feature_clone.setByteArrayRepresentation(feature.getByteArrayRepresentation());
//		double[] feature_clone_data=feature_clone.getDoubleHistogram();
//		System.out.println("feature_data:\t\t"+General.douArrToString(feature_data, ",", "0.000000"));
//		System.out.println("feature_clone_data:\t"+General.douArrToString(feature_clone_data, ",", "0.000000"));
//		double dist1=feature.getDistance(feature_clone);
//		System.out.println("dist1:\t"+dist1);
//		double dist2=getFeatDistance_lire136(feature.getByteArrayRepresentation(),feature_clone.getByteArrayRepresentation(),featClassName);
//		System.out.println("dist2:\t"+dist2);
		
		return feature.getByteArrayRepresentation();
	}
	
	public static double getFeatDistance_lire136(byte[] feat1, byte[] feat2, String featClassName) throws InterruptedException {
		try {
			LireFeature feature_1 = (LireFeature) Class.forName("net.semanticmetadata.lire.imageanalysis." + featClassName).newInstance();
			LireFeature feature_2= (LireFeature) Class.forName("net.semanticmetadata.lire.imageanalysis." + featClassName).newInstance();
			feature_1.setByteArrayRepresentation(feat1);
			feature_2.setByteArrayRepresentation(feat2);
			return feature_1.getDistance(feature_2);
		} catch (InstantiationException e) {
			e.printStackTrace();
			throw new InterruptedException("err in getFeatDistance_lire136, InstantiationException:"+e.getMessage());
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw new InterruptedException("err in getFeatDistance_lire136, IllegalAccessException:"+e.getMessage());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new InterruptedException("err in getFeatDistance_lire136, ClassNotFoundException:"+e.getMessage());
		}
		
	}
	
	public static double getFeatDistance_lire136(LireFeature feature_1, byte[] feat2, String featClassName) throws InterruptedException {
		try {
			LireFeature feature_2= (LireFeature) Class.forName("net.semanticmetadata.lire.imageanalysis." + featClassName).newInstance();
			feature_2.setByteArrayRepresentation(feat2);
			return feature_1.getDistance(feature_2);
		} catch (InstantiationException e) {
			e.printStackTrace();
			throw new InterruptedException("err in getFeatDistance_lire136, InstantiationException:"+e.getMessage());
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw new InterruptedException("err in getFeatDistance_lire136, IllegalAccessException:"+e.getMessage());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new InterruptedException("err in getFeatDistance_lire136, ClassNotFoundException:"+e.getMessage());
		}
	}
	
	public static double getFeatDistance_lire136_test(byte[] feat1, byte[] feat2, String featClassName) throws InstantiationException, IllegalAccessException, ClassNotFoundException{
		LireFeature feature_1= (LireFeature) Class.forName("net.semanticmetadata.lire.imageanalysis." + featClassName).newInstance();
		LireFeature feature_2= (LireFeature) Class.forName("net.semanticmetadata.lire.imageanalysis." + featClassName).newInstance();
		long startTime=System.currentTimeMillis();
		for (int i = 0; i < 100000; i++) {
			feature_1.setByteArrayRepresentation(feat1);
		}
		System.out.println(General.dispTime(System.currentTimeMillis()-startTime, "ms"));
		for (int i = 0; i < 100000; i++) {
			feature_2.setByteArrayRepresentation(feat2);
		}
		System.out.println(General.dispTime(System.currentTimeMillis()-startTime, "ms"));
		double dist=0;
		for (int i = 0; i < 100000; i++) {
			dist=feature_1.getDistance(feature_2);
		}
		System.out.println(General.dispTime(System.currentTimeMillis()-startTime, "ms"));
		return dist;
	}
	
	public static void main(String[] args) throws Exception {
		
//		String[] fieldsArray = {"CEDD", "EdgeHistogram", "FCTH", "ColorLayout", "PHOG", "JCD", "Gabor", "JpegCoeffs", "Tamura", "Luminance_Layout", "Opponent_Histogram", "ScalableColor"};
//      String[] classArray = {"CEDD", "EdgeHistogram", "FCTH", "ColorLayout", "PHOG", "JCD", "Gabor", "JpegCoefficientHistogram", "Tamura", "LuminanceLayout", "OpponentHistogram", "ScalableColor"};

		
//		//test one norm photo
//		int photoName=900002;
//		String photoBasePath="O:\\MediaEval_3185258Images\\trainImages_1-3185258\\900001-1000000\\";
//		String photoPath=photoBasePath+photoName+"_3185258.jpg";
		//test one bug photo
		String photoPath="D:/xinchaoli/Desktop/My research/Code_Tools/Lire/Lire-0.9.3/testBugPhos/4516161526.jpg";
		
		File photoFile=new File(photoPath);
		if (photoFile.exists()) {
			BufferedImage img=ImageIO.read(photoFile);
			long startTime=System.currentTimeMillis();
			//test extractFeat_inStr_lire093
//			String imageFeat=extractFeat_inStr_lire093(img);
//			System.out.println("imageFeat: "+imageFeat+" \n time:"+General.dispTime(System.currentTimeMillis()-startTime, "ms"));
			//test extractFeat_inByteArr_lire136
			byte[] imageFeat=extractFeat_inByteArr_lire136(img,"Gabor");
			System.out.println("imageFeat.length: "+imageFeat.length+" \n time:"+General.dispTime(System.currentTimeMillis()-startTime, "ms"));
		}
		
		System.out.println(System.getProperty("java.runtime.version"));
		
		//test getFeatDistance_lire136
		String featClassName="CEDD";
		String photoBasePath="O:\\MediaEval_3185258Images\\trainImages_1-3185258\\1-100000\\";
		byte[] imageFeat1=extractFeat_inByteArr_lire136(ImageIO.read(new File(photoBasePath+8+"_3185258.jpg")),featClassName);
		byte[] imageFeat2=extractFeat_inByteArr_lire136(ImageIO.read(new File(photoBasePath+43+"_3185258.jpg")),featClassName);
		double dist=getFeatDistance_lire136_test(imageFeat1,imageFeat2,featClassName);
		System.out.println("dist:\t"+dist);
		
	}

}
