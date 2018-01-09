package MyAPI.General;

import edu.emory.mathcs.jtransforms.fft.FloatFFT_2D;
import georegression.struct.point.Point2D_F64;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.color.CMMException;
import java.awt.geom.AffineTransform;
import java.awt.image.AffineTransformOp;
import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.Map.Entry;

import javax.imageio.ImageIO;

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.util.ArithmeticUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.ejml.data.DenseMatrix64F;
import org.ejml.factory.DecompositionFactory;
import org.ejml.factory.QRDecomposition;

import MyAPI.General.BoofCV.AssociationPanel_DIY;
import MyAPI.General.BoofCV.ShowPointsPanel;
import MyAPI.General.BoofCV.ShowPointsPanel.PointLink;
import MyAPI.General.BoofCV.ShowPointsPanel.SURFpoint_Weight;
import MyAPI.General.BoofCV.ShowPointsPanel_Group;
import MyAPI.General.ComparableCls.slave_masterFloat_DES;
import MyAPI.General.Magic.HungarianAlgorithm;
import MyAPI.General.Magic.LineDetection;
import MyAPI.Interface.FeatInd;
import MyAPI.Interface.FeatInd_Score;
import MyAPI.Interface.I_HESig;
import MyAPI.Obj.Disp;
import MyAPI.Obj.GistParam;
import MyAPI.Obj.HistMultiD_Sparse_equalSizeBin_forFloat;
import MyAPI.Obj.ImageInformation;
import MyAPI.Obj.MatchFeat_VW_matchScore;
import MyAPI.Obj.MatchingScore;
import MyAPI.Obj.Pair_int;
import MyAPI.Obj.Point_XY;
import MyAPI.Obj.SumFeat_Num;
import MyAPI.SystemCommand.MySystemCommandExecutor;
import MyAPI.imagR.ExtractFeat.FeatState;
import MyAPI.imagR.ScoreDoc.MatchingInfo;
import MyAPI.imagR.TVectorInfo;
import MyCustomedHaoop.ValueClass.ArrWritableClassCollection.MatchFeat_Arr;
import MyCustomedHaoop.ValueClass.DocAllMatchFeats;
import MyCustomedHaoop.ValueClass.HESig;
import MyCustomedHaoop.ValueClass.ImageRegionMatch;
import MyCustomedHaoop.ValueClass.Int_Float;
import MyCustomedHaoop.ValueClass.Int_MatchFeatArr;
import MyCustomedHaoop.ValueClass.Int_MatchFeatArr_Arr;
import MyCustomedHaoop.ValueClass.MatchFeat;
import MyCustomedHaoop.ValueClass.MatchFeat_VW;
import MyCustomedHaoop.ValueClass.PhotoAllFeats;
import MyCustomedHaoop.ValueClass.SURFfeat;
import MyCustomedHaoop.ValueClass.SURFfeat_ShortArr_AggSig;
import MyCustomedHaoop.ValueClass.SURFfeat_noSig;
import MyCustomedHaoop.ValueClass.SURFpoint;
import MyCustomedHaoop.ValueClass.SURFpointVWs;
import MyCustomedHaoop.ValueClass.SURFpoint_onlyLoc;
import MyCustomedHaoop.ValueClass.VW_AggSig;
import MyCustomedHaoop.ValueClass.VW_Weight_HESig;
import MyCustomedHaoop.ValueClass.VW_Weight_HESig_ShortArr;
import boofcv.abst.feature.detect.extract.FeatureExtractor;
import boofcv.alg.feature.describe.DescribePointSurf;
import boofcv.alg.feature.detect.interest.FastHessianFeatureDetector;
import boofcv.alg.feature.orientation.OrientationIntegral;
import boofcv.alg.transform.ii.GIntegralImageOps;
import boofcv.core.image.ConvertBufferedImage;
import boofcv.core.image.GeneralizedImageOps;
import boofcv.factory.feature.describe.FactoryDescribePointAlgs;
import boofcv.factory.feature.detect.extract.FactoryFeatureExtractor;
import boofcv.factory.feature.orientation.FactoryOrientationAlgs;
import boofcv.gui.image.ShowImages;
import boofcv.struct.feature.ScalePoint;
import boofcv.struct.feature.SurfFeature;
import boofcv.struct.image.ImageFloat32;
import boofcv.struct.image.ImageSingleBand;
import boofcv.struct.image.ImageUInt8;
import boofcv.struct.image.MultiSpectral;

public class General_BoofCV {

	@SuppressWarnings("rawtypes")
	public static <T extends ImageSingleBand> T BoofCV_loadImage(InputStream inputStream, Class<T> imageType ) throws IOException {
		//toUse: ImageFloat32 img=BoofCV_loadImage(new ByteArrayInputStream(value.getBytes()),ImageFloat32.class);
		BufferedImage img_buffer = ImageIO.read(inputStream);
		if( img_buffer == null )
			return null;
		return ConvertBufferedImage.convertFromSingle(img_buffer, (T) null, imageType);
	}
	
	@SuppressWarnings("rawtypes")
	public static <T extends ImageSingleBand> T BoofCV_loadImage(BufferedImage img_buffer, Class<T> imageType ) {
		//toUse: ImageFloat32 img=BoofCV_loadImage(img_buffer,ImageFloat32.class);
		if( img_buffer == null )
			return null;
		BufferedImage colorImage=convertTo3BandColorBufferedImage(img_buffer);
		return ConvertBufferedImage.convertFromSingle(colorImage, (T) null, imageType);
	}
	
	@SuppressWarnings("rawtypes")
	public static <T extends ImageSingleBand> T BoofCV_loadImage(File inputStream, Class<T> imageType ) throws IOException {
		//toUse: ImageFloat32 img=BoofCV_loadImage(new ByteArrayInputStream(value.getBytes()),ImageFloat32.class);
		BufferedImage img_buffer = ImageIO.read(inputStream);
		if( img_buffer == null )
			return null;
		return ConvertBufferedImage.convertFromSingle(img_buffer, (T) null, imageType);
	}
	
	@SuppressWarnings("rawtypes")
	public static <II extends ImageSingleBand> double[][]  computeSURF_boofCV015 (ImageFloat32 image, String paraFE, String paraFD){	
//		//paraFE: "2,0,5,true"  ////defaut: FactoryFeatureExtractor.nonmax(2, 0, 5, true);
//		String[] parFE=paraFE.split(",");
//		//paraDE: "2000,1,9,4,4"  ////defaut: FastHessianFeatureDetector<II>(extractor,200,1, 9,4,4), FH-9 detector in the paper: 1,9,4,4
//		int[] parDE=General.StrArrToIntArr(paraFD.split(","));
//		
//		// SURF works off of integral images
//		Class<II> integralType = GIntegralImageOps.getIntegralType(ImageFloat32.class);
// 
//		// define the feature detection algorithm		
//		NonMaxSuppression extractor = FactoryFeatureExtractor.nonmax(new ConfigExtract(Integer.valueOf(parFE[0]), Integer.valueOf(parFE[1]), Integer.valueOf(parFE[2]), Boolean.parseBoolean(parFE[3]))); //defaut: FactoryFeatureExtractor.nonmax(new ConfigExtract(2, 0, 5, true));
//		FastHessianFeatureDetector<II> detector = 
//				new FastHessianFeatureDetector<II>(extractor,parDE[0],parDE[1],parDE[2],parDE[3],parDE[4]); //defaut: FastHessianFeatureDetector<II>(extractor,200,2, 9,4,4)
//
//		// estimate orientation
//		OrientationIntegral<II> orientation = 
//				FactoryOrientationAlgs.sliding_ii(null, integralType);
// 
//		DescribePointSurf<II> descriptor = FactoryDescribePointAlgs.<II>surfStability(null,integralType);
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
//			int bugPointNum=0;
//			for( ScalePoint p : points ) {
//				try {
//					// estimate orientation
//					orientation.setScale(p.scale);
//					double angle = orientation.compute(p.x,p.y);
//					
//					// extract the SURF description for this region
//					SurfFeature desc = descriptor.createDescription();
//					descriptor.describe(p.x,p.y,angle,p.scale,desc);
//		 
//					// save everything for processing later on
//					descriptions.add(desc);
//				} catch (ArrayIndexOutOfBoundsException e) {
//					bugPointNum++;
//					System.err.println("error in computeSURF, ignor this point!  eMessage:"+e.getMessage());
//					e.printStackTrace();
//				}
//			}
//			
//			int numGoodPoint=descriptions.size();
//			int FeatLength=descriptions.get(0).value.length;
//			double[][] photoFeat=new double[numGoodPoint][FeatLength];
////			int index=0;
//			for(int i=0;i<numGoodPoint;i++){
//				for(int j=0;j<FeatLength;j++){
//					photoFeat[i][j]=descriptions.get(i).value[j];
////					index++;
//				}
//			}
//			//for debug
//			if (bugPointNum!=0) {
//				System.err.println("bugPoint in computeSURF, bugPointNum:"+bugPointNum+", numGoodPoint:"+numGoodPoint);
//			}
//			return photoFeat;
//		}else{ // interest point dose not exist!
//			System.err.println("no interest point!!");
//			return null;
//		}
		
		return null; //for debug
	}

	@SuppressWarnings("rawtypes")
	public static <II extends ImageSingleBand> double[][] computeSURF_boofCV_09(String imgMark, ImageFloat32 image, String paraFE, String paraFD, ArrayList<SURFpoint> interestPoints){	
		//paraFE: "2,1,5,true"  ////defaut: FactoryFeatureExtractor.nonmax(2, 0, 5, true);
		String[] parFE=paraFE.split(",");
		//paraDE: "2000,1,9,4,4"  ////defaut: FastHessianFeatureDetector<II>(extractor,200,1, 9,4,4), FH-9 detector in the paper: 1,9,4,4
		int[] parDE=General.StrArrToIntArr(paraFD.split(","));
		
		//for save location of interestPoints
		boolean saveIOPoints=false;
		if (interestPoints!=null) {//if only for make TVector, no ndeed for interestPoints, only need VW_Sigs, so interestPoints can be null
			interestPoints.clear();
			saveIOPoints=true;
		}

		// SURF works off of integral images
		Class<II> integralType = GIntegralImageOps.getIntegralType(ImageFloat32.class);
 
		// define the feature detection algorithm
		FeatureExtractor extractor = FactoryFeatureExtractor.nonmax(Integer.valueOf(parFE[0]), Integer.valueOf(parFE[1]), Integer.valueOf(parFE[2]), Boolean.parseBoolean(parFE[3])); //defaut: FactoryFeatureExtractor.nonmax(2, 0, 5, true);
		FastHessianFeatureDetector<II> detector = 
				new FastHessianFeatureDetector<II>(extractor,parDE[0],parDE[1],parDE[2],parDE[3],parDE[4]); //defaut: FastHessianFeatureDetector<II>(extractor,200,2, 9,4,4)
 
		// estimate orientation
		OrientationIntegral<II> orientation = 
				FactoryOrientationAlgs.sliding_ii(0.65, Math.PI / 3.0, 8, -1, 6, integralType);
 
		DescribePointSurf<II> descriptor = FactoryDescribePointAlgs.<II>msurf(integralType);
 
		// compute the integral image of 'image'
		II integral = GeneralizedImageOps.createSingleBand(integralType,image.width,image.height);
		GIntegralImageOps.transform(image, integral);
 
		// detect fast hessian features
		detector.detect(integral);
		// tell algorithms which image to process
		orientation.setImage(integral);
		descriptor.setImage(integral);
 
		List<ScalePoint> points = detector.getFoundPoints();
		if (points.size()!=0){ // interest point exist!
			List<SurfFeature> descriptions = new ArrayList<SurfFeature>();
			int bugPointNum=0;
			for( ScalePoint p : points ) {
				try {
					// estimate orientation
					orientation.setScale(p.scale);
					double angle = orientation.compute(p.x,p.y);
					// extract the SURF description for this region
					SurfFeature desc = descriptor.describe(p.x,p.y,p.scale,angle,null);
					// save everything for processing later on
					descriptions.add(desc);
					if (saveIOPoints) {
						interestPoints.add(new SURFpoint((short)p.x,(short)p.y,(float)p.scale,(float)angle));
					}
				} catch (ArrayIndexOutOfBoundsException e) {
					bugPointNum++;
					System.err.println("error in computeSURF, ignor this point!  eMessage:"+e.getMessage());
					e.printStackTrace();
				}
			}
			int numGoodPoint=descriptions.size();
			double[][] photoFeat=new double[numGoodPoint][];
			for(int i=0;i<numGoodPoint;i++){
				photoFeat[i]=descriptions.get(i).value;
			}
			//for debug bug Point
			if (bugPointNum!=0) {
				System.err.println("bugPoint in computeSURF, bugPointNum:"+bugPointNum+", numGoodPoint:"+numGoodPoint);
			}
			General.Assert(photoFeat.length<30000, "err! photoFeat > 30,000, imgMark:"+imgMark+", its size w_h:"+image.getWidth()+"_"+image.getHeight());
			return photoFeat;
		}else{ // interest point dose not exist!
			System.err.println("no interest point!!");
			return null;
		}	
		
//		return null;
	}

	/**
	 * Oxford_extract_features.ln, note: the so-called improved vesion, -noangle does not work!!
	 * Interest point detectors/descriptors implemented by Krystian.Mikolajczyk@inrialpes.fr
		at INRIA Rhone-Alpes.[ref. www.inrialpes.fr/movi/people/Mikolajczyk/Affine]
		Options:
		     -harlap - harris-laplace detector
		     -heslap - hessian-laplace detector
		     -haraff - harris-affine detector
		     -hesaff - hessian-affine detector
		     -harhes - harris-hessian-laplace detector
		     -sedgelap - edge-laplace detector
		     -jla  - steerable filters,  similarity=
		     -sift - sift [D. Lowe],  similarity=
		     -gloh - extended sift,  similarity=
		     -mom  - moments,  similarity=
		     -koen - differential invariants,  similarity=
		     -cf   - complex filters [F. Schaffalitzky],  similarity=
		     -sc   - shape context,  similarity=45000
		     -spin - spin,  similarity=
		     -gpca - gradient pca [Y. Ke],  similarity=
		     -cc - cross correlation,  similarity=
		     -i image.pgm  - input image pgm, ppm, png
		     -pca input.basis - projects the descriptors with pca basis
		     -p1 image.pgm.points - input regions format 1
		     -p2 image.pgm.points - input regions format 2
		     -o1 out.desc - saves descriptors in out.desc output format1
		     -o2 out.desc - saves descriptors in out.desc output format2
		     -noangle - computes rotation variant descriptors (no rotation esimation)
		     -DC - draws regions as circles in out.desc.png
		     -DR - draws regions as ellipses in out.desc.png
		     -c 255 - draws points in grayvalue [0,...,255]
		example:
		     ./Oxford_extract_features.ln -jla -i image.png -p1 image.png.points -DR
		               ./Oxford_extract_features.ln-harlap -gloh -i image.png  -DR
		
		 file format 1:
		vector_dimension
		nb_of_descriptors
		x y a b c desc_1 desc_2 ......desc_vector_dimension
		--------------------
		
		where a(x-u)(x-u)+2b(x-u)(y-v)+c(y-v)(y-v)=1
		
		 file format 2:
		vector_dimension
		nb_of_descriptors
		x y cornerness scale angle object_index  point_type laplacian extremum_type mi11 mi12 mi21 mi22 desc_1 ...... desc_vector_dimension
		--------------------
		
		distance=(descA_1-descB_1)^2+...+(descA_vector_dimension-descB_vector_dimension)^2
		
		 input.basis format:
		nb_of_dimensions
		mean_v1
		mean_v2
		.
		.
		mean_v_nb_of_dim
		nb_of_dimensions*nb_of_pca_vectors
		pca_vector_v1
		pca_vector_v2
		.
		.

	 */
	public static double[][] computeSIFT_binaryOxford1(String imgMark, BufferedImage colorImage, String binaryPath_DetDes, String tempFilesPath, String tmpFileMarker, String param, int DetectThr, int maxPointNum, boolean showFeature, boolean showRunBinaryInfo, boolean deleteTempFile, ArrayList<SURFpoint> interestPoints) throws IOException, InterruptedException{
		/**
		 * if showFeature==true, then it generate a png file with name siftPath+".png"
		 * param: -hesaff -hesThres 500 -sift
		 */
		
		//********** prepare for run binary **********
		String pngPath=tempFilesPath+tmpFileMarker+".png";
		String siftPath=tempFilesPath+tmpFileMarker+".sift";
		colorImage=convertTo3BandColorBufferedImage(colorImage);
	    ImageIO.write(colorImage, "png", new File(pngPath));
		//********** call sift-extractor binary *************//
	    boolean isTooMuchPoint=true; double[][] feats=null;
	    while (isTooMuchPoint) {
	    	//for save location of interestPoints
			boolean saveIOPoints=false;
			if (interestPoints!=null) {
				interestPoints.clear();
				saveIOPoints=true;
			}
			String[] params=param.split(" ");
		    // ./Oxford2_extract_features_64bit.ln -sift -hesaff -hesThres 500 -i image.pgm  -DC
			LinkedList<String> commands=new LinkedList<>(Arrays.asList(binaryPath_DetDes));
			commands.addAll(Arrays.asList(params));
			commands.addAll(Arrays.asList("-thres", DetectThr+"", "-i", pngPath, showFeature?"-DC":"", "-o2", siftPath));
			// execute the command
			MySystemCommandExecutor commandExecutor = new MySystemCommandExecutor(commands,true,false);
			commandExecutor.executeCommand(showRunBinaryInfo,"\t","s",null);
			if (showRunBinaryInfo) {
				System.out.println("\t detecting sift ");
				System.out.println("\t ********* Computing SIFT binary STDOUT: ***************");
			    System.out.println("\t "+commandExecutor.getStandardOutputFromCommand());
			    System.out.println("\t ********* Computing SIFT binary STDERR: ***************");
			    System.out.println("\t "+commandExecutor.getStandardErrorFromCommand());
			    System.out.println("\t *****************************************************");
			}
			//********** read output file from binary *************//
			if (!new File(siftPath).exists()) {// interest point dose not exist!
				isTooMuchPoint=false;
				System.out.println("\t no sift result file for this photo! imgMark:"+imgMark);
				System.out.println("\t exist output files for pgm? "+new File(pngPath).exists()+", file for showFeatPng? "+new File(siftPath+".png").exists()+", params file? "+new File(siftPath+".params").exists());
				System.out.println("\t ********* Computing SIFT binary STDOUT: *************** \n"+commandExecutor.getStandardOutputFromCommand());
			    System.out.println("\t ********* Computing SIFT binary STDERR: *************** \n"+commandExecutor.getStandardErrorFromCommand());
			    System.out.println("\t *****************************************************");
			}else {
				String line1;
				BufferedReader siftFile_content= new BufferedReader(new InputStreamReader(new FileInputStream(siftPath), "UTF-8"));
				/*
				 * file format 2: 
				 		vector_dimension
					 	nb_of_descriptors
					 	x y cornerness scale/patch_size angle object_index  point_type laplacian_value extremum_type mi11 mi12 mi21 mi22 desc_1 ...... desc_vector_dimension
				 */
				String featDim=siftFile_content.readLine(); int pointNum=Integer.valueOf(siftFile_content.readLine()); 
				if (pointNum>0) {
					feats=new double[pointNum][128]; int point_ind=0;
					if (saveIOPoints) {
						while ((line1=siftFile_content.readLine())!=null) {
							//save feat
							String[] lineInfo=line1.split(" ");
							General.Assert(lineInfo.length==141, 
									"err! lineInfo.length should ==141, but "+lineInfo.length+", featDim:"+featDim+"\n"
									+"this line:"+line1+"\n"
									+"\t Computing SIFT binary STDOUT:"+commandExecutor.getStandardOutputFromCommand()+"\n"
									+"\t Computing SIFT binary STDERR:"+commandExecutor.getStandardErrorFromCommand());
							feats[point_ind]=General.StrArrToDouArr(General.selectArrStr(lineInfo, null, 0, 128));
							point_ind++;
							//add point geometric
							interestPoints.add(new SURFpoint(Float.valueOf(lineInfo[0]).shortValue(), Float.valueOf(lineInfo[1]).shortValue(), Float.valueOf(lineInfo[3]), Float.valueOf(lineInfo[4])));
						}
					}else {
						while ((line1=siftFile_content.readLine())!=null) {
							//save feat
							String[] lineInfo=line1.split(" ");
							General.Assert(lineInfo.length==141, 
									"err! lineInfo.length should ==141, but "+lineInfo.length+", featDim:"+featDim+"\n"
									+"this line:"+line1+"\n"
									+"\t Computing SIFT binary STDOUT:"+commandExecutor.getStandardOutputFromCommand()+"\n"
									+"\t Computing SIFT binary STDERR:"+commandExecutor.getStandardErrorFromCommand());
							feats[point_ind]=General.StrArrToDouArr(General.selectArrStr(lineInfo, null, 0, 128));
							point_ind++;
						}
					}
					General.Assert(point_ind==pointNum, "err! point_ind should ==pointNum, but point_ind:"+point_ind+", pointNum:"+pointNum);
					//check point num
					if (feats.length<=maxPointNum) {
						isTooMuchPoint=false;
					}else {
						DetectThr+=100;
						System.out.println("\t warning! photoFeat:"+feats.length+" > "+maxPointNum+", imgMark:"+imgMark+", its size w_h:"+colorImage.getWidth()+"_"+colorImage.getHeight()+", now try a larger DetectThr:"+DetectThr);
					}
				}else {//something may be wrong, because if no sift point detected, the binary will NOT generate .sift file
					isTooMuchPoint=false;
					System.err.println("photo has sift file result, but no interest point!!  ");
					System.out.println("\t Computing SIFT binary STDOUT:"+commandExecutor.getStandardOutputFromCommand());
				    System.out.println("\t Computing SIFT binary STDERR:"+commandExecutor.getStandardErrorFromCommand());
				}
				siftFile_content.close();
				//delete tmp files
				if (isTooMuchPoint) {
					General.Assert(new File(siftPath).delete(), "err! fail to delete siftPath:"+siftPath);
					if (showFeature) {
						General.Assert(new File(siftPath+".png").delete(), "err! fail to delete showSift Path:"+(siftPath+".png"));
					}
				}
			}
		}
	    //delete tmp files
		if (deleteTempFile) {//files that always exist no matter whether photo has sift detected
			General.Assert(new File(pngPath).delete(), "err! fail to delete pngPath:"+pngPath);
//			General.Assert(new File(siftPath+".params").delete(), "err! fail to delete params Path:"+(siftPath+".params"));
			if (feats!=null) {
				General.Assert(new File(siftPath).delete(), "err! fail to delete siftPath:"+siftPath);
			}
			if (showFeature) {
				General.Assert(new File(siftPath+".png").delete(), "err! fail to delete showSift Path:"+(siftPath+".png"));
				System.out.println("warning delectTempFile is true, but showFeature is true, so it will delete the generated png with name:"+(siftPath+".png"));
			}
		}

		return feats;
	} 
	
	/**
	 * INRIA2_compute_descriptors_linux64
	 * Options:
	     -harlap - harris-laplace detector
	     -heslap - hessian-laplace detector
	     -haraff - harris-affine detector
	     -hesaff - hessian-affine detector
	     -harhes - harris-hessian-laplace detector
	     -dense xstep ystep nbscales - dense detector
	     -sedgelap - edge-laplace detector
	     -jla  - steerable filters,  similarity=
	     -sift - sift [D. Lowe],  similarity=
	     -siftnonorm - sift [D. Lowe],  without normalization
	     -msift - Mahalanobis sift, similarity=
	     -gloh - extended sift,  similarity=
	     -mom  - moments,  similarity=
	     -koen - differential invariants,  similarity=
	     -cf   - complex filters [F. Schaffalitzky],  similarity=
	     -sc   - shape context,  similarity=45000
	     -spin - spin,  similarity=
	     -gpca - gradient pca [Y. Ke],  similarity=
	     -cc - cross correlation,  similarity=
	     -i image.pgm  - input image pgm, ppm, png
	     -i2 image.jpg - input of any format of ImageMagick (WARN: uses only green channel)
	     -pca input.basis - projects the descriptors with pca basis
	     -p1 image.pgm.points - input regions format 1
	     -p2 image.pgm.points - input regions format 2
	     -o1 out.desc - saves descriptors in out.desc output format1
	     -o2 out.desc - saves descriptors in out.desc output format2
	     -o3 out.siftbin - saves descriptors in binary format
	     -o4 out.siftgeo - binary descriptor format used by Bigimbaz
	     -coord out.coord - saves coordinates in binary format
	     -noangle - computes rotation variant descriptors (no rotation esimation)
	     -DC - draws regions as circles in out.desc.png
	     -DR - draws regions as ellipses in out.desc.png
	     -c 255 - draws points in grayvalue [0,...,255]
	     -thres - threshod value
	     -max - maximum for the number of computed descriptors in HESSAFF
	example:
	     ./INRIA2_compute_descriptors_linux64 -jla -i image.png -p1 image.png.points -DR
	               ./INRIA2_compute_descriptors_linux64-harlap -gloh -i image.png  -DR

		 file format 1:
		vector_dimension
		nb_of_descriptors
		x y a b c desc_1 desc_2 ......desc_vector_dimension
		--------------------
		
		where a(x-u)(x-u)+2b(x-u)(y-v)+c(y-v)(y-v)=1
		
		 file format 2:
		vector_dimension
		nb_of_descriptors
		x y cornerness scale angle object_index  point_type laplacian extremum_type mi11 mi12 mi21 mi22 desc_1 ...... desc_vector_dimension
		--------------------
		
		distance=(descA_1-descB_1)^2+...+(descA_vector_dimension-descB_vector_dimension)^2
		
		 input.basis format:
		nb_of_dimensions
		mean_v1
		mean_v2
		.
		.
		mean_v_nb_of_dim
		nb_of_dimensions*nb_of_pca_vectors
		pca_vector_v1
		pca_vector_v2
		.
		.
	 */
	public static double[][] computeSIFT_binaryINRIA2(String imgMark, BufferedImage colorImage, String binaryPath_DetDes, String tempFilesPath, String tmpFileMarker, String param, int DetectThr, int maxPointNum, boolean showFeature, boolean showRunBinaryInfo, boolean deleteTempFile, ArrayList<SURFpoint> interestPoints) throws IOException, InterruptedException{
		/**
		 * the result format -o2 is the same with Oxford2's, the difference is the parameters.
		 * if showFeature==true, then it generate a png file with name siftPath+".png"
		 * param: -sift -hesaff -thres 500 -max 5000
		 * if -noangle is used, then it is upRightSIFT
		 */
		//********** prepare for run binary **********
		String pngPath=tempFilesPath+tmpFileMarker+".png";
		String siftPath=tempFilesPath+tmpFileMarker+".sift";
		colorImage=convertTo3BandColorBufferedImage(colorImage);
	    ImageIO.write(colorImage, "png", new File(pngPath));
		//********** call sift-extractor binary *************//
	    double[][] feats=null;
    	//for save location of interestPoints
		boolean saveIOPoints=false;
		if (interestPoints!=null) {
			interestPoints.clear();
			saveIOPoints=true;
		}
		String[] params=param.split(" ");
	    // ./INRIA2_compute_descriptors_linux64 -sift -hesaff -thres 500 -max 5000 -i image.png -DC
		LinkedList<String> commands=new LinkedList<>(Arrays.asList(binaryPath_DetDes));
		commands.addAll(Arrays.asList(params));
		commands.addAll(Arrays.asList("-thres", DetectThr+"", "-max", maxPointNum+"", "-i", pngPath, showFeature?"-DC":"", "-o2", siftPath));
	    // execute the command
		MySystemCommandExecutor commandExecutor = new MySystemCommandExecutor(commands,true,false);
		commandExecutor.executeCommand(showRunBinaryInfo,"\t","s",null);
		//********** read output file from binary *************//
		if (!new File(siftPath).exists()) {// interest point dose not exist!
			System.out.println("\t no sift result file for this photo! imgMark:"+imgMark);
			System.out.println("\t exist output files for pgm? "+new File(pngPath).exists()+", file for showFeatPng? "+new File(siftPath+".png").exists()+", params file? "+new File(siftPath+".params").exists());
			System.out.println("\t ********* Computing SIFT binary STDOUT: *************** \n"+commandExecutor.getStandardOutputFromCommand());
		    System.out.println("\t ********* Computing SIFT binary STDERR: *************** \n"+commandExecutor.getStandardErrorFromCommand());
		    System.out.println("\t *****************************************************");
		}else {
			if (showRunBinaryInfo) {
				System.out.println("\t detecting sift ");
				System.out.println("\t ********* Computing SIFT binary STDOUT: ***************");
			    System.out.println("\t "+commandExecutor.getStandardOutputFromCommand());
			    System.out.println("\t ********* Computing SIFT binary STDERR: ***************");
			    System.out.println("\t "+commandExecutor.getStandardErrorFromCommand());
			    System.out.println("\t *****************************************************");
			}
			String line1;
			BufferedReader siftFile_content= new BufferedReader(new InputStreamReader(new FileInputStream(siftPath), "UTF-8"));
			/*
			 * file format 2: 
			 		vector_dimension
				 	nb_of_descriptors
				 	x y cornerness scale/patch_size angle object_index  point_type laplacian_value extremum_type mi11 mi12 mi21 mi22 desc_1 ...... desc_vector_dimension
			 */
			String featDim=siftFile_content.readLine(); int pointNum=Integer.valueOf(siftFile_content.readLine()); 
			if (pointNum>0) {
				feats=new double[pointNum][128]; int point_ind=0;
				if (saveIOPoints) {
					while ((line1=siftFile_content.readLine())!=null) {
						//save feat
						String[] lineInfo=line1.split(" ");
						General.Assert(lineInfo.length==141, 
								"err! lineInfo.length should ==141, but "+lineInfo.length+", featDim:"+featDim+"\n"
								+"this line:"+line1+"\n"
								+"\t Computing SIFT binary STDOUT:"+commandExecutor.getStandardOutputFromCommand()+"\n"
								+"\t Computing SIFT binary STDERR:"+commandExecutor.getStandardErrorFromCommand());
						feats[point_ind]=General.StrArrToDouArr(General.selectArrStr(lineInfo, null, 0, 128));
						point_ind++;
						//add point geometric
						interestPoints.add(new SURFpoint(Float.valueOf(lineInfo[0]).shortValue(), Float.valueOf(lineInfo[1]).shortValue(), Float.valueOf(lineInfo[3]), Float.valueOf(lineInfo[4])));
					}
				}else {
					while ((line1=siftFile_content.readLine())!=null) {
						//save feat
						String[] lineInfo=line1.split(" ");
						General.Assert(lineInfo.length==141, 
								"err! lineInfo.length should ==141, but "+lineInfo.length+", featDim:"+featDim+"\n"
								+"this line:"+line1+"\n"
								+"\t Computing SIFT binary STDOUT:"+commandExecutor.getStandardOutputFromCommand()+"\n"
								+"\t Computing SIFT binary STDERR:"+commandExecutor.getStandardErrorFromCommand());
						feats[point_ind]=General.StrArrToDouArr(General.selectArrStr(lineInfo, null, 0, 128));
						point_ind++;
					}
				}
				General.Assert(point_ind==pointNum, "err! point_ind should ==pointNum, but point_ind:"+point_ind+", pointNum:"+pointNum);
			}else {//something may be wrong, because if no sift point detected, the binary will NOT generate .sift file
				System.err.println("photo has sift file result, but no interest point!!  ");
				System.out.println("\t Computing SIFT binary STDOUT:"+commandExecutor.getStandardOutputFromCommand());
			    System.out.println("\t Computing SIFT binary STDERR:"+commandExecutor.getStandardErrorFromCommand());
			}
			siftFile_content.close();
		}
	    //delete tmp files
		if (deleteTempFile) {//files that always exist no matter whether photo has sift detected
			General.Assert(new File(pngPath).delete(), "err! fail to delete pngPath:"+pngPath);
			if (new File(siftPath).exists()) {
				General.Assert(new File(siftPath).delete(), "err! fail to delete siftPath:"+siftPath);
			}
			if (showFeature) {
				General.Assert(new File(siftPath+".png").delete(), "err! fail to delete showSift Path:"+(siftPath+".png"));
				System.out.println("warning delectTempFile is true, but showFeature is true, so it will delete the generated png with name:"+(siftPath+".png"));
			}
		}

		return feats;
	}
	
	public static double[][] computeSIFT_binaryVLFeat09(String imgMark, BufferedImage colorImage, String binaryPath_DetDes, String tempFilesPath, String tmpFileMarker, int PeakThr, int EdgeThr, int maxPointNum, boolean showRunBinaryInfo, boolean deleteTempFile, ArrayList<SURFpoint> interestPoints) throws IOException, InterruptedException{
		/**
		 * when ever photo has feat or not, binary always generate a resut file, (if no feat, then no content in result)
		 * param： --peak-thresh=3 --edge-thresh=5
		 */
		
		//********** prepare for run binary **********
		String pgmPath=tempFilesPath+tmpFileMarker+".pgm";
		String siftPath=tempFilesPath+tmpFileMarker+".sift";
		colorImage=convertTo3BandColorBufferedImage(colorImage);
		saveImageUInt8ToPGM_BoofCV(rgbToGray_BoofCV(colorImage),pgmPath);
		//********** call sift-extractor binary *************//
		boolean isTooMuchPoint=true;  double[][] feats=null;
		while (isTooMuchPoint) {
	    	//for save location of interestPoints
			boolean saveIOPoints=false;
			if (interestPoints!=null) {
				interestPoints.clear();
				saveIOPoints=true;
			}
		    // ./sift testDownSamp_windows.pgm --peak-thresh=3 −−edge-thresh=5 -o VLFeat.sift
			List<String> commands = Arrays.asList(binaryPath_DetDes, pgmPath, "--peak-thresh="+PeakThr, "--edge-thresh="+EdgeThr, "-o", siftPath); // build the system command we want to run	    
		    // execute the command
			MySystemCommandExecutor commandExecutor = new MySystemCommandExecutor(commands,true,false);
			commandExecutor.executeCommand(showRunBinaryInfo,"\t","s",null);
			//********** read output file from binary *************//
			if (!new File(siftPath).exists()) {// no sift result file!
				isTooMuchPoint=false;
				System.out.println("\t no sift result file for this photo! imgMark:"+imgMark);
				System.out.println("\t exist output files for pgm? "+new File(pgmPath).exists());
				System.out.println("\t ********* Computing SIFT binary STDOUT: ***************");
			    System.out.println("\t "+commandExecutor.getStandardOutputFromCommand());
			    System.out.println("\t ********* Computing SIFT binary STDERR: ***************");
			    System.out.println("\t "+commandExecutor.getStandardErrorFromCommand());
			    System.out.println("\t *****************************************************");
				throw new InterruptedException("error! no SIFT result file generated");
			}else {
				if (showRunBinaryInfo) {
					System.out.println("\t detecting sift using VLFeat binary");
					System.out.println("\t ********* Computing SIFT binary STDOUT: ***************");
				    System.out.println("\t "+commandExecutor.getStandardOutputFromCommand());
				    System.out.println("\t ********* Computing SIFT binary STDERR: ***************");
				    System.out.println("\t "+commandExecutor.getStandardErrorFromCommand());
				    System.out.println("\t *****************************************************");
				}
				String line1;
				BufferedReader siftFile_content= new BufferedReader(new InputStreamReader(new FileInputStream(siftPath), "US-ASCII")); //resut is in US-ASCII
				/*
				 * file format Ascii: 
					 	x y scale/patch_size angle desc_vector(128 ints)
				 */
				int featDim=128; 
				LinkedList<double[]> feats_list=new LinkedList<double[]>();
				if (saveIOPoints) {
					while ((line1=siftFile_content.readLine())!=null) {
						//save feat
						String[] lineInfo=line1.split(" ");//x y scale angel feat-128
						General.Assert(lineInfo.length==132, 
								"err! lineInfo.length should ==132, but "+lineInfo.length+", featDim:"+featDim+"\n"
								+"this line:"+line1+"\n"
								+"\t Computing SIFT binary STDOUT:"+commandExecutor.getStandardOutputFromCommand()+"\n"
								+"\t Computing SIFT binary STDERR:"+commandExecutor.getStandardErrorFromCommand());
						feats_list.add(General.StrArrToDouArr(General.selectArrStr(lineInfo, null, 0, 128)));
						//add point geometric, x,y,scale,angle
						interestPoints.add(new SURFpoint(Float.valueOf(lineInfo[0]).shortValue(), Float.valueOf(lineInfo[1]).shortValue(), Float.valueOf(lineInfo[2]), Float.valueOf(lineInfo[3])));
					}
				}else {
					while ((line1=siftFile_content.readLine())!=null) {
						//save feat
						String[] lineInfo=line1.split(" ");
						General.Assert(lineInfo.length==132, 
								"err! lineInfo.length should ==132, but "+lineInfo.length+", featDim:"+featDim+"\n"
								+"this line:"+line1+"\n"
								+"\t Computing SIFT binary STDOUT:"+commandExecutor.getStandardOutputFromCommand()+"\n"
								+"\t Computing SIFT binary STDERR:"+commandExecutor.getStandardErrorFromCommand());
						feats_list.add(General.StrArrToDouArr(General.selectArrStr(lineInfo, null, 0, 128)));
					}
				}
				siftFile_content.close();
				if (feats_list.size()>0) {
					if (feats_list.size()>maxPointNum) {
						PeakThr+=1;
						System.out.println("\t warning! photoFeat:"+feats_list.size()+" > "+maxPointNum+", imgMark:"+imgMark+", its size w_h:"+colorImage.getWidth()+"_"+colorImage.getHeight()+", now try a larger PeakThr:"+PeakThr);
						General.Assert(new File(siftPath).delete(), "err! fail to delete siftPath:"+siftPath);
					}else {
						isTooMuchPoint=false;
						feats=feats_list.toArray(new double[0][]);
					}
				}else {
					isTooMuchPoint=false;
					System.err.println("photo has sift file result, but no interest point!!  ");
					if (!showRunBinaryInfo) {
						System.out.println("\t ********* Computing SIFT binary STDOUT: ***************");
					    System.out.println("\t "+commandExecutor.getStandardOutputFromCommand());
					    System.out.println("\t ********* Computing SIFT binary STDERR: ***************");
					    System.out.println("\t "+commandExecutor.getStandardErrorFromCommand());
					    System.out.println("\t *****************************************************");
					}
				}
			}
		}
	    //delete tmp files
		if (deleteTempFile) {//files that always exist no matter whether photo has sift detected
			General.Assert(new File(pgmPath).delete(), "err! fail to delete pgmPath:"+pgmPath);
			General.Assert(new File(siftPath).delete(), "err! fail to delete siftPath:"+siftPath);
		}

		return feats;
	} 
	
	/**
	 * INRIA2_compute_descriptors_linux64
	 * Options:
	     -harlap - harris-laplace detector
	     -heslap - hessian-laplace detector
	     -haraff - harris-affine detector
	     -hesaff - hessian-affine detector
	     -harhes - harris-hessian-laplace detector
	     -dense xstep ystep nbscales - dense detector
	     -sedgelap - edge-laplace detector
	     -jla  - steerable filters,  similarity=
	     -sift - sift [D. Lowe],  similarity=
	     -siftnonorm - sift [D. Lowe],  without normalization
	     -msift - Mahalanobis sift, similarity=
	     -gloh - extended sift,  similarity=
	     -mom  - moments,  similarity=
	     -koen - differential invariants,  similarity=
	     -cf   - complex filters [F. Schaffalitzky],  similarity=
	     -sc   - shape context,  similarity=45000
	     -spin - spin,  similarity=
	     -gpca - gradient pca [Y. Ke],  similarity=
	     -cc - cross correlation,  similarity=
	     -i image.pgm  - input image pgm, ppm, png
	     -i2 image.jpg - input of any format of ImageMagick (WARN: uses only green channel)
	     -pca input.basis - projects the descriptors with pca basis
	     -p1 image.pgm.points - input regions format 1
	     -p2 image.pgm.points - input regions format 2
	     -o1 out.desc - saves descriptors in out.desc output format1
	     -o2 out.desc - saves descriptors in out.desc output format2
	     -o3 out.siftbin - saves descriptors in binary format
	     -o4 out.siftgeo - binary descriptor format used by Bigimbaz
	     -coord out.coord - saves coordinates in binary format
	     -noangle - computes rotation variant descriptors (no rotation esimation)
	     -DC - draws regions as circles in out.desc.png
	     -DR - draws regions as ellipses in out.desc.png
	     -c 255 - draws points in grayvalue [0,...,255]
	     -thres - threshod value
	     -max - maximum for the number of computed descriptors in HESSAFF
	example:
	     ./INRIA2_compute_descriptors_linux64 -jla -i image.png -p1 image.png.points -DR
	               ./INRIA2_compute_descriptors_linux64-harlap -gloh -i image.png  -DR

		 file format 1:
		vector_dimension
		nb_of_descriptors
		x y a b c desc_1 desc_2 ......desc_vector_dimension
		--------------------
		
		where a(x-u)(x-u)+2b(x-u)(y-v)+c(y-v)(y-v)=1
		
		 file format 2:
		vector_dimension
		nb_of_descriptors
		x y cornerness scale angle object_index  point_type laplacian extremum_type mi11 mi12 mi21 mi22 desc_1 ...... desc_vector_dimension
		--------------------
		
		distance=(descA_1-descB_1)^2+...+(descA_vector_dimension-descB_vector_dimension)^2
		
		 input.basis format:
		nb_of_dimensions
		mean_v1
		mean_v2
		.
		.
		mean_v_nb_of_dim
		nb_of_dimensions*nb_of_pca_vectors
		pca_vector_v1
		pca_vector_v2
		.
		.
	 */
	public static double[][] computeUPRightSIFT_binaryPerdoch(String imgMark, BufferedImage colorImage, String binaryPath_DetDes, String tempFilesPath, String tmpFileMarker, boolean showRunBinaryInfo, boolean isFeat, boolean deleteTempFile, ArrayList<SURFpoint> interestPoints) throws IOException, InterruptedException{
		/**
		 *extract up right sift Efficient Representation of Local Geometry for Large Scale Object Retrieval
		 */
		//********** prepare for run binary **********
		String ppmPath=tempFilesPath+tmpFileMarker+".ppm";
		String siftPath=tempFilesPath+tmpFileMarker+".hesaff.sift";
		saveBufImg_to_PPM(colorImage, ppmPath);
		//********** call sift-extractor binary *************//
	    double[][] feats=null;
    	//for save location of interestPoints
		boolean saveIOPoints=false;
		if (interestPoints!=null) {
			interestPoints.clear();
			saveIOPoints=true;
		}
	    // ./haff_cvpr09 image.ppm
		List<String> commands = Arrays.asList(binaryPath_DetDes, ppmPath); // build the system command we want to run	    
	    // execute the command
		MySystemCommandExecutor commandExecutor = new MySystemCommandExecutor(commands,true,false);
		commandExecutor.executeCommand(showRunBinaryInfo,"\t","s",null);
		//********** read output file from binary *************//
		if (!new File(siftPath).exists()) {// interest point dose not exist!
			System.out.println("\t no sift result file for this photo! imgMark:"+imgMark);
			System.out.println("\t exist output files for ppm? "+new File(ppmPath).exists());
			System.out.println("\t ********* Computing SIFT binary STDOUT: *************** \n"+commandExecutor.getStandardOutputFromCommand());
		    System.out.println("\t ********* Computing SIFT binary STDERR: *************** \n"+commandExecutor.getStandardErrorFromCommand());
		    System.out.println("\t *****************************************************");
		}else {
			if (showRunBinaryInfo) {
				System.out.println("\t detecting sift ");
				System.out.println("\t ********* Computing SIFT binary STDOUT: ***************");
			    System.out.println("\t "+commandExecutor.getStandardOutputFromCommand());
			    System.out.println("\t ********* Computing SIFT binary STDERR: ***************");
			    System.out.println("\t "+commandExecutor.getStandardErrorFromCommand());
			    System.out.println("\t *****************************************************");
			}
			String line1;
			BufferedReader siftFile_content= new BufferedReader(new InputStreamReader(new FileInputStream(siftPath), "UTF-8"));
			/*
			 * file format: 
			 		vector_dimension
				 	nb_of_descriptors
				 	x y cornerness scale/patch_size angle object_index  point_type laplacian_value extremum_type mi11 mi12 mi21 mi22 desc_1 ...... desc_vector_dimension
			 */
			String featDim=siftFile_content.readLine(); int pointNum=Integer.valueOf(siftFile_content.readLine()); 
			if (pointNum>0) {
				feats=new double[pointNum][128]; int point_ind=0;
				if (saveIOPoints) {
					while ((line1=siftFile_content.readLine())!=null) {
						//save feat
						String[] lineInfo=line1.split(" ");
						General.Assert(lineInfo.length==141, 
								"err! lineInfo.length should ==141, but "+lineInfo.length+", featDim:"+featDim+"\n"
								+"this line:"+line1+"\n"
								+"\t Computing SIFT binary STDOUT:"+commandExecutor.getStandardOutputFromCommand()+"\n"
								+"\t Computing SIFT binary STDERR:"+commandExecutor.getStandardErrorFromCommand());
						feats[point_ind]=General.StrArrToDouArr(General.selectArrStr(lineInfo, null, 0, 128));
						point_ind++;
						//add point geometric
						interestPoints.add(new SURFpoint(Float.valueOf(lineInfo[0]).shortValue(), Float.valueOf(lineInfo[1]).shortValue(), Float.valueOf(lineInfo[3]), Float.valueOf(lineInfo[4])));
					}
				}else {
					while ((line1=siftFile_content.readLine())!=null) {
						//save feat
						String[] lineInfo=line1.split(" ");
						General.Assert(lineInfo.length==141, 
								"err! lineInfo.length should ==141, but "+lineInfo.length+", featDim:"+featDim+"\n"
								+"this line:"+line1+"\n"
								+"\t Computing SIFT binary STDOUT:"+commandExecutor.getStandardOutputFromCommand()+"\n"
								+"\t Computing SIFT binary STDERR:"+commandExecutor.getStandardErrorFromCommand());
						feats[point_ind]=General.StrArrToDouArr(General.selectArrStr(lineInfo, null, 0, 128));
						point_ind++;
					}
				}
				General.Assert(point_ind==pointNum, "err! point_ind should ==pointNum, but point_ind:"+point_ind+", pointNum:"+pointNum);
			}else {//something may be wrong, because if no sift point detected, the binary will NOT generate .sift file
				System.err.println("photo has sift file result, but no interest point!!  ");
				System.out.println("\t Computing SIFT binary STDOUT:"+commandExecutor.getStandardOutputFromCommand());
			    System.out.println("\t Computing SIFT binary STDERR:"+commandExecutor.getStandardErrorFromCommand());
			}
			siftFile_content.close();
		}
	    //delete tmp files
		if (deleteTempFile) {//files that always exist no matter whether photo has sift detected
			General.Assert(new File(ppmPath).delete(), "err! fail to delete ppmPath:"+ppmPath);
			if (new File(siftPath).exists()) {
				General.Assert(new File(siftPath).delete(), "err! fail to delete siftPath:"+siftPath);
			}
		}

		return feats;
	}
	
	public static int getVideoFrames_binaryFFMPEG(String videoPath, String binaryPath, String resFolder, String param, boolean showRunBinaryInfo, boolean deleteTempFile) throws IOException, InterruptedException{
		/**
		 * 
		 * param: -vf fps=fps=1 %d.png
		 */
		
		//********** prepare for run binary **********
		General.makeORdelectFolder(resFolder);
		//********** call sift-extractor binary *************//
		String[] params=param.split(" ");
	    // ./ffmpeg -i input.flv -vf fps=fps=1 %d.png
		List<String> commands = Arrays.asList(binaryPath, "-i", videoPath, params[0], params[1], resFolder+params[2]); // build the system command we want to run	    
	    // execute the command
		MySystemCommandExecutor commandExecutor = new MySystemCommandExecutor(commands,true,false);
		commandExecutor.executeCommand(showRunBinaryInfo,"\t ", "min",null);
		//********** read output file from binary *************//
		int frameNum=new File(resFolder).listFiles().length;
		if (showRunBinaryInfo) {
			System.out.println("\t done for extarct video frames from "+videoPath+", frameNum:"+frameNum);
			System.out.println("\t ********* extract video frames from FFMPEG binary STDOUT: ***************");
		    System.out.println("\t "+commandExecutor.getStandardOutputFromCommand());
		    System.out.println("\t ********* extract video frames from FFMPEG binary STDERR: ***************");
		    System.out.println("\t "+commandExecutor.getStandardErrorFromCommand());
		    System.out.println("\t *****************************************************");
		}
	    //delete tmp files
		if (deleteTempFile) {//files that always exist no matter whether photo has sift detected
			General.deleteAll(new File(resFolder));
		}
		return frameNum;
	}
	
	public static void computeRootSIFT_fromSIFT(double[][] rawSIFT, boolean checkValue){//2012_CVPR_three things everyone should know to improve object retrieval
		if (rawSIFT!=null) {
			if (checkValue) {//check all value in rawSIFT is >=0
				for (double[] oneVector : rawSIFT) {
					for (double d : oneVector) {
						General.Assert(d>=0, "err! this feat <0! d=="+d+", in vector:"+General.douArrToString(oneVector, "_", "0.0"));
					}
				}
			}
			//l1 normailse the sift vecotor, and square root each element: this is equavelent to: square root each element and then L2 normalise
			for (double[] oneVector : rawSIFT){
				double sum=General.sum_DouArr(oneVector);
				for(int i=0;i<oneVector.length;i++){
					oneVector[i]=Math.sqrt(oneVector[i]/sum);
				}
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static <T extends ImageSingleBand> MultiSpectral<T> loadColorImage_BoofCV(BufferedImage img_buffer, Class<T> imageType ) throws IOException {
		//toUse: MultiSpectral<ImageFloat32> img=BoofCV_loadImage(img_buffer,ImageFloat32.class);
		if( img_buffer == null )
			return null;
		// Convert input image into a BoofCV RGB image
		MultiSpectral<T> rgb= ConvertBufferedImage.convertFromMulti(img_buffer, (MultiSpectral<T>) null, imageType);
		ConvertBufferedImage.orderBandsIntoRGB(rgb,img_buffer);
		return rgb;
	}
	
	public static ImageUInt8 rgbToGray_BoofCV(BufferedImage img_buffer) throws IOException {
		//toUse: ImageUInt8 img_gray = rgbToGray_BoofCV(img_buffer)
		MultiSpectral<ImageFloat32> rgb=loadColorImage_BoofCV(img_buffer,ImageFloat32.class);
		ImageUInt8 gray = new ImageUInt8(rgb.width,rgb.height);
		for (int i = 0; i < gray.data.length; i++) {// [0.298936021293776;0.587043074451121;0.114020904255103]
			gray.data[i]=(byte) (rgb.bands[0].data[i]*0.2989+rgb.bands[1].data[i]*0.587+rgb.bands[2].data[i]*0.114);
		}
		return gray;
	}
	
	public static ImageFloat32 rgbToGray_BoofCV_ImageFloat32(BufferedImage img_buffer) throws IOException {
		//toUse: ImageFloat32 img_gray = rgbToGray_BoofCV_ImageFloat32(img_buffer)
		if (img_buffer.getRaster().getNumBands()!=1) {
			BufferedImage resImage=img_buffer;
			if (img_buffer.getRaster().getNumBands()==4) {//if numBands==4, then it contain a alpha channel
				resImage=convertTo3BandColorBufferedImage(img_buffer);
			}
			MultiSpectral<ImageFloat32> rgb=loadColorImage_BoofCV(resImage,ImageFloat32.class);
			ImageFloat32 gray = new ImageFloat32(rgb.width,rgb.height);
			for (int i = 0; i < gray.data.length; i++) {// [0.298936021293776;0.587043074451121;0.114020904255103]
				gray.data[i]=(float) (rgb.bands[0].data[i]*0.2989+rgb.bands[1].data[i]*0.587+rgb.bands[2].data[i]*0.114);
			}
			return gray;
		}else {
			ImageFloat32 gray=BoofCV_loadImage(img_buffer,ImageFloat32.class);
			return gray;
		}
	}
	
	public static void normliseIntensity(ImageFloat32 img, float lowerPercent, float upperPercent) throws InterruptedException{//normliseIntensity to 0~255
		img.data=General.scaleValue(img.data, 0, 255, lowerPercent, upperPercent);
	}
	
	public static Complex[][] FFT2_JTansform(float[][] data, FloatFFT_2D fft2) throws InterruptedException{//FFT in JTansform. data is real img
		/*
		 * Computes 2D forward DFT of real data leaving the result in a . This method computes full real forward transform, 
		 * i.e. you will get the same result as from complexForward called with all imaginary part equal 0. Because the result is stored in a, 
		 * the input array must be of size rows by 2*columns, with only the first rows by columns elements filled with real data. To get back the original data, use complexInverse on the output of this method.
		 */
		float[][] fullData=new float[data.length][2*data[0].length];//save real and imagnery part sequentially
		for (int i = 0; i < data.length; i++) {
			for (int j = 0; j < data[0].length; j++) {
				fullData[i][j]=data[i][j];
			}
		}
		if (fft2==null) {
			fft2=new FloatFFT_2D(fullData.length, fullData[0].length/2);
		}
		fft2.realForwardFull(fullData);
		return dataArrToComplexArr_JTransform(fullData,"complex");
	}
	
	public static Complex[][] FFT2_Inverse_JTansform(Complex[][] data, FloatFFT_2D fft2) throws InterruptedException{//iFFT in JTansform
		float[][] temp=ComplexArrToDataArr_JTransform(data, "complex");
		if (fft2==null) {
			fft2=new FloatFFT_2D(temp.length, temp[0].length/2);
		}
		fft2.complexInverse(temp, true);//scale shoud set to true, otherwise overfloating
		return dataArrToComplexArr_JTransform(temp,"complex");
	}
	
	public static Complex[][] dataArrToComplexArr_JTransform(float[][] data, String dataModel) throws InterruptedException {
		if (dataModel.equalsIgnoreCase("complex")) {//data is fullData, save real and imagnery part sequentially, fullData[i][2*j]=real, fullData[i][2*j+1]=imagenery
			int rowNum=data.length; int colomnNum=data[0].length/2;
			Complex[][] complexs=new Complex[rowNum][colomnNum];
			for (int i = 0; i < rowNum; i++) {
				for (int j = 0; j < colomnNum; j++) {
					complexs[i][j]=new Complex(data[i][2*j], data[i][2*j+1]);
				}
			}
			return complexs;
		}else if (dataModel.equalsIgnoreCase("real")){//data is only real data, image
			int rowNum=data.length; int colomnNum=data[0].length;
			Complex[][] complexs=new Complex[rowNum][colomnNum];
			for (int i = 0; i < rowNum; i++) {
				for (int j = 0; j < colomnNum; j++) {
					complexs[i][j]=new Complex(data[i][j], 0);
				}
			}
			return complexs;
		}else {
			throw new InterruptedException("err in dataArrToComplexArr_JTransform, dataModel should be complex or real, here:"+dataModel);
		}
	}
	
	public static float[][] ComplexArrToDataArr_JTransform(Complex[][] complexs, String dataModel) throws InterruptedException {
		if (dataModel.equalsIgnoreCase("complex")) {//fullData, save real and imagnery part sequentially, fullData[i][2*j]=real, fullData[i][2*j+1]=imagenery
			int rowNum=complexs.length; int colomnNum=complexs[0].length;
			float[][] data=new float[rowNum][colomnNum*2];
			for (int i = 0; i < rowNum; i++) {
				for (int j = 0; j < colomnNum; j++) {
					data[i][2*j]=(float) complexs[i][j].getReal();
					data[i][2*j+1]=(float) complexs[i][j].getImaginary();
				}
			}
			return data;
		}else if (dataModel.equalsIgnoreCase("real")){//only select real part
			int rowNum=complexs.length; int colomnNum=complexs[0].length;
			float[][] data=new float[rowNum][colomnNum];
			for (int i = 0; i < rowNum; i++) {
				for (int j = 0; j < colomnNum; j++) {
					data[i][j]=(float) complexs[i][j].getReal();
				}
			}
			return data;
		}else {
			throw new InterruptedException("err in ComplexArrToDataArr_JTransform, dataModel should be complex or real, here:"+dataModel);
		}
	}
	
	public static float[][][] creatGabor(int[] numberOfOrientationsPerScale, int[] imageSize){
		/*
		 G = createGabor(numberOfOrientationsPerScale, n);
		 Precomputes filter transfer functions. All computations are done on the Fourier domain. 
		 Input
		     numberOfOrientationsPerScale = vector that contains the number of orientations at each scale (from HF to BF)
		     n = imagesize = [nrows ncols] 
		 output
		     G = transfer functions for a jet of gabor filters
		 */
		int Nscales = numberOfOrientationsPerScale.length;
		int Nfilters = General.sum_IntArr(numberOfOrientationsPerScale);

		if(imageSize.length == 1)
			imageSize =new int[]{imageSize[0],imageSize[0]};

		int l=0; float[][] param=new float[Nfilters][];
		for(int i=0;i<Nscales;i++){
			for(int j=0;j<numberOfOrientationsPerScale[i];j++){
		        param[l]=new float[]{(float) 0.35,(float) (0.3/(Math.pow(1.85, i))),(float) (16*Math.pow(numberOfOrientationsPerScale[i],2)/Math.pow(32,2)),(float) (Math.PI/(numberOfOrientationsPerScale[i])*j)};
		        l=l+1;
			}
		}

		//********   Frequencies  **********
		float[] fx_rang=General.makeRange(new float[]{-(float)imageSize[1]/2,(float)imageSize[1]/2-1,1});
		float[] fy_rang=General.makeRange(new float[]{-(float)imageSize[0]/2,(float)imageSize[0]/2-1,1});
		int fxNum=fx_rang.length; int fyNum=fy_rang.length;
		//sqrt
		float[][] sqrt=new float[fyNum][fxNum];
		for (int i = 0; i < fyNum; i++) {
			for (int j = 0; j < fxNum; j++) {
				sqrt[i][j]=(float) Math.sqrt(Math.pow(fx_rang[j],2)+Math.pow(fy_rang[i],2));
			}
		}
		//angle
		float[][] angle=new float[fyNum][fxNum];
		for (int i = 0; i < fyNum; i++) {
			for (int j = 0; j < fxNum; j++) {
				angle[i][j]=(float) Math.atan2(fy_rang[i],fx_rang[j]);
			}
		}
		//fftshift
		float[][] fr=fftshift(sqrt);
		float[][] t=fftshift(angle);
		
		//********   Transfer functions  **********
		float[][][] G=new float[Nfilters][imageSize[0]][imageSize[1]];
		float PI2=(float) (2*Math.PI);
		for (int i = 0; i < Nfilters; i++) {
			float[][] tr=General.elementAdd(t, param[i][3]);
			tr=General.elementAdd(tr, General.elementMut(General.thresholdingMatrix_Inverse(tr, (float) -Math.PI), PI2));
			tr=General.elementAdd(tr, General.elementMut(General.thresholdingMatrix(tr, (float) Math.PI), -PI2));
			G[i]=General.elementMut(General.elementPower(General.elementAdd(General.elementMut(fr, (float)1/imageSize[1]/param[i][1]),-1), 2), -10*param[i][0]);
			G[i]=General.elementAdd(G[i], General.elementMut(General.elementPower(tr, 2),-PI2*param[i][2]));
			G[i]=General.elementExp(G[i]);
		}
		return G;
	}
	
	public static float[] LMGist(BufferedImage oriImage, GistParam gistParam) throws IOException, InterruptedException {
		//scale image size
		if(oriImage.getHeight()!=gistParam.imgSize[0] || oriImage.getWidth()!=gistParam.imgSize[1]){
			if (oriImage.getHeight()>gistParam.imgSize[0] && oriImage.getWidth()>gistParam.imgSize[1]) {//height and weight all downScaling
				oriImage=General.getScaledInstance(oriImage, gistParam.imgSize[0], gistParam.imgSize[1], RenderingHints.VALUE_INTERPOLATION_BILINEAR, true);//only for downScaling! target sizes(height and weight) are all smaller than ori size. 
			}else {//upScaling
				oriImage=General.getScaledInstance(oriImage, gistParam.imgSize[0], gistParam.imgSize[1], RenderingHints.VALUE_INTERPOLATION_BILINEAR, false);//can be used for all!
			}
		}
		//rgb to YUV's YCompont
		float[][] YCompont= rgbToGray(oriImage);
		// scale intensities to be in the range [0 255]
		int[][] minMaxInd=General.getMinMax_ind(YCompont);
		float[] minMax=new float[]{YCompont[minMaxInd[0][0]][minMaxInd[0][1]],YCompont[minMaxInd[1][0]][minMaxInd[1][1]]};
		float max_min=minMax[1]-minMax[0];
		for (int i = 0; i < YCompont.length; i++) {
			for (int j = 0; j < YCompont[i].length; j++) {
				YCompont[i][j]=General.scaleValue(YCompont[i][j], minMax[0], max_min, 0,255);
			}
		}
		//prefiltering: local contrast scaling
//		long startTime=System.currentTimeMillis();
		float[][] output= gist_prefilt(YCompont, gistParam.fc_prefilt);
//		dispMatrixAsImg(output, "gist_prefilt_output");
//		System.out.println("gist_prefilt:"+General.dispTime(System.currentTimeMillis()-startTime, "s"));
		// get gist:
//		startTime=System.currentTimeMillis();
		float[] gist = gist_Gabor(output, gistParam);
//		System.out.println("gist_Gabor:"+General.dispTime(System.currentTimeMillis()-startTime, "s"));
		return gist;
	}
	
	public static float[][] gist_prefilt(float[][] img, int fc) throws InterruptedException {
		/*
			% ima = prefilt(img, fc);
			% fc  = 4 (default)
			% 
			% Input images are double in the range [0, 255];
			% You can also input a block of images [ncols nrows 3 Nimages]
			%
			% For color images, normalization is done by dividing by the local
			% luminance variance.
		 */
		int w=5;
		float s1=(float) ((float)fc/Math.sqrt(Math.log(2)));
		//Pad images to reduce boundary artifacts
		img=General.elementLog(General.elementAdd(img, 1));
		img=General.padArr_symmetric(img, new int[]{w,w},"both");
		int sn=img.length, sm=img[0].length;
		int n=Math.max(sn, sm);
		n+=General.mod(n, 2);
		img=General.padArr_symmetric(img, new int[]{n-sn,n-sm}, "post");
		//Filter
		int[] fx_rang=General.makeRange(new int[]{-n/2,n/2-1,1});
		int[] fy_rang=fx_rang;
		int fxNum=fx_rang.length; int fyNum=fy_rang.length;
		float[][] gf=new float[fyNum][fxNum];
		for (int i = 0; i < fyNum; i++) {
			for (int j = 0; j < fxNum; j++) {
				gf[i][j]=(float) Math.exp(-(Math.pow(fx_rang[j],2)+Math.pow(fy_rang[i],2))/Math.pow(s1, 2));
			}
		}
		gf=fftshift(gf);
		//************* Whitening ****************
//		//0--- use ScientificTool's java wraper for FFWT
//		ComplexArray gf_ComplexArray=dataArrToComplexArr(gf);
//		ComplexArray img_ComplexArray=dataArrToComplexArr(img);		
//		RealArray output=  img_ComplexArray.eSub(img_ComplexArray.fft().eMul(gf_ComplexArray).ifft()).torRe();
		//1--- use pure java, multithreaded, JTansform
		FloatFFT_2D fft2 = new FloatFFT_2D(img.length, img[0].length);
		Complex[][] temp= General.elementMut(FFT2_JTansform(img,fft2),gf);
		Complex[][] itemp=FFT2_Inverse_JTansform(temp,fft2);
		float[][] output=General.elementSub(img, ComplexArrToDataArr_JTransform(itemp, "real"));
		
		//************ Local contrast normalization  ****************
//		//0--- use ScientificTool's java wraper for FFWT
//		RealArray localstd= output.eMul(output).tocRe().fft().eMul(gf_ComplexArray).ifft().torAbs();
//		General.elementPower(localstd.values(),0.5);
//		General.elementAdd(localstd.values(),0.2);
//		output=output.eDiv(localstd);
//		float[][] res=General.arrToArrArr(General.DouArrToFloatArr(output.values()), "rowFirst", img.length, img[0].length);
		//1--- use pure java, multithreaded, JTansform
		float[][] localstd=General.elementABS(FFT2_Inverse_JTansform(General.elementMut(FFT2_JTansform(General.elementPower(output, 2),fft2),gf),fft2));
		localstd=General.elementPower(localstd,(float) 0.5);
		localstd=General.elementAdd(localstd, (float)0.2);
		float[][] res=General.elementDiv(output, localstd); 
		
		// Crop output to have same size than the input
		return General.selectArrFloat(res, new int[][]{General.makeRange(new int[]{w,sn-w-1,1}),General.makeRange(new int[]{w,sm-w-1,1})});
	}
	
	public static float[] gist_Gabor(float[][] img, GistParam gistParam) throws InterruptedException {
		/*
		% 
		% Input:
		%   img = input image (it can be a block: [nrows, ncols, c, Nimages])
		%   param.w = number of windows (w*w)
		%   param.G = precomputed transfer functions
		%
		% Output:
		%   g: are the global features = [Nfeatures Nimages], 
		%                    Nfeatures = w*w*Nfilters*c
		*/
		int w=gistParam.numberBlocks;
		float[][][] G=gistParam.Gabor;
		int be=gistParam.boundaryExtension;
		int ny=G[0].length; int nx=G[0][0].length; int Nfilters=G.length;
		int W=w*w;
		// pad image
		img=General.padArr_symmetric(img, new int[]{be,be},"both");
		// comput gist
		float[] gist=new float[W*Nfilters];
		
//		//0--- use ScientificTool's java wraper for FFWT
//		ComplexArray img_fft=dataArrToComplexArr(img).fft();
		//1--- use pure java, multithreaded, JTansform
		FloatFFT_2D fft2 = new FloatFFT_2D(img.length, img[0].length);
		Complex[][] img_fft=FFT2_JTansform(img,fft2);
		
		int k=0;
		for (int i = 0; i < Nfilters; i++) {
//			//0--- use ScientificTool's java wraper for FFWT
//			RealArray temp=img_fft.eMul(dataArrToComplexArr(G[i])).ifft().torAbs();
//			float[][] ig=General.arrToArrArr(General.DouArrToFloatArr(temp.values()), "rowFirst", img.length, img[0].length);
			//1--- use pure java, multithreaded, JTansform
			float[][] ig= General.elementABS(General_BoofCV.FFT2_Inverse_JTansform(General.elementMut(img_fft, G[i]), fft2)); 
			ig=General.selectArrFloat(ig, new int[][]{General.makeRange(new int[]{be,ny-be-1,1}),General.makeRange(new int[]{be,nx-be-1,1})});
			//common
			float[][] v = downN(ig, w);
			float[] v_oneArr=General.arrArrToArr(v, "colomnFirst");
			General.assignArrValue(gist, General.makeRange(new int[]{k,k+W-1,1}), v_oneArr);
			k+=W;
		}
		
		return gist;
	}
	
	public static BufferedImage getSubImage(BufferedImage oriImage, int x, int y, int w, int h, Disp disp){
		//as BufferedImage.getSubimage in Java would make two image share the same dataArray, so for some APP, like BoofCV, when transfer from this sub-BufferedImage into other formate, it can be a bug!!
		//so creat a new BufferedImage with new dataArray!
		BufferedImage resImg=oriImage.getSubimage(x,y,w,h);
		resImg=convertTo3BandColorBufferedImage(resImg);
		String Info="info in getSubImage, oriSize width_height:"+oriImage.getWidth()+"_"+oriImage.getHeight()+", it has bounding box: "+x+"_"+y+"_"+w+"_"+h;
		disp.disp(Info+", after box-cut, its Size width_height:"+resImg.getWidth()+"_"+resImg.getHeight());
		return resImg;
	}
	
	public static float[][] downN(float[][] data, int N) throws InterruptedException{
		/*
		% 
		% averaging over non-overlapping square image blocks
		%
		% Input
		%   x = [nrows ncols nchanels]
		% Output
		%   y = [N N nchanels]
		*/
		int[] nx=General.elementFloor(General.makeLinearSpace(0, data.length, N+1));
		int[] ny=General.elementFloor(General.makeLinearSpace(0, data[0].length, N+1));
		float[][] y=new float[N][N];
		for (int xx = 0; xx < N; xx++) {
			for (int yy = 0; yy < N; yy++) {
				float[][] temp=General.selectArrFloat(data, new int[][]{General.makeRange(new int[]{nx[xx],nx[xx+1]-1,1}),General.makeRange(new int[]{ny[yy],ny[yy+1]-1,1})});
				y[xx][yy]=General.get_Sum(temp)/temp.length/temp[0].length;
			}
		}
		return y;
	}
	
	public static float[][] fftshift(float[][] ori){
		int iLength=ori.length; int jLength=ori[0].length;
		int ip=iLength/2; int jp=jLength/2;
		int[] iInds=General.concateArr(new int[][]{General.makeRange(new int[]{ip,iLength-1,1}),General.makeRange(new int[]{0,ip-1,1})});
		int[] jInds=General.concateArr(new int[][]{General.makeRange(new int[]{jp,jLength-1,1}),General.makeRange(new int[]{0,jp-1,1})});
		float[][] res=new float[iLength][jLength];
		for (int i = 0; i < iInds.length; i++) {
			for (int j = 0; j < jInds.length; j++) {
				res[i][j]=ori[iInds[i]][jInds[j]];
			}
		}
		return res;
	}
	
	public static float[][] rgbToGray(BufferedImage img_buffer) throws IOException {
		//toUse: float[][] img_gray = rgbToGray(img_buffer)
		MultiSpectral<ImageFloat32> rgb=loadColorImage_BoofCV(img_buffer,ImageFloat32.class);
		float[][] YComponent = new float[rgb.height][rgb.width];
		for (int i = 0; i < rgb.height; i++) {
			for (int j = 0; j < rgb.width; j++) {// [0.298936021293776;0.587043074451121;0.114020904255103]
				YComponent[i][j]=(float) (rgb.bands[0].get(j, i)*0.2989+rgb.bands[1].get(j, i)*0.587+rgb.bands[2].get(j, i)*0.114);
			}
		}
		return YComponent;
	}
	
	public static BufferedImage rgbToGray_ImageIO(BufferedImage colorImg) throws IOException {
		BufferedImage image = new BufferedImage(colorImg.getWidth(), colorImg.getHeight(),  
			    BufferedImage.TYPE_BYTE_GRAY);  
		Graphics g = image.getGraphics();  
		g.drawImage(colorImg, 0, 0, null);  
		g.dispose();  
		return image;
	}
	
	public static void saveImageUInt8ToPGM_BoofCV(ImageUInt8 img, String pgmFilePath) throws IOException {
		//toUse: saveImageUInt8ToPGM_BoofCV(img,pgmFilePath);
		DataOutputStream out = new DataOutputStream(new FileOutputStream(pgmFilePath));
		//write data head
		out.writeBytes("P5\n"+img.width+" "+img.height+"\n255\n");
		//write img data
		for(byte onePixel:img.data){//img.data is in row-major 1-D array
			out.write(onePixel);
		}
		out.close();
	}
	
	public static void saveBufImg_to_PPM(BufferedImage colorImage, String pgmFilePath) throws IOException{
		BufferedImage img=convertTo3BandColorBufferedImage(colorImage);
		MultiSpectral<ImageUInt8> img_MultiSp = ConvertBufferedImage.convertFromMulti(img,null,ImageUInt8.class);
		//toUse: saveImageUInt8ToPGM_BoofCV(img,pgmFilePath);
		DataOutputStream out = new DataOutputStream(new FileOutputStream(pgmFilePath));
		//write data head
		out.writeBytes("P6\n"+img_MultiSp.width+" "+img_MultiSp.height+"\n255\n");
		//write img data
		out.write(getBytesFromMultiSpectral(img_MultiSp));
		out.close();
	}
	
	public static byte[] getBytesFromMultiSpectral(MultiSpectral<ImageUInt8> img){
		byte[] data = new byte[img.width*img.height*3];
		ImageUInt8 band0 = img.getBand(0);
		ImageUInt8 band1 = img.getBand(1);
		ImageUInt8 band2 = img.getBand(2);
		int indexOut = 0;
		for( int y = 0; y < img.height; y++ ) {
			int index = img.startIndex + y*img.stride;
			for( int x = 0; x < img.width; x++ , index++) {
				data[indexOut++] = band0.data[index];
				data[indexOut++] = band1.data[index];
				data[indexOut++] = band2.data[index];
			}
		}
		return data;
	}
	
	public static byte[] BufferedImageToBytes(BufferedImage img_buffer, String format) throws IOException {//format: jpg,png,bmp
		//transfer BufferedImage to byte[]
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ImageIO.write(convertTo3BandColorBufferedImage(img_buffer), format, baos);
		baos.flush();
		byte[] imageInByte = baos.toByteArray();
		baos.close();
		return imageInByte;
	}
	
	public static BufferedImage BytesToBufferedImage(byte[] img_bytes, String photoInfo, Disp disp)  {
		// convert byte array back to BufferedImage
		try{
			BufferedImage img=null;
			try{
				img= ImageIO.read(new ByteArrayInputStream(img_bytes));
				if (img==null) {
					disp.disp("no sucess and noException for ImageIO, return null for "+photoInfo);
				}
			}catch(CMMException e){
				disp.disp("CMMException when 'BytesToBufferedImage() use ImageIO.read(new ByteArrayInputStream(img_bytes))' for "+photoInfo);
				disp.disp("try JAI.create(\"stream\", new ByteArraySeekableStream(img_bytes)).getAsBufferedImage()");
				img= General_JAI.BytesToBufferedImage_JAI(img_bytes);
				if (img==null) {
					disp.disp("no sucess for JAI");
				}else {
					disp.disp("sucess for JAI");
				}
			}
			if (img!=null) {
				img=getUniOrintaionImg(img, null, new BufferedInputStream(new ByteArrayInputStream(img_bytes)), Disp.getNotDisp());
			}
			return img;
		}catch(Exception e){
			disp.disp("Exception when 'BytesToBufferedImage()' for "+photoInfo+", return null");
			disp.disp(e);
			return null;
		}
	}
	
	public static PhotoAllFeats makePhotoAllFeats(int width, int height, double[][] queryFeat, SURFpoint[] interestPoints, float[][] centers, DenseMatrix64F pMatrix, 
			float[][] HEThreshold, ArrayList<HashSet<Integer>> node_vw_links, float[][] nodes, int vws_NN, double alph_NNDist, double deta) throws IOException, InterruptedException{
		if (queryFeat!=null) {
			int HESigLength=HEThreshold[0].length/8;
			LinkedList<SURFpointVWs> pointVWs=new LinkedList<SURFpointVWs>(); 
			HashMap<Integer, SumFeat_Num> vw_projectFeat=new HashMap<>();
			for(short i=0;i<queryFeat.length;i++){//process one point
				LinkedList<VW_Weight_HESig> onePointVWs=new LinkedList<VW_Weight_HESig>();
				LinkedList<Int_Float> vws=General.assignFeatToCenter_fastGANN_MultiAss(queryFeat[i], centers, nodes,  node_vw_links,alph_NNDist,vws_NN,deta);//visual word index from 0
//				ArrayList<Integer> vws=General.assignFeatToCenter_fastGANN_MultiAss(photoFeat[i], centers, nodes,  node_vw_links,alph_NNDist,vws_NN,nodes_NN);//visual word index from 0
//				int centerIndex_GANN=General.assignFeatToCenter_fastGANN(photoFeat[i], centers, nodes,  node_vw_links);  //visual word index from 0
				double[] projectFeat=General_EJML.projectFeat(pMatrix, queryFeat[i]);
				for (Int_Float vw_weight : vws) {
					//add to this vw's projectFeat
					SumFeat_Num old=vw_projectFeat.get(vw_weight.integerV);
					if (old==null) {//no exist yet
						vw_projectFeat.put(vw_weight.integerV, new SumFeat_Num(projectFeat, 1));
					}else {//add
						old.addOneFeat(projectFeat);
					}
					//make HESig
					BitSet HESig=General_EJML.makeHEsignature_BitSet(projectFeat, HEThreshold[vw_weight.integerV]);
					byte[] HESig_Bytes=General.BitSettoByteArray(HESig);
					General.Assert(HESig_Bytes.length == HESigLength, "HESig to bytes error!! Bytes.length!=(HEThreshold[0].length/8) ");
					onePointVWs.add(new VW_Weight_HESig(vw_weight.integerV, vw_weight.floatV, HESig_Bytes));
				}
				//save this SURFpointVWs
				pointVWs.add(new SURFpointVWs(interestPoints[i], new VW_Weight_HESig_ShortArr(onePointVWs)));
			}
			//make HESig for the aggragated vw_projectFeat
			LinkedList<VW_AggSig> vw_aggSig=new LinkedList<>();
			for (Entry<Integer, SumFeat_Num> one_vw_projectFeat : vw_projectFeat.entrySet()) {
				float[] sumedThr=General.elementMut_returnNew(HEThreshold[one_vw_projectFeat.getKey()], one_vw_projectFeat.getValue().num);
				BitSet aggSig=General_EJML.makeHEsignature_BitSet(one_vw_projectFeat.getValue().sumFeat, sumedThr);
				byte[] aggSig_Bytes=General.BitSettoByteArray(aggSig);
				vw_aggSig.add(new VW_AggSig(one_vw_projectFeat.getKey(), aggSig_Bytes));
			}
			return new PhotoAllFeats(width, height, pointVWs, vw_aggSig);
		}else{
			return null;
		}
	}
	
	public static FeatState group_VW_HESig(HashMap<Integer,ArrayList<HESig>> VW_HESig, SURFpointVWs[] feats){
		if (feats!=null) {
			int vwsNum=0;  
			VW_HESig.clear();
			for(short i=0;i<feats.length;i++){
				for (VW_Weight_HESig assignedVWs : feats[i].vws.getArr()) {
					SURFfeat oneFeat=new SURFfeat(assignedVWs.sig, i, feats[i].point);
					//add this vw-HESURFfeat to HashMap
					if (VW_HESig.containsKey(assignedVWs.vw)){
						VW_HESig.get(assignedVWs.vw).add(oneFeat.getHESigFull());
					}else{
						ArrayList<HESig> temp= new ArrayList<HESig>();
						temp.add(oneFeat.getHESigFull());
						VW_HESig.put(assignedVWs.vw,temp);
					}
				}
				vwsNum+=feats[i].vws.getArr().length;
			}
			
			int aveNum_vwNN_one=vwsNum/feats.length; //aveNum of mult-assigned vwed for each feat
			return new FeatState(feats.length,vwsNum,aveNum_vwNN_one,VW_HESig.size()); //feat num, vw num, mutiAssNum, uniqueVW num
		}else {
			return null;
		}
	}
	
	public static FeatState group_VW_SURFfeat(HashMap<Integer,ArrayList<SURFfeat>> VW_SURFfeat, SURFpointVWs[] feats){
		if (feats!=null) {
			int vwsNum=0;  
			VW_SURFfeat.clear();
			for(short i=0;i<feats.length;i++){
				for (VW_Weight_HESig assignedVWs : feats[i].vws.getArr()) {
					SURFfeat oneFeat=new SURFfeat(assignedVWs.sig, i, feats[i].point);
					//add this vw-HESURFfeat to HashMap
					if (VW_SURFfeat.containsKey(assignedVWs.vw)){
						VW_SURFfeat.get(assignedVWs.vw).add(oneFeat);
					}else{
						ArrayList<SURFfeat> temp= new ArrayList<SURFfeat>();
						temp.add(oneFeat);
						VW_SURFfeat.put(assignedVWs.vw,temp);
					}
				}
				vwsNum+=feats[i].vws.getArr().length;
			}
			
			int aveNum_vwNN_one=vwsNum/feats.length; //aveNum of mult-assigned vwed for each feat
			return new FeatState(feats.length,vwsNum,aveNum_vwNN_one,VW_SURFfeat.size()); //feat num, vw num, mutiAssNum, uniqueVW num
		}else {
			return null;
		}
	}
	
	public static ArrayList<SURFpoint> group_InteresPoints(SURFpointVWs[] feats){
		ArrayList<SURFpoint> interestPoints=new ArrayList<SURFpoint>();
		for(short i=0;i<feats.length;i++){
			interestPoints.add(feats[i].point);
		}
		return interestPoints;
	}
		
	public static float computeBagOfVWVectorNorm(HashMap<Integer,SURFfeat_ShortArr_AggSig> VW_SURFfeat){//L2-norm: squre of Vector*(Vector')
		int normSqure=0;
		for (Entry<Integer, SURFfeat_ShortArr_AggSig> oneEnt: VW_SURFfeat.entrySet()) {
			normSqure+=Math.pow(oneEnt.getValue().feats.length,2);
		}
		return (float) Math.sqrt(normSqure);
	}
	
	public static float computeIDFBagOfVWVectorNorm(HashMap<Integer,SURFfeat_ShortArr_AggSig> VW_SURFfeat, float[] idf_square){//L2-norm: squre of Vector*(Vector')
		double normSqure=0;
		for (Entry<Integer, SURFfeat_ShortArr_AggSig> oneEnt: VW_SURFfeat.entrySet()) {
			normSqure+=(Math.pow(oneEnt.getValue().feats.length,2)*idf_square[oneEnt.getKey()]);
		}
		return (float) Math.sqrt(normSqure);
	}
	
	public static float computeIDF1VW1FeatVectorNorm(HashMap<Integer,SURFfeat_ShortArr_AggSig> VW_SURFfeat, float[] idf_square){//L2-norm: squre of Vector*(Vector')
		double normSqure=0;
		for (Entry<Integer, SURFfeat_ShortArr_AggSig> oneEnt: VW_SURFfeat.entrySet()) {
			normSqure+=idf_square[oneEnt.getKey()];
		}
		return (float) Math.sqrt(normSqure);
	}
			
	public static float makeFinalDocMatchScore(ArrayList<MatchFeat_VW>  goodMatches, float[] hammingW, 
			LinkedList<ImageRegionMatch> matches, float[] idfs, float[] matchWeight) throws InterruptedException{
		float hmScore=0; 
		//make matchScores
		if (matches==null) {//do not need matches return, so make one just for temp use
			if (matchWeight==null) {//no matcheWeight
				for (int i=0;i<goodMatches.size();i++) {//oneMatch: qi_dj_dist_vw
					MatchFeat_VW oneMatch = goodMatches.get(i);
					float matchScore=hammingW[oneMatch.HMDist]*idfs[oneMatch.vw];
					hmScore+=matchScore;
				}
			}else {
				for (int i=0;i<goodMatches.size();i++) {//oneMatch: qi_dj_dist_vw
					MatchFeat_VW oneMatch = goodMatches.get(i);
					float matchScore=hammingW[oneMatch.HMDist]*idfs[oneMatch.vw]*matchWeight[i];
					hmScore+=matchScore;
				}
			}
		}else {
			if (matchWeight==null) {//no matchWeight
				for (int i=0;i<goodMatches.size();i++) {//oneMatch: qi_dj_dist_vw
					MatchFeat_VW oneMatch = goodMatches.get(i);
					float matchScore=hammingW[oneMatch.HMDist]*idfs[oneMatch.vw];
					matches.add(new ImageRegionMatch(oneMatch.QFeatInd, oneMatch.docFeat.getFeatInd(), matchScore));
					hmScore+=matchScore;
				}
			}else {
				for (int i=0;i<goodMatches.size();i++) {//oneMatch: qi_dj_dist_vw
					MatchFeat_VW oneMatch = goodMatches.get(i);
					float matchScore=hammingW[oneMatch.HMDist]*idfs[oneMatch.vw]*matchWeight[i];
					if (matchScore>0) {
						matches.add(new ImageRegionMatch(oneMatch.QFeatInd, oneMatch.docFeat.getFeatInd(), matchScore));
					}
					hmScore+=matchScore;
				}
			}
		}
		return hmScore;
	}
	
	public static float make_Initial_DocMatchScore(ArrayList<Int_MatchFeatArr>  matches, float[] hammingW, float[] idfs) {
		/**
		 * score doc only be hmDist, no 1vs1 check, no HPM check!
		 */
		float hmScore=0; 
		//update hmScore
		for (Int_MatchFeatArr oneVWmatches:matches) {//oneMatch: qi_dj_dist_vw
			int vw=oneVWmatches.Integer;
			MatchFeat[] feats=oneVWmatches.feats.getArr();
			float oneVWScore=0;
			for (MatchFeat oneMatch : feats) {
				oneVWScore+=hammingW[oneMatch.HMDist];
			}
			hmScore+=oneVWScore*idfs[vw];
		}
		return hmScore;
	}
	
	public static ArrayList<DocAllMatchFeats> getDocsFromPreSel(TreeSet<slave_masterFloat_DES<DocAllMatchFeats>> doc_scores_order){
		ArrayList<DocAllMatchFeats> docs=new ArrayList<DocAllMatchFeats>(2*doc_scores_order.size());
		for(slave_masterFloat_DES<DocAllMatchFeats> oneDoc:doc_scores_order.descendingSet()){//initial rank
			docs.add(oneDoc.getSlave());
		}
		return docs;
	}
	
	private static DocAllMatchFeats make1vw1Match(DocAllMatchFeats iniMatches){
		LinkedList<Int_MatchFeatArr> res=new LinkedList<>();
		for (Int_MatchFeatArr oneVW : iniMatches.feats.feats) {
			//find best match
			int minDist=Integer.MAX_VALUE; MatchFeat bestMatch=null;
			for (MatchFeat matchFeat : oneVW.feats.getArr()) {
				if (minDist>matchFeat.HMDist) {
					minDist=matchFeat.HMDist;
					bestMatch=matchFeat;
				}
			}
			//only use 1 bestMatch for 1 vw
			res.add(new Int_MatchFeatArr(oneVW.Integer, new MatchFeat_Arr(new MatchFeat[]{bestMatch})));
		}
		return new DocAllMatchFeats(iniMatches.DocID, new Int_MatchFeatArr_Arr(res));
	}
	
	public static float scoreDoc_byOriHMScore(DocAllMatchFeats docsMatches, int queryID, LinkedList<ImageRegionMatch> finalMatches,
			float[] doc_BoVWVectorNorm, float[] hammingW, float[] idf_squre, 
			boolean disp) throws InterruptedException {
		/**
		 * based on the initial rank(doc_scores_order), for each doc, do 1vs1 and HPM check! make the final rank! 
		 */
		long startTime=System.currentTimeMillis();
		//rank by Ori HMScore
		int thisDocID=docsMatches.DocID;
		//make matches
		ArrayList<MatchFeat_VW> docMatches=new ArrayList<MatchFeat_VW>(2*docsMatches.feats.feats.length);
		int vwNum=0; 
		for (int i = 0; i < docsMatches.feats.feats.length; i++) {
			int vw=docsMatches.feats.feats[i].Integer;
			MatchFeat[] matchFeats=docsMatches.feats.feats[i].feats.getArr();
			for (MatchFeat oneMatchFeat: matchFeats) {
				docMatches.add(new MatchFeat_VW(oneMatchFeat, vw));
			}
			vwNum++;
		}
		//for debug
		if (disp==true) {
			System.out.println("\t show one example, queryID:"+queryID+", thisDocID:"+thisDocID+", 5 of their matches:");
			for (int i=0; i<Math.min(docMatches.size(),5);i++) {
				System.out.println("\t --   one match, "+docMatches.get(i).toString());
			}
			System.out.println("\t total matched vws number: "+vwNum+", tot match Num: "+docMatches.size());
		}
		//get final matchScore for this doc
		float matchScore=General_BoofCV.makeFinalDocMatchScore(docMatches, hammingW, finalMatches, idf_squre, null);
		float normal=doc_BoVWVectorNorm[thisDocID]; //normlize by BoVWVectorNorm
		float finalScore=matchScore/normal;
		//********* debug disp info **********//
		if (disp==true){ 
			System.out.println("\t no-match selection and HPM check, only use ori-HMScore to rank" );
			System.out.println("\t final matchScore for this doc:"+matchScore+", this is normalised by BoVWVectorNorm:"+normal+", finalScore: "+finalScore);
			System.out.println("\t time:"+General.dispTime(System.currentTimeMillis()-startTime, "ms") );
			disp=false; //only disp once!
		}
		return finalScore;	
	}
	
	public static float scoreDoc_by1vs1(DocAllMatchFeats docMatches, int queryID, LinkedList<ImageRegionMatch> showMatches,
			float[] doc_BoVWVectorNorm, float[] hammingW, float[] idf_squre, boolean isUseHMDistFor1Vs1, boolean is1vw1match,
			MatchingInfo matchingInfo, boolean disp) throws InterruptedException {
		/**
		 * based on the initial rank(doc_scores_order), for each doc, do 1vs1 and HPM check! make the final rank! 
		 */
		long startTime=System.currentTimeMillis();		
		int thisDocID=docMatches.DocID;
		boolean isNeedMatchingInfo=(matchingInfo!=null);
		//make1vw1Match
		DocAllMatchFeats doc1vw1Match = is1vw1match?make1vw1Match(docMatches):docMatches;
		//select 1vs1 matches
		ArrayList<MatchFeat_VW> selectedMatches=isUseHMDistFor1Vs1?select1V1Match_basedOnDist(doc1vw1Match,queryID,disp):select1V1Match_basedOnScore(doc1vw1Match, idf_squre, hammingW, queryID, disp);
		int time_1vs1=isNeedMatchingInfo?matchingInfo.add_1vs1(startTime, doc1vw1Match.getMatchNum(), selectedMatches.size()):0;
		//get final matchScore for this doc
		float matchScore=makeFinalDocMatchScore(selectedMatches, hammingW, showMatches, idf_squre, null);
		float normal=doc_BoVWVectorNorm[thisDocID]; //normlize by BoVWVectorNorm
		float finalScore=matchScore/normal; 
		//********* debug disp info **********//
		if (disp==true){ 
			System.out.println("\t 1vs1 matches:"+selectedMatches.size()+", no-HPM check!" );
			System.out.println("\t final matchScore for this doc:"+matchScore+", this is normalised by BoVWVectorNorm:"+normal+", finalScore: "+finalScore);
			if (isNeedMatchingInfo) {
				System.out.println("\t time_1vs1:"+General.dispTime(time_1vs1, "ms") );
			}else {
				System.out.println("\t time:"+General.dispTime(System.currentTimeMillis()-startTime, "ms"));
			}
			disp=false; //only disp once!
		}
		return finalScore;
	}
	
	public static float scoreDoc_by1vs1AndHPM(DocAllMatchFeats docMatches, SURFpoint[] thisQueryFeats, int queryID, LinkedList<ImageRegionMatch> showMatches,
			float[] doc_BoVWVectorNorm, float[] hammingW, float[] idf_squre, boolean isUseHMDistFor1Vs1,  boolean is1vw1match,
			short[] doc_maxDim, double[][] scalingInfo_min_max, int paraDim, int HPM_level, 
			boolean disp) throws InterruptedException, IOException {
		/**
		 * based on the initial rank(doc_scores_order), for each doc, do 1vs1 and HPM check! make the final rank! 
		 */
		long startTime=System.currentTimeMillis();
		int thisDocID=docMatches.DocID;
		//make1vw1Match
		DocAllMatchFeats doc1vw1Match = is1vw1match?make1vw1Match(docMatches):docMatches;
		//select 1vs1 matches
		ArrayList<MatchFeat_VW> selectedMatches=isUseHMDistFor1Vs1?select1V1Match_basedOnDist(doc1vw1Match,queryID,disp):select1V1Match_basedOnScore(doc1vw1Match, idf_squre, hammingW, queryID, disp);
		//do HPM 
		float[] matchWeightHPM=General_BoofCV.weightMatchByHoughPyramidMatching(selectedMatches, thisQueryFeats, scalingInfo_min_max, doc_maxDim[thisDocID], 0, 1, paraDim, HPM_level);
		//get final matchScore for this doc
		float matchScore=General_BoofCV.makeFinalDocMatchScore(selectedMatches, hammingW, showMatches, idf_squre, matchWeightHPM);
		float normal=doc_BoVWVectorNorm[thisDocID]; //normlize by BoVWVectorNorm
		float finalScore=matchScore/normal; 
		//********* debug disp info **********//
		if (disp==true){ 
			System.out.println("\t 1vs1 matches:"+selectedMatches.size() );
			System.out.println("\t final matchScore for this doc:"+matchScore+", this is normalised by BoVWVectorNorm:"+normal+", finalScore: "+finalScore);
			System.out.println("\t time:"+General.dispTime(System.currentTimeMillis()-startTime, "ms") );
			disp=false; //only disp once!
		}
		return finalScore;
	}
	
	public static float scoreDoc_by1vs1AndHist(boolean is1vs1First, DocAllMatchFeats docMatches, SURFpoint[] thisQueryFeats, int queryID, LinkedList<ImageRegionMatch> showMatches,
			float[] doc_BoVWVectorNorm, float[] hammingW, float[] idf_squre, boolean isUseHMDistFor1Vs1,  boolean is1vw1match,
			HistMultiD_Sparse_equalSizeBin_forFloat<Integer> hist, 
			boolean disp) throws InterruptedException, IOException {
		/**
		 * based on the initial rank(doc_scores_order), for each doc, do 1vs1 and Angel check! return both 1vs1 scroe and 1vs1Angel score
		 */
		long startTime=System.currentTimeMillis();		
		int thisDocID=docMatches.DocID; int iniMatchNum=docMatches.getMatchNum(); ArrayList<MatchFeat_VW> matches_1vs1_HV=null;
		//make1vw1Match
		DocAllMatchFeats doc1vw1Match = is1vw1match?make1vw1Match(docMatches):docMatches;
		if (is1vs1First) {
			//select 1vs1 matches
			ArrayList<MatchFeat_VW> matches_1vs1=isUseHMDistFor1Vs1?select1V1Match_basedOnDist(doc1vw1Match,queryID,disp):select1V1Match_basedOnScore(doc1vw1Match, idf_squre, hammingW, queryID, disp);
			//do hist
			matches_1vs1_HV=selectHistMatch(matches_1vs1, thisQueryFeats, hist, true, disp);
		}else{
			//do hist
			ArrayList<MatchFeat_VW> matches_HV=selectHistMatch(doc1vw1Match.getMatchFeat_VW(), thisQueryFeats, hist, true, disp);
			//select 1vs1 matches
			matches_1vs1_HV=isUseHMDistFor1Vs1?select1V1Match_basedOnDist(thisDocID,matches_HV,queryID,disp):select1V1Match_basedOnScore(thisDocID,matches_HV, idf_squre, hammingW, queryID, disp);
		}
		//make final score
		float matchScore=General_BoofCV.makeFinalDocMatchScore(matches_1vs1_HV, hammingW, null, idf_squre, null);
		float normal=doc_BoVWVectorNorm[thisDocID]; //normlize by BoVWVectorNorm		
		float finalScore_1vs1Hist=matchScore/normal; 
		//add matches to show
		if(showMatches!=null) {
			General_BoofCV.makeFinalDocMatchScore(matches_1vs1_HV, General.makeAllOnes_floatArr(hammingW.length, 1f), showMatches, General.makeAllOnes_floatArr(idf_squre.length, 1f), null);
		}
		//********* debug disp info **********//
		if (disp==true){ 
			System.out.println("\t iniMatchNum:"+iniMatchNum+", do 1vs1, hist check, selected num:"+matches_1vs1_HV.size() );
			System.out.println("\t for this doc-"+thisDocID+", final matchScore for this doc:"+matchScore+", this is normalised by BoVWVectorNorm:"+normal+", finalScore: "+finalScore_1vs1Hist);
			System.out.println("\t time:"+General.dispTime(System.currentTimeMillis()-startTime, "ms"));
			disp=false; //only disp once!
		}
		return finalScore_1vs1Hist;
	}
	
	public static float[] scoreDoc_by1vs1AndAngle(DocAllMatchFeats docMatches, SURFpoint[] thisQueryFeats, int queryID, int query_maxDim, LinkedList<ImageRegionMatch> showMatches,
			float[] doc_BoVWVectorNorm, float[] hammingW, float[] idf_squre, boolean isUseHMDistFor1Vs1,  boolean is1vw1match, short[] doc_maxDim, HistMultiD_Sparse_equalSizeBin_forFloat<Pair_int> hist, 
			float PointDisThr, float badPariWeight, float weightThr,
			float lineAngleStep, float lineDistStep, int sameLinePointNumThr, float docScoreThr, boolean disp) throws InterruptedException, IOException {
		/**
		 * based on the initial rank(doc_scores_order), for each doc, do 1vs1 and Angel check! return both 1vs1 scroe and 1vs1Angel score
		 */
		long startTime=System.currentTimeMillis();		
		int thisDocID=docMatches.DocID;
		//make1vw1Match
		DocAllMatchFeats doc1vw1Match = is1vw1match?make1vw1Match(docMatches):docMatches;
		//select 1vs1 matches
		ArrayList<MatchFeat_VW> selectedMatches=isUseHMDistFor1Vs1?select1V1Match_basedOnDist(doc1vw1Match,queryID,disp):select1V1Match_basedOnScore(doc1vw1Match, idf_squre, hammingW, queryID, disp);
		//make final score
		float finalScore_1vs1=0; float finalScore_1vs1Angle=0;
		float normal=doc_BoVWVectorNorm[thisDocID]; //normlize by BoVWVectorNorm
		if (selectedMatches.size()<3) {//only 1 or 2 matches, then no match pairs or only 1 match pair
			//get final matchScore for this doc
			float matchScore=General_BoofCV.makeFinalDocMatchScore(selectedMatches, hammingW, showMatches, idf_squre, null);
			finalScore_1vs1=matchScore/normal; 
		}else {
			//do Angle
			float[] matchWeightAngle=General_BoofCV.weightMatchByAngle(selectedMatches, thisQueryFeats, hist, 
					(float) (doc_maxDim[thisDocID]*PointDisThr), badPariWeight, weightThr, 
					lineAngleStep, lineDistStep, query_maxDim, doc_maxDim[thisDocID], sameLinePointNumThr, disp);
			finalScore_1vs1Angle=General.sum_FloatArr(matchWeightAngle);
			//prevent overFlow
			finalScore_1vs1Angle=finalScore_1vs1Angle<0?Float.MAX_VALUE:finalScore_1vs1Angle;
			//if no match can pass angle check, use 1vs1 score instead;
			if (finalScore_1vs1Angle<=docScoreThr) {
				float matchScore=General_BoofCV.makeFinalDocMatchScore(selectedMatches, hammingW, showMatches, idf_squre, null);
				finalScore_1vs1=matchScore/normal; 
				finalScore_1vs1Angle=0;
			}else if(showMatches!=null) {//add matches to show
				General_BoofCV.makeFinalDocMatchScore(selectedMatches, General.makeAllOnes_floatArr(hammingW.length, 1f), showMatches, General.makeAllOnes_floatArr(idf_squre.length, 1f), matchWeightAngle);
			}
			//********* debug disp info **********//
			if (disp==true){ 
				System.out.println("\t 1vs1 matches:"+selectedMatches.size()+", do Angle check!" );
				System.out.println("\t for this doc-"+thisDocID+", finalScore_1vs1Angle:"+finalScore_1vs1Angle+", finalScore_1vs1:"+finalScore_1vs1);
				System.out.println("\t time:"+General.dispTime(System.currentTimeMillis()-startTime, "ms") );
				disp=false; //only disp once!
			}
		}
		return new float[]{finalScore_1vs1Angle,finalScore_1vs1};
	}
	
	public static float[] scoreDoc_by1vs1AndHistAndAngle(boolean is1vs1First, DocAllMatchFeats docMatches, SURFpoint[] thisQueryFeats, boolean isUPRightFeat, int queryID, int query_maxDim, LinkedList<ImageRegionMatch> showMatches,
			float[] doc_BoVWVectorNorm, float[] hammingW, float[] idf_squre, boolean isOnlyUseHMDistFor1Vs1,  boolean is1vw1match, short[] doc_maxDim, HistMultiD_Sparse_equalSizeBin_forFloat<Integer> hist, float binScaleRate, 
			boolean isUseHPM, double[][] scalingInfo_min_max, int HPM_ParaDim, int HPM_level, 
			float PointDisThr, float badPariWeight, float weightThr,
			float lineAngleStep, float lineDistStep, int sameLinePointNumThr, float docScoreThr, 
			MatchingInfo matchingInfo, boolean disp, 
			LinkedList<SURFpoint> selPoints_l, LinkedList<SURFpoint> selPoints_r) throws InterruptedException, IOException {
		/**
		 * based on the initial rank(doc_scores_order), for each doc, do 1vs1 and Angel check! return both 1vs1 scroe and 1vs1Angel score
		 */
		if (isUseHPM) {
			General.Assert(is1vs1First, "err! isUseHPM==true, then is1vs1First must be true.");
		}
		long startTime=System.currentTimeMillis();		
		int thisDocID=docMatches.DocID; int iniMatchNum=docMatches.getMatchNum();
		boolean isNeedMatchingInfo=(matchingInfo!=null); ArrayList<MatchFeat_VW> matches_1vs1=null; ArrayList<MatchFeat_VW> matches_1vs1HV=null;
		int time_1vs1=0; int time_HV=0; int time_PG=0;
		//make1vw1Match
		DocAllMatchFeats doc1vw1Match = is1vw1match?make1vw1Match(docMatches):docMatches;
		int matchNum_after1vw1Match=doc1vw1Match.getMatchNum();
		if (is1vs1First) {
			//select 1vs1 matches
			startTime=System.currentTimeMillis();
			matches_1vs1=isOnlyUseHMDistFor1Vs1?select1V1Match_basedOnDist(doc1vw1Match,queryID,disp):select1V1Match_basedOnScore(doc1vw1Match, idf_squre, hammingW, queryID, disp);
			time_1vs1=isNeedMatchingInfo?matchingInfo.add_1vs1(startTime, matchNum_after1vw1Match, matches_1vs1.size()):0;
			//do hist
			startTime=System.currentTimeMillis();
			matches_1vs1HV=selectHistMatch(matches_1vs1, thisQueryFeats, hist, true, disp);
			if (isNeedMatchingInfo) {
				time_HV=matchingInfo.add_HV(startTime, matches_1vs1.size(), matches_1vs1HV.size());
				matchingInfo.add_1vs1_HV(matchNum_after1vw1Match, matches_1vs1HV.size());
			}
		}else {
			//do hist
			startTime=System.currentTimeMillis();
			ArrayList<MatchFeat_VW> matches_HV=selectHistMatch(doc1vw1Match.getMatchFeat_VW(), thisQueryFeats, hist, true, disp);
			time_HV=isNeedMatchingInfo?matchingInfo.add_HV(startTime, matchNum_after1vw1Match, matches_HV.size()):0;
			//select 1vs1 matches
			startTime=System.currentTimeMillis();
			matches_1vs1HV=isOnlyUseHMDistFor1Vs1?select1V1Match_basedOnDist(thisDocID,matches_HV,queryID,disp):select1V1Match_basedOnScore(thisDocID,matches_HV, idf_squre, hammingW, queryID, disp);
			if (isNeedMatchingInfo) {
				time_1vs1=matchingInfo.add_1vs1(startTime, matches_HV.size(), matches_1vs1HV.size());
				matchingInfo.add_1vs1_HV(matchNum_after1vw1Match, matches_1vs1HV.size());
			}		
		}
		//do PG
		startTime=System.currentTimeMillis();
		MatchingScore matchWeights_angel_hist=General_BoofCV.weightMatchByHistAndAngle(matches_1vs1HV, thisQueryFeats, isUPRightFeat, hist, 
				binScaleRate, (float) (doc_maxDim[thisDocID]*PointDisThr), badPariWeight, weightThr, 
				lineAngleStep, lineDistStep, query_maxDim, doc_maxDim[thisDocID], sameLinePointNumThr, disp, 
				selPoints_l, selPoints_r);
		if (isNeedMatchingInfo) {
			time_PG=matchingInfo.add_PG(startTime, matches_1vs1HV.size(), matchWeights_angel_hist.activeMatchNum_inMastScore);
			matchingInfo.add_1vs1_HV_PG(matchNum_after1vw1Match, matchWeights_angel_hist.activeMatchNum_inMastScore);
		}
		float finalScore_1vs1Angle=General.sum_FloatArr(matchWeights_angel_hist.mastScore);
		//prevent overFlow
		finalScore_1vs1Angle=finalScore_1vs1Angle<0?Float.MAX_VALUE:finalScore_1vs1Angle;
		//if no match can pass angle check, use 1vs1 and Hist score instead;
		float finalScore_1vs1Hist=0;
		if (finalScore_1vs1Angle<=docScoreThr) {
			float matchScore=0;
			float normal=doc_BoVWVectorNorm[thisDocID]; //normlize by BoVWVectorNorm		
			if (isUseHPM) {
				float[] matchWeightHPM=General_BoofCV.weightMatchByHoughPyramidMatching(matches_1vs1, thisQueryFeats, scalingInfo_min_max, doc_maxDim[thisDocID], 0, 1, HPM_ParaDim, HPM_level);
				matchScore=General_BoofCV.makeFinalDocMatchScore(matches_1vs1, hammingW, null, idf_squre, matchWeightHPM);
			}else {
				matchScore=General_BoofCV.makeFinalDocMatchScore(matches_1vs1HV, hammingW, null, idf_squre, matchWeights_angel_hist.seconderyScore);
			}
			finalScore_1vs1Hist=matchScore/normal; 
			finalScore_1vs1Angle=0;
		}
		//add matches to show
		if(showMatches!=null) {
			General_BoofCV.makeFinalDocMatchScore(matches_1vs1HV, General.makeAllOnes_floatArr(hammingW.length, 1f), showMatches, General.makeAllOnes_floatArr(idf_squre.length, 1f), matchWeights_angel_hist.mastScore);
		}
		//********* debug disp info **********//
		if (disp==true){ 
			System.out.println("\t iniMatchNum:"+iniMatchNum+", matchNum_after1vw1Match:"+matchNum_after1vw1Match+", do hist&angle check, matches_1vs1:"+matches_1vs1.size()+", matches_1vs1HV:"+matches_1vs1HV.size()+", final PG matches:"+matchWeights_angel_hist.activeMatchNum_inMastScore );
			System.out.println("\t for this doc-"+thisDocID+", finalScore_1vs1Angle:"+finalScore_1vs1Angle+", finalScore_1vs1Hist:"+finalScore_1vs1Hist);
			if (isNeedMatchingInfo) {
				System.out.println("\t time_1vs1:"+General.dispTime(time_1vs1, "ms")+", time_HV:"+time_HV+General.dispTime(time_HV, "ms")+", time_PG:"+time_HV+General.dispTime(time_PG, "ms") );
			}else {
				System.out.println("\t time:"+General.dispTime(System.currentTimeMillis()-startTime, "ms"));
			}
			disp=false; //only disp once!
		}
		
		return new float[]{finalScore_1vs1Angle,finalScore_1vs1Hist};
	}
	
//	public static float[] scoreDoc_byHistAnd1vs1AndAngle(DocAllMatchFeats docMatches, SURFpoint[] thisQueryFeats, int queryID, int query_maxDim, LinkedList<ImageRegionMatch> showMatches,
//			float[] doc_BoVWVectorNorm, float[] hammingW, float[] idf_squre, boolean isUseHMDistFor1Vs1, short[] doc_maxDim, HistMultiD_Sparse_equalSizeBin_forFloat<Integer> hist, 
//			boolean isUseHPM, double[][] scalingInfo_min_max, int HPM_ParaDim, int HPM_level, 
//			float PointDisThr, float badPariWeight, float weightThr,
//			float lineAngleStep, float lineDistStep, int sameLinePointNumThr, float docScoreThr, 
//			MatchingInfo matchingInfo, boolean disp, 
//			LinkedList<SURFpoint> selPoints_l, LinkedList<SURFpoint> selPoints_r) throws InterruptedException, IOException {
//		/**
//		 * based on the initial rank(doc_scores_order), for each doc, do Hist and 1vs1 check! 
//		 */
//		long startTime=System.currentTimeMillis();		
//		int thisDocID=docMatches.DocID; int iniMatchNum=docMatches.getMatchNum();
//		boolean isNeedMatchingInfo=(matchingInfo!=null);
//		int iniMatchNum=docMatches.getMatchNum();
//		//do hist, directly use ini matches
//		ArrayList<MatchFeat_VW> histMatches=selectHistMatch(docMatches.getMatchFeat_VW(), thisQueryFeats, hist, true, disp);
//		int time_HV=isNeedMatchingInfo?matchingInfo.add_HV(startTime, iniMatchNum, iniMatchNum, histMatches.size()):0;
//		//select 1vs1 matches
//		startTime=System.currentTimeMillis();
//		ArrayList<MatchFeat_VW> final1vs1Matches=isUseHMDistFor1Vs1?select1V1Match_basedOnDist(thisDocID, histMatches,queryID,disp):select1V1Match_basedOnScore(thisDocID, histMatches, idf_squre, hammingW, queryID, disp);
//		int time_1vs1=isNeedMatchingInfo?matchingInfo.add_1vs1(startTime, docMatches.getMatchNum(), final1vs1Matches.size()):0;
//		//do hist and angle check
//		startTime=System.currentTimeMillis();
//		float[][] matchWeights_angel_hist=General_BoofCV.weightMatchByHistAndAngle(final1vs1Matches, thisQueryFeats, hist, 
//				(float) (doc_maxDim[thisDocID]*PointDisThr), badPariWeight, weightThr, 
//				lineAngleStep, lineDistStep, query_maxDim, doc_maxDim[thisDocID], sameLinePointNumThr, disp, 
//				selPoints_l, selPoints_r);
//		int time_PG=isNeedMatchingInfo?matchingInfo.add_PG(startTime, docMatches.getMatchNum()):0;
//		float finalScore_1vs1Angle=General.sum_FloatArr(matchWeights_angel_hist[0]);
//		//prevent overFlow
//		finalScore_1vs1Angle=finalScore_1vs1Angle<0?Float.MAX_VALUE:finalScore_1vs1Angle;
//		//if no match can pass angle check, use 1vs1 and Hist score instead;
//		float finalScore_1vs1Hist=0;
//		if (finalScore_1vs1Angle<=docScoreThr) {
//			float matchScore=0;
//			float normal=doc_BoVWVectorNorm[thisDocID]; //normlize by BoVWVectorNorm		
//			if (isUseHPM) {
//				float[] matchWeightHPM=General_BoofCV.weightMatchByHoughPyramidMatching(final1vs1Matches, thisQueryFeats, scalingInfo_min_max, doc_maxDim[thisDocID], 0, 1, HPM_ParaDim, HPM_level);
//				matchScore=General_BoofCV.makeFinalDocMatchScore(final1vs1Matches, hammingW, null, idf_squre, matchWeightHPM);
//			}else {
//				matchScore=General_BoofCV.makeFinalDocMatchScore(final1vs1Matches, hammingW, null, idf_squre, matchWeights_angel_hist[1]);
//			}
//			finalScore_1vs1Hist=matchScore/normal; 
//			finalScore_1vs1Angle=0;
//		}
//		//add matches to show
//		if(showMatches!=null) {
//			General_BoofCV.makeFinalDocMatchScore(final1vs1Matches, General.makeAllOnes_floatArr(hammingW.length), showMatches, General.makeAllOnes_floatArr(idf_squre.length), matchWeights_angel_hist[0]);
//		}
//		//********* debug disp info **********//
//		if (disp==true){ 
//			System.out.println("\t final1vs1Matches:"+final1vs1Matches.size()+", do angle check!" );
//			System.out.println("\t for this doc-"+thisDocID+", finalScore_1vs1Angle:"+finalScore_1vs1Angle+", finalScore_1vs1Hist:"+finalScore_1vs1Hist);
//			if (isNeedMatchingInfo) {
//			}else {
//				System.out.println("\t time:"+General.dispTime(System.currentTimeMillis()-startTime, "ms"));
//			}
//			disp=false; //only disp once!
//		}
//		
//		return new float[]{finalScore_1vs1Angle,finalScore_1vs1Hist};
//	}
	
	public static int readTVectorNumOnlyIntoMemory(Path TVPath, Configuration conf, int vw, ArrayList<Integer> TVector_docIDs, ArrayList<Integer> TVector_featNums) throws IOException{
		SequenceFile.Reader TVector_Reader=new SequenceFile.Reader(conf, SequenceFile.Reader.file(TVPath));
		IntWritable TVector_key = new IntWritable(vw) ; //docIDs
		IntWritable TVector_value = new IntWritable();//featNum
		//******* check TVector's vw **************
		TVector_Reader.next(TVector_key, TVector_value);//1st element is to mark vw, value is not useful! 
		General.Assert(TVector_key.get()==vw, "err in readTVectorIntoMemory: TVector's first element(vw) is:"+TVector_key.get()+", not vw:"+vw);
		//******* read TVector into memory **************
		int TVectorFeatNum=0;
		while (TVector_Reader.next(TVector_key, TVector_value)) {
			TVector_docIDs.add(TVector_key.get());
			TVector_featNums.add(TVector_value.get());
			TVectorFeatNum+=TVector_value.get();
		}
		TVector_Reader.close();
		TVector_docIDs.trimToSize(); TVector_featNums.trimToSize(); 
		return TVectorFeatNum;
	}
	
	public static DenseMatrix64F make_HM_ProjectionMatrix(int HEBitNum, int featDim) throws InterruptedException{
		int maxDim=Math.max(HEBitNum, featDim);
		DenseMatrix64F matrix_toFac = new DenseMatrix64F(maxDim,maxDim) ; 
		Random rand=new Random();
		//Gaussian random generate 
		for(int i=0;i<maxDim;i++){
			for(int j=0;j<maxDim;j++){
				matrix_toFac.set(i, j, rand.nextGaussian());
			}
		}
		//QR factorization
		QRDecomposition<DenseMatrix64F> qr = DecompositionFactory.qr(matrix_toFac.numRows,matrix_toFac.numCols);
		if( !qr.decompose(matrix_toFac) )
            throw new InterruptedException("Decomposition failed");
		 
		//QR decompositions decompose a rectangular matrix A such that A=QR. Where A =n * m , n >= m, Q = n * n is an orthogonal matrix, and R = n * m is an upper triangular matrix. Some implementations of QR decomposition require that A has full rank. 

		boolean compact=true; 
		DenseMatrix64F matrix_Q=qr.getQ(null, compact);//a squre matrix with maxDim*maxDim
		//get the first HEBitNum-row of matrix_Q
		DenseMatrix64F matrix_res = new DenseMatrix64F(HEBitNum,featDim); 
		for(int i=0;i<HEBitNum;i++){
			for(int j=0;j<featDim;j++){
				matrix_res.set(i, j, matrix_Q.get(i, j));
			}
		}
		return matrix_res;
	}
	
	public static float[] make_HMWeigthts(double deta, int HMDistThr){//used in CVPR09: on the burstiness of visual elements
		//** set Hamming weight **//
		float[] hammingW=new float[HMDistThr+1]; 
		if (deta==0) {//no-hamming weighting
			for (int i = 0; i < hammingW.length; i++) {
				hammingW[i]=1;
			}
		}else {
			for (int i = 0; i < hammingW.length; i++) {
				hammingW[i]=(float) Math.exp(-Math.pow(i/deta, 2));
			}
		}
		return hammingW;
	}
	
	public static float[] make_HMWeigthts_IJCV09(int HEbit, int HMDistThr){
		//** set Hamming weight **//
		float[] hammingW=new float[HMDistThr+1]; long[] binomialCoefficients=new long[HMDistThr+1];
		for (int i = 0; i < binomialCoefficients.length; i++) {
			binomialCoefficients[i]=ArithmeticUtils.binomialCoefficient(HEbit, i);
		}
		double C=Math.pow(2, HEbit/2);
		for (int i = 0; i < hammingW.length; i++) {
			long sum=0;
			for (int j = 0; j <= i; j++) {
				sum+=binomialCoefficients[i];//when j is ==32, sum is too large, may overload!
			}
			hammingW[i]=(float) -General.log(2, sum/C/C);
//			hammingW[i]=1;
		}
		return hammingW;
	}
	
	public static float[] make_idf_squre(int[][] TVectorInfo, int totDocNum){//photoNum,featNum
		float[] idf_squre=new float[TVectorInfo.length];
		for (int i = 0; i < idf_squre.length; i++) {
			if (TVectorInfo[i]!=null) {//this vw has photos
				idf_squre[i]=(float)Math.pow(make_idf_logE(totDocNum,TVectorInfo[i][0]), 2);
			}
		}
		return idf_squre;
	}
	
	public static float[] make_idf_squre(TVectorInfo[] tVectorInfos, int totDocNum){//photoNum,featNum
		float[] idf_squre=new float[tVectorInfos.length];
		for (int i = 0; i < idf_squre.length; i++) {
			if (tVectorInfos[i].photoNum!=0) {//this vw has photos
				idf_squre[i]=(float)Math.pow(make_idf_logE(totDocNum,tVectorInfos[i].photoNum), 2);
			}
		}
		return idf_squre;
	}
	
	public static float make_idf_logE(double totDocNum, double thisDocNum){//this idf use log_e
		General.Assert(totDocNum>0 && thisDocNum>0, "err! totDocNum and thisDocNum should all > 0!");
		return (float) Math.log((double)totDocNum/thisDocNum);
	}
	
	public static float make_idf_log10(double totDocNum, double thisDocNum){//this idf use log_10
		General.Assert(totDocNum>0 && thisDocNum>0, "err! totDocNum and thisDocNum should all > 0!");
		return (float) Math.log10((double)totDocNum/thisDocNum);
	}
	
	public static float[] weightMatchByHoughPyramidMatching(ArrayList<MatchFeat_VW> goodMatches, SURFpoint[] interestPoints_Q,
			double[][] scalingInfo_min_max, double docMaxDim, double lower, double upper, int parmDim, int levelNum){
		/*
		 * fast! each match vote for only 1 bin!
		 */
		//check levelNum
		General.Assert(parmDim==2 || parmDim==4, "err! parmDim should be 2 or 4, here parmDim:"+parmDim);
		General.Assert(levelNum<=(int)(32-1)/parmDim, "err in weightMatchByHoughPyramidMatching, the maxmum levelNum is "+(32-1)/parmDim+", here is: "+levelNum);
		//make scalingInfo_min_max
		double XYQuantize=3*docMaxDim; 
		scalingInfo_min_max[2]=new double[]{-XYQuantize,XYQuantize};//x
		scalingInfo_min_max[3]=new double[]{-XYQuantize,XYQuantize};//y
		//initialize usefulLengthes in different level
		int[] usefulLengths=new int[levelNum];
		for (int lev_i = 0; lev_i < levelNum; lev_i++) {
			usefulLengths[lev_i]=levelNum-lev_i;
		}
		//initialize HPMWeights
		int matchNum=goodMatches.size(); 
		float[] HPMWeights=new float[matchNum]; 
		//********  make match-c parameters **********
		double[][] matchParams= new double[matchNum][4];
		for (int i = 0; i < matchNum; i++) {
			MatchFeat_VW oneMatch = goodMatches.get(i);
			int qi=oneMatch.QFeatInd;
//			try{
				SURFpoint point_qi= interestPoints_Q[qi];
				SURFpoint point_dj= oneMatch.docFeat.getSURFpoint();
				//get angle, scale, x, y
				double[] oneMatchPara= new double[parmDim];
				oneMatchPara[0]=makeRotationTo_NPtoPP(point_dj.angle-point_qi.angle);
				double normScale=point_dj.scale/point_qi.scale;
				oneMatchPara[1]=Math.log10(normScale);//use logarithmic scale
//				oneMatchPara[0]=1.22;//fix oritation, swicth off oritation
//				oneMatchPara[1]=Math.log10(3);//fix logarithmic scale
				if (parmDim==4) {
					double cos_match_angle=Math.cos(oneMatchPara[0]);
					double sin_match_angle=Math.sin(oneMatchPara[0]);
					oneMatchPara[2]=point_dj.x-normScale*(point_qi.x*cos_match_angle-point_qi.y*sin_match_angle);
					oneMatchPara[3]=point_dj.y-normScale*(point_qi.x*sin_match_angle+point_qi.y*cos_match_angle);
				}
				//quantization
				for (int j = 0; j < parmDim; j++) {
					if (oneMatchPara[j]<scalingInfo_min_max[j][0] || oneMatchPara[j]>scalingInfo_min_max[j][1]) {//parameter exceed legal rang, delete this one
						HPMWeights[i]=-1;//Temporarily mark illegal match
						break;
					}else {
						HPMWeights[i]=+1;//Temporarily mark legal match
						matchParams[i][j]=General.scaleValue(oneMatchPara[j], scalingInfo_min_max[j], lower, upper);
					}
				}
//			}catch (ArrayIndexOutOfBoundsException e) {
//				System.err.println("ArrayIndexOutOfBoundsException! oneMatch, HMDist:"+oneMatch.HMDist+", QFeatInd:"+oneMatch.QFeatInd+", vw:"+oneMatch.vw);
//				System.err.println("\t interestPoints_Q length:"+interestPoints_Q.length+", goodMatches number:"+goodMatches.size());
//				throw e;
//			}
		}
		//********  weight Match By Hough Pyramid Matching ************
		double FinestLevel_Inter = Math.pow(2, -levelNum);
		//initialize matchesInBin
		ArrayList<HashMap<Integer, ArrayList<Integer>>> matchesInBin=new ArrayList<HashMap<Integer, ArrayList<Integer>>>(levelNum);
		for (int i = 0; i <levelNum; i++) {
			matchesInBin.add(new HashMap<Integer, ArrayList<Integer>>());
		}
		//assign matches In Bin
		for (int match_i = 0; match_i < matchNum; match_i++) {
			if (HPMWeights[match_i]>0) {//this match has legal parameters 
				//make eachDimInd_inFinestLev
				int[] eachDimInd_inFinestLev=new int[parmDim];
				for (int j = 0; j < parmDim; j++) {
					eachDimInd_inFinestLev[j]=(int) (matchParams[match_i][j]/FinestLevel_Inter);
				}
				//make eachDimInd and totInd in different level
				for (int lev_i = 0; lev_i < levelNum; lev_i++) {//level 0 is the finest level
					//make eachDimInd in this level: represent binInd in the finest level by bit array, then use this to caculate binInd in each level
					int[] eachDimInd_inThisLev=new int[parmDim];
					for (int j = 0; j < parmDim; j++) {
						eachDimInd_inThisLev[j]=(eachDimInd_inFinestLev[j]>>lev_i);
					}
					/**	concatenate eachDimInd into one int value, and use this as totInd: 
						eachDimInd is one int, can be represent by a bit array with the useful length=(levelNum-levelInd), 
						if concatenate parmDim eachDimInds into one int bit array, 32-1(1 bit for sign)=31 bits, so the maximum levelNum allowed is 31/parmDim 
					**/
					int totInd=+0;
					for (int j = 0; j < parmDim; j++) {//concatenate [eachDimInd_(Dim-1), ..., eachDimInd_0]
						totInd=totInd^eachDimInd_inThisLev[j]<<(usefulLengths[lev_i]*j);
					}
					//save thisMatch to the bin in this level
					HashMap<Integer, ArrayList<Integer>> thisLevel= matchesInBin.get(lev_i);
					if (thisLevel.containsKey(totInd)) {
						thisLevel.get(totInd).add(match_i);
					}else {
						ArrayList<Integer> binMatches=new ArrayList<Integer>();
						binMatches.add(match_i);
						thisLevel.put(totInd, binMatches);
					}
				}
			}
		}
		//make groupW in different level for each match
		int[][] match_lev_groupW=new int[matchNum][levelNum];
		for (int lev_i = 0; lev_i < levelNum; lev_i++) {
			HashMap<Integer, ArrayList<Integer>> thisLevel= matchesInBin.get(lev_i);
			for (Entry<Integer, ArrayList<Integer>>  oneBin : thisLevel.entrySet()) {
				int groupW= oneBin.getValue().size()-1;
				for (Integer oneMatch : oneBin.getValue()) {
					match_lev_groupW[oneMatch][lev_i]=groupW;
				}
			}
		}
		//calculate HPM weight for each match
		double[] lev_Ws=new double[levelNum];
		for (int lev_i = 0; lev_i < levelNum; lev_i++) {//2^(-lev_i)
			lev_Ws[lev_i]=Math.pow(2, -lev_i);
		}
		for (int match_i = 0; match_i < matchNum; match_i++) {
			if (HPMWeights[match_i]>0) {//this match has legal parameters 
				HPMWeights[match_i]=match_lev_groupW[match_i][0];
				for (int lev_i = 1; lev_i < levelNum; lev_i++) {
					HPMWeights[match_i]+=lev_Ws[lev_i]*(match_lev_groupW[match_i][lev_i]-match_lev_groupW[match_i][lev_i-1]);
				}
			}else {
				HPMWeights[match_i]=0;
			}
		}
		return HPMWeights;
	}
	
	public static float[] weightMatchByHoughPyramidMatching_1VoteFor2Bins(ArrayList<MatchFeat_VW> goodMatches, SURFpoint[] interestPoints_Q,
			double[][] scalingInfo_min_max, double docMaxDim, double lower, double upper, int levelNum){
		/*
		 * slow! each match vote for 2 nearby bins!
		 */
		//check levelNum
		int parmDim=4; 
		General.Assert(levelNum<=(int)(32-1)/parmDim, "err in weightMatchByHoughPyramidMatching, the maxmum levelNum is "+(32-1)/parmDim+", here is: "+levelNum);
		//make scalingInfo_min_max
		double XYQuantize=3*docMaxDim; 
		scalingInfo_min_max[2]=new double[]{-XYQuantize,XYQuantize};//x
		scalingInfo_min_max[3]=new double[]{-XYQuantize,XYQuantize};//y
		//initialize usefulLengthes in different level
		int[] usefulLengths=new int[levelNum];
		for (int lev_i = 0; lev_i < levelNum; lev_i++) {
			usefulLengths[lev_i]=levelNum-lev_i;
		}
		//initialize HPMWeights
		int matchNum=goodMatches.size(); 
		float[] HPMWeights=new float[matchNum]; 
		//********  make match-c parameters **********
		double[][] matchParams= new double[matchNum][4];
		for (int i = 0; i < matchNum; i++) {
			MatchFeat_VW oneMatch = goodMatches.get(i);
			int qi=oneMatch.QFeatInd;
			SURFpoint point_qi= interestPoints_Q[qi];
			SURFpoint point_dj= oneMatch.docFeat.getSURFpoint();
			//get angle, scale, x, y
			double[] oneMatchPara= new double[parmDim];
			oneMatchPara[0]=makeRotationTo_NPtoPP(point_dj.angle-point_qi.angle);
			double normScale=point_dj.scale/point_qi.scale;
			oneMatchPara[1]=Math.log10(normScale);//use logarithmic scale
			double cos_match_angle=Math.cos(oneMatchPara[0]);
			double sin_match_angle=Math.sin(oneMatchPara[0]);
			oneMatchPara[2]=point_dj.x-normScale*(point_qi.x*cos_match_angle-point_qi.y*sin_match_angle);
			oneMatchPara[3]=point_dj.y-normScale*(point_qi.x*sin_match_angle+point_qi.y*cos_match_angle);
			//quantization
			for (int j = 0; j < parmDim; j++) {
				if (oneMatchPara[j]<scalingInfo_min_max[j][0] || oneMatchPara[j]>scalingInfo_min_max[j][1]) {//parameter exceed legal rang, delete this one
					HPMWeights[i]=-1;//Temporarily mark illegal match
					break;
				}else {
					HPMWeights[i]=+1;//Temporarily mark legal match
					matchParams[i][j]=General.scaleValue(oneMatchPara[j], scalingInfo_min_max[j], lower, upper);
				}
			}
		}
		//********  weight Match By Hough Pyramid Matching ************
		//initialize Level_Inters
		double[][] Level_Inters = new double[levelNum][2];//each level, 2 inters: this level inter, and half of this inter
		for (int lev_i = 0; lev_i <levelNum; lev_i++) {
			Level_Inters[lev_i][0]=Math.pow(2, lev_i-levelNum);
			Level_Inters[lev_i][1]=Level_Inters[lev_i][0]/2;
		}
		//initialize Dim-vote-combination
		int[] possibleValue={0,1}; //each match on each dim vote for 2 bins!
		int dimVoteCombinationNum=(int) Math.pow(possibleValue.length, parmDim);
		int[][] dimVoteCombination=new int[dimVoteCombinationNum][parmDim];
		int comb_ind=0;
		for (int dim_0 :possibleValue) {
			for (int dim_1 :possibleValue) {
				for (int dim_2 :possibleValue) {
					for (int dim_3 :possibleValue) {
						dimVoteCombination[comb_ind]=new int[]{dim_0,dim_1,dim_2,dim_3};
						comb_ind++;
					}
				}
			}
		}
		//initialize matchesInBin
		ArrayList<HashMap<Integer, ArrayList<Short>>> matchesInBin=new ArrayList<HashMap<Integer, ArrayList<Short>>>(levelNum);
		for (int lev_i = 0; lev_i <levelNum; lev_i++) {
			matchesInBin.add(new HashMap<Integer, ArrayList<Short>>());
		}
		//assign matches In Bin
		for (short match_i = 0; match_i < matchNum; match_i++) {
			if (HPMWeights[match_i]>0) {//this match has legal parameters 
				//make eachDimInd and totInd in different level
				for (int lev_i = 0; lev_i < levelNum; lev_i++) {//level 0 is the finest level
					//make eachDimInd in this level: each match vote for two nearby bins
					int[][] eachDimInd_inThisLev=new int[parmDim][2];
					for (int j = 0; j < parmDim; j++) {
						eachDimInd_inThisLev[j][0]=(int) (matchParams[match_i][j]/Level_Inters[lev_i][0]);//vote 0
						if (matchParams[match_i][j]>(eachDimInd_inThisLev[j][0]*Level_Inters[lev_i][0]+Level_Inters[lev_i][1])) {//vote 1: vote for the right nearbour bin
							eachDimInd_inThisLev[j][1]=eachDimInd_inThisLev[j][0]+1;
						}else {
							eachDimInd_inThisLev[j][1]=eachDimInd_inThisLev[j][0]-1;
						}
					}
					/**	concatenate eachDimInd into one int value, and use this as totInd: 
						eachDimInd is one int, can be represent by a bit array with the useful length=(levelNum-levelInd), 
						if concatenate parmDim eachDimInds into one int bit array, 32-1(1 bit for sign)=31 bits, so the maximum levelNum allowed is 31/parmDim 
					**/
					for (int vote_i = 0; vote_i < dimVoteCombinationNum; vote_i++) {//each match on each dim vote for 2 bins, in total 2^parmDim blocks in the parmDim space!
						int totInd=+0;
						for (int j = 0; j < parmDim; j++) {//concatenate [eachDimInd_(Dim-1), ..., eachDimInd_0]
							totInd=totInd^eachDimInd_inThisLev[j][dimVoteCombination[vote_i][j]]<<(usefulLengths[lev_i]*j);
						}
						//save thisMatch to the bin in this level
						HashMap<Integer, ArrayList<Short>> thisLevel= matchesInBin.get(lev_i);
						if (thisLevel.containsKey(totInd)) {
							thisLevel.get(totInd).add(match_i);
						}else {
							ArrayList<Short> binMatches=new ArrayList<Short>();
							binMatches.add(match_i);
							thisLevel.put(totInd, binMatches);
						}
					}
					
				}
			}
		}
		//make groupW in different level for each match
		ArrayList<ArrayList<HashSet<Short>>> match_lev_links=new ArrayList<ArrayList<HashSet<Short>>>(matchNum);
		for (int match_i = 0; match_i < matchNum; match_i++) {
			ArrayList<HashSet<Short>> oneMatch=new ArrayList<HashSet<Short>>();
			for (int lev_i = 0; lev_i < levelNum; lev_i++) {
				oneMatch.add(new HashSet<Short>());
			}
			match_lev_links.add(oneMatch);
		}
		for (int lev_i = 0; lev_i < levelNum; lev_i++) {
			HashMap<Integer, ArrayList<Short>> thisLevel= matchesInBin.get(lev_i);
			for (Entry<Integer, ArrayList<Short>>  oneBin : thisLevel.entrySet()) {
				for (Short oneMatch : oneBin.getValue()) {
					match_lev_links.get(oneMatch).get(lev_i).addAll(oneBin.getValue());
				}
			}
			
		}
		int[][] match_lev_groupW=new int[matchNum][levelNum];
		for (int match_i = 0; match_i < matchNum; match_i++) {
			for (int lev_i = 0; lev_i < levelNum; lev_i++) {
				match_lev_groupW[match_i][lev_i]=match_lev_links.get(match_i).get(lev_i).size()-1;
			}
		}
		//calculate HPM weight for each match
		double[] lev_Ws=new double[levelNum];
		for (int lev_i = 0; lev_i < levelNum; lev_i++) {//2^(-lev_i)
			lev_Ws[lev_i]=Math.pow(2, -lev_i);
		}
		for (int match_i = 0; match_i < matchNum; match_i++) {
			if (HPMWeights[match_i]>0) {//this match has legal parameters 
				HPMWeights[match_i]=match_lev_groupW[match_i][0];
				for (int lev_i = 1; lev_i < levelNum; lev_i++) {
					HPMWeights[match_i]+=lev_Ws[lev_i]*(match_lev_groupW[match_i][lev_i]-match_lev_groupW[match_i][lev_i-1]);
				}
			}else {
				HPMWeights[match_i]=0;
			}
		}
		return HPMWeights;
	}
	
	public static float[] weightMatchByAngle(ArrayList<MatchFeat_VW> goodMatches, SURFpoint[] interestPoints_Q, 
			HistMultiD_Sparse_equalSizeBin_forFloat<Pair_int> hist, float PointDisThr, float badPariWeight, float weightThr, 
			float lineAngleStep, float lineDistStep, int QueryMaxSize, int DocMaxSize, int sameLinePointNumThr, 
			boolean disp){
		
		hist.iniHist(); //initialise hist
		int total = goodMatches.size();
		long startTime=System.currentTimeMillis();
		//pick final gold matches, romove same-line effect
		boolean isLineDetection=lineDistStep>0.0001;//use lineDistStep to mark wether needs line detection
		HashSet<Integer> oneLinePoints_q=null; HashSet<Integer> oneLinePoints_d=null;
		boolean isSameLineCheck_Q=false, isSameLineCheck_D=false;
		if (isLineDetection) {
			LineDetection detectLine_Query=new LineDetection(lineAngleStep, new float[]{-QueryMaxSize,QueryMaxSize,QueryMaxSize*lineDistStep});
			LineDetection detectLine_Doc=new LineDetection(lineAngleStep, new float[]{-DocMaxSize,DocMaxSize,DocMaxSize*lineDistStep});
			for (int i = 0; i < goodMatches.size(); i++) {//i is the index in goodMatches
				int ind=i;
				SURFpoint qFeat=interestPoints_Q[goodMatches.get(ind).QFeatInd];
				SURFpoint dFeat=goodMatches.get(ind).docFeat.getSURFpoint();
				detectLine_Query.addOneSample(null, new Point_XY(ind, qFeat.x, qFeat.y));
				detectLine_Doc.addOneSample(null, new Point_XY(ind, dFeat.x, dFeat.y));
			}
			oneLinePoints_q=Point_XY.getUniPoints(detectLine_Query.getMaxBinDeSamples());
			oneLinePoints_d=Point_XY.getUniPoints(detectLine_Doc.getMaxBinDeSamples());
			isSameLineCheck_Q=oneLinePoints_q.size()>sameLinePointNumThr;
			isSameLineCheck_D=oneLinePoints_d.size()>sameLinePointNumThr;
			General.dispInfo_ifNeed(disp, "\t --", "time for lineDetection in Q&D:"+General.dispTime(System.currentTimeMillis()-startTime, "ms"));
		}else {
			General.dispInfo_ifNeed(disp, "\t --", "no lineDetection in Q&D, time:"+General.dispTime(System.currentTimeMillis()-startTime, "ms"));
		}
		//make pair angle hist
		int sameLinePari=0; 
		for(int i=0; i < total; i++) {
			for(int j=i+1; j < total; j++) {
				if ((isSameLineCheck_Q && oneLinePoints_q.contains(i) && oneLinePoints_q.contains(j))
						|| (isSameLineCheck_D && oneLinePoints_d.contains(i) && oneLinePoints_d.contains(j))) {//i,j is not both from the major line
					sameLinePari++;
				}else {
					//i,j is a pair in goodMatches, and is two point in docImage, so they generate a vector!
					float[] vector0=new float[]{goodMatches.get(i).docFeat.getSURFpoint().x-goodMatches.get(j).docFeat.getSURFpoint().x,
							goodMatches.get(i).docFeat.getSURFpoint().y-goodMatches.get(j).docFeat.getSURFpoint().y};
					float[] vector1=new float[]{interestPoints_Q[goodMatches.get(i).QFeatInd].x-interestPoints_Q[goodMatches.get(j).QFeatInd].x,
							interestPoints_Q[goodMatches.get(i).QFeatInd].y-interestPoints_Q[goodMatches.get(j).QFeatInd].y};
					if ((Math.abs(vector0[0])>PointDisThr || Math.abs(vector0[1])>PointDisThr)
							&& (Math.abs(vector1[0])>PointDisThr || Math.abs(vector1[1])>PointDisThr)) {//two point is not so close
						//get vectorLength
						double vectorLength_0=Math.sqrt(General.vectorInnerMut(vector0,vector0));
						double vectorLength_1=Math.sqrt(General.vectorInnerMut(vector1,vector1));
						//get angle
						float angle=General.vectorRotationAngle(vector0, vector1, vectorLength_0, vectorLength_1);//(-pi~pi]
						//get scale
						float scale=(float) Math.log10(vectorLength_0/vectorLength_1);
						//add to hist
						hist.addOneSample(new float[]{angle,scale},new Pair_int(i, j));
					}
				}
			}
		}
		//find good points
		float[] res=new float[goodMatches.size()];
		int goldPoints=0; 
		if (hist.getSampleNum()>5) {
			ArrayList<Pair_int> goodPair=hist.getMaxBinDeSamples(); //only use maxBin's point pair as good candidate
			HashMap<Integer, Integer> pointFreq=Pair_int.CountPointFreq(goodPair);
			int otherPointsNum=pointFreq.size()-1;//remove the point itselft
			for (Entry<Integer, Integer> point_freq : pointFreq.entrySet()) {
				float weight=(float) (point_freq.getValue()-(otherPointsNum-point_freq.getValue())*badPariWeight);	
				if (weight>weightThr) {
					res[point_freq.getKey()]=weight;
					goldPoints++;
				}
			}
			if (disp) {
				System.out.println("\t Info in weightMatchByAngle: "+hist.makeRes("0.00", false, false));
				System.out.println("\t "+goodPair.size()+" goodPairs");
				System.out.println("\t pointInd_Freq in goodPair: "+pointFreq);
				System.out.println("\t in tot-"+pointFreq.size()+" unique points from goodPair.");
			}
		}else if (disp) {
			System.out.println("\t Info in weightMatchByAngle: "+hist.makeRes("0.00", false, false));
			System.out.println("too few pairs in Hist, so no furher check, res=0!");
		}
		//show info
		if (disp) {
			System.out.println("\t Info in weightMatchByAngle: ");
			if (isLineDetection) {
				System.out.println("\t do pair-wise angle check, within these points, samelinePoints in Q/D:"+oneLinePoints_q.size()+"/"+oneLinePoints_d.size()
						+", takingIntoAccount this sameline effect, sameLinePari:"+sameLinePari+" in tot-"+goodMatches.size()*(goodMatches.size()-1)/2+" pairs"
						+", final pari-wise angel selected pointNum:"+goldPoints);
			}else {
				System.out.println("\t do pair-wise angle check, no lineDetection, in tot-"+goodMatches.size()*(goodMatches.size()-1)/2+" pairs"
						+", final pari-wise angel selected pointNum:"+goldPoints);
			}
		}
		return res;
	}
	
	public static MatchingScore weightMatchByHistAndAngle(ArrayList<MatchFeat_VW> goldMatches, SURFpoint[] interestPoints_Q, boolean isUPRightFeat,
			HistMultiD_Sparse_equalSizeBin_forFloat<Integer> hist, float binScaleRate, float PointDisThr, float badPariWeight, float weightThr, 
			float lineAngleStep, float lineDistStep, int QueryMaxSize, int DocMaxSize, int sameLinePointNumThr, boolean disp, 
			LinkedList<SURFpoint> selPoints_ls, LinkedList<SURFpoint> selPoints_rs) throws InterruptedException{
		long startTime=System.currentTimeMillis();
		int goodBinInd=hist.getMaxBin_ind_val()[0];
		//********** scheme 1 ************
		//directly use gold matches, when calculat final score, res_hist is combined with HMweight,idf..
		float[] res_hist=new float[goldMatches.size()]; 
		for (int i = 0; i < goldMatches.size(); i++) {
			res_hist[i]=1;
		}
		//********** scheme 2 ************
		//refine these good points by pari-wise angle or scale
		float[] res_angle=new float[goldMatches.size()]; 
		float[][] goodBins_rot_sca=hist.getOneBinRang(goodBinInd);
		for (float[] oneBin : goodBins_rot_sca) {//allow to use different bins size for HV and PG
			General.scaleArr(oneBin, binScaleRate);
		}
		if (isUPRightFeat) {//when feat is upRight, then rotation is turned off, so the rotation bin found by HV is useless!
			float step_half=hist.getOneDim_BegEndStp(0)[2]/2;
			goodBins_rot_sca[0]=new float[]{-step_half,step_half};//-step/2 ~ step/2
		}
		int total_goldMatches = goldMatches.size(); int sameLinePari=0; int finalSelNum=0;
		if (total_goldMatches>2) {
			//pick final gold matches, romove same-line effect
			startTime=System.currentTimeMillis();
			boolean isLineDetection=lineDistStep>0.0001;//use lineDistStep to mark wether needs line detection
			HashSet<Integer> oneLinePoints_q=null; HashSet<Integer> oneLinePoints_d=null;
			boolean isSameLineCheck_Q=false, isSameLineCheck_D=false;
			if (isLineDetection) {
				LineDetection detectLine_Query=new LineDetection(lineAngleStep, new float[]{-QueryMaxSize,QueryMaxSize,QueryMaxSize*lineDistStep});
				LineDetection detectLine_Doc=new LineDetection(lineAngleStep, new float[]{-DocMaxSize,DocMaxSize,DocMaxSize*lineDistStep});
				for (int i = 0; i < goldMatches.size(); i++) {//i is the index in goodMatches
					SURFpoint qFeat=interestPoints_Q[goldMatches.get(i).QFeatInd];
					SURFpoint dFeat=goldMatches.get(i).docFeat.getSURFpoint();
					detectLine_Query.addOneSample(null, new Point_XY(i, qFeat.x, qFeat.y));
					detectLine_Doc.addOneSample(null, new Point_XY(i, dFeat.x, dFeat.y));
				}
				oneLinePoints_q=Point_XY.getUniPoints(detectLine_Query.getMaxBinDeSamples());
				oneLinePoints_d=Point_XY.getUniPoints(detectLine_Doc.getMaxBinDeSamples());
				isSameLineCheck_Q=oneLinePoints_q.size()>sameLinePointNumThr;
				isSameLineCheck_D=oneLinePoints_d.size()>sameLinePointNumThr;
				General.dispInfo_ifNeed(disp, "\t --", "time for lineDetection in Q&D:"+General.dispTime(System.currentTimeMillis()-startTime, "ms"));
			}else {
				General.dispInfo_ifNeed(disp, "\t --", "no lineDetection in Q&D, time:"+General.dispTime(System.currentTimeMillis()-startTime, "ms"));
			}
			//add sameLine Points back for check
			if (selPoints_ls!=null) {
				//***** 1st, add gold matches points **********
				//left
				selPoints_ls.clear();
				for (MatchFeat_VW one : goldMatches) {
					selPoints_ls.add(interestPoints_Q[one.QFeatInd]);
				}
				//right
				selPoints_rs.clear();
				for (MatchFeat_VW one : goldMatches) {
					selPoints_rs.add(one.docFeat.getSURFpoint());
				}
				//***** 2st, add same line ponit ***************
				if (isLineDetection) {
					//left
					selPoints_ls.clear();
					for (int ind : oneLinePoints_q) {
						selPoints_ls.add(interestPoints_Q[goldMatches.get(ind).QFeatInd]);
					}
					//right
					selPoints_rs.clear();
					for (int ind : oneLinePoints_d) {
						selPoints_rs.add(goldMatches.get(ind).docFeat.getSURFpoint());
					}
				}
			}
			//make pair angle hist
			startTime=System.currentTimeMillis();
			for(int i=0; i < total_goldMatches; i++) {
				for(int j=i+1; j < total_goldMatches; j++) {
					//i,j is a pair in goodMatches, and is two point in docImage, so they generate a vector, vector0 in doc, vector1 in query!
					if ((isSameLineCheck_Q && oneLinePoints_q.contains(i) && oneLinePoints_q.contains(j))
							|| (isSameLineCheck_D && oneLinePoints_d.contains(i) && oneLinePoints_d.contains(j))) {//i,j is not both from the major line
						sameLinePari++;
					}else {
						float[] vector0=new float[]{goldMatches.get(i).docFeat.getSURFpoint().x-goldMatches.get(j).docFeat.getSURFpoint().x,
								goldMatches.get(i).docFeat.getSURFpoint().y-goldMatches.get(j).docFeat.getSURFpoint().y}; //doc vector
						float[] vector1=new float[]{interestPoints_Q[goldMatches.get(i).QFeatInd].x-interestPoints_Q[goldMatches.get(j).QFeatInd].x,
								interestPoints_Q[goldMatches.get(i).QFeatInd].y-interestPoints_Q[goldMatches.get(j).QFeatInd].y};//query vector
						if ((Math.abs(vector0[0])>PointDisThr || Math.abs(vector0[1])>PointDisThr)
								&& (Math.abs(vector1[0])>PointDisThr || Math.abs(vector1[1])>PointDisThr)) {//two point is not so close
							//get vectorLength
							double vectorLength_0=Math.sqrt(General.vectorInnerMut(vector0,vector0));
							double vectorLength_1=Math.sqrt(General.vectorInnerMut(vector1,vector1));
							//get angle
							float angle=General.vectorRotationAngle(vector1, vector0, vectorLength_1, vectorLength_0);//from query vector_1 to doc vector_0, (-pi~pi]
							//get scale
							float scale=(float) Math.log10(vectorLength_0/vectorLength_1);
							//judge goodness
							if (General.isInRange(angle, goodBins_rot_sca[0]) && General.isInRange(scale, goodBins_rot_sca[1])) {
								res_angle[i]++; res_angle[j]++;
							}else {
								res_angle[i]-=badPariWeight; res_angle[j]-=badPariWeight;
							}
						}
					}
				}
			}
			General.dispInfo_ifNeed(disp, "\t --", "time for pair angle check:"+General.dispTime(System.currentTimeMillis()-startTime, "ms"));
			//threhoding and getFinalScore
			for (int i = 0; i < res_angle.length; i++) {
				if (res_angle[i]>weightThr) {
					finalSelNum++;
				}else {
					res_angle[i]=0;
				}
			}
			//show info
			if (disp) {
				System.out.println("\t Info in weightMatchByHistAndAngle: ");
				if (isLineDetection) {
					System.out.println("\t do pair-wise angle check, within these points, samelinePoints in Q/D:"+oneLinePoints_q.size()+"/"+oneLinePoints_d.size()
							+", takingIntoAccount this sameline effect, sameLinePari:"+sameLinePari+" in tot-"+total_goldMatches*(total_goldMatches-1)/2+" pairs"
							+", final pari-wise angel selected pointNum:"+finalSelNum);
				}else {
					System.out.println("\t do pair-wise angle check, no lineDetection, in tot-"+total_goldMatches*(total_goldMatches-1)/2+" pairs"
							+", final pari-wise angel selected pointNum:"+finalSelNum);
				}
			}
		}else if (disp) {
			System.out.println("\t Info in weightMatchByHistAndAngle: ");
			System.out.println("\t no pair-wise angle check! as hist selected matches is less than 3");
		}
		return new MatchingScore(res_angle,res_hist, finalSelNum);
	}
	
	public static ArrayList<MatchFeat_VW> selectOriMatch(HashMap<Short, ArrayList<MatchFeat_VW>>  matchCandidates) throws InterruptedException{
		ArrayList<MatchFeat_VW> goodMatches=new ArrayList<MatchFeat_VW>(); 
		for (Entry<Short, ArrayList<MatchFeat_VW>> oneQi : matchCandidates.entrySet()) {
			for (MatchFeat_VW oneDj:oneQi.getValue()) {
    			goodMatches.add(oneDj);
			}
		}
		return goodMatches;
	}
	
	/**
	 * only works for the case of 1 to many mapping
	 */
	public static <K, T extends FeatInd_Score> ArrayList<T> select1V1Match_for1vsM_basedOnScore(Map<K, ArrayList<T>> matchCandidates) {
		//find 1vs1 matches from 1 to many mapping
		ArrayList<T> goodMatches=new ArrayList<T>();
		for (Entry<K, ArrayList<T>> one1toM : matchCandidates.entrySet()) {
			T bestDj=findBestDj_maxScore(one1toM.getValue()); //dj,maxScore
			goodMatches.add(bestDj);
		}
		return goodMatches;
	}
	
	public static <T extends FeatInd_Score> ArrayList<T> select1V1Match_basedOnScore(HashMap<Short, ArrayList<T>> matchCandidates) {
		//find 1vs1 matches
		ArrayList<T> goodMatches=new ArrayList<T>(); int qi_num=matchCandidates.size();
    	if (qi_num!=0) {
    		if (qi_num==1) {//only 1 in qis
    			short qi =matchCandidates.keySet().toArray(new Short[0])[0];
    			T bestDj=findBestDj_maxScore(matchCandidates.get(qi)); //dj,maxScore
    			goodMatches.add(bestDj);
			}else {
	    		//rank qi based on link num, 
	    		for (int i = 0; i < qi_num; i++) {
	    			Entry<Short, ArrayList<T>> current=null; int minlinkNum=Integer.MAX_VALUE;
	    			for (Entry<Short, ArrayList<T>> oneQi : matchCandidates.entrySet()) {
						if (minlinkNum>oneQi.getValue().size()) {
							minlinkNum=oneQi.getValue().size();
							current=oneQi;
						}
					}
	    			if (minlinkNum==1) {
	    				float maxScore=Float.MIN_VALUE; 
	        			for (Entry<Short, ArrayList<T>> oneQi : matchCandidates.entrySet()) {
	    					if (minlinkNum==oneQi.getValue().size()) {//only 1 link
	    						float thisMaxScore=oneQi.getValue().get(0).getScore();
	    						if (maxScore<thisMaxScore) {
	    							maxScore=thisMaxScore;
	    							current=oneQi;
	    						}
	    					}
	    				}
					}
	    			short qi=current.getKey();
	    			if (!current.getValue().isEmpty()) {//this qi has matches!
	    				T bestDj=findBestDj_maxScore(matchCandidates.get(qi)); //dj,minHammingDist,vwInd
	        			goodMatches.add(bestDj);
	    				//remove qi
	    				matchCandidates.remove(qi);
	    				//remove dj
	    				for (Entry<Short, ArrayList<T>> oneQi : matchCandidates.entrySet()) {
	    					int toRemoveDjInd=-1;
	    					for(int k=0; k<oneQi.getValue().size();k++){
	    	    				if (oneQi.getValue().get(k).getFeatInd() == bestDj.getFeatInd() ) {
	    	    					toRemoveDjInd=k;
	    	    					break;
	    						}
	    	    			}
	    					if (toRemoveDjInd>-1) {//this oneQi contains dj
	    						oneQi.getValue().remove(toRemoveDjInd);
							}
						}
					}else{//attention: if empty, shoud not break! the rest may still have match exist!
						matchCandidates.remove(qi);
					}
				}
			}
		}
    	return goodMatches;
	}
	
	public static ArrayList<MatchFeat_VW> select1V1Match_basedOnScore(DocAllMatchFeats docMatches, float[] idf, float[] hammingW, int queryID, boolean disp) {
		int thisDocID=docMatches.DocID;
		//make QFeatInd links
		HashMap<Short, ArrayList<MatchFeat_VW_matchScore>> matchCandidates=new HashMap<Short, ArrayList<MatchFeat_VW_matchScore>>();
		int vwNum=0; int totHMmatchNum=0;
		for(Int_MatchFeatArr oneVW_matches:docMatches.feats.feats){// loop over all vw_MatchFeats
			int vw=oneVW_matches.Integer;
			MatchFeat[] matchFeats=oneVW_matches.feats.getArr();
			for (MatchFeat oneMatchFeat:matchFeats) {
				short queryFeatInd=oneMatchFeat.QFeatInd;
				float matchScore=idf[vw]*hammingW[oneMatchFeat.HMDist];
    			if (matchCandidates.containsKey(queryFeatInd)) {
    				matchCandidates.get(queryFeatInd).add(new MatchFeat_VW_matchScore(new MatchFeat_VW(oneMatchFeat,vw),matchScore));
				}else {
					ArrayList<MatchFeat_VW_matchScore> newList=new ArrayList<MatchFeat_VW_matchScore>();
					newList.add(new MatchFeat_VW_matchScore(new MatchFeat_VW(oneMatchFeat,vw),matchScore));
					matchCandidates.put(queryFeatInd, newList);
				}
			}
			vwNum++;
			totHMmatchNum+=matchFeats.length;
		}
		//find 1vs1 matches
		ArrayList<MatchFeat_VW_matchScore> goodMatches_temp =select1V1Match_basedOnScore(matchCandidates);
		//return
		ArrayList<MatchFeat_VW> goodMatches=new ArrayList<MatchFeat_VW>(goodMatches_temp.size());
		for (MatchFeat_VW_matchScore one : goodMatches_temp) {
			goodMatches.add(one.matchFeat_VW);
		}
		//for debug
		if (disp==true) {
			System.out.println("\t show one example for 1VS1 check of one doc, queryID:"+queryID+", thisDocID:"+thisDocID+", their matches:");
//					for (Entry<Integer, ArrayList<MatchFeat_VW>> oneQFeatMatches : matcheCandidates.entrySet()) {
//						System.out.println("\t --QFeat ind:"+oneQFeatMatches.getKey());
//						for (MatchFeat_VW oneMatch : oneQFeatMatches.getValue()) {
//							System.out.println("\t --   one match, vw:"+oneMatch.vw+", HMDist:"+oneMatch.HMDist+", QFeatInd:"+oneMatch.QFeatInd+", DFeatInd:"+oneMatch.docFeat.getFeatInd());
//						}
//					}
			System.out.println("\t total matched vws number: "+vwNum+", tot match Num: "+totHMmatchNum);
			System.out.println("\t 1vs1 matches: "+goodMatches.size());
		}
    	return goodMatches;
	}
	
	public static ArrayList<MatchFeat_VW> select1V1Match_basedOnScore(int thisDocID, ArrayList<MatchFeat_VW> docMatches, float[] idf, float[] hammingW, int queryID, boolean disp) {
		//make QFeatInd links
		HashMap<Short, ArrayList<MatchFeat_VW_matchScore>> matchCandidates=new HashMap<Short, ArrayList<MatchFeat_VW_matchScore>>();
		int totMatchNum=0;
		for(MatchFeat_VW oneMatch:docMatches){// loop over all vw_MatchFeats
			short queryFeatInd=oneMatch.QFeatInd;
			float matchScore=idf[oneMatch.vw]*hammingW[oneMatch.HMDist];
			if (matchCandidates.containsKey(queryFeatInd)) {
				matchCandidates.get(queryFeatInd).add(new MatchFeat_VW_matchScore(oneMatch,matchScore));
			}else {
				ArrayList<MatchFeat_VW_matchScore> newList=new ArrayList<MatchFeat_VW_matchScore>();
				newList.add(new MatchFeat_VW_matchScore(oneMatch,matchScore));
				matchCandidates.put(queryFeatInd, newList);
			}
			totMatchNum++;
		}
		//for debug
		if (disp==true) {
			System.out.println("\t show one example for 1VS1 check of one doc, queryID:"+queryID+", thisDocID:"+thisDocID+", their matches:");
//			for (Entry<Integer, ArrayList<MatchFeat_VW>> oneQFeatMatches : matcheCandidates.entrySet()) {
//				System.out.println("\t --QFeat ind:"+oneQFeatMatches.getKey());
//				for (MatchFeat_VW oneMatch : oneQFeatMatches.getValue()) {
//					System.out.println("\t --   one match, vw:"+oneMatch.vw+", HMDist:"+oneMatch.HMDist+", QFeatInd:"+oneMatch.QFeatInd+", DFeatInd:"+oneMatch.docFeat.getFeatInd());
//				}
//			}
			System.out.println("\t tot match Num: "+totMatchNum);
		}
		//find 1vs1 matches
		ArrayList<MatchFeat_VW_matchScore> goodMatches_temp =select1V1Match_basedOnScore(matchCandidates);
		//return
		ArrayList<MatchFeat_VW> goodMatches=new ArrayList<MatchFeat_VW>(goodMatches_temp.size());
		for (MatchFeat_VW_matchScore one : goodMatches_temp) {
			goodMatches.add(one.matchFeat_VW);
		}
    	return goodMatches;
	}
	
	public static ArrayList<MatchFeat_VW> select1V1Match_basedOnDist(HashMap<Short, ArrayList<MatchFeat_VW>> matchCandidates) {
		ArrayList<MatchFeat_VW> goodMatches=new ArrayList<MatchFeat_VW>(); int qi_num=matchCandidates.size();
    	if (qi_num!=0) {
    		if (qi_num==1) {//only 1 in qis
    			short qi =matchCandidates.keySet().toArray(new Short[0])[0];
    			MatchFeat_VW bestDj=findBestDj(qi, matchCandidates); //dj,minHammingDist,vwInd
    			goodMatches.add(bestDj);
			}else {
	    		//rank qi based on link num, 
	    		for (int i = 0; i < qi_num; i++) {
	    			Entry<Short, ArrayList<MatchFeat_VW>> current=null; int minlinkNum=Integer.MAX_VALUE;
	    			for (Entry<Short, ArrayList<MatchFeat_VW>> oneQi : matchCandidates.entrySet()) {
						if (minlinkNum>oneQi.getValue().size()) {
							minlinkNum=oneQi.getValue().size();
							current=oneQi;
						}
					}
	    			if (minlinkNum==1) {
	    				int minHammingDist=Integer.MAX_VALUE;
	        			for (Entry<Short, ArrayList<MatchFeat_VW>> oneQi : matchCandidates.entrySet()) {
	    					if (minlinkNum==oneQi.getValue().size()) {//only 1 link
	    						short thisHammingDist=oneQi.getValue().get(0).HMDist;
	    						if (thisHammingDist<minHammingDist) {
	    							minHammingDist=thisHammingDist;
	    							current=oneQi;
	    						}
	    					}
	    				}
					}
	    			short qi=current.getKey();
	    			if (!current.getValue().isEmpty()) {//this qi has matches!
	    				MatchFeat_VW bestDj=findBestDj(qi, matchCandidates); //dj,minHammingDist,vwInd
	        			goodMatches.add(bestDj);
	    				//remove qi
	    				matchCandidates.remove(qi);
	    				//remove dj
	    				for (Entry<Short, ArrayList<MatchFeat_VW>> oneQi : matchCandidates.entrySet()) {
	    					int toRemoveDjInd=-1;
	    					for(int k=0; k<oneQi.getValue().size();k++){
	    	    				if (oneQi.getValue().get(k).docFeat.getFeatInd() == bestDj.docFeat.getFeatInd() ) {
	    	    					toRemoveDjInd=k;
	    	    					break;
	    						}
	    	    			}
	    					if (toRemoveDjInd>-1) {//this oneQi contains dj
	    						oneQi.getValue().remove(toRemoveDjInd);
							}
						}
					}else{//attention: if empty, shoud not break! the rest may still have match exist!
						matchCandidates.remove(qi);
					}
				}
			}
		}
    	return goodMatches;
	}
	
	public static ArrayList<MatchFeat_VW> select1V1Match_basedOnDist(DocAllMatchFeats docMatches, int queryID, boolean disp) {
		//rank by 1vs1
		int thisDocID=docMatches.DocID;
		HashMap<Short,ArrayList<MatchFeat_VW>>  matcheCandidates = new HashMap<Short,ArrayList<MatchFeat_VW>>(); 
		int vwNum=0; int totHMmatchNum=0;
		for(Int_MatchFeatArr oneVW_matches:docMatches.feats.feats){// loop over all vw_MatchFeats
			int vw=oneVW_matches.Integer;
			MatchFeat[] matchFeats=oneVW_matches.feats.getArr();
			for (MatchFeat oneMatchFeat:matchFeats) {
				short queryFeatInd=oneMatchFeat.QFeatInd;
    			if (matcheCandidates.containsKey(queryFeatInd)) {
    				matcheCandidates.get(queryFeatInd).add(new MatchFeat_VW(oneMatchFeat,vw));
				}else {
					ArrayList<MatchFeat_VW> newList=new ArrayList<MatchFeat_VW>();
					newList.add(new MatchFeat_VW(oneMatchFeat,vw));
					matcheCandidates.put(queryFeatInd, newList);
				}
			}
			vwNum++;
			totHMmatchNum+=matchFeats.length;
		}
		//select 1vs1 matches
		ArrayList<MatchFeat_VW> selectedMatches=General_BoofCV.select1V1Match_basedOnDist(matcheCandidates);
		//for debug
		if (disp==true) {
			System.out.println("\t show one example for 1VS1 check of one doc, queryID:"+queryID+", thisDocID:"+thisDocID+", their matches:");
//					for (Entry<Integer, ArrayList<MatchFeat_VW>> oneQFeatMatches : matcheCandidates.entrySet()) {
//						System.out.println("\t --QFeat ind:"+oneQFeatMatches.getKey());
//						for (MatchFeat_VW oneMatch : oneQFeatMatches.getValue()) {
//							System.out.println("\t --   one match, vw:"+oneMatch.vw+", HMDist:"+oneMatch.HMDist+", QFeatInd:"+oneMatch.QFeatInd+", DFeatInd:"+oneMatch.docFeat.getFeatInd());
//						}
//					}
			System.out.println("\t total matched vws number: "+vwNum+", tot match Num among all vws: "+totHMmatchNum);
			System.out.println("\t 1vs1 matches: "+selectedMatches.size());
		}
		return selectedMatches;
	}
	
	public static ArrayList<MatchFeat_VW> select1V1Match_basedOnDist(int thisDocID, ArrayList<MatchFeat_VW> docMatches, int queryID, boolean disp) {
		//rank by 1vs1
		HashMap<Short,ArrayList<MatchFeat_VW>>  matcheCandidates = new HashMap<Short,ArrayList<MatchFeat_VW>>(); 
		int totMatchNum=0;
		for(MatchFeat_VW oneMatch:docMatches){// loop over all vw_MatchFeats
			short queryFeatInd=oneMatch.QFeatInd;
			if (matcheCandidates.containsKey(queryFeatInd)) {
				matcheCandidates.get(queryFeatInd).add(oneMatch);
			}else {
				ArrayList<MatchFeat_VW> newList=new ArrayList<MatchFeat_VW>();
				newList.add(oneMatch);
				matcheCandidates.put(queryFeatInd, newList);
			}
			totMatchNum++;
		}
		//for debug
		if (disp==true) {
			System.out.println("\t show one example for 1VS1 check of one doc, queryID:"+queryID+", thisDocID:"+thisDocID+", their matches:");
//			for (Entry<Integer, ArrayList<MatchFeat_VW>> oneQFeatMatches : matcheCandidates.entrySet()) {
//				System.out.println("\t --QFeat ind:"+oneQFeatMatches.getKey());
//				for (MatchFeat_VW oneMatch : oneQFeatMatches.getValue()) {
//					System.out.println("\t --   one match, vw:"+oneMatch.vw+", HMDist:"+oneMatch.HMDist+", QFeatInd:"+oneMatch.QFeatInd+", DFeatInd:"+oneMatch.docFeat.getFeatInd());
//				}
//			}
			System.out.println("\t tot match Num: "+totMatchNum);
		}
		//select 1vs1 matches
		ArrayList<MatchFeat_VW> selectedMatches=General_BoofCV.select1V1Match_basedOnDist(matcheCandidates);
		return selectedMatches;
	}
	
	public static ArrayList<MatchFeat_VW> select1V1Match_optimAssign(HashMap<Short, ArrayList<MatchFeat_VW>>  matchCandidates) throws InterruptedException{
		/**
		 * an optimal way to find 1vs1 matches by solve the "assignment problem" using Hungarian algorithm in time O(n^3)
		 */
		ArrayList<MatchFeat_VW> goodMatches=new ArrayList<MatchFeat_VW>(); int qi_num=matchCandidates.size();
    	if (qi_num!=0) {
    		if (qi_num==1) {//only 1 in qis
    			short qi =matchCandidates.keySet().toArray(new Short[0])[0];
    			MatchFeat_VW bestDj=findBestDj(qi, matchCandidates); //dj,minHammingDist,vwInd
    			goodMatches.add(bestDj);
			}else {
				ArrayList<Short> qis=new ArrayList<Short>(); HashSet<Short> djs_hashSet=new HashSet<Short>();
				for (Entry<Short, ArrayList<MatchFeat_VW>> oneQi : matchCandidates.entrySet()) {
					qis.add(oneQi.getKey());
					for (MatchFeat_VW oneDj : oneQi.getValue()) {
						djs_hashSet.add(oneDj.docFeat.getFeatInd());
					}
				}
				//make cost matrix
		    	ArrayList<Short> djs=new ArrayList<Short>(djs_hashSet);
		    	double[][] costMatrix=new double[qis.size()][djs.size()];
		    	//initial costMatrix to toMarkNoLink
		    	int toMarkNoLink=Integer.MAX_VALUE;//maximum HM dist is the bit number of HESig
		    	for (int i = 0; i < costMatrix.length; i++) {
					for (int j = 0; j < costMatrix[0].length; j++) {
						costMatrix[i][j]=toMarkNoLink;
					}
				}
		    	//save link dist
		    	for (Entry<Short, ArrayList<MatchFeat_VW>> oneQi : matchCandidates.entrySet()) {
		    		int qi=oneQi.getKey();
		    		int qi_Ind=qis.indexOf(qi);
					for (MatchFeat_VW oneDj : oneQi.getValue()) {
						int dj=oneDj.docFeat.getFeatInd();
						int dj_Ind=djs.indexOf(dj);
						costMatrix[qi_Ind][dj_Ind]=oneDj.HMDist;//use dist as cost
					}
				}
		    	//find assignment 
		    	HungarianAlgorithm assignProb_hug=new HungarianAlgorithm(costMatrix);
		    	int[] assig=assignProb_hug.execute();
		    	//make hmWeights
		    	for (int i = 0; i < assig.length; i++) {
					if (assig[i]!=-1) {//this qi do have assignment!
						if (costMatrix[i][assig[i]]!=toMarkNoLink) {//if costMatrix[i][assig[i]]==toMarkNoLink: qi have assignment, but this is a "unreal-link", because the HungarianAlgorithm must provide assignment for nodes
							int qi=qis.get(i);
							int dj=djs.get(assig[i]);
							MatchFeat_VW bestDj=null;
							for (MatchFeat_VW oneDj : matchCandidates.get(qi)) {
								if (oneDj.docFeat.getFeatInd()==dj) {
									bestDj=oneDj;
									break;
								}
							}
							//update hmScore
							if (bestDj==null) {
								throw new InterruptedException("err in select1V1Match_optimAssign, for qi:"+qi+", no corresponding dj:"+dj);
							}else {
								goodMatches.add(bestDj);
							}
						}
					}
				}
			}
		}
		return goodMatches;
	}

	public static ArrayList<MatchFeat_VW> selectHistMatch(ArrayList<MatchFeat_VW> matches, SURFpoint[] interestPoints_Q, HistMultiD_Sparse_equalSizeBin_forFloat<Integer> hist, boolean isReturn, boolean disp){
		//calculate transfer from query to doc, select matches that are in the major transfer bin
		hist.iniHist(); //initialise hist
		int total = matches.size();
		//make pair angle hist
		for(int i=0; i < total; i++) {
			SURFpoint docPoint=matches.get(i).docFeat.getSURFpoint();
			SURFpoint queryPoint=interestPoints_Q[matches.get(i).QFeatInd];
			//rotation
			float rotation=makeRotationTo_NPtoPP(docPoint.angle-queryPoint.angle);
			//scale
			float scale=(float)Math.log10(docPoint.scale/queryPoint.scale);
			//add to hist
			hist.addOneSample(new float[]{rotation,scale}, i);//use point index i in goodMatches as sample 
		}
		if (disp) {
			int goodBinInd=hist.getMaxBin_ind_val()[0];
			System.out.println("\t Info in selectHistMatch: "+hist.makeRes("0.00", false, false));
			System.out.println("\t in tot-"+total+" goodMatches, hist selected "+hist.getOneBinDeSamples(goodBinInd).size()+" goldMatches, "
			+"in bin: "+hist.getOneBinRang_inString(goodBinInd));
		}
		if (isReturn) {
			ArrayList<Integer> goldMatches_ind=hist.getMaxBinDeSamples(); //only use maxBin's point as good candidate
			ArrayList<MatchFeat_VW> goldMatches=new ArrayList<MatchFeat_VW>(goldMatches_ind.size()*2);
			for (int ind : goldMatches_ind) {
				goldMatches.add(matches.get(ind));
			}
			return goldMatches;
		}else {
			return null;
		}
	}
	
	public static MatchFeat_VW findBestDj(short qi, HashMap<Short, ArrayList<MatchFeat_VW>>  matchCandidates){
		short minHammingDist=Short.MAX_VALUE; 
		MatchFeat_VW bestOne=null;
		for(MatchFeat_VW one_dj_HMDist : matchCandidates.get(qi)){
			if (one_dj_HMDist.HMDist < minHammingDist) {
				minHammingDist=one_dj_HMDist.HMDist; 
				bestOne=one_dj_HMDist;
			}
		}
		return bestOne;
	}
	
	public static <T extends FeatInd_Score> T findBestDj_maxScore(ArrayList<T>  matchCandidates){
		float maxScore=-1; 
		T bestOne=null;
		for(T one_match : matchCandidates){
			if (one_match.getScore() > maxScore) {
				maxScore=one_match.getScore(); 
				bestOne=one_match;
			}
		}
		return bestOne;
	}
	
	public static HESig[] SURFfeats_to_HESigs(SURFfeat[] surfFeats){
		HESig[] HESigs=new HESig[surfFeats.length];
		for (int i = 0; i < HESigs.length; i++) {
			HESigs[i]=new HESig(surfFeats[i]);
		}
		return HESigs;
	}
	
	public static int compare_HESigs(int vw, SURFfeat[] Sigs_A, SURFfeat[] Sigs_B, int HMDistThr, HashMap<Integer,ArrayList<MatchFeat_VW>>  matcheCandidates) throws InterruptedException{
		//get match link: featInd_A,[featInd_B,hammingDist]
		int matchNum=0; 
		int start_HESig=0; int end_HESig=Sigs_A[0].getHESig().length;
    	for(int qi=0;qi<Sigs_A.length;qi++){
    		boolean existMatch=false; ArrayList<MatchFeat_VW> onePointMatches=new ArrayList<MatchFeat_VW>(Sigs_A.length);
    		for(int dj=0;dj<Sigs_B.length;dj++){
				int hammingDist=General.get_DiffBitNum(Sigs_A[qi].getHESig(), Sigs_B[dj].getHESig(), start_HESig, end_HESig);// computing time: 15% of BigInteger!!
//				int hammingDist=(new BigInteger(querySigs.get(ei))).xor(new BigInteger(TSigs[i][dj])).bitCount();
				if(hammingDist<=HMDistThr){
					onePointMatches.add(new MatchFeat_VW((short) hammingDist, (short) Sigs_A[qi].getFeatInd(), new SURFfeat_noSig(Sigs_B[dj]),vw));//vw rang is short! 
					existMatch=true;
					matchNum++;
				}
			}
    		if (existMatch) {
    			int featInd_A=Sigs_A[qi].getFeatInd();
    			if (matcheCandidates.containsKey(featInd_A)) {
    				matcheCandidates.get(featInd_A).addAll(onePointMatches);
				}else {
					matcheCandidates.put(featInd_A, onePointMatches);
				}
			}
		}
		return matchNum;
	}
	
	public static float makeRotationTo_NPtoPP(float angleDif) {
		/**angleDif here is in the form of -2pi~2pi
		 * make oritaion differenct from [-2pi~2pi] to (-pi~pi], 
		 * (pi~2pi] is corresponding to (-pi~0]
		 * [-2pi~-pi] is corresponding to [0~pi]
		 */
		if (angleDif>Math.PI) {
			return (float) (angleDif-2*Math.PI);
		}else if (angleDif<=-Math.PI) {
			return (float) (angleDif+2*Math.PI);
		}else {
			return angleDif;
		}		
	}
	
	public static ArrayList<MatchFeat> compare_HESigs(SURFfeat[] feats_A, SURFfeat[] feats_B, int HMDistThr) throws InterruptedException{
		//get MatchFeat: short HMDist, int QFeatInd, SURFfeat_noSig docFeat;
		int start_HESig=0; int end_HESig=feats_A[0].getHESig().length;
		ArrayList<MatchFeat> MatchFeats=new ArrayList<MatchFeat>();
		if (end_HESig==HMDistThr) {//this for pure BOF
			for(int qi=0;qi<feats_A.length;qi++){
	    		for(int dj=0;dj<feats_B.length;dj++){
	    			MatchFeats.add(new MatchFeat((short) 0, (short) feats_A[qi].getFeatInd(), new SURFfeat_noSig(feats_B[dj])));
	    		}
			}
		}else {
			for(int qi=0;qi<feats_A.length;qi++){
	    		for(int dj=0;dj<feats_B.length;dj++){
					int hammingDist=General.get_DiffBitNum(feats_A[qi].getHESig(), feats_B[dj].getHESig(), start_HESig, end_HESig);// computing time: 15% of BigInteger!!
//					int hammingDist=(new BigInteger(querySigs.get(ei))).xor(new BigInteger(TSigs[i][dj])).bitCount();
					if(hammingDist<=HMDistThr){
						MatchFeats.add(new MatchFeat((short) hammingDist, (short) feats_A[qi].getFeatInd(), new SURFfeat_noSig(feats_B[dj])));
					}
				}
			}
		}
    	
    	if (MatchFeats.size()!=0) {
    		return MatchFeats;
		}else {
			return null;
		}
	}
	
	public static int compare_HESigs_1vs1_matrix(int vw, SURFfeat[] Sigs_A, SURFfeat[] Sigs_B, int HMDistThr, HashMap<Integer,ArrayList<MatchFeat_VW>>  matcheCandidates) throws InterruptedException{
		//get match link 
		int oriMatchNum=0; //original match num without 1vs1 check!
		int start_HESig=0; int end_HESig=Sigs_A[0].getHESig().length;
		short[][] qi_dj_dist =new short[Sigs_A.length][Sigs_B.length]; ArrayList<Integer> qis=new ArrayList<Integer>(); ArrayList<Integer> qi_matchNum=new ArrayList<Integer>();
    	for(int qi=0;qi<Sigs_A.length;qi++){
    		int existMatchNum=0;
    		for(int dj=0;dj<Sigs_B.length;dj++){
    			int hammingDist=General.get_DiffBitNum(Sigs_A[qi].getHESig(), Sigs_B[dj].getHESig(), start_HESig, end_HESig);// computing time: 15% of BigInteger!!
//				int hammingDist=(new BigInteger(querySigs.get(ei))).xor(new BigInteger(TSigs[i][dj])).bitCount();
				if(hammingDist<=HMDistThr){
					qi_dj_dist[qi][dj]=(short) hammingDist;
					existMatchNum++;
				}else {
					qi_dj_dist[qi][dj]=Short.MAX_VALUE;
				}
			}
    		if (existMatchNum>0) {
    			qis.add(qi);
    			qi_matchNum.add(existMatchNum);
    			oriMatchNum+=existMatchNum;
			}
		}
    	
    	if (qis.size()!=0) {
    		if (qis.size()==1) {//only 1 in qis
    			int qi=qis.get(0);
    			int dj=-1; short hmDist_min=Short.MAX_VALUE;
    			for(int dj_c=0; dj_c< qi_dj_dist[qi].length; dj_c++){
    				if (qi_dj_dist[qi][dj_c] < hmDist_min) {
    					dj=dj_c;
    					hmDist_min=qi_dj_dist[qi][dj_c];
					}
    			}
    			//add match
    			MatchFeat_VW oneMatch=new MatchFeat_VW(qi_dj_dist[qi][dj], (short) Sigs_A[qi].getFeatInd(), new SURFfeat_noSig(Sigs_B[dj]),vw);
    			int featInd_A=Sigs_A[qi].getFeatInd();
    			if (matcheCandidates.containsKey(featInd_A)) {
    				matcheCandidates.get(featInd_A).add(oneMatch);
				}else {
					ArrayList<MatchFeat_VW> onePointMatches=new ArrayList<MatchFeat_VW>(Sigs_A.length);
					onePointMatches.add(oneMatch);
					matcheCandidates.put(featInd_A, onePointMatches);
				}
			}else {
				//rank qi based on link num, 
	    		int existQi=qis.size(); int[] qi_matchNum_intArr=General.ListToIntArr(qi_matchNum);
	    		for (int i = 0; i < existQi; i++) {
	    			int minInd= General.getMin_ind_val(qi_matchNum_intArr)[0];
	    			if (qi_matchNum_intArr[minInd]!=0) {
	    				//find link: qi, dj, hmScore
	    				int qi=qis.get(minInd);
	    				if (qi_matchNum_intArr[minInd]==1) {//link num ==1
	    					short hmDist_min=Short.MAX_VALUE;
	        				for (int j = 0; j < qis.size(); j++) {
	        					if (qi_matchNum_intArr[j]==1) {//only 1 link
	        						for (int j2 = 0; j2 < qi_dj_dist[qis.get(j)].length; j2++) {
	        							if (qi_dj_dist[qis.get(j)][j2]<hmDist_min) {
	        								hmDist_min=qi_dj_dist[qis.get(j)][j2];
	            							qi=qis.get(j);
	            							break;
	            						}
	    							}    						
	        					}
	    					}
	        				minInd=qis.indexOf(qi);
	    				}
	    				int dj=-1; short hmDist_min=Short.MAX_VALUE;
	        			for(int dj_c=0; dj_c< qi_dj_dist[qi].length; dj_c++){
	        				if (qi_dj_dist[qi][dj_c] < hmDist_min) {
	        					dj=dj_c;
	        					hmDist_min=qi_dj_dist[qi][dj_c];
	    					}
	        			}
	        			//add match
	        			MatchFeat_VW oneMatch=new MatchFeat_VW(qi_dj_dist[qi][dj], (short) Sigs_A[qi].getFeatInd(), new SURFfeat_noSig(Sigs_B[dj]),vw);
	        			int featInd_A=Sigs_A[qi].getFeatInd();
	        			if (matcheCandidates.containsKey(featInd_A)) {
	        				matcheCandidates.get(featInd_A).add(oneMatch);
	    				}else {
	    					ArrayList<MatchFeat_VW> onePointMatches=new ArrayList<MatchFeat_VW>(Sigs_A.length);
	    					onePointMatches.add(oneMatch);
	    					matcheCandidates.put(featInd_A, onePointMatches);
	    				}
	        			//remove dj from qi_dj_dist, update qi_matchNum_intArr
	        			for (int qi_c = 0; qi_c < qi_dj_dist.length; qi_c++) {
	        				if (qi_dj_dist[qi_c][dj]!=Short.MAX_VALUE) {
	        					qi_dj_dist[qi_c][dj]=Short.MAX_VALUE;
	        					qi_matchNum_intArr[qis.indexOf(qi_c)]-=1;
	    					}
	    				}
	        			//remove qi
	        			qi_matchNum_intArr[minInd]=Integer.MAX_VALUE;
					}else{//attention: if empty, shoud not break! the rest may still have match exist!
						//remove qi
	        			qi_matchNum_intArr[minInd]=Integer.MAX_VALUE;
					}
	    		}
			}
		}
		return oriMatchNum;
	}
	
	public static MatchFeat_Arr compare_HESigs(HESig[] feats_A, SURFfeat[] feats_B, int HMDistThr) throws InterruptedException{
		//get MatchFeat: short HMDist, int QFeatInd, SURFfeat_noSig docFeat;
		int start_HESig=0; int end_HESig=feats_A[0].getHESig().length;
		ArrayList<MatchFeat> MatchFeats=new ArrayList<MatchFeat>();
    	for(int qi=0;qi<feats_A.length;qi++){
    		for(int dj=0;dj<feats_B.length;dj++){
				int hammingDist=General.get_DiffBitNum(feats_A[qi].getHESig(), feats_B[dj].getHESig(), start_HESig, end_HESig);// computing time: 15% of BigInteger!!
//				int hammingDist=(new BigInteger(querySigs.get(ei))).xor(new BigInteger(TSigs[i][dj])).bitCount();
				if(hammingDist<=HMDistThr){
					MatchFeats.add(new MatchFeat((short) hammingDist, (short) feats_A[qi].getFeatInd(), new SURFfeat_noSig(feats_B[dj])));
				}
			}
		}
    	if (MatchFeats.size()!=0) {
    		return new MatchFeat_Arr(MatchFeats);
		}else {
			return null;
		}
	}
	
	public static <T extends I_HESig & FeatInd> float compare_HESigs(T[] feats_A, SURFfeat[] feats_B, int HMDistThr, float[] hammingW, List<MatchFeat> MatchFeats) throws InterruptedException{
		//get MatchFeat: short HMDist, int QFeatInd, SURFfeat_noSig docFeat;
		int start_HESig=0; int end_HESig=feats_A[0].getHESig().length;
		float HMScore=0; boolean isNeedMatches=(MatchFeats!=null);
		if (end_HESig==HMDistThr) {//this for pure BOF
			HMScore=feats_A.length*feats_B.length;
			if (isNeedMatches) {
				for(int qi=0;qi<feats_A.length;qi++){
					short qFeatInd=(short) feats_A[qi].getFeatInd();
		    		for(int dj=0;dj<feats_B.length;dj++){
		    			MatchFeats.add(new MatchFeat((short) 0, qFeatInd, new SURFfeat_noSig(feats_B[dj])));
		    		}
				}
			}
		}else{//this for HE
			if (isNeedMatches) {
				for(int qi=0;qi<feats_A.length;qi++){
					short qFeatInd=(short) feats_A[qi].getFeatInd();
		    		for(int dj=0;dj<feats_B.length;dj++){
						int hammingDist=General.get_DiffBitNum(feats_A[qi].getHESig(), feats_B[dj].getHESig(), start_HESig, end_HESig);// computing time: 15% of BigInteger!!
						if(hammingDist<=HMDistThr){
							HMScore+=hammingW[hammingDist];
							MatchFeats.add(new MatchFeat((short) hammingDist, qFeatInd, new SURFfeat_noSig(feats_B[dj])));
						}
					}
				}
			}else{
				for(int qi=0;qi<feats_A.length;qi++){
		    		for(int dj=0;dj<feats_B.length;dj++){
						int hammingDist=General.get_DiffBitNum(feats_A[qi].getHESig(), feats_B[dj].getHESig(), start_HESig, end_HESig);// computing time: 15% of BigInteger!!
						if(hammingDist<=HMDistThr){
							HMScore+=hammingW[hammingDist];
						}
					}
				}
			}
		}
    	return HMScore;
	}
	
	/**
	 * 2015_IJCV_Image Search with Selective Match Kernels: Aggregation Across Single and Multiple Images
	 * eq.7: 2h(a, b) = B(1 − <a, b>h)
	 * HESim is [1,-2/B,-1] cossponding to HEDist 0~B, and it is >0 if hammingDist < B/2;
	 */
	public static double computeHESim_fromHESig(byte[] sigA, byte[] sigB, double[] HEDistWeights){
		int hammingDist=General.get_DiffBitNum(sigA, sigB);
//		return 1-((double)hammingDist/(sigA.length*4));//1-((double)2/(sigA.length*8))*(hammingDist)
		return HEDistWeights[hammingDist];
	}
	
	public static void showMatchingPoint(ArrayList<SURFpoint> pointsA, ArrayList<SURFpoint> pointsB, List<ImageRegionMatch> matches, 
			BufferedImage A, BufferedImage B, String title, int RGBInd, double[] scalingInfo_matchStrength, int pointEnlargeFactor){
		// display the results
		AssociationPanel_DIY panel = new AssociationPanel_DIY(20,pointEnlargeFactor);
		panel.setAssociation(pointsA, pointsB, matches, RGBInd, scalingInfo_matchStrength);
		panel.setImages(A,B);
		ShowImages.showWindow(panel, title);
	}
	
	public static void showImage(BufferedImage img, String caption){
		ShowImages.showWindow(img, caption);
	}
	
	public static void showFeaturePoint(ArrayList<SURFpoint> interestPoints, BufferedImage img, String title, int pointEnlargeFactor){
		// display the result for one img
		LinkedList<LinkedList<SURFpoint>> points=new LinkedList<LinkedList<SURFpoint>>();
		points.add(new LinkedList<SURFpoint>(interestPoints));
		showFeaturePoint(points, null, new LinkedList<Color>(Arrays.asList(new Color[]{Color.GREEN})), img, img, title, pointEnlargeFactor);
	}
	
	public static void showFeaturePoint(List<SURFpoint_Weight> pointsA, List<PointLink> pointsLink_l, List<SURFpoint_Weight> pointsB, List<PointLink> pointsLink_r, 
			BufferedImage A, BufferedImage B, String title, int pointEnlargeFactor, int RGBInd, double[] scalingInfo_pointWeight, double[] scalingInfo_pointLink, boolean isDrawOritation) throws InterruptedException{
		// display the results
		ShowPointsPanel panel = new ShowPointsPanel(20, pointEnlargeFactor, isDrawOritation);
		panel.setPoints(pointsA, pointsLink_l, pointsB, pointsLink_r, RGBInd, scalingInfo_pointWeight, scalingInfo_pointLink);
		panel.setImages(A,B);
		ShowImages.showWindow(panel, title);
	}
	
	public static void showFeaturePoint(LinkedList<LinkedList<SURFpoint>> pointsA, LinkedList<LinkedList<SURFpoint>> pointsB, LinkedList<Color> setColors,
			BufferedImage A, BufferedImage B, String title, int pointEnlargeFactor){
		// display the results
		ShowPointsPanel_Group panel = new ShowPointsPanel_Group(20,pointEnlargeFactor);
		panel.setPoints(pointsA, pointsB, setColors);
		panel.setImages(A,B);
		ShowImages.showWindow(panel, title);
	}
	
	public static ArrayList<Point2D_F64> SURFpoint_to_Point2D_F64 (ArrayList<SURFpoint> pointsA){
		ArrayList<Point2D_F64> points=new ArrayList<Point2D_F64>(pointsA.size());
		for (SURFpoint one:pointsA) {
			points.add(new Point2D_F64(one.x,one.y));
		}
		return points;
	}
	
	// Look at http://chunter.tistory.com/143 for information
	public static AffineTransform getExifTransformation_for1norm(ImageInformation info) {
		//make Transformation info that all image's rotation is 1, normal view
	    AffineTransform t = new AffineTransform();
	    
	    switch (info.orientation) {
	    case 1:
	        break;
	    case 2: // Flip X
	        t.scale(-1.0, 1.0);
	        t.translate(-info.width, 0);
	        break;
	    case 3: // PI rotation 
	        t.translate(info.width, info.height);
	        t.rotate(Math.PI);
	        break;
	    case 4: // Flip Y
	        t.scale(1.0, -1.0);
	        t.translate(0, -info.height);
	        break;
	    case 5: // - PI/2 and Flip X
	        t.rotate(-Math.PI / 2);
	        t.scale(-1.0, 1.0);
	        break;
	    case 6: // -PI/2 and -width
	        t.translate(info.height, 0);
	        t.rotate(Math.PI / 2);
	        break;
	    case 7: // PI/2 and Flip
	        t.scale(-1.0, 1.0);
	        t.translate(-info.height, 0);
	        t.translate(0, info.width);
	        t.rotate(  3 * Math.PI / 2);
	        break;
	    case 8: // PI / 2
	        t.translate(0, info.width);
	        t.rotate(  3 * Math.PI / 2);
	        break;
	    }
	    return t;
	}

	public static BufferedImage transformImage_to1normOritation(BufferedImage image, ImageInformation info) throws IOException {
	    AffineTransformOp op = new AffineTransformOp(getExifTransformation_for1norm(info), AffineTransformOp.TYPE_BICUBIC);
	    BufferedImage destinationImage = op.createCompatibleDestImage(image,  (image.getType() == BufferedImage.TYPE_BYTE_GRAY)? image.getColorModel() : null );
	    Graphics2D g = destinationImage.createGraphics();
	    g.setBackground(Color.WHITE);
	    g.clearRect(0, 0, destinationImage.getWidth(), destinationImage.getHeight());
	    destinationImage = op.filter(image, destinationImage); 
	    //show img
	    General_BoofCV.showImage(image, "oriImage");
	    General_BoofCV.showImage(destinationImage, "normOritation");
	    return destinationImage;
	}
	
	public static BufferedImage convertTo3BandColorBufferedImage(BufferedImage sourceImg) {
		// write data into an RGB buffered image, no transparency, 
		//because someTimes, scource BufferedImage contains 4th band: alpha channel(transparency), when Java's ImageIO.write to jpg, it ignores alpha channel, make image looks pink color
	    BufferedImage resImage = new BufferedImage(sourceImg.getWidth(),
	    		sourceImg.getHeight(), BufferedImage.TYPE_INT_RGB);
	    for (int i = 0; i < sourceImg.getWidth(); i++) {
			for (int j = 0; j < sourceImg.getHeight(); j++) {
				resImage.setRGB(i, j, sourceImg.getRGB(i, j));
			}
		}
	    return resImage;
	}
	
	public static BufferedImage getUniOrintaionImg(BufferedImage image, File imgFile, BufferedInputStream inputStream, Disp disp) throws IOException, InterruptedException {
		//this ImageIO.read did not check oritation info
		//read oritation info from metadata
		ImageInformation info = null;
		try {
			info = General_imgMeta.readImageInformation(imgFile, inputStream);
		} catch (InterruptedException e) {
			disp.disp("warn, return ori image, because InterruptedException for "+imgFile+", e: "+e);
			return image;
		} catch (NullPointerException e) {
			disp.disp( "warn, return ori image, because NullPointerException for "+imgFile+", e: "+e);
			return image;
		} 
		//rotate image to norm
		if (info.orientation!=1 && info.orientation!=0) {//1 is the norm oritation, 0 is for oritatation unknow.
			image=transformImage_to1normOritation(image, info);
		}
		return image;
	}
	
	public static int[] drawInterestPoint(Graphics2D g2 , SURFpoint point, double imageReSizeScale, int shift_x, int shift_y, Color color, int pointEnlargeSize, boolean isDrawOritation){
		int point_x=point.x; int point_y=point.y; int point_scale=(int) point.scale; float point_oritation=point.angle;
		//adjust point with imageReSizeScale, shift_x, shift_y
		point_x=(int) (point.x*imageReSizeScale+shift_x);
		point_y=(int) (point.y*imageReSizeScale+shift_y);
		point_scale=(int) (point.scale*imageReSizeScale*pointEnlargeSize);			
		//draw point as circle
		g2.setColor(color); g2.setStroke(new BasicStroke(2));
		int w = (int) (point_scale*2+1);
		g2.drawOval(point_x-point_scale, point_y-point_scale, w, w);
		g2.fillOval(point_x-1, point_y-1, 3, 3);
		//draw oritation as a line 
		if (isDrawOritation) {
			g2.setColor(Color.BLUE);
			int dx = (int)(Math.cos(point_oritation)*point_scale);
			int dy = (int)(Math.sin(point_oritation)*point_scale);
			g2.drawLine(point_x, point_y, point_x+dx, point_y+dy);
		}
		return new int[]{point_x,point_y,point_scale};
	}
	
	public static void drawPointLink(Graphics2D g2 , SURFpoint_onlyLoc pointA, SURFpoint_onlyLoc pointB, double imageReSizeScale, int shift_x, int shift_y, Color LinkColor){
		int pointA_x=pointA.x; int pointA_y=pointA.y; int pointB_x=pointB.x; int pointB_y=pointB.y; 
		//adjust point with imageReSizeScale, shift_x, shift_y
		pointA_x=(int) (pointA_x*imageReSizeScale+shift_x);
		pointA_y=(int) (pointA_y*imageReSizeScale+shift_y);
		pointB_x=(int) (pointB_x*imageReSizeScale+shift_x);
		pointB_y=(int) (pointB_y*imageReSizeScale+shift_y);
		//draw link as line
		g2.setColor(LinkColor); g2.setStroke(new BasicStroke(2));
		g2.drawLine(pointA_x, pointA_y, pointB_x, pointB_y);
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		
//		String photoBasePath="O://MediaEval_3185258Images//trainImages_1-3185258//Random_100000//";
//		
//		//test one photo
//		int photoName=900002;
//		String photoPath=photoBasePath+photoName+"_3185258.jpg";
//		File photoFile=new File(photoPath);
//		if (photoFile.exists()) {
//			ImageFloat32 image=General_BoofCV.BoofCV_loadImage(photoFile,ImageFloat32.class);
//			long startTime=System.currentTimeMillis();
//			ArrayList<SURFpoint> interestPoints=new ArrayList<SURFpoint>();
//			double[][] photoFeat=computeSURF_boofCV_09(photoName+"", image,"2,1,5,true","2000,1,9,4,4",interestPoints); //"2,0,5,true","2000,1,9,4,4": 2000 and 1 maylead to bug
//			System.out.println("boofCV_0.15: "+photoName+", feat-point-num:"+photoFeat.length+", interestPoints length: "+interestPoints.size()+", time:"+General.dispTime(System.currentTimeMillis()-startTime, "ms"));
//		
//		}
				
//		//extract SURF features
//		for (int i = 900001; i < 900001+10; i++) {
//			long startTime=System.currentTimeMillis();
//			String photoPath=photoBasePath+i+"_3185258.jpg";
//			File photoFile=new File(photoPath);
//			if (photoFile.exists()) {
//				ImageFloat32 image=BoofCV_loadImage(photoFile,ImageFloat32.class);
//				double[][] photoFeat=General_BoofCV.computeSURF(image,"2,1,5,true","2000,1,9,4,4"); //"2,1,5,true","2000,1,9,4,4": 2000 and 1 maylead to bug
//				System.out.println(i+"-th photo, feat-point-num:"+photoFeat.length+", extracion time: "+General.dispTime(System.currentTimeMillis()-startTime, "ms"));
//			}
//		}
		
		
		
//		saveImageUInt8ToPGM_BoofCV(rgbToGray_BoofCV(convertTo3BandColorBufferedImage(img)),photoName+"_resize.pgm");
		
//		saveBufImg_to_PPM(convertTo3BandColorBufferedImage(img), photoName+"_resize.ppm");
		
//		//ImageIO suppor format
//		String[] writerNames = ImageIO.getWriterFormatNames();
//		for (String writerFormatName : writerNames) {
//			System.out.println(writerFormatName);
//		}
		
//		//load color image, transfer to gray, and save to pgm
//		BufferedImage colorImage=ImageIO.read(new File("P:/Ori-Data/ConceptDetector/vireo374feature_bow/vireo374feature_bow/images/3227351075_d3938cb575.jpg")); 
//		if(colorImage.getWidth()>400){
//			float ratio=(float)400/colorImage.getWidth();
//			colorImage=General.getScaledInstance(colorImage, (int)(colorImage.getWidth()*ratio), (int)(colorImage.getHeight()*ratio), RenderingHints.VALUE_INTERPOLATION_BILINEAR, true);
//		}
//		ImageUInt8 img_gray = BoofCV_rgbToGray(colorImage);
//		boofCV_saveImageUInt8ToPGM(img_gray,"P:/Ori-Data/ConceptDetector/vireo374feature_bow/vireo374feature_bow/images/3227351075_d3938cb575_gray.pgm");
//		BufferedImage grayImage_BufferedImage = ConvertBufferedImage.convertTo(img_gray, null);
//		ImageIO.write(grayImage_BufferedImage, "jpg", new File("P:/Ori-Data/ConceptDetector/vireo374feature_bow/vireo374feature_bow/images/3227351075_d3938cb575_gray.jpg"));
		
	}

}
