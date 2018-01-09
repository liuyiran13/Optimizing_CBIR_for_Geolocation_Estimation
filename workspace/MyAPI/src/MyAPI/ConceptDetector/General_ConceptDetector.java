package MyAPI.ConceptDetector;

import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.imageio.ImageIO;

import org.ejml.data.DenseMatrix64F;
import org.ejml.ops.CommonOps;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;
import MyAPI.General.General_EJML;
import MyAPI.SystemCommand.MySystemCommandExecutor;
import boofcv.struct.image.ImageUInt8;

public class General_ConceptDetector {
	
	public static void main(String[] args) throws IOException {
		String line1;   String basePath="/home/nfs/xinchaoli/Code/Concept_Detector/vireo374feature_bow/";
		String imgName="3828827895_07b6443fa6.jpg";
		String conceptName=args[0]; //Actor  Apartments
		//read vw centers
		ArrayList<double[]> centers=new ArrayList<double[]>(500);
		BufferedReader vw_centers= new BufferedReader(new InputStreamReader(new FileInputStream(basePath+"transferToJava/conceptDet_VW_cen-dog554k-500.txt"), "UTF-8"));
		while ((line1=vw_centers.readLine())!=null) {
			centers.add(General.StrArrToDouArr(line1.split(" ")));
		}
		vw_centers.close();
		//read image
		BufferedImage colorImage=ImageIO.read(new File(basePath+"images/"+imgName)); 
		//load concept-model
		svm_model model = svm.svm_load_model(basePath+conceptName+"_soft500.model");
		//detect
		String binaryPath_Detector=basePath+"Detect";
		String binaryPath_Descriptor=basePath+"ComputeDescriptor";
		String tempFilesPath=basePath+"temp/";
		float pred=-1;
		try {
			pred = detect(colorImage, binaryPath_Detector, binaryPath_Descriptor, tempFilesPath, centers,  model, imgName);
		} catch (InterruptedException e) {
			System.out.println("error when run binary, e:"+e.getMessage());
		}
		System.out.println(imgName+", done! concept-"+conceptName+": "+pred);
	}

	public static float detect(BufferedImage colorImage, String binaryPath_Detector, String binaryPath_Descriptor, String tempFilesPath, ArrayList<double[]> centers, svm_model model, String tmpFileMarker) throws IOException, InterruptedException{
		//softFeat
		float[] softFeat=makeSoftFeat( colorImage,  binaryPath_Detector,  binaryPath_Descriptor,  tempFilesPath,  centers, tmpFileMarker);
		//Prediction
		float pred=predict(softFeat, model);
		return pred;
	} 
	
	public static float[] makeSoftFeat(BufferedImage colorImage, String binaryPath_Detector, String binaryPath_Descriptor, String tempFilesPath, ArrayList<double[]> centers, String tmpFileMarker) throws IOException, InterruptedException{
		String pgmPath=tempFilesPath+tmpFileMarker+".pgm";
		String cornerPath=tempFilesPath+tmpFileMarker+".corner";
		String siftPath=tempFilesPath+tmpFileMarker+".dogsift";
		int vw_num=centers.size(); int vw_dim=centers.get(0).length;	
		int grid=1; 
		//load color image, transfer to gray, and save to pgm
		if(colorImage.getWidth()>400){
			float ratio=(float)400/colorImage.getWidth();
			colorImage=General.getScaledInstance(colorImage, (int)(colorImage.getWidth()*ratio), (int)(colorImage.getHeight()*ratio), RenderingHints.VALUE_INTERPOLATION_BILINEAR, true);
		}
		ImageUInt8 img_gray = General_BoofCV.rgbToGray_BoofCV(colorImage);
		General_BoofCV.saveImageUInt8ToPGM_BoofCV(img_gray,pgmPath);
		int img_x=img_gray.height; int img_y=img_gray.width;
		
		//********** call sift-extractor binary *************//
	    //1--Detect
		List<String> commands = Arrays.asList(binaryPath_Detector, "-dtype", "dog", pgmPath, cornerPath); // build the system command we want to run	    
	    // execute the command
		MySystemCommandExecutor commandExecutor = new MySystemCommandExecutor(commands,true,false);
		commandExecutor.executeCommand(true, "\t", "s",null);
		//2--ComputeDescriptor
		commands = Arrays.asList(binaryPath_Descriptor, "-textout",pgmPath,cornerPath,siftPath); // build the system command we want to run	    
	    // execute the command
		commandExecutor = new MySystemCommandExecutor(commands,true,false);
	    commandExecutor.executeCommand(true, "\t", "s",null);
	    
	    //********** make softFeature *************//
		//readdescwf
	    String line1;
		ArrayList<float[]> pos=new ArrayList<float[]>(400); ArrayList<double[]> desc=new ArrayList<double[]>(400);
		BufferedReader siftFile_content= new BufferedReader(new InputStreamReader(new FileInputStream(siftPath), "UTF-8"));
		while ((line1=siftFile_content.readLine())!=null) {
			if(line1.startsWith("Position (x,y)")){
				pos.add(General.StrArrToFloatArr(line1.split(":")[1].split(",")));
			}else if (line1.startsWith("Descriptor")) {
				String[] temp=line1.split(":")[1].split(", ");
				desc.add(General.StrArrToDouArr(temp));
			}
		}
		siftFile_content.close();
		General.Assert(pos.size()==desc.size(), "pos.size should == des.size, here pos.size:"+pos.size()+", desc.size:"+desc.size());
		//delete temp files
		File toDel=new File(pgmPath);toDel.delete();
		toDel=new File(cornerPath);toDel.delete();
		toDel=new File(siftPath);toDel.delete();
		//make softFeature
		int desc_num=desc.size();
		if(desc_num!=0){
			General.Assert(desc.get(0).length==vw_dim, "desc.get(0).length should == vw_dim, here desc.get(0).length:"+desc.get(0).length+", vw_dim:"+vw_dim);
			//make matrix for centers'
			DenseMatrix64F centers_T_Matrix=new DenseMatrix64F(vw_dim,vw_num);
			for (int i = 0; i < vw_dim; i++) {
				for (int j = 0; j < vw_num; j++) {
					centers_T_Matrix.set(i,j, centers.get(j)[i]);
				}
			}
			//make matrix for desc
			DenseMatrix64F desc_Matrix=new DenseMatrix64F(desc_num,vw_dim);
			for (int i = 0; i < desc_num; i++) {
				for (int j = 0; j < vw_dim; j++) {
					desc_Matrix.set(i,j,desc.get(i)[j]);
				}
			}
			//make grid ID for each point
			int[] block=new int[desc_num];
			float bx=(float)img_x/grid; float by=(float)img_y/grid;
			for (int i = 0; i < desc_num; i++) {
				int xind=(int) (pos.get(i)[0]/by+1);
				int yind=(int) (pos.get(i)[1]/bx+1);
				block[i]=(yind-1)*grid+xind;
			}
			//Calculate similarity with all visual words
			DenseMatrix64F sim= new DenseMatrix64F(desc_Matrix.getNumRows(),centers_T_Matrix.getNumCols());
			CommonOps.mult(desc_Matrix,centers_T_Matrix,sim);
			float[] softFeat=new float[vw_num*grid*grid];
			for (int feat_i = 0; feat_i < desc_num; feat_i++) {
				int sp=(block[feat_i]-1)*vw_num;
				double[] oneSimRow=General_EJML.getOneRow_from_DenseMatrix(sim, feat_i);
				for (int i = 0; i < 4; i++) {
					double[] ind_max=General.getMax_ind_val(oneSimRow); 
					int ind=(int) ind_max[0]; double max=ind_max[1];
					softFeat[sp+ind]+=max*Math.pow(0.5,i);
					oneSimRow[ind]=0;
				}
			}
			return softFeat;
		}else {
			return null;
		}
		
	} 

	public static float predict(float[] softFeat, svm_model model){
		//Prediction
		if (softFeat!=null) {
			svm_node[] x=svm_tools.data_sparse(softFeat);
			double[] prob_estimates=new double[2];
			svm.svm_predict_probability(model,x,prob_estimates);//two classes in concept detector model, 1: have this concept, -1: does not have this concept
			float pred=(float) prob_estimates[0];
			return pred;
		}else {
			return -1;
		}
	} 

}
