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


import boofcv.struct.image.ImageUInt8;

import MyAPI.General.General;
import MyAPI.General.General_BoofCV;
import MyAPI.General.General_EJML;
import MyAPI.SystemCommand.MySystemCommandExecutor;

public class forTest_featureExtraction {
	/**
	 * from jpg to pgm is different with matlab version(picture resize method is different!), but if use the same pgm, the feature from matlab and java is the same!!
	 * the concept score for these two version, (pgm from matlab, pgm from java) are slightly different! +-0.01
	 * 
	 * krenew -s -- java -Xms2048m -Xmx3500m -cp $JAPI/MyAPI.jar:$JAPI/BoofCV.jar:$JAPI/EJML.jar:$JAPI/hadoop-core-0.20.2-cdh3u4.jar:ConceptDetector_featExtraction.jar ConceptDetector.forTest_featureExtraction .pgm Flood
	 */
	
	public static void main(String[] args) throws IOException {
		String line1;  int grid=1; String basePath="/home/nfs/xinchaoli/Code/Concept_Detector/vireo374feature_bow/";
		String imgName="3828827895_07b6443fa6.jpg";
		String forTestPGM=args[0]; //.pgm_Java .pgm
		String conceptName=args[1]; //Actor  Apartments
		//read vw centers
		ArrayList<double[]> centers=new ArrayList<double[]>(500);
		BufferedReader vw_centers= new BufferedReader(new InputStreamReader(new FileInputStream(basePath+"transferToJava/conceptDet_VW_cen-dog554k-500.txt"), "UTF-8"));
		while ((line1=vw_centers.readLine())!=null) {
			centers.add(General.StrArrToDouArr(line1.split(" ")));
		}
		vw_centers.close();
		int vw_num=centers.size(); int vw_dim=centers.get(0).length;	
		//load color image, transfer to gray, and save to pgm
		BufferedImage colorImage=ImageIO.read(new File(basePath+"images/"+imgName)); 
		if(colorImage.getWidth()>400){
			float ratio=(float)400/colorImage.getWidth();
			colorImage=General.getScaledInstance(colorImage, (int)(colorImage.getWidth()*ratio), (int)(colorImage.getHeight()*ratio), RenderingHints.VALUE_INTERPOLATION_BILINEAR, true);
		}
		ImageUInt8 img_gray = General_BoofCV.rgbToGray_BoofCV(colorImage);
		General_BoofCV.saveImageUInt8ToPGM_BoofCV(img_gray,basePath+imgName+".pgm_Java");
		int img_x=img_gray.height; int img_y=img_gray.width;
		//call sift-extractor binary
		String binaryPath=basePath;
	    //1--Detect
		List<String> commands = Arrays.asList(binaryPath+"Detect", "-dtype", "dog",imgName+forTestPGM,imgName+".corner_Java"); // build the system command we want to run	    
	    // execute the command
		MySystemCommandExecutor commandExecutor = new MySystemCommandExecutor(commands,true,false);
		try {
			int result = commandExecutor.executeCommand(true, "\t", "s",null);
			// get the stdout and stderr from the command that was run
		    String stdout = commandExecutor.getStandardOutputFromCommand();
		    String stderr = commandExecutor.getStandardErrorFromCommand();
		    // print the stdout and stderr
		    System.out.println("The numeric result of the command was: " + result);
		    System.out.println("STDOUT:");
		    System.out.println(stdout);
		    System.out.println("STDERR:");
		    System.out.println(stderr);
		} catch (InterruptedException e) {
			System.out.println("InterruptedException, e:"+e.getMessage());
		}
		//2--ComputeDescriptor
		commands = Arrays.asList(binaryPath+"ComputeDescriptor", "-textout",imgName+forTestPGM,imgName+".corner_Java",imgName+".dogsift_Java"); // build the system command we want to run	    
	    // execute the command
		commandExecutor = new MySystemCommandExecutor(commands,true,false);
		try {
			int result = commandExecutor.executeCommand(true, "\t", "s",null);
			// get the stdout and stderr from the command that was run
		    String stdout = commandExecutor.getStandardOutputFromCommand();
		    String stderr = commandExecutor.getStandardErrorFromCommand();
		    // print the stdout and stderr
		    System.out.println("The numeric result of the command was: " + result);
		    System.out.println("STDOUT:");
		    System.out.println(stdout);
		    System.out.println("STDERR:");
		    System.out.println(stderr);
		} catch (InterruptedException e) {
			System.out.println("InterruptedException, e:"+e.getMessage());
		}
		//readdescwf
		ArrayList<float[]> pos=new ArrayList<float[]>(400); ArrayList<double[]> desc=new ArrayList<double[]>(400);
		BufferedReader siftFile_content= new BufferedReader(new InputStreamReader(new FileInputStream(basePath+imgName+".dogsift_Java"), "UTF-8"));
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
		General.Assert(desc.get(0).length==vw_dim, "desc.get(0).length should == vw_dim, here desc.get(0).length:"+desc.get(0).length+", vw_dim:"+vw_dim);
		//make softFeature
		int desc_num=desc.size();
		if(desc_num!=0){
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
			System.out.println(General.floatArrToString(softFeat, " ", "0.00000"));
			//Prediction
			svm_model model = svm.svm_load_model(conceptName+"_soft500.model");
			svm_node[] x=svm_tools.data_sparse(softFeat);
			double[] prob_estimates=new double[2];
			svm.svm_predict_probability(model,x,prob_estimates);//two classes in concept detector model, 1: have this concept, -1: does not have this concept
			System.out.println(imgName+", done! concept-"+conceptName+": "+prob_estimates[0]);
		}
//	    
//	    if m ~= 0
//	        % Calculate similarity with all visual words...        
//	        simi = desc*centers;
//	        for wi = 1:m       
//	            sp = (block(wi)-1)*cn;
//
//	            [Y I]        = max(simi(wi,:));
//	            softtf(sp+I) = softtf(sp+I)+Y;
//	            simi(wi,I)   = 0;
//	            
//	            [Y1 I]       = max(simi(wi,:));
//	            softtf(sp+I) = softtf(sp+I)+Y1*0.5;
//	            simi(wi,I)   = 0;
//	            
//	            [Y2 I]       = max(simi(wi,:));
//	            softtf(sp+I) = softtf(sp+I)+Y2*0.25;
//	            simi(wi,I)   = 0;
//
//	            [Y3 I]       = max(simi(wi,:));
//	            softtf(sp+I) = softtf(sp+I)+Y3*0.125;
//	        end
//	    end
//	    for j = 1:cn*grid*grid
//	        fprintf(fid,'%f ',softtf(j));
//	    end
//	    fprintf(fid,'\n');
//	    fclose(fid);
//	    
//	    fprintf('finish %i out of %i\n',i,length(imglist));
		
	}

}
