package MyAPI.ConceptDetector;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

import MyAPI.General.General;
import MyAPI.SystemCommand.SystemCommandExecutor;

public class forTest_prediction {
	/**
	 *  java svm prediction code is confirmed with binary exe!!
	 *  for the same feature, output is the same!
	 */
	public static void main(String[] args) throws IOException {
		String featureFolder="D:/xinchaoli/Desktop/My research/My Code/Matlab/Concept_Detector/vireo374feature_bow/features/";
		String modelFolder="P:/Ori-Data/ConceptDetector/vireo.cs.cityu.edu.hk/vireodetector/models/model_file_keypoint/";
		String conceptName="Apartments"; //Actor  Apartments
		String imgName="3828827895_07b6443fa6";
		String svmModelName=modelFolder+conceptName+"_soft500.model";
		svm_model model = svm.svm_load_model(svmModelName);
		
		String svmDataName=featureFolder+imgName+".svmData";
		PrintWriter outStr_dataForSVM = new PrintWriter(new OutputStreamWriter(new FileOutputStream(svmDataName,false), "UTF-8")); 
		File oneFeatFile=new File(featureFolder+imgName+".txt"); 
		
		//one photo-local feat
		BufferedReader oneFeatFile_content= new BufferedReader(new InputStreamReader(new FileInputStream(oneFeatFile), "UTF-8"));
		String locFeat=oneFeatFile_content.readLine();
		oneFeatFile_content.close();
		svm_node[] x=svm_tools.data_sparse(General.StrArrToFloatArr(locFeat.split(" ")));
		
		//save feat data in libsvm format
		outStr_dataForSVM.print("999 ");
		for (int j = 0; j < x.length; j++) {
			outStr_dataForSVM.print(x[j].index+":"+x[j].value+" ");
		}
		outStr_dataForSVM.println();
		outStr_dataForSVM.close();
		
		//********** A. prediction using java-svm
		double[] prob_estimates=new double[2];
		svm.svm_predict_probability(model,x,prob_estimates);//two classes in concept detector model, 1: have this concept, -1: does not have this concept
		System.out.println(imgName+", done! concept-"+conceptName+": "+prob_estimates[0]);
		
		//********** B. prediction using binary
		//call prediction binary
		String binaryPath="P:/Ori-Data/ConceptDetector/vireo.cs.cityu.edu.hk/vireodetector/svm_win_binary/svm_win_binary/";
	    //1--Detect
		String svmPredName=svmDataName+".pred";
		List<String> commands = Arrays.asList(binaryPath+"svmpredict-yg.exe", "-b", "1",svmDataName,svmModelName,svmPredName); // build the system command we want to run	    
	    // execute the command
	    SystemCommandExecutor commandExecutor = new SystemCommandExecutor(commands);
		try {
			int result = commandExecutor.executeCommand();
			// get the stdout and stderr from the command that was run
		    StringBuilder stdout = commandExecutor.getStandardOutputFromCommand();
		    StringBuilder stderr = commandExecutor.getStandardErrorFromCommand();
		    // print the stdout and stderr
		    System.out.println("The numeric result of the command was: " + result);
		    System.out.println("STDOUT:");
		    System.out.println(stdout);
		    System.out.println("STDERR:");
		    System.out.println(stderr);
		} catch (InterruptedException e) {
			System.out.println("InterruptedException, e:"+e.getMessage());
		}
	}

}
