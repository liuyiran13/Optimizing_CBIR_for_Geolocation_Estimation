package QDP;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;

import MyLibSVM.svm;
import MyLibSVM.svm_model;
import MyLibSVM.svm_node;
import MyLibSVM.svm_parameter;
import MyLibSVM.svm_problem;

import MyAPI.General.General;
import MyAPI.General.General_LibSVM;
import MyAPI.General.General_Weka;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.functions.LibSVM;
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;
import weka.filters.Filter;
import weka.filters.unsupervised.instance.Normalize;


public class QDP_predictor {
public static void main(String[] args) throws Exception {

		
		int random=100*1000; String label;
		
		String servePath="D:/xinchaoli/Desktop/My research/My Code/DataSaved/"; //  /  is ok for both linux and windows!!
//		String servePath="/tudelft.net/staff-bulk/ewi/mm/D-MIRLab/XinchaoLi/";	
		
		String basePath=servePath+"ICMR2013/QDP/";	
		
		String feat_Path=basePath+"Feat/";

      	//set feat path
      	int visScale=500; double geoExpanScale=0.01;
 		label="_VisScal_"+visScale+"_expScal_"+geoExpanScale;  //"_ori"  "_VisScal_"+visScale+"_expScal_"+geoExpanScale
 		String[] className={"CorrectQuery","InCorrectQuery"};
 		//set log
 		PrintWriter log_testResult = new PrintWriter(new OutputStreamWriter(new FileOutputStream(basePath+"testResult"+label, false), "UTF-8"),true); 
 		
 		//form train test 
 		int[] train_test_ratio={3,1,1}; //train develop test
 		String[] instansName={"train", "develop", "test"};
 		svm_problem[] datas= General_LibSVM.makeData_from_MapFile_intArr_floatArr(feat_Path+"Feat_GVR_random"+random+label ,train_test_ratio);
 		svm_problem data_train=datas[0]; svm_problem data_dev=datas[1];  svm_problem data_test=datas[2]; 
 		General.dispInfo(log_testResult, "data_train finished: total "+data_train.l+" samples");
 		General.dispInfo(log_testResult, "data_dev finished: total "+data_dev.l+" samples");
 		General.dispInfo(log_testResult, "data_test finished: total "+data_test.l+" samples");

 		int[] C2Power_range_Linear = {-10,  15, 1}; //C -5,  15, 2, G -15, 3, 2, We found that trying exponentially growing sequences of C and is apractical method to identify good parameters (for example, C = 2^-5,2^-3....2^15, G=2^-15,2^-13,....2^3
 		int[] C_range_Arry=General.makeRange(C2Power_range_Linear);
 		for(int loop_i=0;loop_i<C_range_Arry.length;loop_i++){
 			double Cost=C_range_Arry[loop_i];
 			//set libSVM parameters
 	 		svm_parameter param = new svm_parameter();
 		    param.probability = 1;
 		    param.gamma = 0.5;
 		    param.nu = 0.5;
 		    param.C = Math.pow(2, Cost);
 		    param.svm_type = svm_parameter.C_SVC;
 		    param.kernel_type = svm_parameter.LINEAR;       
 		    param.cache_size = 200;
 		    param.eps = 0.001;      
 		    
 	 		String checkParam=svm.svm_check_parameter(data_train, param);
 		    if (checkParam==null){
 		    	General.dispInfo(log_testResult,"Parameter is OK! loop_"+loop_i+", C:"+param.C);
 		    	int nr_fold_proModel=5;
 			    svm_model model = svm.svm_train(data_train, param,nr_fold_proModel);
 			    ArrayList<Short> QDP_trueClass=new ArrayList<Short>(); ArrayList<Short> QDP_predClass=new ArrayList<Short>();
 			    svm_problem usedData=data_dev;
 			    for(int i=0;i<usedData.l;i++){
 			    	QDP_predClass.add((short) svm.svm_predict(model,usedData.x[i]));
 			    	QDP_trueClass.add((short) usedData.y[i]);
 			    }
 			    int[][] QDP_ConfMatrix=General.mkConfusionMatrix(QDP_trueClass, QDP_predClass, 2);
 			    General.dispInfo(log_testResult, General.gtInfo_from_ConfusionMatrix(QDP_ConfMatrix, className));
 		    }else{
 		    	General.dispInfo(log_testResult, "Parameter is Incorrect! error message: "+checkParam);
 		    }
 		}
 		log_testResult.close();
	    
//		//scale data
//   		Normalize normalizer=new Normalize();
//   		normalizer.setOptions(weka.core.Utils.splitOptions("-unset-class-temporarily no -S 1 -T 0"));//do not scale class attribute, other attributes to T~T+S
//   		normalizer.setInputFormat(Inst_train); // batch filtering, make train and test compatible
//   		Inst_train=Filter.useFilter(Inst_train, normalizer);
//   		Inst_test=Filter.useFilter(Inst_test, normalizer);
//   		
//   		
//   		//*************** build classifer ********************//
//   		Classifier cModel;Evaluation eTest; double[][] cmMatrix; String info_cfM; String strSummary; double[] bestPars; int nr_fold ; int kernel; double bestC,bestG,bestAccInTrain;
//   		
//   		//1, RandomForest
//   		General.dispInfo(log_testResult, "Classifier: RandomForest"); 
//		cModel = (Classifier)new RandomForest();
//		String[] options = {"-D"};            // If set, classifier is run in debug mode and may output additional info to the console
//		cModel.setOptions(options);     // set the options
//		cModel.buildClassifier(Inst_train);
//		// Test the model
//		eTest = new Evaluation(Inst_train);
//		eTest.evaluateModel(cModel, Inst_train);
//		// Print the result:
//		strSummary = eTest.toSummaryString();
//		General.dispInfo(log_testResult, strSummary);
//		// Get the confusion matrix
//		cmMatrix= eTest.confusionMatrix();
//		info_cfM=General.gtInfo_from_ConfusionMatrix(cmMatrix, className);
//		General.dispInfo(log_testResult, info_cfM);
//   		
////   		//2, LibSVM, C-SVC, RBF
////		General.dispInfo(log_testResult, "Classifier: LibSVM, C-SVC, RBF"); 
////		kernel=2;//RBF kernel
////		//search for the best paremeter
////		nr_fold = 5; // default  5
////		double[] C2Power_range_RBF = {-1,  9, 2}; //C -5,  15, 2, G -15, 3, 2, We found that trying exponentially growing sequences of C and is apractical method to identify good parameters (for example, C = 2^-5,2^-3....2^15, G=2^-15,2^-13,....2^3
////		double[] G2Power_range_RBF = {-20, 1, 2}; 
////		bestPars=General_Weka.selectPraForLibSVM_RBF(Inst_train, nr_fold, C2Power_range_RBF, G2Power_range_RBF,log_testResult);
////		bestC=bestPars[0];  bestG=bestPars[1];  bestAccInTrain=bestPars[2];
////		General.dispInfo(log_testResult, "best Parameter for CSVC RBF is: C="+bestC+", G="+bestG+", with an accuracy: "+bestAccInTrain);
//////		//use default parameter
//////		double bestC=1; double bestG=0.5;System.out.println("no Parameter selection for CSVC RBF, use default: C="+bestC+", G="+bestG+", ");
////		//make classifier
////		cModel = (Classifier)new LibSVM();
////		cModel.setOptions(weka.core.Utils.splitOptions("-S 0 -K "+kernel+" -C "+bestC+" -G "+bestG));     // set the options
////		// -S <int>, Set type of SVM (default: 0), 0 = C-SVC, 1 = nu-SVC, 2 = one-class SVM, 3 = epsilon-SVR, 4 = nu-SVR
////		// -K <int>, Set type of kernel function (default: 2), 0 = linear: u'*v, 1 = polynomial: (gamma*u'*v + coef0)^degree, 2 = radial basis function: exp(-gamma*|u-v|^2), 3 = sigmoid: tanh(gamma*u'*v + coef0)
////		// -G <double>, Set gamma in kernel function (default: 1/k)
////		// -C <double>, Set the parameter C of C-SVC, epsilon-SVR, and nu-SVR (default: 1)
////		cModel.buildClassifier(Inst_train);
////		// Test the model
////		eTest = new Evaluation(Inst_train);
////		eTest.evaluateModel(cModel, Inst_train);
////		// Print the result
////		strSummary = eTest.toSummaryString();
////		General.dispInfo(log_testResult, strSummary);
////		// Get the confusion matrix
////		cmMatrix= eTest.confusionMatrix();
////		info_cfM=General.gtInfo_from_ConfusionMatrix(cmMatrix, className);
////		General.dispInfo(log_testResult, info_cfM);
//		
//	   	//3, LibSVM, C-SVC, Linear
//		General.dispInfo(log_testResult,"Classifier: LibSVM, C-SVC, Linear"); 
//		kernel=0;//Linear kernel
//		//search for the best paremeter
//		nr_fold = 5; // default  5
//		double[] C2Power_range_linear = {-4, 0, 1}; //C -5,  15, 2, We found that trying exponentially growing sequences of C and is apractical method to identify good parameters (for example, C = 2^-5,2^-3....2^15, G=2^-15,2^-13,....2^3
//		bestPars=General_Weka.selectPraForLibSVM_Linear(Inst_train, kernel, nr_fold, C2Power_range_linear,log_testResult);
//		bestC=bestPars[0];  bestAccInTrain=bestPars[1];
//		General.dispInfo(log_testResult,"best Parameter for CSVC Linear is: C="+bestC+", with an accuracy: "+bestAccInTrain);
////		//use default parameter
////		double bestC=1; General.dispInfo(log_testResult,"no Parameter selection for CSVC Linear, use default: C="+bestC);
//		//make classifier
//		cModel = (Classifier)new LibSVM();
//		cModel.setOptions(weka.core.Utils.splitOptions("-S 0 -K "+kernel+" -C "+bestC));     // set the options
//		// -S <int>, Set type of SVM (default: 0), 0 = C-SVC, 1 = nu-SVC, 2 = one-class SVM, 3 = epsilon-SVR, 4 = nu-SVR
//		// -K <int>, Set type of kernel function (default: 2), 0 = linear: u'*v, 1 = polynomial: (gamma*u'*v + coef0)^degree, 2 = radial basis function: exp(-gamma*|u-v|^2), 3 = sigmoid: tanh(gamma*u'*v + coef0)
//		// -G <double>, Set gamma in kernel function (default: 1/k)
//		// -C <double>, Set the parameter C of C-SVC, epsilon-SVR, and nu-SVR (default: 1)
//		cModel.buildClassifier(Inst_train);
//		// Test the model
//		eTest = new Evaluation(Inst_train);
//		eTest.evaluateModel(cModel, Inst_train);
//		// Print the result
//		strSummary = eTest.toSummaryString();
//		General.dispInfo(log_testResult, strSummary);
//		// Get the confusion matrix
//		cmMatrix= eTest.confusionMatrix();
//		info_cfM=General.gtInfo_from_ConfusionMatrix(cmMatrix, className);
//		General.dispInfo(log_testResult, info_cfM);
//		
////		// save classifier, normalizer, attSel.    serialize model
////		weka.core.SerializationHelper.write(basePath+tr_percentage+"_traindata", Inst_train);
////		weka.core.SerializationHelper.write(Path_regionIndex+tr_percentage+"_trainedClassifier", cModel);
////		weka.core.SerializationHelper.write(Path_regionIndex+tr_percentage+"_normalizer", normalizer);
////		// read classifier, normalizer, attSel.     deserialize model
////		cModel = (Classifier) weka.core.SerializationHelper.read(Path_regionIndex+tr_percentage+"_trainedClassifier");
////		normalizer = (Normalize) weka.core.SerializationHelper.read(Path_regionIndex+tr_percentage+"_normalizer");
//		log_testResult.close();
		
	}
}
