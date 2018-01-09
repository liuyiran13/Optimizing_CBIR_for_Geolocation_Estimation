package QDP;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import MyAPI.General.General;
import MyAPI.General.General_Weka;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.functions.LibSVM;
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;
import weka.filters.Filter;
import weka.filters.unsupervised.instance.Normalize;


public class QDP_predictor_wekaVesion {
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
 		int[] train_test_ratio={2,1}; //train Vs. test
 		String[] instansName={"train", "test"};
 		Instances[] wekaInsts=General_Weka.makeWekaInsts_from_MapFile_intArr_floatArr(feat_Path+"Feat_geoExpan_random"+random+label, className, instansName ,train_test_ratio);
 		Instances Inst_train=wekaInsts[0];  Instances Inst_test=wekaInsts[1];
 		General.dispInfo(log_testResult, "Inst_train data finished: total "+Inst_train.numInstances()+" instances, AttNum="+Inst_train.numAttributes()+",Class:"+Inst_train.classAttribute());
 		General.dispInfo(log_testResult, "Inst_test data finished: total "+Inst_test.numInstances()+" instances, AttNum="+Inst_test.numAttributes()+",Class: "+Inst_test.classAttribute());

		//scale data
   		Normalize normalizer=new Normalize();
   		normalizer.setOptions(weka.core.Utils.splitOptions("-unset-class-temporarily no -S 1 -T 0"));//do not scale class attribute, other attributes to T~T+S
   		normalizer.setInputFormat(Inst_train); // batch filtering, make train and test compatible
   		Inst_train=Filter.useFilter(Inst_train, normalizer);
   		Inst_test=Filter.useFilter(Inst_test, normalizer);
   		
   		
   		//*************** build classifer ********************//
   		Classifier cModel;Evaluation eTest; double[][] cmMatrix; String info_cfM; String strSummary; double[] bestPars; int nr_fold ; int kernel; double bestC,bestG,bestAccInTrain;
   		
   		//1, RandomForest
   		General.dispInfo(log_testResult, "Classifier: RandomForest"); 
		cModel = (Classifier)new RandomForest();
		String[] options = {"-D"};            // If set, classifier is run in debug mode and may output additional info to the console
		cModel.setOptions(options);     // set the options
		cModel.buildClassifier(Inst_train);
		// Test the model
		eTest = new Evaluation(Inst_train);
		eTest.evaluateModel(cModel, Inst_train);
		// Print the result:
		strSummary = eTest.toSummaryString();
		General.dispInfo(log_testResult, strSummary);
		// Get the confusion matrix
		cmMatrix= eTest.confusionMatrix();
		info_cfM=General.gtInfo_from_ConfusionMatrix(cmMatrix, className);
		General.dispInfo(log_testResult, info_cfM);
   		
//   		//2, LibSVM, C-SVC, RBF
//		General.dispInfo(log_testResult, "Classifier: LibSVM, C-SVC, RBF"); 
//		kernel=2;//RBF kernel
//		//search for the best paremeter
//		nr_fold = 5; // default  5
//		double[] C2Power_range_RBF = {-1,  9, 2}; //C -5,  15, 2, G -15, 3, 2, We found that trying exponentially growing sequences of C and is apractical method to identify good parameters (for example, C = 2^-5,2^-3....2^15, G=2^-15,2^-13,....2^3
//		double[] G2Power_range_RBF = {-20, 1, 2}; 
//		bestPars=General_Weka.selectPraForLibSVM_RBF(Inst_train, nr_fold, C2Power_range_RBF, G2Power_range_RBF,log_testResult);
//		bestC=bestPars[0];  bestG=bestPars[1];  bestAccInTrain=bestPars[2];
//		General.dispInfo(log_testResult, "best Parameter for CSVC RBF is: C="+bestC+", G="+bestG+", with an accuracy: "+bestAccInTrain);
////		//use default parameter
////		double bestC=1; double bestG=0.5;System.out.println("no Parameter selection for CSVC RBF, use default: C="+bestC+", G="+bestG+", ");
//		//make classifier
//		cModel = (Classifier)new LibSVM();
//		cModel.setOptions(weka.core.Utils.splitOptions("-S 0 -K "+kernel+" -C "+bestC+" -G "+bestG));     // set the options
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
		
	   	//3, LibSVM, C-SVC, Linear
		General.dispInfo(log_testResult,"Classifier: LibSVM, C-SVC, Linear"); 
		kernel=0;//Linear kernel
		//search for the best paremeter
		nr_fold = 5; // default  5
		int[] C2Power_range_linear = {-4, 0, 1}; //C -5,  15, 2, We found that trying exponentially growing sequences of C and is apractical method to identify good parameters (for example, C = 2^-5,2^-3....2^15, G=2^-15,2^-13,....2^3
		bestPars=General_Weka.selectPraForLibSVM_Linear(Inst_train, kernel, nr_fold, C2Power_range_linear,log_testResult);
		bestC=bestPars[0];  bestAccInTrain=bestPars[1];
		General.dispInfo(log_testResult,"best Parameter for CSVC Linear is: C="+bestC+", with an accuracy: "+bestAccInTrain);
//		//use default parameter
//		double bestC=1; General.dispInfo(log_testResult,"no Parameter selection for CSVC Linear, use default: C="+bestC);
		//make classifier
		cModel = (Classifier)new LibSVM();
		cModel.setOptions(weka.core.Utils.splitOptions("-S 0 -K "+kernel+" -C "+bestC));     // set the options
		// -S <int>, Set type of SVM (default: 0), 0 = C-SVC, 1 = nu-SVC, 2 = one-class SVM, 3 = epsilon-SVR, 4 = nu-SVR
		// -K <int>, Set type of kernel function (default: 2), 0 = linear: u'*v, 1 = polynomial: (gamma*u'*v + coef0)^degree, 2 = radial basis function: exp(-gamma*|u-v|^2), 3 = sigmoid: tanh(gamma*u'*v + coef0)
		// -G <double>, Set gamma in kernel function (default: 1/k)
		// -C <double>, Set the parameter C of C-SVC, epsilon-SVR, and nu-SVR (default: 1)
		cModel.buildClassifier(Inst_train);
		// Test the model
		eTest = new Evaluation(Inst_train);
		eTest.evaluateModel(cModel, Inst_train);
		// Print the result
		strSummary = eTest.toSummaryString();
		General.dispInfo(log_testResult, strSummary);
		// Get the confusion matrix
		cmMatrix= eTest.confusionMatrix();
		info_cfM=General.gtInfo_from_ConfusionMatrix(cmMatrix, className);
		General.dispInfo(log_testResult, info_cfM);
		
//		// save classifier, normalizer, attSel.    serialize model
//		weka.core.SerializationHelper.write(basePath+tr_percentage+"_traindata", Inst_train);
//		weka.core.SerializationHelper.write(Path_regionIndex+tr_percentage+"_trainedClassifier", cModel);
//		weka.core.SerializationHelper.write(Path_regionIndex+tr_percentage+"_normalizer", normalizer);
//		// read classifier, normalizer, attSel.     deserialize model
//		cModel = (Classifier) weka.core.SerializationHelper.read(Path_regionIndex+tr_percentage+"_trainedClassifier");
//		normalizer = (Normalize) weka.core.SerializationHelper.read(Path_regionIndex+tr_percentage+"_normalizer");
		log_testResult.close();
		
	}
}
