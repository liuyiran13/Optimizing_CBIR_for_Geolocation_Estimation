package MyAPI.General;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;

import weka.classifiers.Evaluation;
import weka.classifiers.functions.LibSVM;
import weka.clusterers.SimpleKMeans;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import MyCustomedHaoop.ValueClass.IntArr_FloatArr;

public class General_Weka {

	@SuppressWarnings("deprecation")
	public static Instances[] makeWekaInsts_from_MapFile_intArr_floatArr(String path_MapFile, String[] className, String[] instName, int[] train_test_ratio) throws IOException {
		assert instName.length==train_test_ratio.length;
		//set FileSystem
		Configuration conf = new Configuration();
        FileSystem hdfs  = FileSystem.get(conf);
        //set MapFile
        MapFile.Reader MapFile_R_Feat=new MapFile.Reader(hdfs,path_MapFile, conf);
 		System.out.println("MapFile Feat Key-Class: "+MapFile_R_Feat.getKeyClass().getName());
 		System.out.println("MapFile Feat Value-Class: "+MapFile_R_Feat.getValueClass().getName());
 		IntWritable Key_queryName= new IntWritable();
 		IntArr_FloatArr Value_feat=new IntArr_FloatArr();
 		//caculate attribute nums
 		MapFile_R_Feat.next(Key_queryName, Value_feat);
 		int attNum=Value_feat.getFloatArr().length+1;//feat nums + 1 ( attribute for class name)
 		//form attribute vector
		FastVector fvWekaAttributes = new FastVector(attNum);
		for(int i=0;i<attNum-1;i++){
			fvWekaAttributes.addElement(new Attribute("Feat_"+i));    
		}
		//declare the class attribute along with its values
		FastVector fvClassVal = new FastVector(className.length);
		for(String clNam: className){
			fvClassVal.addElement(clNam);
		}
		Attribute ClassAttribute = new Attribute("theClass_"+(attNum-1), fvClassVal);
		fvWekaAttributes.addElement(ClassAttribute);// last attribute is class label
		// Create an empty Instances set
		Instances[] wekaInsts = new Instances[train_test_ratio.length];   
		for(int i=0;i<wekaInsts.length;i++){
			// initial
			wekaInsts[i] = new Instances(instName[i], fvWekaAttributes, 10);   //train, test
			// Set class index
			wekaInsts[i].setClassIndex(attNum-1); // last attribute is class label, index starts with 0, If the class index is negative there is assumed to be no class. (ie. it is undefined)
		}		
		Random rand=new Random(); 
		// read feat from MapFile 
 		while(MapFile_R_Feat.next(Key_queryName, Value_feat)){ //loop over all queries, key-value(query-feat)
 			int classIndex = Value_feat.getIntArr()[0]; 
 			float[] feat = Value_feat.getFloatArr(); 
 			//new instance
 			Instance iInst = new Instance(attNum); 
 			//set class
 			iInst.setValue((Attribute)fvWekaAttributes.elementAt(attNum-1), className[classIndex]);
 			//set feat-data
 			for(int i=0;i<feat.length;i++){
 				iInst.setValue((Attribute)fvWekaAttributes.elementAt(i), feat[i]);
 			}
 			//add to train or test instances-set
 			int part=General.randSplit(rand, train_test_ratio);
 			wekaInsts[part].add(iInst); //add one instance to wekaInsts
 		}
 		MapFile_R_Feat.close();
		return wekaInsts;
	}
	
	public static Instances makeWekaInstsforClustering_from_Arr(double[][] data, String[] attNames, String instName) throws Exception{
		//caculate attribute nums
 		int attNum=data[0].length;//feat nums ( for cluster, data must not has class!!!)
 		int dataNum=data.length;
 		//form attribute vector
		FastVector fvWekaAttributes = new FastVector(attNum);
		if(attNames==null){
			for(int i=0;i<attNum;i++){
				fvWekaAttributes.addElement(new Attribute("Feat_"+i));    
			}
		}else{
			for(int i=0;i<attNum;i++){
				fvWekaAttributes.addElement(new Attribute(attNames[i]));    
			}
		}
		// Create an empty Instances set
		Instances wekaInsts = new Instances(instName, fvWekaAttributes, dataNum);   
		for(int i=0;i<dataNum;i++){
			wekaInsts.add(makeWekaInstforClustering_from_Arr(data[i],attNum,fvWekaAttributes)); //add one instance to wekaInsts
		}
		return wekaInsts;
	}
	
	public static Instance makeWekaInstforClustering_from_Arr(double[] data, int attNum, FastVector fvWekaAttributes) throws Exception{
		Instance iInst = new Instance(attNum);
		for(int j=0;j<attNum;j++){
			iInst.setValue((Attribute)fvWekaAttributes.elementAt(j), data[j]); //lat + long
		}			
		return iInst;
	}
	
	public static SimpleKMeans KMeans(Instances wekaInsts, int clusterNum, int randomSeed, int maxInteration, ArrayList<double[]> centers, ArrayList<Integer> classLabels) throws Exception{
		// new instance of clusterer
		SimpleKMeans  clusterer= new SimpleKMeans();
		//set options
		clusterer.setOptions(weka.core.Utils.splitOptions("-N "+clusterNum+" -S "+randomSeed+" -I "+maxInteration+" -O -V"));		
		// build the clusterer
		clusterer.buildClusterer(wekaInsts);   
		// extract centroids
		Instances centroids=clusterer.getClusterCentroids(); 
		int resuClNum=centroids.numInstances(); int dataDim=centroids.instance(0).numValues();
		for(int ClassID=0;ClassID<resuClNum;ClassID++){
			double[] oneCenter=new double[dataDim];
			for(int i=0;i<dataDim;i++)
				oneCenter[i]=centroids.instance(ClassID).value(i);
			centers.add(oneCenter);
		}
		//get data-sample class labels
		if(classLabels!=null){
			int[] labels=clusterer.getAssignments();
			for(int i=0;i<labels.length;i++){
				classLabels.add(labels[i]);
			}
		}
		return clusterer;
	}
	
	public static double[] selectPraForLibSVM_RBF(Instances instances, int nr_fold, int[] C_range, int[] G_range, PrintWriter log) throws Exception{
		/*  C, G parameter selection for 
		 * 	parameter selection tool for C-SVM classification using the RBF (radial basis function) kernel. It uses cross validation (CV)
			technique to estimate the accuracy of each parameter combination in the specified range and helps you to decide the best parameters for your problem.
		 */
		int kernel=2; //RBF kernel 
		double[] bestCG={0,0,0}; //best C,G and its correspond accuracy 
//		Classifier classifier = null;
		Evaluation evaluation = null;
		/* C_range to C_range_Arry*/
		int[] C_range_Arry=General.makeRange(C_range);
		/* G_range to G_range_Arry*/
		int[] G_range_Arry=General.makeRange(G_range);
		int totLoops=C_range_Arry.length*G_range_Arry.length;
		General.dispInfo(log, "begin parameter selection, C2Power_range_RBF:"+General.IntArrToString(C_range, ", ")+", G2Power_range_RBF:"+General.IntArrToString(G_range, ", "));
		General.dispInfo(log, "total paremeter_loops: "+totLoops+", each loop do "+nr_fold+" folds crossValidation!");
		//start selection
		int loop_i=0; int dispInter=1;
		long startTime=System.currentTimeMillis(); //start time 
		for(int i=0;i<C_range_Arry.length;i++){
			for(int j=0;j<G_range_Arry.length;j++){
				loop_i++;
				double cost = Math.pow(2, C_range_Arry[i]); //We found that trying exponentially growing sequences of C and is a practical method to identify good parameters (for example, C = 2^-5,2^-3....2^15, G=2^-15,2^-13,....2^3
                double gamma = Math.pow(2, G_range_Arry[j]);
//				classifier = new LibSVM();
//				classifier.setOptions(weka.core.Utils.splitOptions("-S 0 -K "+kernel+" -C "+cost+" -G "+gamma));
//				classifier.buildClassifier(instances);         
                evaluation = new Evaluation(instances);
                evaluation.crossValidateModel(new LibSVM().getClass().getName(), instances, nr_fold, weka.core.Utils.splitOptions("-S 0 -K "+kernel+" -C "+cost+" -G "+gamma),new Random(1));
                double acc = evaluation.pctCorrect(); // the percent of correctly classified instances (between 0 and 100)
				if(acc>bestCG[2]){//  updata C,G,acc
					bestCG[0]=cost;bestCG[1]=gamma;bestCG[2]=acc;
				}
				//disp info
				if (loop_i%dispInter==0 ){
					long endTime=System.currentTimeMillis(); //end time 
					double percentage=(double)loop_i/totLoops;
					General.dispInfo(log,"parameter selection..loop_"+loop_i+" finished ....  cost:"+cost+", gamma:"+gamma+", acc="+acc+", current bestC="+bestCG[0]+", bestG="+bestCG[1]+", best_acc="+bestCG[2]+",...."
							+new DecimalFormat("00%").format(percentage)+"....."+General.dispTime(endTime-startTime,"min"));
				}
			}
		}		
		return bestCG;
	}
	
	public static double[] selectPraForLibSVM_Linear(Instances instances, int kernel, int nr_fold, int[] C_range, PrintWriter log) throws Exception{
		/*  C,  parameter selection for 
		 * 	parameter selection tool for C-SVM classification using the Linear kernel. It uses cross validation (CV)
			technique to estimate the accuracy of each parameter combination in the specified range and helps you to decide the best parameters for your problem.
		 */
		double[] bestC={0,0}; //best C,G and its correspond accuracy 
		/* C_range to C_range_Arry*/
		int[] C_range_Arry=General.makeRange(C_range);
		int totLoops=C_range_Arry.length;
		General.dispInfo(log, "begin parameter selection, C2Power_range_Linear:"+General.IntArrToString(C_range, ", "));
		General.dispInfo(log, "total paremeter_loops: "+totLoops+", each loop do "+nr_fold+" folds crossValidation!");
		//start selection
		int loop_i=0; int dispInter=1;
		long startTime=System.currentTimeMillis(); //start time 
		for(int i=0;i<C_range_Arry.length;i++){
			loop_i++;
			double cost = Math.pow(2, C_range_Arry[i]); //We found that trying exponentially growing sequences of C and is a practical method to identify good parameters (for example, C = 2^-5,2^-3....2^15, G=2^-15,2^-13,....2^3
			LibSVM classifier = new LibSVM();
			classifier.setOptions(weka.core.Utils.splitOptions("-S 0 -K "+kernel+" -C "+cost+" -W 9 1"));
			General.dispInfo(log,"classifier.getCost():"+classifier.getCost());
			General.dispInfo(log,"classifier.getShrinking(): "+classifier.getShrinking());
			General.dispInfo(log,"classifier.getOptions(): "+General.StrArrToStr(classifier.getOptions(), ", "));
			classifier.buildClassifier(instances);
			Evaluation evaluation = new Evaluation(instances);
            evaluation.evaluateModel(classifier, instances);
//            evaluation.crossValidateModel(new LibSVM().getClass().getName(), instances, nr_fold, weka.core.Utils.splitOptions("-S 0 -K "+kernel+" -C "+cost),new Random(1));
            double acc = evaluation.pctCorrect(); // the percent of correctly classified instances (between 0 and 100)
			if(acc>bestC[1]){//  updata C,,acc
				bestC[0]=cost;bestC[1]=acc;
			}
			//disp info
			if (loop_i%dispInter==0 ){
				long endTime=System.currentTimeMillis(); //end time 
				double percentage=(double)loop_i/totLoops;
				General.dispInfo(log,"parameter selection..loop_"+loop_i+" finished .....  cost:"+cost+", acc="+acc+", current bestC="+bestC[0]+", best_acc="+bestC[1]+",...."
						+new DecimalFormat("00%").format(percentage)+"....."+General.dispTime(endTime-startTime,"min"));
			}
		}		
		return bestC;
	}

	public static void main(String[] args) throws IOException {//debug
		//*****		test	******//
		
	}
}
