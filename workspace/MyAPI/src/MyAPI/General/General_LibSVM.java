package MyAPI.General;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

import MyLibSVM.Kernel;
import MyLibSVM.svm;
import MyLibSVM.svm_modelInfo;
import MyLibSVM.svm_node;
import MyLibSVM.svm_problem;
import MyLibSVM.svm_model;
import MyLibSVM.svm_parameter;
import MyLibSVM.svm.decision_function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import MyCustomedHaoop.KeyClass.Key_IntArr;
import MyCustomedHaoop.ValueClass.IntArr_FloatArr;
import MyCustomedHaoop.ValueClass.IntList_FloatList;

public class General_LibSVM {

	@SuppressWarnings("deprecation")
	public static svm_problem[] makeData_from_MapFile_intArr_floatArr(String path_MapFile, int[] train_test_ratio) throws IOException {
		//set FileSystem
		Configuration conf = new Configuration();
        FileSystem hdfs  = FileSystem.get(conf);
        //set MapFile
        MapFile.Reader MapFile_R_Feat=new MapFile.Reader(hdfs,path_MapFile, conf);
 		System.out.println("MapFile Feat Key-Class: "+MapFile_R_Feat.getKeyClass().getName());
 		System.out.println("MapFile Feat Value-Class: "+MapFile_R_Feat.getValueClass().getName());
 		IntWritable Key_queryName= new IntWritable();
 		IntArr_FloatArr Value_feat=new IntArr_FloatArr();
 		//compute data split
 		Random rand=new Random(); ArrayList<Integer> partIndex=new ArrayList<Integer> ();
 		int[] dataNums=new int[train_test_ratio.length];
 	 	while(MapFile_R_Feat.next(Key_queryName, Value_feat)){ //loop over all queries, key-value(query-feat)
 			//add to train or test instances-set
 			int part=General.randSplit(rand, train_test_ratio);
 			partIndex.add(part);
 			dataNums[part]++;
 	 	}
 	 	MapFile_R_Feat.reset();
 	 	// Create an empty data set
 		svm_problem[] datas = new svm_problem[train_test_ratio.length];
 		for(int i=0;i<datas.length;i++){
			// initial
 			svm_problem oneData= new svm_problem();   //train, test
 			int dataNum=dataNums[i]; //data num
 			oneData.l = dataNum;
 			oneData.y = new double[dataNum]; // data class labels
 			oneData.x = new svm_node[dataNum][];  // data features, each sample represent by one svm_node[];
 			datas[i]=oneData;
		}		
 		// make datas
 		int samp_i=0; int[] part_i=new int[train_test_ratio.length]; //save current sample index in one data part
 	 	while(MapFile_R_Feat.next(Key_queryName, Value_feat)){ //loop over all queries, key-value(query-feat)
 			int classIndex = Value_feat.getIntArr()[0]; 
 			float[] feat = Value_feat.getFloatArr(); 
 			//form spare format
			svm_node[] nodes=data_sparse(feat);
 			//add to train or test instances-set
 			int part=partIndex.get(samp_i);
 			datas[part].x[part_i[part]]=nodes; //add sample to datas[part] at index part_i[part]
 			datas[part].y[part_i[part]]=classIndex;
 			part_i[part]++;
 			samp_i++;
 	 	}
 	 	MapFile_R_Feat.close();
		return datas;
	}
	
	public static svm_node[] data_sparse(float[] oneArray) {// tranfer from one array to sparse svm_node[]
	    int realFtNum=0; //array total feat number
	    for (int j = 0; j < oneArray.length; j++){
	    	if (oneArray[j]!=0)
	    		realFtNum++;
	    }	    
		svm_node[] nodeArray = new svm_node[realFtNum];
		int nodeIndex=0;
	    for (int j = 0; j < oneArray.length; j++){
	    	if (oneArray[j]!=0){
	    		nodeArray[nodeIndex] = new svm_node();
	    		nodeArray[nodeIndex].index = j;
	    		nodeArray[nodeIndex].value = oneArray[j];
	            nodeIndex++;
	    	}        
	    }    
	    return nodeArray;
	}
	
	public static svm_node[] data_sparse(String[] Ind_Vals, String diameter, int startInd) {// tranfer from one array of Ind_Vals to sparse svm_node[]
	    int realFtNum=0; //array total feat number
	    ArrayList<Integer> inds=new ArrayList<Integer>(Ind_Vals.length);
	    ArrayList<Float> vals=new ArrayList<Float>(Ind_Vals.length);
	    for (int j = 0; j < Ind_Vals.length; j++){
	    	String[] Ind_Val =Ind_Vals[j].split(diameter);
	    	float val=Float.valueOf(Ind_Val[1]);
	    	if (val!=0){
	    		realFtNum++;
	    		inds.add(Integer.valueOf(Ind_Val[0])-startInd); //some feat file, ind_val pair, ind is not start from 0! here unit to index from 0!
	    		vals.add(val);
	    	}
	    }	    
		svm_node[] nodeArray = new svm_node[realFtNum];
	    for (int j = 0; j < inds.size(); j++){
    		nodeArray[j] = new svm_node();
    		nodeArray[j].index = inds.get(j);
    		nodeArray[j].value = vals.get(j);
	    }    
	    return nodeArray;
	}
	
	public static svm_node[] data_sparse(ArrayList<Integer> inds, ArrayList<Float> vals, HashSet<Integer> FeatInds_Sel) {// tranfer from one array of Ind_Vals to sparse svm_node[]
	    General.Assert(inds.size()==vals.size(), "error in General_LibSVM.data_sparse: inds.size() should == vals.size()");
		if (FeatInds_Sel!=null) {//feat selection
			//make sellected nodes
			ArrayList<svm_node> nodes=new ArrayList<svm_node>(inds.size());
			for (int i = 0; i < inds.size(); i++) {
				if (FeatInds_Sel.contains(inds.get(i))) {
					svm_node one = new svm_node();
					one.index = inds.get(i);
					one.value = vals.get(i);
					nodes.add(one);
				}
			}
			//make svm_node[]
			int realFtNum=nodes.size();
			svm_node[] nodeArray = new svm_node[realFtNum];
		    for (int j = 0; j < realFtNum; j++){
	    		nodeArray[j] = nodes.get(j);
		    }    
		    return nodeArray;
		}else {//no feat selection
			int realFtNum=inds.size(); //array total feat number 
			svm_node[] nodeArray = new svm_node[realFtNum];
		    for (int j = 0; j < inds.size(); j++){
	    		nodeArray[j] = new svm_node();
	    		nodeArray[j].index = inds.get(j);
	    		nodeArray[j].value = vals.get(j);
		    }    
		    return nodeArray;
		}
	    
	}
	
	public static IntList_FloatList data_sparse_intList_floatList(String[] Ind_Vals, String diameter, int startInd) {// tranfer from one array of Ind_Vals to sparse IntList_FloatList
	    ArrayList<Integer> inds=new ArrayList<Integer>(Ind_Vals.length);
	    ArrayList<Float> vals=new ArrayList<Float>(Ind_Vals.length);
	    for (int j = 0; j < Ind_Vals.length; j++){
	    	String[] Ind_Val =Ind_Vals[j].split(diameter);
	    	float val=Float.valueOf(Ind_Val[1]);
	    	if (val!=0){
	    		inds.add(Integer.valueOf(Ind_Val[0])-startInd); //some feat file, ind_val pair, ind is not start from 0! here unit to index from 0!
	    		vals.add(val);
	    	}
	    }	    
	    return new IntList_FloatList(inds, vals);
	}
	
	public static float[] data_sparse_floatArr(String[] Ind_Vals, String diameter, int startInd, int Dim) {// tranfer from one array of Ind_Vals to sparse IntList_FloatList
		float[] feat=new float[Dim];
	    for (int j = 0; j < Ind_Vals.length; j++){
	    	String[] Ind_Val =Ind_Vals[j].split(diameter);
	    	float val=Float.valueOf(Ind_Val[1]);
	    	if (val!=0){
	    		int ind=Integer.valueOf(Ind_Val[0])-startInd;//some feat file, ind_val pair, ind is not start from 0! here unit to index from 0!
	    		feat[ind]=val;
	    	}
	    }	    
	    return feat;
	}
	
	public static float[] data_sparse_floatArr(ArrayList<Integer> Inds, ArrayList<Float> Vals, int Dim, int startInd) {// tranfer from one array of Ind_Vals to sparse IntList_FloatList
		float[] feat=new float[Dim];
	    for (int j = 0; j < Vals.size(); j++){
	    	float val=Vals.get(j);
	    	if (val!=0){
	    		int ind=Inds.get(j)-startInd;//some feat file, ind_val pair, ind is not start from 0! here unit to index from 0!
	    		feat[ind]=val;
	    	}
	    }	    
	    return feat;
	}
	
	public static float[][] dataScaling(svm_problem oneData, float[][] scalingInfo) {// scaling data according to scalingInfo, scalingInfo[featDim][min_max], 
		if (scalingInfo==null) {// if scalingInfo=null, then this for trainData, so first build scalingInfo
			//get feat-dim
			int featDim=-1;
			for (svm_node[] oneSample : oneData.x) {
				for (svm_node one_node : oneSample) {
					featDim=Math.max(featDim, one_node.index); //asume featDim index from 0!!
				}
			}
			featDim=featDim+1;
			//initial scalingInfo
			scalingInfo=new float[featDim][2];// each element is: min_max
			for (int i = 0; i < scalingInfo.length; i++) {
				scalingInfo[i][0]=9999;
				scalingInfo[i][1]=-9999;
			}
			//make scalingInfo
			for (svm_node[] oneSample : oneData.x ) {
				for (svm_node oneNode : oneSample) {
					int dimInd=oneNode.index;
					float dimVal=(float) oneNode.value;
					scalingInfo[dimInd][0]=Math.min(scalingInfo[dimInd][0], dimVal);
					scalingInfo[dimInd][1]=Math.max(scalingInfo[dimInd][1], dimVal);
				}
			}
		}
		//scale data
		double lower = -1.0;
		double upper = 1.0;
		for (svm_node[] oneSample : oneData.x ) {
			dataSampleScaling( oneSample,  scalingInfo,  lower,   upper);
		}
		return scalingInfo;
	}
	
	public static void dataSampleScaling(svm_node[] oneDataSample, float[][] scalingInfo, double lower,  double upper) {// scaling data according to scalingInfo, scalingInfo[featDim][min_max], 
		//scale data
		for (svm_node oneNode : oneDataSample) {
			oneNode.value= General.scaleValue(oneNode.value, scalingInfo[oneNode.index], lower, upper);
		}
	}
	
	public static svm_model svm_train_DIY_ClassificationModel_finalProb(svm_problem prob, svm_parameter param, svm_problem prob_forProAB){ 
		/*
		 * compare with svm.svm_train(data_SVM, param,nr_fold_proModel): the svm train part is the same, so SVs of these two models are the same!, 
		 * but, for probability estimation, ProAB of each submodel is different, 
		 * in svm.svm_train, it use train data, and mutiple fold cross-validataion, to get the decionValues for each trainSample, and finally get the ProAB.
		 * in svm_train_DIY_ClassificationModel, it use separate data, prob_forProAB, for estimating ProAB, 
		 * so the ProA and ProB of each submodel between these two method are different!
		 * 
		 * compare svm_train_DIY_ClassificationModel_finalProb and svm_train_DIY_ClassificationModel_seprateProb, 
		 * _finalProb first train decision functions for each 1vs1 model, and combine them together, and do prob estimation together at last.(prob_forProAB only operate with SVectors once!)
		 * _seprateProb train decision functions and do prob estimation for each 1vs1 model. (prob_forProAB need to operate with SVector for each model)
		 * _finalProb is fast, and is suitable for local machine!
		 * _seprateProb is slow, only to test for Hadoop distributed computing!
		 * 
		 */
		svm_model model = new svm_model();
		model.param = param;
		
		if(param.svm_type == svm_parameter.ONE_CLASS || param.svm_type == svm_parameter.EPSILON_SVR || param.svm_type == svm_parameter.NU_SVR){
			// regression or one-class-svm
			System.err.println("this svm_train_DIY_ClassificationModel only handle classification problem, not for regression or one-class-svm!!");
			return null;
		}else if (param.probability == 1 && prob_forProAB==null) {
			System.err.println("error, param.probability == 1, need data:prob_forProAB to make probability estimation for sub-models!");
			return null;
		}else {// classification
			
			int l = prob.l;
			int[] tmp_nr_class = new int[1];
			int[][] tmp_label = new int[1][];
			int[][] tmp_start = new int[1][];
			int[][] tmp_count = new int[1][];			
			int[] perm = new int[l];

			// group training data of the same class
			svm.svm_group_classes(prob,tmp_nr_class,tmp_label,tmp_start,tmp_count,perm);
			int nr_class = tmp_nr_class[0];			
			int[] label = tmp_label[0];
			int[] start = tmp_start[0];
			int[] count = tmp_count[0];
 			
			if(nr_class == 1) 
				System.out.println("WARNING: all training data are in only one class. See README for details.\n");
			
			svm_node[][] x = new svm_node[l][];
			for(int i=0;i<l;i++)
				x[i] = prob.x[perm[i]];

			// calculate weighted C
			double[] weighted_C = new double[nr_class];
			for(int i=0;i<nr_class;i++)
				weighted_C[i] = param.C;
			for(int i=0;i<param.nr_weight;i++)
			{
				//check weight label
				int j;
				for(j=0;j<nr_class;j++)
					if(param.weight_label[i] == label[j])
						break;
				if(j == nr_class)
					System.err.print("WARNING: class label "+param.weight_label[i]+" specified in weight is not found\n");
				else
					weighted_C[j] *= param.weight[i];
			}

			// train k*(k-1)/2 models
			boolean[] nonzero = new boolean[l];
			for(int i=0;i<l;i++)
				nonzero[i] = false;
			decision_function[] f = new decision_function[nr_class*(nr_class-1)/2];

			ArrayList<HashSet<Integer>> svInvolvedModelList=new ArrayList<HashSet<Integer>>(l);
			for(int i=0;i<l;i++)
				svInvolvedModelList.add(new HashSet<Integer>());
			
			int p = 0; 
			for(int i=0;i<nr_class;i++)
				for(int j=i+1;j<nr_class;j++)
				{
					svm_problem sub_prob = new svm_problem();
					int si = start[i], sj = start[j];
					int ci = count[i], cj = count[j];
					sub_prob.l = ci+cj;
					sub_prob.x = new svm_node[sub_prob.l][];
					sub_prob.y = new double[sub_prob.l];
					int k;
					for(k=0;k<ci;k++)
					{
						sub_prob.x[k] = x[si+k];
						sub_prob.y[k] = +1;
					}
					for(k=0;k<cj;k++)
					{
						sub_prob.x[ci+k] = x[sj+k];
						sub_prob.y[ci+k] = -1;
					}

					f[p] = svm.svm_train_one(sub_prob,param,weighted_C[i],weighted_C[j]);
					for(k=0;k<ci;k++)
						if(Math.abs(f[p].alpha[k]) > 0){
							nonzero[si+k] = true;
							svInvolvedModelList.get(si+k).add(p);
						}
					for(k=0;k<cj;k++)
						if(Math.abs(f[p].alpha[ci+k]) > 0){
							nonzero[sj+k] = true;
							svInvolvedModelList.get(sj+k).add(p);
						}
					++p;
				}

			// build output

			model.nr_class = nr_class;

			model.label = new int[nr_class];
			for(int i=0;i<nr_class;i++)
				model.label[i] = label[i];

			model.rho = new double[nr_class*(nr_class-1)/2];
			for(int i=0;i<nr_class*(nr_class-1)/2;i++)
				model.rho[i] = f[i].rho;

			int nnz = 0;
			int[] nz_count = new int[nr_class];
			model.nSV = new int[nr_class];
			for(int i=0;i<nr_class;i++)
			{
				int nSV = 0;
				for(int j=0;j<count[i];j++)
					if(nonzero[start[i]+j])
					{
						++nSV;
						++nnz;
					}
				model.nSV[i] = nSV;
				nz_count[i] = nSV;
			}

//			svm.info("Total nSV = "+nnz+"\n");

			model.l = nnz;
			model.SV = new svm_node[nnz][];
			p = 0;
			ArrayList<HashSet<Integer>> svInvolvedModelList_zeroRemoved=new ArrayList<HashSet<Integer>>(nnz);
			for(int i=0;i<l;i++)
				if(nonzero[i]) {
					model.SV[p++] = x[i];
					svInvolvedModelList_zeroRemoved.add(svInvolvedModelList.get(i));
				}
			General.Assert(svInvolvedModelList_zeroRemoved.size()==nnz, "error in svInvolvedModelList_zeroRemoved, its size is not equal to nnz!");

			int[] nz_start = new int[nr_class];
			nz_start[0] = 0;
			for(int i=1;i<nr_class;i++)
				nz_start[i] = nz_start[i-1]+nz_count[i-1];

			model.sv_coef = new double[nr_class-1][];
			for(int i=0;i<nr_class-1;i++)
				model.sv_coef[i] = new double[nnz];

			p = 0;
			for(int i=0;i<nr_class;i++)
				for(int j=i+1;j<nr_class;j++)
				{
					// classifier (i,j): coefficients with
					// i are in sv_coef[j-1][nz_start[i]...],
					// j are in sv_coef[i][nz_start[j]...]

					int si = start[i];
					int sj = start[j];
					int ci = count[i];
					int cj = count[j];

					int q = nz_start[i];
					int k;
					for(k=0;k<ci;k++)
						if(nonzero[si+k])
							model.sv_coef[j-1][q++] = f[p].alpha[k];
					q = nz_start[j];
					for(k=0;k<cj;k++)
						if(nonzero[sj+k])
							model.sv_coef[i][q++] = f[p].alpha[ci+k];
					++p;
				}
			
			if(param.probability == 1){
				int subModelNum=nr_class*(nr_class-1)/2;
				// initialize subModelDecValLabel
				ArrayList<ArrayList<double[]>> subModelDecValLabel=new ArrayList<ArrayList<double[]>>(subModelNum);
				for (int i = 0; i < subModelNum; i++) {
					subModelDecValLabel.add(new ArrayList<double[]>());
				}
				// make index-mapping
				start = new int[nr_class]; start[0] = 0;
				for(int i=1;i<nr_class;i++){
					start[i] = start[i-1]+model.nSV[i-1];
				}
				int[] modelInOrderLabels=model.label;
				//loop over all prob_forProAB data-sample, find sample involved 1vs1 model, and add decionValue, label to subModelDecValLabel
				for (int sample_i = 0; sample_i < prob_forProAB.l; sample_i++) {
					int model_svNum = model.l;
					svm_node[] oneSample=prob_forProAB.x[sample_i];
					int trueLabel=(int) prob_forProAB.y[sample_i];
					//make kvalue for all classes sv-vector;
					double[] kvalue = new double[model_svNum];
					for(int ii=0;ii<model_svNum;ii++)
						kvalue[ii] = Kernel.k_function(oneSample,model.SV[ii],model.param);
					//make dec_values_label
					int modelID=0;
					for(int i=0;i<nr_class;i++)
						for(int j=i+1;j<nr_class;j++)
						{
							if (trueLabel==modelInOrderLabels[i] || trueLabel==modelInOrderLabels[j]) {//this sample is involved in this model
								double sum = 0;
								int si = start[i];
								int sj = start[j];
								int ci = model.nSV[i];
								int cj = model.nSV[j];
								double[] coef1 = model.sv_coef[j-1];
								double[] coef2 = model.sv_coef[i];
								for(int k=0;k<ci;k++)
									if (svInvolvedModelList_zeroRemoved.get(si+k).contains(modelID)) { //this sv is envolved in this 1vs1 model
										sum += coef1[si+k] * kvalue[si+k];
									}
								for(int k=0;k<cj;k++)
									if (svInvolvedModelList_zeroRemoved.get(sj+k).contains(modelID)) { //this sv is envolved in this 1vs1 model
										sum += coef2[sj+k] * kvalue[sj+k];
									}
								sum -= model.rho[modelID];
								if (trueLabel==modelInOrderLabels[i]){//this sample is in the positive class of this 1vs1model
									double[] dec_values_label = {sum,+1};
									subModelDecValLabel.get(modelID).add(dec_values_label);
								}else {//this sample is in the negative class of this 1vs1model
									double[] dec_values_label = {sum,-1};
									subModelDecValLabel.get(modelID).add(dec_values_label);
								}
							}
							modelID++;
						}
				}
				//make probA and probA for each 1vs1 sub-model
				double[] probA=new double[nr_class*(nr_class-1)/2];
				double[] probB=new double[nr_class*(nr_class-1)/2];
				for (int modelID = 0; modelID < subModelNum; modelID++) {
					int sampleNum=subModelDecValLabel.get(modelID).size();
					double[] dec_values=new double[sampleNum];
					double[] sampleLabels=new double[sampleNum];
					for (int i = 0; i < sampleNum; i++) {
						dec_values[i]=subModelDecValLabel.get(modelID).get(i)[0];
						sampleLabels[i]=subModelDecValLabel.get(modelID).get(i)[1];
					}
					double[] probAB=new double[2];
					svm.sigmoid_train(sampleNum,dec_values,sampleLabels,probAB);
					probA[modelID]=probAB[0];
					probB[modelID]=probAB[1];
				}
				model.probA = probA;
				model.probB = probB;
			}else{
				model.probA=null;
				model.probB=null;
			}
			
			return model;
		}
	}
	
	public static svm_model svm_train_DIY_ClassificationModel_seprateProb(svm_problem prob, svm_parameter param, svm_problem prob_forProAB){ 
		/*
		 * compare with svm.svm_train(data_SVM, param,nr_fold_proModel): the svm train part is the same, so SVs of these two models are the same!, 
		 * but, for probability estimation, ProAB of each submodel is different, 
		 * in svm.svm_train, it use train data, and mutiple fold cross-validataion, to get the decionValues for each trainSample, and finally get the ProAB.
		 * in svm_train_DIY_ClassificationModel, it use separate data, prob_forProAB, for estimating ProAB, 
		 * so the ProA and ProB of each submodel between these two method are different!
		 * 
		 * compare svm_train_DIY_ClassificationModel_finalProb and svm_train_DIY_ClassificationModel_seprateProb, 
		 * _finalProb first train decision functions for each 1vs1 model, and combine them together, and do prob estimation together at last.(prob_forProAB only operate with SVectors once!)
		 * _seprateProb train decision functions and do prob estimation for each 1vs1 model. (prob_forProAB need to operate with SVector for each model)
		 * _finalProb is fast, and is suitable for local machine!
		 * _seprateProb is slow, only to test for Hadoop distributed computing!
		 * 
		 */
		svm_model model = new svm_model();
		model.param = param;
		
		if(param.svm_type == svm_parameter.ONE_CLASS || param.svm_type == svm_parameter.EPSILON_SVR || param.svm_type == svm_parameter.NU_SVR){
			// regression or one-class-svm
			System.err.println("this svm_train_DIY_ClassificationModel only handle classification problem, not for regression or one-class-svm!!");
			return null;
		}else if (param.probability == 1 && prob_forProAB==null) {
			System.err.println("error, param.probability == 1, need data:prob_forProAB to make probability estimation for sub-models!");
			return null;
		}else {// classification
			
			int l = prob.l;
			int[] tmp_nr_class = new int[1];
			int[][] tmp_label = new int[1][];
			int[][] tmp_start = new int[1][];
			int[][] tmp_count = new int[1][];			
			int[] perm = new int[l];

			// group training data of the same class
			svm.svm_group_classes(prob,tmp_nr_class,tmp_label,tmp_start,tmp_count,perm);
			int nr_class = tmp_nr_class[0];			
			int[] label = tmp_label[0];
			int[] start = tmp_start[0];
			int[] count = tmp_count[0];
 			
			if(nr_class == 1) 
				System.out.println("WARNING: all training data are in only one class. See README for details.\n");
			
			svm_node[][] x = new svm_node[l][];
			for(int i=0;i<l;i++)
				x[i] = prob.x[perm[i]];

			// calculate weighted C
			double[] weighted_C = new double[nr_class];
			for(int i=0;i<nr_class;i++)
				weighted_C[i] = param.C;
			for(int i=0;i<param.nr_weight;i++)
			{
				//check weight label
				int j;
				for(j=0;j<nr_class;j++)
					if(param.weight_label[i] == label[j])
						break;
				if(j == nr_class)
					System.err.print("WARNING: class label "+param.weight_label[i]+" specified in weight is not found\n");
				else
					weighted_C[j] *= param.weight[i];
			}

			// train k*(k-1)/2 models
			boolean[] nonzero = new boolean[l];
			for(int i=0;i<l;i++)
				nonzero[i] = false;
			decision_function[] f = new decision_function[nr_class*(nr_class-1)/2];
			
			model.probA=new double[nr_class*(nr_class-1)/2];
			model.probB=new double[nr_class*(nr_class-1)/2];
			
			int p = 0; 
			for(int i=0;i<nr_class;i++)
				for(int j=i+1;j<nr_class;j++)
				{
					svm_problem sub_prob = new svm_problem();
					int si = start[i], sj = start[j];
					int ci = count[i], cj = count[j];
					sub_prob.l = ci+cj;
					sub_prob.x = new svm_node[sub_prob.l][];
					sub_prob.y = new double[sub_prob.l];
					int k;
					for(k=0;k<ci;k++)
					{
						sub_prob.x[k] = x[si+k];
						sub_prob.y[k] = +1;
					}
					for(k=0;k<cj;k++)
					{
						sub_prob.x[ci+k] = x[sj+k];
						sub_prob.y[ci+k] = -1;
					}

					
					if (param.probability == 1) {// probability model, 
						//make sub_prob_forProAB
						svm_problem sub_prob_forProAB = new svm_problem();
						ArrayList<Integer> PdataIndex=new ArrayList<Integer>(); ArrayList<Integer> NdataIndex=new ArrayList<Integer>();
						for (int sample_i = 0; sample_i < prob_forProAB.l; sample_i++) {
							int trueLabel=(int) prob_forProAB.y[sample_i];
							if (trueLabel==label[i]){ //this sample is involved in this model's Positive class 
								PdataIndex.add(sample_i);
							}else if (trueLabel==label[j]) { //this sample is involved in this model's Negative class 
								NdataIndex.add(sample_i);
							}
						}
						sub_prob_forProAB.l=PdataIndex.size()+NdataIndex.size();
						sub_prob_forProAB.x = new svm_node[sub_prob_forProAB.l][];
						sub_prob_forProAB.y = new double[sub_prob_forProAB.l];
						for (int m = 0; m < PdataIndex.size(); m++) {//add positive data samples
							sub_prob_forProAB.x[m]=prob_forProAB.x[PdataIndex.get(m)];
							sub_prob_forProAB.y[m]=+1;
						}
						int negDataStart=PdataIndex.size();
						for (int m = 0; m < NdataIndex.size(); m++) {//add negative data samples
							sub_prob_forProAB.x[negDataStart+m]=prob_forProAB.x[NdataIndex.get(m)];
							sub_prob_forProAB.y[negDataStart+m]=-1;
						}
						//make decision function, and probability ProAB
						svm_modelInfo modelInfo=svm_trainOneModel_DIY_ClassificationModel(sub_prob, param, weighted_C[i], weighted_C[j],  sub_prob_forProAB);
						f[p] = modelInfo.decfunction;
						model.probA[p] = modelInfo.proAB[0];
						model.probB[p] = modelInfo.proAB[1];
					}else {
						svm_modelInfo modelInfo=svm_trainOneModel_DIY_ClassificationModel(sub_prob, param, weighted_C[i], weighted_C[j],  null);
						f[p] = modelInfo.decfunction;
					}
					
					//update nonzero
					for(k=0;k<ci;k++)
						if(Math.abs(f[p].alpha[k]) > 0){
							nonzero[si+k] = true;
						}
					for(k=0;k<cj;k++)
						if(Math.abs(f[p].alpha[ci+k]) > 0){
							nonzero[sj+k] = true;
						}
					
					++p;
				}

			// build output

			model.nr_class = nr_class;

			model.label = new int[nr_class];
			for(int i=0;i<nr_class;i++)
				model.label[i] = label[i];

			model.rho = new double[nr_class*(nr_class-1)/2];
			for(int i=0;i<nr_class*(nr_class-1)/2;i++)
				model.rho[i] = f[i].rho;

			int nnz = 0;
			int[] nz_count = new int[nr_class];
			model.nSV = new int[nr_class];
			for(int i=0;i<nr_class;i++)
			{
				int nSV = 0;
				for(int j=0;j<count[i];j++)
					if(nonzero[start[i]+j])
					{
						++nSV;
						++nnz;
					}
				model.nSV[i] = nSV;
				nz_count[i] = nSV;
			}

//			svm.info("Total nSV = "+nnz+"\n");

			model.l = nnz;
			model.SV = new svm_node[nnz][];
			p = 0;
			for(int i=0;i<l;i++)
				if(nonzero[i]) {
					model.SV[p++] = x[i];
				}

			int[] nz_start = new int[nr_class];
			nz_start[0] = 0;
			for(int i=1;i<nr_class;i++)
				nz_start[i] = nz_start[i-1]+nz_count[i-1];

			model.sv_coef = new double[nr_class-1][];
			for(int i=0;i<nr_class-1;i++)
				model.sv_coef[i] = new double[nnz];

			p = 0;
			for(int i=0;i<nr_class;i++)
				for(int j=i+1;j<nr_class;j++)
				{
					// classifier (i,j): coefficients with
					// i are in sv_coef[j-1][nz_start[i]...],
					// j are in sv_coef[i][nz_start[j]...]

					int si = start[i];
					int sj = start[j];
					int ci = count[i];
					int cj = count[j];

					int q = nz_start[i];
					int k;
					for(k=0;k<ci;k++)
						if(nonzero[si+k])
							model.sv_coef[j-1][q++] = f[p].alpha[k];
					q = nz_start[j];
					for(k=0;k<cj;k++)
						if(nonzero[sj+k])
							model.sv_coef[i][q++] = f[p].alpha[ci+k];
					++p;
				}
			
			return model;
		}
	}
	
	public static svm_model svm_combineTrained1vs1Model_DIY_ClassificationModel(svm_modelInfo[] modelInfos, svm_parameter param, svm_problem data_SVM, int[] classLabelInOrder){ 
		/*
		 * combine trained 1vs1 model infos into one final model 
		 * 
		 * data_SVM is the training data, should be in the order of classLabelInOrder!!
		 * 
		 */
		svm_model model = new svm_model();
		model.param = param;
		
		int nr_class =classLabelInOrder.length;
		
		if(param.svm_type == svm_parameter.ONE_CLASS || param.svm_type == svm_parameter.EPSILON_SVR || param.svm_type == svm_parameter.NU_SVR){
			// regression or one-class-svm
			System.err.println("this svm_combineTrained1vs1Model_DIY_ClassificationModel only handle classification problem, not for regression or one-class-svm!!");
			return null;
		}else {// classification
			
			int totDataNum=data_SVM.l;
			
			boolean[] nonzero = new boolean[totDataNum];
			for(int i=0;i<totDataNum;i++)
				nonzero[i] = false;
			
			//find class data sample number
			int[] classDataNum=new int[nr_class];
			for (int i = 0; i < data_SVM.l; i++) {
				for (int j = 0; j < nr_class; j++) {//find this sample's class
					if(data_SVM.y[i]==classLabelInOrder[j]){
						classDataNum[j]++;
						break;
					}
				}
			}
			
			int[] classDataStarts=new int[nr_class];
			classDataStarts[0] = 0;
			for(int i=1;i<nr_class;i++)
				classDataStarts[i] = classDataStarts[i-1]+classDataNum[i-1];
				
			if (param.probability==1) {
				model.probA=new double[nr_class*(nr_class-1)/2];
				model.probB=new double[nr_class*(nr_class-1)/2];
			}
			
			model.rho = new double[nr_class*(nr_class-1)/2];
			
			int modelID=0;
			for (int i = 0; i < nr_class; i++) {
				for (int j = i+1; j < nr_class; j++) {
					//find 2-class data sample number
					int ci=classDataNum[i]; int cj=classDataNum[j];
					//update nonzero
					decision_function decfun=modelInfos[modelID].decfunction;
					General.Assert(decfun.alpha.length==(ci+cj), "error in model-"+modelID+": alpha numbers in the decision function are not equal to these twoo class's data number, "
							+", decfun.alpha.length:"+decfun.alpha.length+", dataNum in these two class:"+(ci+cj));
					for(int k=0; k<ci; k++)
						if(!nonzero[classDataStarts[i]+k] && Math.abs(decfun.alpha[k]) > 0){
							nonzero[classDataStarts[i]+k] = true;//class-i
						}
					for(int k=0;k<cj;k++)
						if(!nonzero[classDataStarts[j]+k] && Math.abs(decfun.alpha[ci+k]) > 0){
							nonzero[classDataStarts[j]+k] = true;//class-j
						}
					//add proAB
					model.probA[modelID]=modelInfos[modelID].proAB[0];
					model.probB[modelID]=modelInfos[modelID].proAB[1];
					//add rho
					model.rho[modelID]=decfun.rho;
					//
					modelID++;
				}
			}

			// build output

			model.nr_class = nr_class;

			model.label = classLabelInOrder;

			int nnz = 0;
			int[] nz_count = new int[nr_class];
			model.nSV = new int[nr_class];
			for(int i=0;i<nr_class;i++)
			{
				int nSV = 0;
				for(int j=0;j<classDataNum[i];j++)
					if(nonzero[classDataStarts[i]+j])
					{
						++nSV;
						++nnz;
					}
				model.nSV[i] = nSV;
				nz_count[i] = nSV;
			}

			model.l = nnz;
			model.SV = new svm_node[nnz][];
			int sv_i = 0;
			for(int i=0;i<nonzero.length;i++)
				if(nonzero[i]) {
					model.SV[sv_i++] = data_SVM.x[i];
				}

			int[] nz_start = new int[nr_class];
			nz_start[0] = 0;
			for(int i=1;i<nr_class;i++)
				nz_start[i] = nz_start[i-1]+nz_count[i-1];

			model.sv_coef = new double[nr_class-1][];
			for(int i=0;i<nr_class-1;i++)
				model.sv_coef[i] = new double[nnz];

			modelID = 0;
			for(int i=0;i<nr_class;i++)
				for(int j=i+1;j<nr_class;j++)
				{
					// classifier (i,j): coefficients with
					// i are in sv_coef[j-1][nz_start[i]...],
					// j are in sv_coef[i][nz_start[j]...]

					int si = classDataStarts[i];
					int sj = classDataStarts[j];
					int ci = classDataNum[i];
					int cj = classDataNum[j];

					int q = nz_start[i];
					decision_function decfun=modelInfos[modelID].decfunction;
					for(int k=0;k<ci;k++)
						if(nonzero[si+k])
							model.sv_coef[j-1][q++] = decfun.alpha[k];
					q = nz_start[j];
					for(int k=0;k<cj;k++)
						if(nonzero[sj+k])
							model.sv_coef[i][q++] = decfun.alpha[ci+k];
					modelID++;
				}
			
			return model;
		}
	}

	public static svm_model svm_combineTrained1vs1Model_DIY_ClassificationModel(svm_modelInfo[] modelInfos, svm_parameter param, SequenceFile.Reader[] ClassFeats,int[][] ClassFeatRandomOrders, int[] classLabelInOrder, float[][] scalingInfo, HashSet<Integer> FeatInds_Sel) throws IOException{ 
		/*
		 * combine trained 1vs1 model infos into one final model 
		 * 
		 * modelInfos, ClassFeats, ClassFeatRandomOrders, should be in the order of classLabelInOrder!!
		 * 
		 * when train each 1vs1 model, small classID should be positive, and the data is first positive and then negative, 
		 * 
		 * 
		 */
		svm_model model = new svm_model();
		model.param = param;
		
		int nr_class =classLabelInOrder.length;
		
		if(param.svm_type == svm_parameter.ONE_CLASS || param.svm_type == svm_parameter.EPSILON_SVR || param.svm_type == svm_parameter.NU_SVR){
			// regression or one-class-svm
			System.err.println("this svm_combineTrained1vs1Model_DIY_ClassificationModel only handle classification problem, not for regression or one-class-svm!!");
			return null;
		}else {// classification
			
			//find class data sample number
			int[] classDataNum=new int[nr_class];
			int modelID=0;
			for (int i = 0; i < nr_class; i++) {
				if(i < nr_class-2){
					classDataNum[i]= modelInfos[modelID].uniLabel_num.get(+1);
					for (int j = i+1; j < nr_class; j++) {
						modelID++;
					}
				}else if(i == nr_class-2){// nr_class-2, nr_class-1
					General.Assert(modelID==modelInfos.length-1, "error! in classDataNum of svm_combineTrained1vs1Model_DIY_ClassificationModel, modelID:"+modelID);
					classDataNum[i]= modelInfos[modelID].uniLabel_num.get(+1);//1st class always be positive class +1
					classDataNum[i+1]= modelInfos[modelID].uniLabel_num.get(-1);//2nd class always be negative class -1
				}
			}
			
			int totDataNum=General.sum_IntArr(classDataNum);
			
			boolean[] nonzero = new boolean[totDataNum];
			for(int i=0;i<totDataNum;i++)
				nonzero[i] = false;
			
			int[] classDataStarts=new int[nr_class];
			classDataStarts[0] = 0;
			for(int i=1;i<nr_class;i++)
				classDataStarts[i] = classDataStarts[i-1]+classDataNum[i-1];
				
			if (param.probability==1) {
				model.probA=new double[nr_class*(nr_class-1)/2];
				model.probB=new double[nr_class*(nr_class-1)/2];
			}
			
			model.rho = new double[nr_class*(nr_class-1)/2];
			
			modelID=0;
			for (int i = 0; i < nr_class; i++) {
				for (int j = i+1; j < nr_class; j++) {
					//find 2-class data sample number
					int ci=classDataNum[i]; int cj=classDataNum[j];
					//update nonzero
					decision_function decfun=modelInfos[modelID].decfunction;
					General.Assert(decfun.alpha.length==(ci+cj), "error in model-"+modelID+": alpha numbers in the decision function are not equal to these twoo class's data number, "
							+", decfun.alpha.length:"+decfun.alpha.length+", dataNum in these two class:"+(ci+cj));
					for(int k=0; k<ci; k++)
						if(!nonzero[classDataStarts[i]+k] && Math.abs(decfun.alpha[k]) > 0){
							nonzero[classDataStarts[i]+k] = true;//class-i
						}
					for(int k=0;k<cj;k++)
						if(!nonzero[classDataStarts[j]+k] && Math.abs(decfun.alpha[ci+k]) > 0){
							nonzero[classDataStarts[j]+k] = true;//class-j
						}
					//add proAB
					model.probA[modelID]=modelInfos[modelID].proAB[0];
					model.probB[modelID]=modelInfos[modelID].proAB[1];
					//add rho
					model.rho[modelID]=decfun.rho;
					//
					modelID++;
				}
			}

			// build output

			model.nr_class = nr_class;

			model.label = classLabelInOrder;

			int nnz = 0;
			int[] nz_count = new int[nr_class];
			model.nSV = new int[nr_class];
			for(int i=0;i<nr_class;i++)
			{
				int nSV = 0;
				for(int j=0;j<classDataNum[i];j++)
					if(nonzero[classDataStarts[i]+j])
					{
						++nSV;
						++nnz;
					}
				model.nSV[i] = nSV;
				nz_count[i] = nSV;
			}

			model.l = nnz;
			
			//make model SVs
			model.SV = new svm_node[nnz][];
			Key_IntArr key_featF=new Key_IntArr();
	        IntList_FloatList value_featF=new IntList_FloatList();
	        int sv_i = 0; int samp_i_inWholeData=0;
	        double lower = -1.0; double upper = 1.0;
			for (int class_i = 0; class_i < nr_class; class_i++) {//loop over all classes's 
				int trainDataNum=classDataNum[class_i];
				HashSet<Integer> randSel_svmTrain=new HashSet<Integer>(trainDataNum); 
				for (int j = 0; j < trainDataNum; j++) {
					randSel_svmTrain.add(ClassFeatRandomOrders[class_i][j]);
				}
				int sampleIndex=0;
				while(ClassFeats[class_i].next(key_featF, value_featF)){ //loop over all  key-value [photoID,classID]-IntArrFloatArr
					if (randSel_svmTrain.contains(sampleIndex)) {//selected photo, add into svm_train data
						if(nonzero[samp_i_inWholeData]) {//this is the support vector
							svm_node[] oneSample=General_LibSVM.data_sparse(value_featF.getIntegers(),value_featF.getFloats(),FeatInds_Sel);
							General_LibSVM.dataSampleScaling(oneSample, scalingInfo,lower,upper);
							model.SV[sv_i++] = oneSample;
						}
						samp_i_inWholeData++;
					}
					sampleIndex++;
		 		}
			}
			

			int[] nz_start = new int[nr_class];
			nz_start[0] = 0;
			for(int i=1;i<nr_class;i++)
				nz_start[i] = nz_start[i-1]+nz_count[i-1];

			model.sv_coef = new double[nr_class-1][];
			for(int i=0;i<nr_class-1;i++)
				model.sv_coef[i] = new double[nnz];

			modelID = 0;
			for(int i=0;i<nr_class;i++)
				for(int j=i+1;j<nr_class;j++)
				{
					// classifier (i,j): coefficients with
					// i are in sv_coef[j-1][nz_start[i]...],
					// j are in sv_coef[i][nz_start[j]...]

					int si = classDataStarts[i];
					int sj = classDataStarts[j];
					int ci = classDataNum[i];
					int cj = classDataNum[j];

					int q = nz_start[i];
					decision_function decfun=modelInfos[modelID].decfunction;
					for(int k=0;k<ci;k++)
						if(nonzero[si+k])
							model.sv_coef[j-1][q++] = decfun.alpha[k];
					q = nz_start[j];
					for(int k=0;k<cj;k++)
						if(nonzero[sj+k])
							model.sv_coef[i][q++] = decfun.alpha[ci+k];
					modelID++;
				}
			
			return model;
		}
	}
	
	public static boolean svm_isSameModel(svm_model model_1, svm_model model_2){ 
		/*
		 * check whether two model are exactly the same
		 */
		boolean isSame=true;
		if(model_1.l!=model_2.l){
			isSame=false;
			System.out.println("total SV number in model is not the same! ");
		}else if (model_1.nr_class!=model_2.nr_class) {
			isSame=false;
			System.out.println("total class number in model is not the same! ");
		}else if (!General.isSameArr(model_1.label, model_2.label)) {
			isSame=false;
			System.out.println("classLabelInOrder in model is not the same! ");
		}else if (!General.isSameArr(model_1.nSV, model_2.nSV)) {
			isSame=false;
			System.out.println("nSV in model is not the same! ");
		}else if (!General.isSameArr(model_1.probA, model_2.probA)) {
			isSame=false;
			System.out.println("probA in model is not the same! ");
		}else if (!General.isSameArr(model_1.probB, model_2.probB)) {
			isSame=false;
			System.out.println("probB in model is not the same! ");
		}else if (!General.isSameArr(model_1.rho, model_2.rho)) {
			isSame=false;
			System.out.println("rho in model is not the same! ");
		}else if (!General.isSameArrArr(model_1.sv_coef, model_2.sv_coef)) {
			isSame=false;
			System.out.println("sv_coef in model is not the same! ");
		}else if (!General_LibSVM.svm_isSameParam(model_1.param, model_2.param)) {
			isSame=false;
			System.out.println("param in model is not the same! ");
		}else if (!General_LibSVM.svm_isSameSVs(model_1.SV, model_2.SV)) {
			isSame=false;
			System.out.println("SVs in model is not the same! ");
		}
		
		return isSame;
	}
	
	public static boolean svm_isSameParam(svm_parameter param_1, svm_parameter param_2){ 
		/*
		 * check whether two svm_parameter are exactly the same
		 */
		boolean isSame=true;
		if(param_1.C!=param_2.C){
			isSame=false;
			System.out.println("Cost in svm_parameter is not the same! ");
		}else if (param_1.cache_size!=param_2.cache_size) {
			isSame=false;
			System.out.println("cache_size in svm_parameter is not the same! ");
		}else if (param_1.coef0!=param_2.coef0) {
			isSame=false;
			System.out.println("coef0 in svm_parameter is not the same! ");
		}else if (param_1.degree!=param_2.degree) {
			isSame=false;
			System.out.println("degree in svm_parameter is not the same! ");
		}else if (param_1.eps!=param_2.eps) {
			isSame=false;
			System.out.println("eps in svm_parameter is not the same! ");
		}else if (param_1.gamma!=param_2.gamma) {
			isSame=false;
			System.out.println("gamma in svm_parameter is not the same! ");
		}else if (param_1.kernel_type!=param_2.kernel_type) {
			isSame=false;
			System.out.println("kernel_type in svm_parameter is not the same! ");
		}else if (param_1.nr_weight!=param_2.nr_weight) {
			isSame=false;
			System.out.println("nr_weight in svm_parameter is not the same! ");
		}else if (param_1.nu!=param_2.nu) {
			isSame=false;
			System.out.println("nu in svm_parameter is not the same! ");
		}else if (param_1.p!=param_2.p) {
			isSame=false;
			System.out.println("p in svm_parameter is not the same! ");
		}else if (param_1.probability!=param_2.probability) {
			isSame=false;
			System.out.println("probability in svm_parameter is not the same! ");
		}else if (param_1.shrinking!=param_2.shrinking) {
			isSame=false;
			System.out.println("shrinking in svm_parameter is not the same! ");
		}else if (param_1.svm_type!=param_2.svm_type) {
			isSame=false;
			System.out.println("svm_type in svm_parameter is not the same! ");
		}else if (!General.isSameArr(param_1.weight_label, param_2.weight_label)) {
			isSame=false;
			System.out.println("weight_label in svm_parameter is not the same! ");
		}else if (!General.isSameArr(param_1.weight, param_2.weight)) {
			isSame=false;
			System.out.println("weight in svm_parameter is not the same! ");
		}

		return isSame;
	}
	
	public static boolean svm_isSameSVs(svm_node[][] samples_1, svm_node[][] samples_2){ 
		/*
		 * check whether two svm_parameter are exactly the same
		 */
		boolean isSame=true;
		if(samples_1.length!=samples_2.length){
			isSame=false;
		}else {
			for (int i = 0; i < samples_1.length; i++) {
				if (!svm_isSameSample(samples_1[i],samples_2[i])) {
					isSame=false;
					break;
				}
			}
		}

		return isSame;
	}
	
	public static boolean svm_isSameSample(svm_node[] sample_1, svm_node[] sample_2){ 
		/*
		 * check whether two svm_parameter are exactly the same
		 */
		boolean isSame=true;
		if(sample_1.length!=sample_2.length){
			isSame=false;
		}else {
			for (int i = 0; i < sample_1.length; i++) {
				if (!svm_isSameNode(sample_1[i],sample_2[i])) {
					isSame=false;
					break;
				}
			}
		}

		return isSame;
	}
	
	public static boolean svm_isSameNode(svm_node node_1, svm_node node_2){ 
		/*
		 * check whether two svm_node are exactly the same
		 */
		boolean isSame=true;
		if(node_1.index!=node_2.index){
			isSame=false;
		}else if (node_1.value!=node_2.value){
			isSame=false;
		}
		return isSame;
	}

	public static svm_modelInfo svm_trainOneModel_DIY_ClassificationModel(svm_problem prob, svm_parameter param, double Cp, double Cn, svm_problem prob_forProAB){ 
		/*
		 * train one 1vs1 model, data in prob and prob_forProAB should be two-class data, labels should be +1, -1,
		 */
		decision_function f = svm.svm_train_one(prob,param,Cp,Cn);
					
		if (param.probability == 1) {// probability model, 
			int sampleNum=prob.l;
			//make svNum_thisModel
			int svNum_thisModel = 0;
			for(int k=0; k<sampleNum; k++)
				if(Math.abs(f.alpha[k]) > 0){
					svNum_thisModel++;
				}
			//make SVs
			svm_node[][] SVs=new svm_node[svNum_thisModel][]; double svCoef[]=new double[svNum_thisModel];
			int SVs_i=0;
			for(int k=0;k<sampleNum;k++)
				if(Math.abs(f.alpha[k]) > 0){
					SVs[SVs_i]=prob.x[k];
					svCoef[SVs_i]=f.alpha[k];
					SVs_i++;
				}
			//make decision values for prob_forProAB
			double[] decVs=new double[prob_forProAB.l];
			for (int sample_i = 0; sample_i < prob_forProAB.l; sample_i++) {
				svm_node[] oneSample=prob_forProAB.x[sample_i];
				//make dec_value;
				double decV = 0;
				for(int ii=0;ii<svNum_thisModel;ii++)
					decV +=  Kernel.k_function(oneSample,SVs[ii],param)*svCoef[ii];
				decV -= f.rho;	
				decVs[sample_i]=decV;
			}
			//make probAB
			double[] probAB=new double[2];
			svm.sigmoid_train(decVs.length,decVs,prob_forProAB.y,probAB);
			//make svm_modelInfo
			svm_modelInfo modelInfo=new svm_modelInfo(f.alpha,f.rho,probAB,prob.y);
			return modelInfo;
		}else {
			//make svm_modelInfo
			svm_modelInfo modelInfo=new svm_modelInfo(f.alpha,f.rho,null,prob.y);
			return modelInfo;
		}
	}
	
	public static double svm_predict_DIYForMulti_1Vs1Model(svm_model[] models_1Vs1, svm_node[] x, int[] classLabels){
		/*
		 *  models_1Vs1 should be in the order, 0_1,0_2,...,0_n,1_2,1_3,...,n-1_n
		 */
		String errorTitle="error in svm_predict_probability_DIYForMultiModel, ";
		//check model
		for (int i = 0; i < models_1Vs1.length; i++) {
			//check probability
			General.Assert(svm.svm_check_probability_model(models_1Vs1[i])==1, 
					errorTitle+"model-"+i+" is not probability_model! do not turn-on probability during train!");
			//check class number
			General.Assert(models_1Vs1[i].nr_class==2, errorTitle+"model-"+i+" nr_class !=2, nr_class:"+models_1Vs1[i].nr_class);
		}
		//make dec_values
		int modelNum=models_1Vs1.length;
		double[] dec_values = new double[modelNum];
		for (int i = 0; i < modelNum; i++) {
			double[] dec_value = new double[1];
			svm.svm_predict_values(models_1Vs1[i], x, dec_value);
			dec_values[i]=dec_value[0];
		}
		
		//check classNum and modelNum
		int classNum=classLabels.length;
		General.Assert(modelNum==classNum*(classNum-1)/2, errorTitle
				+"modelNum should == classNum*(classNum-1)/2 which is "+classNum*(classNum-1)/2+", but modelNum="+modelNum);
		//make vote
		int p=0; int[] vote=new int[classNum];
		for(int i=0;i<classNum;i++)
			for(int j=i+1;j<classNum;j++)
				if(dec_values[p++] > 0)
					++vote[i];
				else
					++vote[j];
		//find max-vote class
		int vote_max_idx = 0;
		for(int i=1;i<classNum;i++)
			if(vote[i] > vote[vote_max_idx])
				vote_max_idx = i;
		return classLabels[vote_max_idx];
	}
	
	public static double svm_predict_probability_DIYForMulti_1Vs1Model(svm_model[] models_1Vs1, svm_node[] x, double[] prob_estimates, int[] classLabels){
		/*
		 *  models_1Vs1 should be in the order, 0_1,0_2,...,0_n,1_2,1_3,...,n-1_n
		 */
		String errorTitle="error in svm_predict_probability_DIYForMultiModel, ";
		for (int i = 0; i < models_1Vs1.length; i++) {
			//check probability
			General.Assert(svm.svm_check_probability_model(models_1Vs1[i])==1, 
					errorTitle+"model-"+i+" is not probability_model! do not turn-on probability during train!");
			//check class number
			General.Assert(models_1Vs1[i].nr_class==2, errorTitle+"model-"+i+" nr_class !=2, nr_class:"+models_1Vs1[i].nr_class);
		}
		//make dec_values
		int modelNum=models_1Vs1.length;
		double[] dec_values = new double[modelNum];
//		long tempTime=System.currentTimeMillis();
		for (int i = 0; i < modelNum; i++) {
			double[] dec_value = new double[1];
			svm.svm_predict_values(models_1Vs1[i], x, dec_value);
			dec_values[i]=dec_value[0];
		}
//		System.out.println(modelNum+" dec_values, time:"+General.dispTime(System.currentTimeMillis()-tempTime, "ms"));
		//check classNum and modelNum
		int classNum=prob_estimates.length;
		General.Assert(modelNum==classNum*(classNum-1)/2, errorTitle
				+"modelNum should == classNum*(classNum-1)/2 which is "+classNum*(classNum-1)/2+", but modelNum="+modelNum);
		//make prob_estimates
		int nr_class = classNum;
		double min_prob=1e-7;
		double[][] pairwise_prob=new double[nr_class][nr_class];
		int k=0;
		for(int i=0;i<nr_class;i++)
			for(int j=i+1;j<nr_class;j++)
			{
				pairwise_prob[i][j]=Math.min(Math.max(svm.sigmoid_predict(dec_values[k],models_1Vs1[k].probA[0],models_1Vs1[k].probB[0]),min_prob),1-min_prob);
				pairwise_prob[j][i]=1-pairwise_prob[i][j];
				k++;
			}
		svm.multiclass_probability(nr_class,pairwise_prob,prob_estimates);
		//find max pro
		int prob_max_idx = 0;
		for(int i=1;i<nr_class;i++)
			if(prob_estimates[i] > prob_estimates[prob_max_idx])
				prob_max_idx = i;
		return classLabels[prob_max_idx];
	}
	
	@SuppressWarnings("deprecation")
	public static void makeTrainDataFileNameSeq(String seqFilePath, int[] classLabelInOrder, String[] classFeatFileName, PrintWriter outStr_rep) throws IOException{
		//set traindata class file name SeqFile
		//set FileSystem
		Configuration conf = new Configuration();
        FileSystem hdfs  = FileSystem.get(conf);
 		SequenceFile.Writer SeqFileWriter = new SequenceFile.Writer(hdfs, conf, new Path(seqFilePath), IntWritable.class, Text.class);
 		int classNum=classLabelInOrder.length;
 		int ModelID=0; int ModelNum=0; 
 		for(int i=0;i<classNum;i++){
 			int classA=classLabelInOrder[i];
			for (int j = i+1; j < classNum; j++) {
				int classB=classLabelInOrder[j];
				SeqFileWriter.append(new IntWritable(ModelID++), new Text(classFeatFileName[classA]+","+classFeatFileName[classB])); //class File name: 
				ModelNum++;
			}
		}
 		SeqFileWriter.close();
 		General.dispInfo(outStr_rep, seqFilePath+" finished! total classes:"+classNum+", total 1vs1 models need to be train:"+ModelNum);
	}
	
	public static void main(String[] args) throws Exception {//debug
		//*****		test	******//
		test_svm_predict_probability_DIYForMulti_1Vs1Model(args);
	}

	@SuppressWarnings("unchecked")
	private static void test_svm_predict_probability_DIYForMulti_1Vs1Model(String[] args) throws Exception {
		                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
		String dataPath="D:/xinchaoli/Desktop/My research/My Code/DataSaved/MM13_Yahoo/M1_SVM/testDIY/";
//		String dataPath="/tudelft.net/staff-bulk/ewi/insy/D-MIRLab/XinchaoLi/MM13_Yahoo/M1_SVM/testDIY/";
		PrintWriter outStr_report=new PrintWriter(new OutputStreamWriter(new FileOutputStream(dataPath+"MyAPI.General.General_LibSVM.test_svm_predict_probability_DIYForMulti_1Vs1Model.report",false), "UTF-8"),true); 
		
//		int trainTestLabel=Integer.valueOf(args[0]);//test data is scaled according to train data, so must use at combination! set:50, 100 or 1000
		int trainTestLabel=100;
		int sampleNumPerClass_train=trainTestLabel;  int sampleNumPerClass_probAB=trainTestLabel; int testSamplePerClass=trainTestLabel; 
		
		String timeAcc="min";
		
		General.dispInfo(outStr_report,"test General_LibSVM.svm_predict_probability_DIYForMulti_1Vs1Model: " +
				"train sampleNumPerClass_train:"+sampleNumPerClass_train+", sampleNumPerClass_probAB:"+sampleNumPerClass_probAB+", testSamplePerClass:"+testSamplePerClass);

		HashMap<String, Integer> class_to_index=(HashMap<String, Integer>) General.readObject(dataPath+"class_to_index.hashMap_StringInteger");
		int classNum=class_to_index.keySet().size(); String[] classNames=new String[classNum];
		for (String className : class_to_index.keySet()) {
			int classID=class_to_index.get(className);
			classNames[classID]=className;
		}
		
		svm_problem data_SVM=(svm_problem) General.readObject(dataPath+"10class_SVM_"+sampleNumPerClass_train+"samplePerClass_scaledTrainData.svm_problem");
		svm_problem data_forProAB=(svm_problem) General.readObject(dataPath+"10class_SVM_"+sampleNumPerClass_probAB+"samplePerClass_scaledTrainDataForProbAB.svm_problem");
		svm_node[][] testSamples_data=(svm_node[][]) General.readObject(dataPath+"10class_SVM_"+testSamplePerClass+"testSamplesPerClass_data.svm_nodeArrArr");
		int[] testSamples_label=(int[]) General.readObject(dataPath+"10class_SVM_"+testSamplePerClass+"testSamplesPerClass_TrueLabels.intArr");
		
		int[] C2Power_range_Linear = {-10,  15, 2}; //C -5,  15, 2, G -15, 3, 2, We found that trying exponentially growing sequences of C and is apractical method to identify good parameters (for example, C = 2^-5,2^-3....2^15, G=2^-15,2^-13,....2^3
		int[] C_range_Arry=General.makeRange(C2Power_range_Linear);
		double Cost=C_range_Arry[0];
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
 		String checkParam=svm.svm_check_parameter(data_SVM, param);
	    if (checkParam==null){
	    	General.dispInfo(outStr_report,"Parameter is OK!  C:"+param.C);
	    	long startTime=System.currentTimeMillis();
	    	//****** 	train-method1: all together
//	    	int nr_fold_proModel=5;
//	    	svm_model model = svm.svm_train(data_SVM, param,nr_fold_proModel);
	    	svm_model model_1 = svm_train_DIY_ClassificationModel_seprateProb(data_SVM, param,  data_forProAB);
		    General.dispInfo(outStr_report,"train-method1: all together in one model, finished! .....time:"+General.dispTime(System.currentTimeMillis()-startTime, timeAcc));
		    int[] classLabelInOrder=model_1.label;
		    //******	train-method2: individual 1vs1, 
		    /**
		     * attention: when build 1vs1 models, the class-index order should be 0_1,0_2,..0_n-1,1_2,1_3,.....,n-1_n. 
		     * the actual class-label should be in modelLabels, here we want to check the DIYed method with the ori method, so the modelLabels is the same as in ori
		     */
		    int modelNum=classNum*(classNum-1)/2;
		    int model_i=0; svm_modelInfo[] modelInfos=new svm_modelInfo[modelNum];
		    for (int i = 0; i < classNum; i++) {
				for (int j = i+1; j < classNum; j++) {
					//make data_SVM_i
					svm_problem data_SVM_i=new svm_problem();
					data_SVM_i.l=sampleNumPerClass_train*2; data_SVM_i.x=new svm_node[data_SVM_i.l][]; data_SVM_i.y=new double[data_SVM_i.l];
					int p=0;
					for (int k = 0; k < data_SVM.l; k++) {
						if(data_SVM.y[k]==classLabelInOrder[i]){
							data_SVM_i.x[p]=data_SVM.x[k];
							data_SVM_i.y[p]=+1;
							p++;
						}
						if(data_SVM.y[k]==classLabelInOrder[j]){
							data_SVM_i.x[p]=data_SVM.x[k];
							data_SVM_i.y[p]=-1;
							p++;
						}
					}
					//make data_forProAB_i
					svm_problem data_forProAB_i=new svm_problem();
					data_forProAB_i.l=sampleNumPerClass_probAB*2; data_forProAB_i.x=new svm_node[data_forProAB_i.l][]; data_forProAB_i.y=new double[data_forProAB_i.l];
					p=0;
					for (int k = 0; k < data_forProAB.l; k++) {
						if(data_forProAB.y[k]==classLabelInOrder[i]){
							data_forProAB_i.x[p]=data_forProAB.x[k];
							data_forProAB_i.y[p]=+1;
							p++;
						}
						if(data_forProAB.y[k]==classLabelInOrder[j]){
							data_forProAB_i.x[p]=data_forProAB.x[k];
							data_forProAB_i.y[p]=-1;
							p++;
						}
					}
					modelInfos[model_i++]=svm_trainOneModel_DIY_ClassificationModel(data_SVM_i, param, param.C, param.C,  data_forProAB_i);
				}
			}
		    General.dispInfo(outStr_report,"train-method2: individual multiple 1vs1 models, finished! .....time:"+General.dispTime(System.currentTimeMillis()-startTime, timeAcc));
		    svm_model model_2= svm_combineTrained1vs1Model_DIY_ClassificationModel(modelInfos,  param,  data_SVM, classLabelInOrder);
		    General.Assert(General_LibSVM.svm_isSameModel(model_1, model_2), "error! model_1 and model_2 are not the same!");
		    General.dispInfo(outStr_report,"train-method2: combine multiple 1vs1 models, finished! .....time:"+General.dispTime(System.currentTimeMillis()-startTime, timeAcc));
		    //test-eval from method1 model
		    ArrayList<Short> trueClass=new ArrayList<Short>(); ArrayList<Short> predClass_vote=new ArrayList<Short>(); ArrayList<Short> predClass_prob=new ArrayList<Short>();
		    double[][] prob_estimates_ori=new double[testSamples_data.length][]; long aveTestTime_prob=0;long aveTestTime_vote=0;
		    for (int i = 0; i < testSamples_data.length; i++) {
		    	trueClass.add((short) testSamples_label[i]);
		    	//use svm_predict_probability
		    	double[] prob_estimate=new double[classNum];
		    	long tempTime=System.currentTimeMillis();
		    	predClass_prob.add((short) svm.svm_predict_probability(model_1,testSamples_data[i],prob_estimate));
		    	aveTestTime_prob+=System.currentTimeMillis()-tempTime;
		    	prob_estimates_ori[i]=prob_estimate;
		    	//use svm_predict, vote
		    	tempTime=System.currentTimeMillis();
		    	predClass_vote.add((short) svm.svm_predict(model_1,testSamples_data[i]));
		    	aveTestTime_vote+=System.currentTimeMillis()-tempTime;
			}
		    aveTestTime_prob/=testSamples_data.length;  aveTestTime_vote/=testSamples_data.length;
		    General.dispInfo(outStr_report,"using svm_predict_probability, aveTestTime for one sample:"+General.dispTime(aveTestTime_prob, "ms"));
		    General.dispInfo(outStr_report,"using svm_predict vote, aveTestTime for one sample:"+General.dispTime(aveTestTime_vote, "ms"));
		    int[][] ConfMatrix=General.mkConfusionMatrix(trueClass, predClass_prob, classNum);
		    General.dispInfo(outStr_report,"using svm_predict_probability, "+General.gtInfo_from_ConfusionMatrix(ConfMatrix, classNames));
		    ConfMatrix=General.mkConfusionMatrix(trueClass, predClass_vote, classNum);
		    General.dispInfo(outStr_report,"using svm_predict vote, "+General.gtInfo_from_ConfusionMatrix(ConfMatrix, classNames));
		    General.dispInfo(outStr_report,"VTest from method1 model finished!......time:"+General.dispTime(System.currentTimeMillis()-startTime, timeAcc)+"\n");
		    //test-eval from method2 model
		    trueClass=new ArrayList<Short>(); predClass_vote=new ArrayList<Short>(); predClass_prob=new ArrayList<Short>();
		    double[][] prob_estimates_DIY=new double[testSamples_data.length][];  aveTestTime_prob=0; aveTestTime_vote=0;
		    for (int i = 0; i < testSamples_data.length; i++) {
		    	trueClass.add((short) testSamples_label[i]);
		    	//use svm_predict_probability
		    	double[] prob_estimate=new double[classNum];
		    	long tempTime=System.currentTimeMillis();
		    	predClass_prob.add((short) svm.svm_predict_probability(model_2,testSamples_data[i],prob_estimate));
		    	aveTestTime_prob+=System.currentTimeMillis()-tempTime;
		    	prob_estimates_DIY[i]=prob_estimate;
		    	//use svm_predict, vote
		    	tempTime=System.currentTimeMillis();
		    	predClass_vote.add((short) svm.svm_predict(model_1,testSamples_data[i]));
		    	aveTestTime_vote+=System.currentTimeMillis()-tempTime;
			}
		    aveTestTime_prob/=testSamples_data.length;  aveTestTime_vote/=testSamples_data.length;
		    General.dispInfo(outStr_report,"using svm_predict_probability, aveTestTime for one sample:"+General.dispTime(aveTestTime_prob, "ms"));
		    General.dispInfo(outStr_report,"using svm_predict vote, aveTestTime for one sample:"+General.dispTime(aveTestTime_vote, "ms"));
		    ConfMatrix=General.mkConfusionMatrix(trueClass, predClass_prob, classNum);
		    General.dispInfo(outStr_report,"using svm_predict_probability, "+General.gtInfo_from_ConfusionMatrix(ConfMatrix, classNames));
		    ConfMatrix=General.mkConfusionMatrix(trueClass, predClass_vote, classNum);
		    General.dispInfo(outStr_report,"using svm_predict vote, "+General.gtInfo_from_ConfusionMatrix(ConfMatrix, classNames));
		    General.dispInfo(outStr_report,"VTest from method1 model finished!......time:"+General.dispTime(System.currentTimeMillis()-startTime, timeAcc)+"\n");
		    //check probability match
		    double aveErr=0;
		    for (int i = 0; i < prob_estimates_DIY.length; i++) {
				double sum=0;
				for (int j = 0; j < prob_estimates_DIY[i].length; j++) {
					sum+=Math.abs(prob_estimates_DIY[i][j]-prob_estimates_ori[i][j]);
				}
				aveErr+=sum/prob_estimates_DIY[i].length;
			}
		    aveErr/=prob_estimates_DIY.length;
		    General.dispInfo(outStr_report,"aveErr for each number between prob_estimates_DIY and prob_estimates_oir: "+ aveErr);
		    //check MAP for ori
		    double map=General_IR.MAP( classNum, testSamples_label, prob_estimates_ori, classLabelInOrder);
		    double rand_map=General_IR.random_MAP( classNum, testSamples_label, classLabelInOrder);
		    General.dispInfo(outStr_report,"Map for prob_estimates_ori: "+map+", rand_map: "+rand_map);
		    //check MAP for DIYed
		    map=General_IR.MAP( classNum, testSamples_label, prob_estimates_DIY, classLabelInOrder);
		    rand_map=General_IR.random_MAP( classNum, testSamples_label, classLabelInOrder);
		    General.dispInfo(outStr_report,"Map for prob_estimates_DIY: "+map+", rand_map: "+rand_map);
	    }else{
	    	 General.dispInfo(outStr_report,"Parameter is Incorrect! error message: "+checkParam);
	    }
	    outStr_report.close();
	}
}
