package MyLibSVM;

import java.util.HashMap;

import MyLibSVM.svm.decision_function;

@SuppressWarnings("serial")
public class svm_modelInfo implements java.io.Serializable{
	public decision_function decfunction;
	public double[] proAB;
	public double[] labels;
//	public ArrayList<Double> uniLabels;
//	public int[] uniLabel_nums;
	public HashMap<Integer,Integer> uniLabel_num;
	public svm_modelInfo (double[] alpha,double rho,double[] proAB, double[] labels) {
		this.decfunction=new decision_function();
		this.decfunction.alpha=alpha;
		this.decfunction.rho=rho;
		this.proAB=proAB;
		this.labels=labels;
		//make uniLabel_num
		HashMap<Integer,Integer> uniLabel_num=new HashMap<Integer,Integer>();
		for (int i = 0; i < labels.length; i++) {
			int oneLabel=(int)labels[i];
			if (uniLabel_num.containsKey(oneLabel)) {
				uniLabel_num.put(oneLabel,uniLabel_num.get(oneLabel)+1);
			}else {
				uniLabel_num.put(oneLabel,1);
			}
		} 
//		ArrayList<Double> uniLabels=new ArrayList<Double>(uniLabel_num.size());
//		int[] uniLabel_nums=new int[uniLabel_num.size()];
//		int i=0;
//		for (Double oneLabel : uniLabel_num.keySet()) {
//			uniLabels.add(oneLabel);
//			uniLabel_nums[i]=uniLabel_num.get(oneLabel);
//			i++;
//		}
//		this.uniLabels=uniLabels;
//		this.uniLabel_nums=uniLabel_nums;
		this.uniLabel_num=uniLabel_num;
	}
}
