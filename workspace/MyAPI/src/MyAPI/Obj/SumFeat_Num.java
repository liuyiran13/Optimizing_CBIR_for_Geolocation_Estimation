package MyAPI.Obj;

import MyAPI.General.General;

public class SumFeat_Num {
	public double[] sumFeat;
	public int num;
	
	public SumFeat_Num(double[] projFeat, int num) {
		this.sumFeat=projFeat;
		this.num=num;
	}
	
	public void addOneFeat(double[] toAddFeat){
		General.addDoubleArr(sumFeat, toAddFeat);
		num++;
	}

}
