package MyAPI.Geo.CocReg;

import MyAPI.General.General;

public class CocRegs {//to make unit comparisons, regIDs should always in the ascending order! if not, then [1,2] and [2,1] will be not the same according to equals()!!

	Integer[] regIDs;
			
	public CocRegs(Integer[] regIDs) {
		General.Assert(General.isOrdered(regIDs, true), "err! order in regIDs must be always in the ascending order! here regIDs: "+General.IntArrToString(regIDs, ","));
		this.regIDs=regIDs;
	}
	
	public int getLastReg(){
		return regIDs[regIDs.length-1];
	}
	
	public int getRegDim(){
		return regIDs.length;
	}
	
	@Override
	public int hashCode(){
		int cont=341; //31*11;
		for (int one : regIDs) {
			cont+=one;
		}
		return cont;
	}
	
	@Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CocRegs)) 
        	return false;
        CocRegs that = (CocRegs) o;
        return General.isSameArr(regIDs, that.regIDs);    
    }
	
	@Override
	public String toString(){
		return General.ArrToString(regIDs, "_");
	}

}
