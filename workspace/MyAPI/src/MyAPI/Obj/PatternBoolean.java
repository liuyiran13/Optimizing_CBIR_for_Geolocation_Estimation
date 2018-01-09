package MyAPI.Obj;

import MyAPI.General.General;

public class PatternBoolean {

	
	String[] targetCorrParten;
	boolean[] neutrals;
	private boolean[] targetCorrParten_act;
	
	public PatternBoolean(String[] targetCorrParten) {//true, false, neutral
		this.targetCorrParten=targetCorrParten;
		targetCorrParten_act=new boolean[targetCorrParten.length];
		neutrals=new boolean[targetCorrParten.length];
		for (int i = 0; i < targetCorrParten.length; i++) {
			if (targetCorrParten[i].equalsIgnoreCase("neutral")) {
				neutrals[i]=true;
				targetCorrParten_act[i]=true;
			}else {
				neutrals[i]=false;
				targetCorrParten_act[i]=Boolean.valueOf(targetCorrParten[i]);
			}
		}
	}
	
	public boolean isSamePattern(boolean[] one){
		return General.isSameArr(General.orBooleanArr(one, neutrals), targetCorrParten_act);
	}
	
	public String getTargetPattern(){
		return General.ArrToString(targetCorrParten, "_");
	}

}
