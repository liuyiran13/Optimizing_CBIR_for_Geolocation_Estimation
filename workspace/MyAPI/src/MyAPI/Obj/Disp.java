package MyAPI.Obj;

import java.io.PrintWriter;

import MyAPI.General.General;

public class Disp {

	public boolean disp;
	public String spacer;
	public PrintWriter outputStream_report;
	
	public Disp(boolean disp, String spacer, PrintWriter outputStream_report){
		this.disp=disp;
		this.spacer=spacer;
		this.outputStream_report=outputStream_report;
	}
	
	public static Disp makeHardCopy(Disp disp){
		return new Disp(disp.disp, disp.spacer, disp.outputStream_report);
	}
	
	public static Disp makeHardCopy(Disp disp, String spacer){
		return new Disp(disp.disp, spacer, disp.outputStream_report);
	}
	
	public static Disp makeHardCopyAddSpacer(Disp disp, String spacer){
		return new Disp(disp.disp, disp.spacer+spacer, disp.outputStream_report);
	}
	
	public static Disp getNotDisp(){
		return new Disp(false, null, null);
	}
	
	public void disp(String info){
		if(disp){
			General.dispInfo(outputStream_report, spacer+info);
		}
	}
	
	public void disp(Exception e){
		e.printStackTrace(System.out);
		if (outputStream_report!=null) {
			e.printStackTrace(outputStream_report);
		}
	}
	
	public void disp(int inter, int i, String info){
		if(disp && i%inter==0){
			General.dispInfo(outputStream_report, spacer+info);
		}
	}
	
	public static void main(String[] args) {

	}

}
