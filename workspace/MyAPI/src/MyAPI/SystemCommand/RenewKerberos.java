package MyAPI.SystemCommand;

import java.io.IOException;
import java.util.Arrays;

import MyAPI.General.General;

public class RenewKerberos {

	String passWord;
	
	public RenewKerberos() {
		System.out.println("Please Enter the password for Kerberos:");
		passWord=General.getUserInputHidden();
	}
	
	public void renewTicket(boolean disp) throws IOException, InterruptedException{
		General.dispInfo_ifNeed(disp, "", "Renew Kerberos...");
		General.runSysCommand(Arrays.asList("kinit", "yliu"), passWord, disp);
	}
}
