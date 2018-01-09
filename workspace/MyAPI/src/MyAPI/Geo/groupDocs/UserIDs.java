package MyAPI.Geo.groupDocs;

import MyAPI.Geo.UserID;

public class UserIDs {
	long[] userIDs_0;
	int[] userIDs_1;
	
	public UserIDs(long[] userIDs_0, int[] userIDs_1) {
		this.userIDs_0=userIDs_0;
		this.userIDs_1=userIDs_1;
	}
	
	public UserID getOneUsr(int ind){
		return new UserID(userIDs_0[ind], userIDs_1[ind]);
	}
	
	public String getUserIDlabel(int phoID){
		if (userIDs_0==null) {
			return "";
		} else {
			return "_"+userIDs_0[phoID]+"@"+userIDs_1[phoID];
		}
	}

}
