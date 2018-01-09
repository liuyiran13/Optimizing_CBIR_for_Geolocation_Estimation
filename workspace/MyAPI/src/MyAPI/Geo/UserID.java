package MyAPI.Geo;

/**
 * flickr usrID: long@int
 * 
 *
 */
public class UserID {
	long userID_0;
	int userID_1;
	
	public UserID(long userID_0, int userID_1) {
		this.userID_0=userID_0;
		this.userID_1=userID_1;
	}
	
	@Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UserID)) 
        	return false;

        UserID that = (UserID) o;

        if (this.userID_0==that.userID_0 && this.userID_1==that.userID_1) {
			return false;
		}else{
			return true;
		}
        
    }
	
	@Override
    public int hashCode() {
        return (int)(userID_0/1000)+userID_1;
    }
	
	 @Override
    public String toString() {
        return userID_0+"@"+userID_1;
    }

}
