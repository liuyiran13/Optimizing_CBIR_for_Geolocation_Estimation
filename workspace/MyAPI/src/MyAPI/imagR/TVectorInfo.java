package MyAPI.imagR;

public class TVectorInfo {
	public int photoNum;
	public int featNum;
	
	public TVectorInfo(int photoNum, int featNum) {
		this.photoNum=photoNum;
		this.featNum=featNum;
	}
	
	public TVectorInfo() {
		this.photoNum=0;
		this.featNum=0;
	}
	
	public void addOneDoc(int featNum){
		photoNum++;
		this.featNum+=featNum;
	}

}
