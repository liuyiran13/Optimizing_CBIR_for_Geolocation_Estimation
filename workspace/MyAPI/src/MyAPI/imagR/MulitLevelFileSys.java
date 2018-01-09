package MyAPI.imagR;

import MyAPI.General.General;

public class MulitLevelFileSys {

	String rootPath;
	int fileInter;
	String suffix;
	
	public MulitLevelFileSys(String rootPath, int fileInter, String suffix, boolean disp) {
		this.rootPath=rootPath;
		this.fileInter=fileInter;
		this.suffix=suffix; //".seq"
		General.dispInfo_ifNeed(disp, "", "in MulitLevelFileSys, rootPath: "+rootPath+", fileInter: "+fileInter+", suffix: "+suffix);
	}

	String getOneFilePath(int vw){//vw should index from 0!
		return rootPath+"/"+vw/fileInter+"/"+vw+suffix;
	}
}
