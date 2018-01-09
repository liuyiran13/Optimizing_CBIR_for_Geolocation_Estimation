package GVM;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import MyAPI.General.BoofCV.ComparePhotos_runLocal;
import MyAPI.Obj.Conf_localMachine;
import MyAPI.Obj.Disp;
import MyAPI.imagR.ExtractFeat;
import MyAPI.imagR.ImageDataManager;
import MyAPI.imagR.MutiAssVW;
import MyAPI.imagR.ScoreDoc;
import MyAPI.imagR.ShowMatches;

public class Analysis {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Conf_localMachine conf_LocMac= new Conf_localMachine();
		String basePath="O:/SanFrancisco_StreetView/";
		String indexLabel="_SanFran_20K-VW_SURF";
		String rankModel="_1vs1AndHistAndAngle@0.52@0.2@0@0@0@0@0@0@0"; //"_1vs1AndHistAndAngle@0.52@0.2@0@0@0@0@0@0@0"
		boolean isHE=true; int HEThr=20; int HE_Delta=12; 
		
		int query=23; List<Integer> docs=Arrays.asList(188163,187961,3613); //8540345:8770155,7483384//8718234:7994646,7812793,3847661
		
		Disp disp=new Disp(true, "", null);
		//setup extractFeat
		ExtractFeat extractFeat_Q=ComparePhotos_runLocal.setupExtractFeat_SURF(disp, null, new MutiAssVW(true, 0.05));
		ExtractFeat extractFeat_D=ComparePhotos_runLocal.setupExtractFeat_SURF(disp, null, new MutiAssVW(false, 0));
		//setup imageDataManager: mapFiles feats
		ImageDataManager imageDataManager_Q=new ImageDataManager("MapFile", 100*1000, null, new String[]{"N:/ewi/insy/MMC/XinchaoLi/PhotoDataBase/SanFrancisco_StreetView/SanFrancisco_inSInd_MFiles/"}, 0, null, extractFeat_Q);
		ImageDataManager imageDataManager_D=new ImageDataManager("MapFile", 100*1000, null, new String[]{"N:/ewi/insy/MMC/XinchaoLi/PhotoDataBase/SanFrancisco_StreetView/SanFrancisco_inSInd_MFiles/"}, 0, null, extractFeat_D);
		imageDataManager_Q.loadPhoFeat_InMemory(Arrays.asList(query), disp);
		imageDataManager_D.loadPhoFeat_InMemory(docs, disp);
		//setup scoreDoc
		String docInfoPath=basePath+"index/docInfo"+indexLabel+"/part-r-00000";
		String TVectorInfoPath=basePath+"index/TVectorInfo"+indexLabel;
		ScoreDoc scoreDoc=new ScoreDoc(disp, "_iniR-noBurst", rankModel, HEThr, HE_Delta, isHE, null, docInfoPath, TVectorInfoPath, conf_LocMac.conf, true);
		//setup ShowMatches
		ShowMatches showMatches=new ShowMatches(true, imageDataManager_Q, imageDataManager_D, 0, 5);
		//setup ComparePhotos_runLocal
		ComparePhotos_runLocal comparePhotos_runLocal=new ComparePhotos_runLocal(imageDataManager_Q, imageDataManager_D, scoreDoc, null, showMatches, null, null);
		//run
		comparePhotos_runLocal.compareOneQueryWithMutDoc(query, docs, disp);
		//disp
		disp.disp(comparePhotos_runLocal.getComputingTimeReport());
		showMatches.disp();
		//clean up
		imageDataManager_Q.cleanUp();
		imageDataManager_D.cleanUp();
	}
}
