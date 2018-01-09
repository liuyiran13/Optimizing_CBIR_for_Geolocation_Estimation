package MyAPI.imagR;

public class RankModel {
	
	public static class ReRankTopRank{
		public int rerankLen;
		public int topRankLen;
		
		public ReRankTopRank(int rerankLen, int finalRankLen){
			this.rerankLen=rerankLen;
			this.topRankLen=finalRankLen;
		}
		
		public ReRankTopRank(String label){
			//rerankLen: reR@1000@1000
			String[] info_rerankLen=label.split("@");
			rerankLen=Integer.valueOf(info_rerankLen[1]);
			topRankLen=Integer.valueOf(info_rerankLen[2]);
		}
		
		public String toString(){
			return "reR"+rerankLen+"_topR"+topRankLen;
			
		}
	}
	
	private String rankModelStr;
	//rank
	String iniR_weight;
	ReRankTopRank reRankTopRank;
	String rerank_HEPara;
	String rerank_scoreFlag;
	
	/**
	 * "_iniR-noBurst@18@12_reR@1000@1000_HE@18@12_1vs1AndHistAndAngle@truee@false@0.52@0.2@1@0@0@0@0@0@0@0"
	 * 
	 * "_iniR-noBurst@64@0_reR@1000@1000_HE@64@0_1vs1AndHistAndAngle@truee@false@0.52@0.2@1@0@0@0@0@0@0@0"
	 */
	public RankModel(String rankModelStr) {
		this.rankModelStr=rankModelStr;
		String[] info=rankModelStr.split("_");
		//iniR_weight
		iniR_weight="_"+info[1];
		//rerankLen: reR@1000@1000
		reRankTopRank=new ReRankTopRank(info[2]);
		//rerank_HEPara
		rerank_HEPara=info[3];
		//rerank_scoreFlag
		rerank_scoreFlag="_"+info[4];
	}
	
	@Override
	public String toString(){
		return rankModelStr;
	}

}
