package MyAPI.General;

import java.awt.color.CMMException;
import java.awt.image.BufferedImage;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import javax.net.ssl.HttpsURLConnection;
import javax.xml.parsers.SAXParser;

import org.apache.commons.io.IOUtils;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class General_http {
	public static class SAX_XML_handler_videoMeta extends DefaultHandler { //SAXhandler

		public boolean bool_lat=false;
		public boolean bool_lon=false;
		public boolean bool_tag=false;
		
		public String Lat;
		public String Long;
		public String Tag; 

		public void startElement(String uri, String localName,String qName, 
	               Attributes attributes) throws SAXException {

			if(qName.equalsIgnoreCase("Latitude")){
				bool_lat=true;
//				System.out.println("Start Element :" + qName);
			}
			
			if(qName.equalsIgnoreCase("Longitude")){
				bool_lon=true;
//				System.out.println("Start Element :" + qName);
			}
			
			if(qName.equalsIgnoreCase("Keywords")){
				bool_tag=true;
//				System.out.println("Start Element :" + qName);
			}
		}

		public void endElement(String uri, String localName,
			String qName) throws SAXException {

		}

		public void characters(char ch[], int start, int length) throws SAXException {
			if (bool_lat) {
				Lat = new String(ch, start, length);
//			    System.out.println("Latitude : " + Lat);
			    bool_lat = false;
			}
			if (bool_lon) {
				Long = new String(ch, start, length);
//			    System.out.println("Longitude : " + Long);
			    bool_lon = false;
			}
			if (bool_tag) {
				Tag = new String(ch, start, length);
//			    System.out.println("tag : " + Tag);
				if(Tag.contains("\n")){
					Tag="NoTag";
				}
			    bool_tag = false;
			}
		}

	}

	public static class SAX_XML_handler_flickrPhoSearch extends DefaultHandler { //SAXhandler, only a
		
//		public int page;
		public int pages;
//		public int perPage;
//		public int total;
		public long total;
		public ArrayList<Long> photoIDs; 
		public ArrayList<String> userIDs;
		public ArrayList<Long> dateUploads;
		public ArrayList<String> dateTakens;
		public ArrayList<Float> lats;
		public ArrayList<Float> lons;
		public ArrayList<Integer> accs;
		public ArrayList<String> tags;
		public ArrayList<String> urls;
		public ArrayList<Integer> views;
		public ArrayList<Integer> licenseIDs;
		
		static public Set<String> allowedAttributeNames;
		static { //static constructor
			allowedAttributeNames = new HashSet<String>(Arrays.asList(
					"id", "owner", "dateupload", "datetaken", "latitude", "longitude",
					"accuracy", "tags", "url_m", "views", "license"
			));
		}

	    public SAX_XML_handler_flickrPhoSearch (int listLength)
	    {
	    	super();
	    	//initial arrayList
			photoIDs=new ArrayList<Long>(listLength);
			userIDs= new ArrayList<String>(listLength);
			dateUploads=new ArrayList<Long>(listLength);
			dateTakens=new ArrayList<String>(listLength);
			lats= new ArrayList<Float>(listLength);
			lons= new ArrayList<Float>(listLength);
			accs= new ArrayList<Integer>(listLength);
			tags= new ArrayList<String>(listLength);
			urls= new ArrayList<String>(listLength);
			views= new ArrayList<Integer>(listLength);
			licenseIDs= new ArrayList<Integer>(listLength);
	    }

		
		public void startElement(String uri, String localName,String qName, Attributes attributes) throws SAXException {
			
			int length = attributes.getLength();
			HashMap<String, String> onePhoto= new HashMap<String, String>();
			//Each attribute
			for (int i=0; i<length; i++) {
				// Get names and values to each attribute
				String attriName = attributes.getQName(i);
				String attriValue = attributes.getValue(i);
				// extract attributes
//				if(attriName.equalsIgnoreCase("page"))
//					page= Integer.valueOf(attriValue);
//				else 
				if (attriName.equalsIgnoreCase("pages"))
					pages= Integer.valueOf(attriValue);
				if (attriName.equalsIgnoreCase("total"))
					total= Long.valueOf(attriValue);
//				else if (attriName.equalsIgnoreCase("perpage"))
//					perPage= Integer.valueOf(attriValue);
//				else if (attriName.equalsIgnoreCase("total"))
//					total= Integer.valueOf(attriValue);
				else if (allowedAttributeNames.contains(attriName.toLowerCase())) {
					onePhoto.put(attriName, attriValue);
				}
			}
			
			if(!onePhoto.containsKey("views"))
				onePhoto.put("views", "0");
			if(!onePhoto.containsKey("license"))
				onePhoto.put("license", "0");
			
			if(onePhoto.size()==11){// this photo has all required attributes
				photoIDs.add(Long.valueOf(onePhoto.get("id")));
				userIDs.add(onePhoto.get("owner"));
				dateUploads.add(Long.valueOf(onePhoto.get("dateupload")));
				dateTakens.add(onePhoto.get("datetaken"));
				lats.add(Float.valueOf(onePhoto.get("latitude")));
				lons.add(Float.valueOf(onePhoto.get("longitude")));
				accs.add(Integer.valueOf(onePhoto.get("accuracy")));
				tags.add(onePhoto.get("tags"));
				urls.add(onePhoto.get("url_m"));
				views.add(Integer.valueOf(onePhoto.get("views")));
				licenseIDs.add(Integer.valueOf(onePhoto.get("license")));
			}
		}

		public void endElement(String uri, String localName,
			String qName) throws SAXException {
		}

	}

	public static class SAX_XML_handler_flickrPhoInfo extends DefaultHandler { //SAXhandler, only a
		
		public StringBuffer mTags;
		public String temp;
		
	    public SAX_XML_handler_flickrPhoInfo (){
	    	super();
	    	//initial arrayList
	    	mTags=new StringBuffer();
	    }

	    /*
        * When the parser encounters plain text (not XML elements),
        * it calls(this method, which accumulates them in a string buffer
        */
	    public void characters(char[] buffer, int start, int length) {
	    	temp = new String(buffer, start, length);
	    }
      
       /*
        * Every time the parser encounters the beginning of a new element,
        * it calls this method, which resets the string buffer
        */ 
       public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
    	   temp = "";
       }

       /*
        * When the parser encounters the end of an element, it calls this method
        */
       public void endElement(String uri, String localName, String qName) throws SAXException {
    	   if (qName.equalsIgnoreCase("tag")) {
    		   // add it to the mTags
    		   mTags.append(temp+" ");
    	   }
       }

	}

	public static SAX_XML_handler_flickrPhoSearch search(String min_upload_date_whole, String max_upload_date_whole,  float i_lat, float i_lon, float blockSize, int page, String key, int accuracy, String extras, int perPage, SAXParser saxParser, int sleepSec, int block_index, String repLabel, PrintWriter outputStream_Report){
		//get min,max time
		String min_upload_date=min_upload_date_whole+"";
		String max_upload_date=max_upload_date_whole+"";	
		//search one block
		//set search parametes
		String bb_minLong=i_lon+""; String bb_minLat=i_lat+""; String bb_maxLong=(i_lon+blockSize)+""; String bb_maxLat=(i_lat+blockSize)+"";
		String url_Query="http://api.flickr.com/services/rest/?method=flickr.photos.search&api_key="+key+"&bbox="+bb_minLong+"%2C"+bb_minLat+"%2C"+bb_maxLong+"%2C"+bb_maxLat
				+"&extras="+extras+"&min_upload_date="+min_upload_date+"&max_upload_date="+max_upload_date+"&accuracy="+accuracy+"&page="+page+"&per_page="+perPage+"&format=rest";
		//set xml paser handler
		SAX_XML_handler_flickrPhoSearch handler = new SAX_XML_handler_flickrPhoSearch(perPage); //SAXhandler
		//http-config
		HttpURLConnection.setFollowRedirects(false); //do not allow redirect link
		//query flickr
		boolean ok = true;
		do {
			try {
				//set url connection
				URL imageURL = new URL(url_Query);
				URLConnection connection = imageURL.openConnection();
				HttpURLConnection httpConnection = (HttpURLConnection) connection; 
				int code = httpConnection.getResponseCode();
				if(code==200){
					Reader reader = new InputStreamReader(httpConnection.getInputStream(),"UTF-8");
					InputSource xml = new InputSource(reader); xml.setEncoding("UTF-8");
					saxParser.parse(xml, handler);
					ok = true;
				}else{
					General.dispInfo(outputStream_Report, repLabel+"...url-respond error!, block_index: "+block_index+", code:"+code);
					General.dispInfo(outputStream_Report, repLabel+"...sleep for "+sleepSec+" seconds!");
					try {
		                Thread.sleep(1000*sleepSec);
					} catch (Exception ex) {}
					ok = false;
				}
				httpConnection.disconnect();
			} catch (ConnectException e) {//time pase expceptiion dfm.parse(dataTaken).getTime()/1000; 
				General.dispInfo(outputStream_Report,repLabel+"...ConnectException!, block_index: "+block_index+", message:"+e.getMessage());
				General.dispInfo(outputStream_Report,repLabel+"...sleep for "+sleepSec+" seconds!");
				try {
	                Thread.sleep(1000*sleepSec);
				} catch (Exception ex) {}
				ok = false;
			} catch (IOException e) {
				General.dispInfo(outputStream_Report,repLabel+"...IOException! skipped_block, block_index: "+block_index+", message:"+e.getMessage()+", url_Query:"+url_Query);
				ok = true;
			} catch (SAXException e) {
				General.dispInfo(outputStream_Report,repLabel+"...xml SAXException!, skipped_block, block_index: "+block_index+", message:"+e.getMessage()+", url_Query:"+url_Query);
				ok = true;
			}
		} while (!ok);
		
		return handler;    	
	}
	
	public static SAX_XML_handler_flickrPhoInfo getPhoInfo(String photoID, String key, SAXParser saxParser, int sleepMin, PrintWriter outputStream_Report){
		//set search parametes
		String url_Query="http://api.flickr.com/services/rest/?method=flickr.photos.getInfo&api_key="+key+"&photo_id="+photoID+"&format=rest";
		//set xml paser handler
		SAX_XML_handler_flickrPhoInfo handler = new SAX_XML_handler_flickrPhoInfo(); //SAXhandler
		//http-config
		HttpURLConnection.setFollowRedirects(false); //do not allow redirect link
		//query flickr
		boolean ok = true;
		do {
			try {
				//set url connection
				URL imageURL = new URL(url_Query);
				URLConnection connection = imageURL.openConnection();
				HttpURLConnection httpConnection = (HttpURLConnection) connection; 
				int code = httpConnection.getResponseCode();
				if(code==200){
					Reader reader = new InputStreamReader(httpConnection.getInputStream(),"UTF-8");
					InputSource xml = new InputSource(reader); xml.setEncoding("UTF-8");
					saxParser.parse(xml, handler);
					ok = true;
				}else{
					General.dispInfo(outputStream_Report, "...url-respond error!, photoID: "+photoID+", code:"+code);
					General.dispInfo(outputStream_Report, "...sleep for "+sleepMin+" mins! retry!");
					try {
		                Thread.sleep(60*1000*sleepMin);
					} catch (Exception ex) {}
					ok = false;
				}
				httpConnection.disconnect();
			} catch (ConnectException e) {//time pase expceptiion dfm.parse(dataTaken).getTime()/1000; 
				General.dispInfo(outputStream_Report, "...ConnectException!, photoID: "+photoID+", message:"+e.getMessage());
				General.dispInfo(outputStream_Report, "...sleep for "+sleepMin+" mins! retry!");
				try {
	                Thread.sleep(60*1000*sleepMin);
				} catch (Exception ex) {}
				ok = false;
			} catch (IOException e) {
				General.dispInfo(outputStream_Report, "...IOException!, photoID: "+photoID+", message:"+e.getMessage());
				General.dispInfo(outputStream_Report, "...ignor this photo!");
				ok = true;
			} catch (SAXException e) {
				General.dispInfo(outputStream_Report, "...xml SAXException!, photoID: "+photoID+", message:"+e.getMessage());
				General.dispInfo(outputStream_Report, "...ignor this photo!");
				ok = true;
			}
		} while (!ok);
		
		return handler;    	
	}
	
	public static ArrayList<float[]> block_split(float[] oneBlock, float splitSize){
		float lat=oneBlock[0]; float lon=oneBlock[1]; float blockSize=oneBlock[2];
		int splitNum=(int) (blockSize/splitSize);
		ArrayList<float[]> splitedBlocks=new ArrayList<float[]>();
		for(int i =0; i<splitNum;i++){	//each block get one page, maximum photos per page for flickr geo-tagged is 250
			for(int j =0; j<splitNum;j++){
				float[] one_split={lat+i*splitSize,lon+j*splitSize,splitSize,-1};
				splitedBlocks.add(one_split);
			}
		}
		return splitedBlocks;    	
	}
	
	public static ArrayList<float[]> block_split_bugVersion(float[] oneBlock, float splitSize){//bug version of block_split
		float lat=oneBlock[0]; float lon=oneBlock[1]; float blockSize=oneBlock[2];
		ArrayList<float[]> splitedBlocks=new ArrayList<float[]>();
		for(float i_lat =lat; i_lat<=lat+blockSize-splitSize;i_lat+=splitSize){// have rounding bug!!
			for(float j_lon =lon; j_lon<=lon+blockSize-splitSize;j_lon+=splitSize){
				float[] one_split={i_lat,j_lon,splitSize,-1};
				splitedBlocks.add(one_split);
			}
		}
		return splitedBlocks;    	
	}
	
	public static ArrayList<float[]> findMissing_block_split(float[] oneBlock, float splitSize){
		float lat=oneBlock[0]; float lon=oneBlock[1]; float blockSize=oneBlock[2];
		int splitNum=(int) (blockSize/splitSize);
		ArrayList<float[]> Missing_blocks=null;
		
		//judge whether latIsBad
		boolean latIsBad=false;
		float i_lat=0; int lat_loopNum=0;
		for(i_lat =lat; i_lat<=lat+blockSize-splitSize;i_lat+=splitSize){
			lat_loopNum++;
		}
		if (!General.isEqual_float(i_lat, lat+blockSize, (float) (splitSize*0.2))) {// have rounding bug!!
			latIsBad=true;
			General.Assert(lat_loopNum==splitNum-1, "lat_loopNum:"+lat_loopNum+", splitNum:"+splitNum);
		}
		
		//judge whether lonIsBad
		boolean lonIsBad=false;
		float i_lon=0; int lon_loopNum=0;
		for(i_lon =lon; i_lon<=lon+blockSize-splitSize;i_lon+=splitSize){
			lon_loopNum++;
		}
		if (!General.isEqual_float(i_lon, lon+blockSize, (float) (splitSize*0.2))) {// have rounding bug!!
			lonIsBad=true;
			General.Assert(lon_loopNum==splitNum-1, "lon_loopNum:"+lon_loopNum+", splitNum:"+splitNum);
		}
		
		//make splitedBlocks
		if (latIsBad&&lonIsBad) {
			Missing_blocks=new ArrayList<float[]>();
			
			int i =splitNum-1;
			for(int j =0; j<splitNum;j++){
				float[] one_split={lat+i*splitSize,lon+j*splitSize,splitSize,-1};
				Missing_blocks.add(one_split);
			}
			int j =splitNum-1;
			for(i =0; i<splitNum-1;i++){
				float[] one_split={lat+i*splitSize,lon+j*splitSize,splitSize,-1};
				Missing_blocks.add(one_split);
			}
		}else if (latIsBad) {
			Missing_blocks=new ArrayList<float[]>();
			int i =splitNum-1;
			for(int j =0; j<splitNum;j++){
				float[] one_split={lat+i*splitSize,lon+j*splitSize,splitSize,-1};
				Missing_blocks.add(one_split);
			}
		}else if (lonIsBad) {
			Missing_blocks=new ArrayList<float[]>();
			int j =splitNum-1;
			for(int i =0; i<splitNum;i++){
				float[] one_split={lat+i*splitSize,lon+j*splitSize,splitSize,-1};
				Missing_blocks.add(one_split);
			}
		}

		return Missing_blocks;    	
	}
	
	public static int saveOnePho(SAX_XML_handler_flickrPhoSearch handler, PrintWriter outputStream_Meta, HashSet<Long> indexed_phoIDs, DateFormat dfm, String De) throws ParseException{
		int indexedPhoNum=0;
		for(int i_pho=0;i_pho<handler.photoIDs.size();i_pho++){
			long photoID=handler.photoIDs.get(i_pho);
			if(indexed_phoIDs.add(photoID)){ // this photoID is not indexed
				String userID=handler.userIDs.get(i_pho);
				String url=handler.urls.get(i_pho);
				float lat=handler.lats.get(i_pho);
				float lon=handler.lons.get(i_pho);
				int acc=handler.accs.get(i_pho);
				String tag=handler.tags.get(i_pho);
				String dataTaken=handler.dateTakens.get(i_pho); //2007-07-17 11:58:22
				long dataTaken_UT = dfm.parse(dataTaken).getTime()/1000;  
				long dataUpload=handler.dateUploads.get(i_pho);
				int views=handler.views.get(i_pho);
				int licenseID=handler.licenseIDs.get(i_pho);
				String onePhoto_meta=userID+De+photoID+De+url+De+lat+De+lon+De+acc+De+tag+De+dataTaken_UT+De+dataUpload+De+views+De+licenseID;
				outputStream_Meta.println(onePhoto_meta);
				indexedPhoNum++;
			}
		}
		return indexedPhoNum;
	}

	public static BufferedImage url_to_BufferedImage(String url, int maxRetry,String photoName, int sleepSec, ArrayList<Integer> skippeds) {
		//skippeds: NoExistAnyMore Flickr_skipped,errorURL,IllArgumentE_skipped, CMME_skipped, IOE_skipped, OutOfBound_skipped,RunTime_skipped
		/**to use:
		 * 	String url=conf.get("mapred.photo_unavailable_url");
			int maxRetry=99999; int sleepSec=2; 
			String[] skipExceptions={"NoExistAnyMore","Flickr_skipped","errorURL","IllArgumentE_skipped", "CMME_skipped", "IOE_skipped", "OutOfBound_skipped","RunTime_skipped"};
			ArrayList<Integer> skippeds=new ArrayList<Integer>(skipExceptions.length);
			for(int i=0;i<skipExceptions.length;i++)
				skippeds.add(0);
			photo_unavailable= General_http.url_to_BufferedImage(url, maxRetry,"photo_unavailable", sleepSec, skippeds);
			.......
			System.out.println("one reducer finished! total re-crawled photos: "+procPhotos_reCraw);
			//get skipped static
			for(int i=0;i<skipExceptions.length;i++){
				System.out.println( "toatal "+skippeds.get(i)+" photos skipped because "+skipExceptions[i]+" !! "+percformat.format((double)skippeds.get(i)/procPhotos_reCraw));
			}
		 */
		BufferedImage oneImage = null;
		HttpURLConnection.setFollowRedirects(false); //do not allow redirect link!!
		boolean ok = true; int retry=0;
		do {
    		try{
    		    //check image link
    		    URL imageURL = new URL(url);
    			URLConnection connection = imageURL.openConnection();
    			connection.connect(); int code;     	
    			// Cast to a HttpURLConnection
    			if ( connection instanceof HttpURLConnection){
    			   HttpURLConnection httpConnection = (HttpURLConnection) connection; 
    			   code = httpConnection.getResponseCode();	        	
    			   // do something with code .....
    			   if (code==200){// good link, no redirected link
    				   	oneImage = General_JAI.readImage_url(imageURL); //better than ImageIO
    				   	ok = true; 
       			   }else if(code==302){//redirected link, may due to this photo is deleted in the webSite!
//	       				retry++;
//	    				if (retry<=1){
//	    					ok = false;
//	    					System.out.println("Photo:"+photoName+" wait and re-try, redirected link, response-code:"+code+", url: "+url+", sleep for "+2+"s");
//	    				   	General.sleep(2, "s");
//	    				}else{
	    					ok = true; // skip this photo!
	    					System.out.println( "Photo "+photoName+" skipped, redirected link, not exist anymore in the Filckr,  response-code:"+code+", url: "+url);
	    					skippeds.set(0, skippeds.get(0)+1);
//	    				}
       			   }else{ //other http error in the Filckr
    				   	retry++;
	    				if (retry<=maxRetry){
	    					ok = false;
	    					System.out.println("Photo:"+photoName+" wait and re-try, http error in the Filckr,  response-code:"+code+", url: "+url+", sleep for "+sleepSec+"s");
        				   	General.sleep(sleepSec, "s");
	    				}else{
	    					ok = true; // skip this photo!
	    					System.out.println( "Photo "+photoName+" skipped, http error in the Filckr,  response-code:"+code+", url: "+url);
	    					skippeds.set(1, skippeds.get(1)+1);
	    				}
    			   }
    			}else{
    				ok = true; // skip this photo!
    				skippeds.set(2, skippeds.get(2)+1);
    				System.out.println( "Photo "+photoName+", error - not a http request! skipped, URL: "+url);
    			}
    		}catch(IllegalArgumentException eIllegal){
    			ok = true; // skip this photo!
    			System.out.println( "Photo"+photoName+", skipped, IllegalArgumentException: "+eIllegal.getMessage()+", url:"+url);
    			skippeds.set(3, skippeds.get(3)+1);
    		}catch(CMMException eIllegal){
    			ok = true; // skip this photo!
    			System.out.println( "Photo"+photoName+", skipped, CMMException: "+eIllegal.getMessage()+", url:"+url);
    			skippeds.set(4, skippeds.get(4)+1);
    		}catch(IOException eIO){
    			retry++;
				if (retry<=maxRetry){
					ok = false;
					System.out.println( "Photo"+photoName+", wait and re-try, IOException: "+eIO.getMessage()+", sleep for "+sleepSec+"s");				        			
					General.sleep(sleepSec, "s");
				}else{
					ok = true; // skip this photo!
					System.out.println("Photo"+photoName+", skipped, IOException: "+eIO.getMessage()+", url:"+url);
					skippeds.set(5, skippeds.get(5)+1);
				}
    		}catch(ArrayIndexOutOfBoundsException eOutOfBounds){
    			retry++;
				if (retry<=maxRetry){
					ok = false;
					System.out.println( "Photo"+photoName+", wait and re-try, ArrayIndexOutOfBoundsException: "+eOutOfBounds.getMessage()+", sleep for "+sleepSec+"s");				        			
					General.sleep(sleepSec, "s");
				}else{
					ok = true; // skip this photo!
					System.out.println("Photo"+photoName+", skipped, ArrayIndexOutOfBoundsException: "+eOutOfBounds.getMessage()+", url:"+url);
					skippeds.set(6, skippeds.get(6)+1);
				}
    		}catch(RuntimeException eRunTime){
    			retry++;
				if (retry<=maxRetry){
					ok = false;
					System.out.println( "Photo"+photoName+", wait and re-try, RuntimeException: "+eRunTime.getMessage()+", sleep for "+sleepSec+"s");				        			
					General.sleep(sleepSec, "s");
				}else{
					ok = true; // skip this photo!
					System.out.println("Photo"+photoName+", skipped, RuntimeException: "+eRunTime.getMessage()+", url:"+url);
					skippeds.set(7, skippeds.get(7)+1);
				}
    		}
		} while (!ok);	
	   	return oneImage;
	}

	/**
	 * skipped_reasons: "not available on web","not a http request","IOException"
	 */
	public static byte[] readByteFromURL(boolean isForwardLink, String url, int maxRetry, String dataMarker, int sleepSec, int[] skipped_statics, boolean disp, String spacer) throws IOException, InterruptedException {
		byte[] content_byteArr=null;
		boolean ok = false; int retry=0;
        //read byte from url connection, speed: B per http call
	    HttpURLConnection.setFollowRedirects(isForwardLink); //1. for flickr photosdo not allow redirect link, 2. for video, the real content is in a temporally forwarded url, openned with a https connection
		while (!ok) {
			try{
		        //check data link
				URLConnection connection = new URL(url).openConnection();
				connection.connect(); int code;  
				// Cast to a HttpURLConnection
				if ( connection instanceof HttpURLConnection){
					HttpURLConnection httpConnection = (HttpURLConnection) connection; 
					code = httpConnection.getResponseCode();	    
					// do something with code .....
					if (code==200){// good link, no redirected link
						content_byteArr = IOUtils.toByteArray(connection.getInputStream());
						ok = true;
					}else if (code==302 && isForwardLink) {//for video, it is then forwarded to a https connection
						String newURL=connection.getHeaderField("Location");
						HttpsURLConnection httpsConnection = (HttpsURLConnection) new URL(newURL).openConnection(); 
						int newReponseCode=httpsConnection.getResponseCode();
						if (newReponseCode==200) {//good video file link
							content_byteArr = IOUtils.toByteArray(httpsConnection.getInputStream());
							ok = true;
						}else {
							retry++;
		    				if (retry<=maxRetry){
		    					ok = false;
		    					General.dispInfo_ifNeed(disp, spacer+"--", "data: "+dataMarker+" wait and re-try, not available in the Filckr,  response-code:"+code+", ori-url: "+url+", forwared url, newReponseCode:"+newReponseCode+", newURL: "+newURL+", sleep for "+sleepSec+"s");
			                    Thread.sleep(sleepSec*1000); //sleep for Exception
		    				}else{
		    					ok = true; // skip this photo!
		    					General.dispInfo_ifNeed(true, spacer, "data: "+dataMarker+" skipped, not available in the web, re-tried time:"+maxRetry+",  response-code:"+code+", ori-url: "+url+", forwared url, newReponseCode:"+newReponseCode+", newURL: "+newURL);
		    					skipped_statics[0]++;
		    				}
						}
					}else {
						retry++;
	    				if (retry<=maxRetry){
	    					ok = false;
	    					General.dispInfo_ifNeed(disp, spacer+"--", "data: "+dataMarker+" wait and re-try, not available in the Filckr,  response-code:"+code+", url: "+url+", sleep for "+sleepSec+"s");
		                    Thread.sleep(sleepSec*1000); //sleep for Exception
	    				}else{
	    					ok = true; // skip this photo!
	    					General.dispInfo_ifNeed(true, spacer, "data: "+dataMarker+" skipped, not available in the web, re-tried time:"+maxRetry+",  response-code:"+code+", url: "+url);
	    					skipped_statics[0]++;
	    				}
					}
				}else {
					skipped_statics[1]++;
					ok = true; // skip this photo!
					General.dispInfo_ifNeed(true, spacer, "data: "+dataMarker+", error - not a http request! skipped, URL: "+url);
				}
			}catch(IOException eIO){
    			retry++;
				if (retry<=maxRetry*2){
					ok = false;
					General.dispInfo_ifNeed(disp, spacer+"--", "data: "+dataMarker+", wait and re-try, IOException: "+eIO.getMessage()+", sleep for "+(sleepSec*2)+"s");	        			
                    Thread.sleep(sleepSec*2*1000); //sleep for Exception
				}else{
					ok = true; // skip this one!
					General.dispInfo_ifNeed(true, spacer, "data: "+dataMarker+" skipped, re-tried time:"+maxRetry*2+", IOException: "+eIO.getMessage()+", url:"+url);
        			skipped_statics[2]++;
				}
    		}catch(OutOfMemoryError err){//for some big sample, may get OutOfMemoryError
				ok = true; // skip this one!
				General.dispInfo_ifNeed(true, spacer, "data: "+dataMarker+" OutOfMemoryError: "+err.getMessage()+", url:"+url+", current memory: "+General.memoryInfo());
				err.printStackTrace();
				throw err;
    		}
		}
				
		return content_byteArr;
	}

	@SuppressWarnings("resource")
	public static void saveByteFromURL(String website, String destination) throws MalformedURLException {
        // ÏÂÔØÍøÂçÎÄ¼þ
        int byteread = 0;

        URL url = new URL(website);

        try {
            URLConnection conn = url.openConnection();
            InputStream inStream = conn.getInputStream();

            FileOutputStream fs = new FileOutputStream(destination);
            byte[] buffer = new byte[1204];
            int totByteNum=0;
            while ((byteread = inStream.read(buffer)) != -1) {
                totByteNum+=byteread;
                fs.write(buffer, 0, byteread);
            }
            System.out.println("totByteNum:"+totByteNum);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	
	public static void main(String[] args) {
//		String attriValue="id=\"9805036-4655948957-202\" author=\"9897849@N08\" raw=\"wedding\" machine_tag=\"0\">wedding<"; 
//		String tag=attriValue.split(" ")[3].split(">")[1].split("<")[0];
//		System.out.println("tag: "+tag);
		
		//test block_split
		float[] oneBlock={(float) -32.0,(float) 115.79999,(float) 0.1}; float splitSize=(float) 0.01;
		
		ArrayList<float[]> splitted= block_split(oneBlock, splitSize);
		for (int i = 0; i < splitted.size(); i++) {
			System.out.println(i+"-th splitted sub-block: "+General.floatArrToString(splitted.get(i), "_", "0.000000"));
		}
		System.out.println("splitted done!");
				
		ArrayList<float[]> splitted_bug= block_split_bugVersion(oneBlock, splitSize);
		for (int i = 0; i < splitted_bug.size(); i++) {
			System.out.println(i+"-th splitted sub-block: "+General.floatArrToString(splitted_bug.get(i), "_", "0.000000"));
		}
		System.out.println("splitted_bug done!");
		
		//test findMissing_block_split
		ArrayList<float[]> splitted_missing= findMissing_block_split(oneBlock, splitSize);
		if (splitted_missing!=null) {
			for (int i = 0; i < splitted_missing.size(); i++) {
				System.out.println(i+"-th splitted sub-block: "+General.floatArrToString(splitted_missing.get(i), "_", "0.000000"));
			}
		}
		
		
		int k= (int) (0.1/0.01);
		k=(int) -34.0000336;
		System.out.println(k);
		
		for (float i=(float) 1.3; i<=1.39f; i+=0.01f) {
			System.out.println(i);
		}
		
		BigDecimal bd5=new BigDecimal("1.0");
        BigDecimal bd6=new BigDecimal("0.1");
        BigDecimal bd7=new BigDecimal("1.9");

        for(BigDecimal bd51=bd5;bd51.compareTo(bd7) <= 0;bd51 = bd51.add(bd6)){
            System.out.println(bd51);
        }

	}
}
