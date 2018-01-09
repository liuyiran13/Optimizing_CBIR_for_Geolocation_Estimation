package MyAPI.General;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

import javax.imageio.ImageIO;
import javax.media.jai.JAI;

import com.sun.media.jai.codec.ByteArraySeekableStream;

public class General_JAI {

	public static BufferedImage readImage_url(URL url) throws IOException {
		BufferedImage image = JAI.create("url", url).getAsBufferedImage(); 
		return image;
	}
	
	public static BufferedImage BytesToBufferedImage_JAI(byte[] img_bytes) throws IOException {
		// convert byte array back to BufferedImage
		return JAI.create("stream", new ByteArraySeekableStream(img_bytes)).getAsBufferedImage(); 
	}
	
	public static BufferedImage download_FlickrPhoto(String url, int maxRetry, String photoIndex, int[] skipped_statics, int sleepSec) {
		//download photo from Flickr, 
		//skipped_statics: Flickr_skipped, not a http request, IOE_skipped, RunTime_skipped
		BufferedImage oneImage=null;
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
    				   	oneImage = General_JAI.readImage_url(imageURL); 
    				   	ok = true; 
    			   }else{ //not available in the Filckr
    				   	retry++;
	    				if (retry<=maxRetry){
	    					ok = false;
//	    					System.out.println("Photo "+photoIndex+" wait and re-try, not available in the Filckr,  response-code:"+code+", url: "+url+", sleep for 15s");
        				   	try {
		                          Thread.sleep(sleepSec*1000); //sleep 5s for Exception
		                    } catch (Exception ex) {}  
	    				}else{
	    					ok = true; // skip this photo!
	    					System.out.println( "Photo "+photoIndex+" skipped, not available in the Filckr, re-tried time:"+maxRetry+",  response-code:"+code+", url: "+url);
	    					skipped_statics[0]++;
	    				}
    			   }
    			}else{
    				ok = true; // skip this photo!
    				skipped_statics[1]++;
    				System.out.println( "Photo "+photoIndex+", error - not a http request! skipped, URL: "+url);
    			}
    		}catch(IOException eIO){
    			retry++;
				if (retry<=maxRetry){
					ok = false;
//					System.out.println( "Photo"+photoIndex+", wait and re-try, IOException: "+eIO.getMessage()+", sleep for 15s");				        			
                    try {
                    	Thread.sleep(sleepSec*1000); //sleep 5s for Exception
                    } catch (Exception ex) {}  
				}else{
					ok = true; // skip this photo!
					System.out.println("Photo"+photoIndex+" skipped, re-tried time:"+maxRetry+", IOException: "+eIO.getMessage()+", url:"+url);
        			skipped_statics[2]++;
				}
    		}
		} while (!ok);
		
		return oneImage;	
	}
	
	public static void main(String[] args) throws IOException {
		String str_URL="http://farm6.staticflickr.com/5261/5748844524_49a343a786.jpg";
		BufferedImage image= readImage_url(new URL(str_URL));
		ImageIO.write(image, "jpg", new File("Q:/temp"+".jpg"));
	}

}
