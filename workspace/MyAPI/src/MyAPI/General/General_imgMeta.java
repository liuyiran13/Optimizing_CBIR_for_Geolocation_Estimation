package MyAPI.General;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;

import MyAPI.Obj.ImageInformation;

import com.drew.imaging.ImageMetadataReader;
import com.drew.imaging.ImageProcessingException;
import com.drew.metadata.Metadata;
import com.drew.metadata.MetadataException;
import com.drew.metadata.exif.ExifIFD0Directory;
import com.drew.metadata.jpeg.JpegDirectory;

public class General_imgMeta {
	public static ImageInformation readImageInformation(File file, BufferedInputStream inputStream)  throws IOException, InterruptedException {
		//inputStream=new BufferedInputStream(new ByteArrayInputStream(img_bytes));
		General.Assert(file!=null || inputStream!=null, "either file or inputStream should not be null");
		try {
			Metadata metadata = (file==null)?ImageMetadataReader.readMetadata(inputStream, false):ImageMetadataReader.readMetadata(file); 
		    ExifIFD0Directory ifd0directory = metadata.getDirectory(ExifIFD0Directory.class);
		    JpegDirectory jpegDirectory = metadata.getDirectory(JpegDirectory.class);

		    int orientation = 1; int width=0; int height=0;
	    
		    if (ifd0directory!=null) {
		    	orientation = ifd0directory.getInt(ExifIFD0Directory.TAG_ORIENTATION);
			}else {
				throw new NullPointerException("warn, Could not get ifd0directory: mainImage's EXIF");
			}
		    if (jpegDirectory!=null) {	    
		    	width = jpegDirectory.getImageWidth();
		    	height = jpegDirectory.getImageHeight();
		    }else {
		    	throw new NullPointerException("warn, Could not get jpegDirectory: Image's basic JpegInfo");
			}
		    return new ImageInformation(orientation, width, height);
		} catch (MetadataException e) {
			throw new InterruptedException("MetadataException, e: "+e);
		} catch (ImageProcessingException e) {
			throw new InterruptedException("ImageProcessingException, e: "+e);
		}
	}
}
