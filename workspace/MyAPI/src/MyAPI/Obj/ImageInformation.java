package MyAPI.Obj;

//Inner class containing image information
public class ImageInformation {
 public final int orientation;
 public final int width;
 public final int height;

 public ImageInformation(int orientation, int width, int height) {
     this.orientation = orientation;
     this.width = width;
     this.height = height;
 }

 public String toString() {
     return String.format("%dx%d,%d", this.width, this.height, this.orientation);
 }
}