package scheduler;

public class FileQuantity implements Comparable<FileQuantity> {
	public	String fileName;
	public int quantity;
	
	public FileQuantity(String file, int quan){
		fileName = file;
		quantity = quan;
	}
	
	public int compareTo(FileQuantity that){
		return that.quantity - this.quantity;
	}
}
