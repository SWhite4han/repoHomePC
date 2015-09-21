package coordination;

public class FileStatus implements java.io.Serializable {
	public String path;
	public long size;
	
	public FileStatus(String path,long size){
		this.path = path;
		this.size = size;
	}
	
}