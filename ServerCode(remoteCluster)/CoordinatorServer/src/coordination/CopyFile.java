package coordination;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class CopyFile {
	
	public CopyFile(Configuration conf ,String filePath) throws IOException{
		/*Configuration conf = new Configuration();
		conf.set("fs.default.name","hdfs://node3:9000");*/
		FileSystem hdfs = FileSystem.get(conf);
		
		
		Path src = new Path(filePath);
		Path dst = new Path("/user/hadoop/FPG/testData");
		
		long startTime = System.currentTimeMillis();
		hdfs.copyFromLocalFile(false,src,dst);
		long endTime = System.currentTimeMillis();
		FileStatus files[] = hdfs.listStatus(dst);
		for(FileStatus file:files){
			System.out.print(file.getPath());
		}System.out.println();
		System.out.println("cost "+ ((startTime-endTime)/1000)  +"s to update");
	}
	
	public CopyFile(String filePath, String JobID) throws IOException{
		Configuration conf = new Configuration();
		conf.set("fs.default.name","hdfs://node3:9000");
		FileSystem hdfs = FileSystem.get(conf);
		
		Path src = new Path(filePath);
		Path dst = new Path("/user/hadoop/FPG/testData"+JobID);
		System.out.println("Starting update "+JobID+" to Cluster1");
		hdfs.copyFromLocalFile(false,src,dst);
		
		FileStatus files[] = hdfs.listStatus(dst);
		for(FileStatus file:files){
			System.out.print(file.getPath());
		}
		System.out.println();
	}
}