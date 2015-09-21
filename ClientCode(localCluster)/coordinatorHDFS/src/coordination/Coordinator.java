/*********************
 * Main process
 * Ver1.0
 * Paper Code
 * coding by White
 *********************/
package coordination;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import javax.swing.JFileChooser;
import javax.swing.JOptionPane;


public class Coordinator {

	public static ArrayList<String> mergeFiles(ArrayList<String> mergePath , String FileID) throws IOException{
		String command = "cat ";
		for(int i=0;i < mergePath.size();i++){
			command += mergePath.get(i)+" ";
		}
		command+="> /home/hadoop/paperTestData/testFile"+FileID;

		//System.out.println(command);
		ArrayList<String> result = new ArrayList<String>();
		result.add("/home/hadoop/paperTestData/testFile"+FileID);
		System.out.println("Final Test Data is testFile"+FileID);
		try {
			Runtime runtime = Runtime.getRuntime();
			DataOutputStream dataOut;
			Process ps = runtime.exec("sh ");
			BufferedReader in = new BufferedReader(new InputStreamReader(ps.getInputStream()));
			String line = null;
			dataOut = new DataOutputStream(ps.getOutputStream());
			dataOut.writeBytes(command);
			dataOut.flush();
			dataOut.close();
			ps.waitFor();
			while ((line = in.readLine()) != null){
				System.out.println(line);
			}
			
			FileWriter fw = new FileWriter("/home/hadoop/paperTestData/"+FileID+"Path");
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write("/home/hadoop/paperTestData/testFile"+FileID);
			bw.close();
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
	
	// default for experiment
	public static void devideFiles(String devidePath,String dS) throws IOException, InterruptedException{
		long devideSize = Long.parseLong(dS);
		/*if (devideSize > 640000000){
			if(devideSize-640000000>384000000){
				devideSize = 384000000*2;
			}else{
				devideSize = 384000000 + (devideSize%640000000);
			}
			
		}else if (devideSize <= 384000000){
			
		}else if (devideSize > 384000000 && devideSize<640000000){
			devideSize = 384000000;
		}*/
		devideSize = (long) (devideSize * (0.6));
		
		String command = "split --line-bytes=";
		command += devideSize;
		command += " "+devidePath+" ";
		command +="/home/hadoop/paperTestData/output";
		try {
			System.out.println("Strat devide file");
			Runtime runtime = Runtime.getRuntime();
			DataOutputStream dataOut;
			Process ps = runtime.exec("sh ");
			BufferedReader in = new BufferedReader(new InputStreamReader(ps.getInputStream()));
			String line = null;
			dataOut = new DataOutputStream(ps.getOutputStream());
			//System.out.println(command);
			dataOut.writeBytes(command);
			//dataOut.writeBytes("split --line-bytes=60000000 /home/hadoop/paperTestData/final20030 /home/hadoop/paperTestData/op");
			dataOut.flush();
			dataOut.close();
			ps.waitFor();
			while ((line = in.readLine()) != null){
				System.out.println(line);
			}
			System.out.println("----------------------------------");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	
	// default
	public static void scpToCluster2() throws IOException{
		long startTime = System.currentTimeMillis();
		System.out.println("Starting scp to Cluster2");
		Process pl = Runtime.getRuntime().exec("scp -C /home/hadoop/paperTestData/outputab hadoop@120.107.172.194:/home/hadoop/paperTestData/outputab");
		 try {
			 pl.waitFor();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis();
		System.out.println("Total cost of SCP is " + (endTime - startTime)/1000 + " s...");
		System.out.println("----------------------------------");
	}
	
	// transfer file by choosing file path
	public static void scpToCluster2(String path) throws IOException{
		long startTime = System.currentTimeMillis();
		System.out.println("Starting scp to Cluster2 Final Test --- ");
		System.out.println("The scp FILE is " +path +"  HAHAHAHAHAHAAAAAAAAAAAAAAAA");
		String com =("scp -C "+path+" hadoop@120.107.172.194:"+path);
		Process pl = Runtime.getRuntime().exec(com);
		 try {
			 pl.waitFor();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis();
		System.out.println("Total cost of SCP is " + (endTime - startTime)/1000 + " s...");
		System.out.println("----------------------------------");
	}
	
	// New!!!
	public static void scpToCluster2(ArrayList<String> pathArray) throws IOException{
		// C2Path
		scpToCluster2("/home/hadoop/paperTestData/C2Path");
		// all files
		for(int i=0;i<pathArray.size();i++){
			scpToCluster2(pathArray.get(i));
		}
	}
	
	public static void process(ArrayList<String> processDataPath) throws IOException{
		for(int i=0;i<processDataPath.size();i++){
			String JobID = "J0"+String.valueOf(i+1);
			//hdfsClient p=new hdfsClient();
			CopyFile cf = new CopyFile(processDataPath.get(i),JobID);	//test update file to HDFS
			callHadoopTest cht=new callHadoopTest(JobID);
		}
	}
	
	public static void processT(String[] pathsQueue) throws IOException{
		for(int i=0;i<pathsQueue.length;i++){
			String JobID = "J0"+String.valueOf(i+1);
			//hdfsClient p=new hdfsClient();
			CopyFile cf = new CopyFile(pathsQueue[i],JobID);	//test update file to HDFS
			callHadoopTest cht=new callHadoopTest(JobID);
		}
	}
	
	public static void processC(String path,long size) throws IOException, InterruptedException{
		devideFiles(path,String.valueOf(size));
		
		FileWriter fw = new FileWriter("/home/hadoop/paperTestData/C2Path");
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write("/home/hadoop/paperTestData/outputab ");
		bw.close();	
		
		scpToCluster2("/home/hadoop/paperTestData/outputab");
		scpToCluster2("/home/hadoop/paperTestData/C2Path");
		
		hdfsClient p=new hdfsClient();
		String[] ps = {"/home/hadoop/paperTestData/outputaa"};
		processT(ps);
	}
	
	
	
	public static void main(String args[]) throws IOException, InterruptedException
	{
		System.out.println("SYSTEM START TIME IS : "+System.currentTimeMillis());
		GetFileSize g = new GetFileSize();
		String[] pathsQueue = g.getPaths();	// using getPaths() to get all file's paths
		long[] sizeQueue = g.getSize(pathsQueue);	// print file Size V1.2 modify to get size array of files
		//64 * 1048576	one Block size
		
		// test print size of dataset
		for(int i=0;i<sizeQueue.length;i++){
			System.out.println("file "+ i + "'s size is " +sizeQueue[i]);
		}
		//System.setProperty("HADOOP_USER_NAME","rooSystem.out.println();t");	//if need
		
		/*
		// paper test code
		DataScheduler DS = new DataScheduler(pathsQueue,sizeQueue);
		ArrayList<String> pinC1 = DS.pinC1;
		ArrayList<String> pinC2 = DS.pinC2;
		
		scpToCluster2(pinC2);
		hdfsClient p=new hdfsClient();
		process(pinC1);
		*/
		
		
		/*
		// for multi-cluster (capacity) testing
		// just can choose one file
		processC(pathsQueue[0],sizeQueue[0]);
		*/
		
		
		// for multi-cluster (capacity on small files) testing
		capacityScheduler CS = new capacityScheduler(pathsQueue,sizeQueue);
		ArrayList<String> pinC1 = CS.pinC1;
		ArrayList<String> pinC2 = CS.pinC2;
		
		scpToCluster2(pinC2);
		hdfsClient p=new hdfsClient();
		process(pinC1);
		
		
		// for single cluster 
		//processT(pathsQueue);
		
		
		/*
		// for compare 2 Clusters capacity test
		FileWriter fw = new FileWriter("/home/hadoop/paperTestData/C2Path");
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write(pathsQueue[0]+" ");
		bw.close();	
		scpToCluster2("/home/hadoop/paperTestData/C2Path");
		scpToCluster2(pathsQueue[0]);	//test scp dataset to another Cluster
		hdfsClient p=new hdfsClient();
		CopyFile cf = new CopyFile(pathsQueue[0],"J01");	//test update file to HDFS
		callHadoopTest cht=new callHadoopTest("J01");
		*/
		
		/* test merge file
		try {
			mergeFiles(pathsQueue);
		} catch (IOException e) {
			e.printStackTrace();
		}*/
		//System.exit( 0 );
	}

}
