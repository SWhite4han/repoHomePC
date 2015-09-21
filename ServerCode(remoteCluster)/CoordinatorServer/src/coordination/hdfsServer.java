package coordination;

import java.io.*;
import java.net.*;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class hdfsServer {
	    
	public static void main(String[] args) {
	    hdfsServer frame = new hdfsServer();
	}
	public hdfsServer(){
		server s=new server();
	   
		s.start();
	   
	}
	
	public static void process(String[] processDataPath) throws IOException{
		for(int i=0;i<processDataPath.length;i++){
			String JobID = "J0"+String.valueOf(i+1);
			//hdfsClient p=new hdfsClient();
			CopyFile cf = new CopyFile(processDataPath[i],JobID);	//test update file to HDFS
			callHadoopTest cht=new callHadoopTest(JobID);
		}
	}
	
	public void CopyFile(Configuration conf ,String filePath) throws IOException{
		/*Configuration conf = new Configuration();
		conf.set("fs.default.name","hdfs://node3:9000");*/
		FileSystem hdfs = FileSystem.get(conf);
		
		
		Path src = new Path(filePath);
		Path dst = new Path("/user/hadoop/FPG/testData");
		System.out.println("Starting update to Cluster2");
		
		hdfs.copyFromLocalFile(false,src,dst);
		
		FileStatus files[] = hdfs.listStatus(dst);
		for(FileStatus file:files){
			System.out.print(file.getPath());
		}
		System.out.println();
		System.out.println("update OK");
	}
	/*
	public static void callHadoopTest() throws IOException, InterruptedException{
		long startTime = System.currentTimeMillis();
		System.out.println("Starting process dataset in wc...");
		Process rmwc = Runtime.getRuntime().exec("hadoop fs -rmr dataFrequencies");
		rmwc.waitFor();
		Process pwc = Runtime.getRuntime().exec("hadoop jar /opt/hadoop/WordCountN.jar WordCountN FPG/testData dataFrequencies");
		pwc.waitFor();
		long endTime = System.currentTimeMillis();
		System.out.println("Total cost of wc is " + (endTime - startTime)/1000 + " s...");
		System.out.println();
		
		Process rmfpg = Runtime.getRuntime().exec("hadoop fs -rmr FPGrowthFinalOutput");
		rmfpg.waitFor();
		
		long startTime2 = System.currentTimeMillis();
		System.out.println("Starting process dataset in FPG...");
		Process pfp = Runtime.getRuntime().exec("hadoop jar /opt/hadoop/FPGrowth.jar FPGrowth FPG/testData FPGrowthFinalOutput");
		pfp.waitFor();
		long endTime2 = System.currentTimeMillis();
		System.out.println("Total cost of FPG is " + (endTime2 - startTime2)/1000 + " s...");
		System.out.println();

		long startTime3 = System.currentTimeMillis();
		System.out.println("Starting download output...");
		Process pfdfp = Runtime.getRuntime().exec("hadoop fs -get FPGrowthFinalOutput /home/hadoop/");
		pfp.waitFor();
		long endTime3 = System.currentTimeMillis();
		System.out.println("Total cost of download output is " + (endTime3 - startTime3)/1000 + " s...");
		System.out.println();
	}*/
	
	public static void scpToCluster1() throws IOException{
		long startTime = System.currentTimeMillis();
		System.out.println("Starting scp to Cluster1");
		Process pl = Runtime.getRuntime().exec("scp -C /home/hadoop/FPGrowthFinalOutput/part-r-00000 hadoop@node1:/home/hadoop/FPGrowthFinalOutput/partRemote1");
		Process ps = Runtime.getRuntime().exec("scp -C /home/hadoop/FPGrowthFinalOutput/part-r-00001 hadoop@node1:/home/hadoop/FPGrowthFinalOutput/partRemote2");
		 try {
			 pl.waitFor();
			 ps.waitFor();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis();
		System.out.println("Total cost of SCP is " + (endTime - startTime)/1000 + " s...");
	}
	
	
	class  server extends Thread{
	    BufferedReader br;
	    public server(){}
	    public void run() {
	    	try {
	    		ServerSocket sk = new ServerSocket(8888);
	    	    System.out.println("Server端接收到訊息內容顯示如下...\n");
	    	    Socket insk = sk.accept();
	    	    br = new BufferedReader(new InputStreamReader(insk.getInputStream()));
	    	    PrintWriter output = new PrintWriter(insk.getOutputStream(), true);
	    	    BufferedReader buff = new BufferedReader(new InputStreamReader(System.in));
	    	    //DataOutputStream output = new DataOutputStream(insk.getOutputStream());
	    	    Thread msgReceiver = new Thread(new Runnable() {
	    	    	public void run() {
	    	    		try {
	    	    			while (true) {
	    	    				System.out.println("訊息內容>>");
	    	  	    	        String ss = buff.readLine();
	    	  	    	        output.write(ss + "\n");
	    	  	    	        output.flush();
	    	  	    	    }
	    	            } catch (Exception e) {
	    	                e.printStackTrace();
	    	            }
	    	        }
	    	    });
	    	    msgReceiver.start();
	    	    String str;
	    	    while ((str = br.readLine()) != null) {
                    if (str.length() > 0){
                    	if(str.equals("quit")){
                    		System.out.println("Server Close");
                        	output.write("quit");
    	  	    	        output.flush();
                        	insk.close();
                        	msgReceiver.stop();
                        	break;
                        }else if(str.equals("1")){
                        	// tell Cluster1
                        	output.write("Getting file and Starting process in Cluster2\n");
                        	System.out.println("Getting file and Starting process in Cluster2\n");
    	  	    	        output.flush();
    	  	    	        FileReader fr = new FileReader("/home/hadoop/paperTestData/C2Path");
    	  	    	        BufferedReader br = new BufferedReader(fr);
    	  	    	        String s = br.readLine();
    	  	    	        String[] paths = s.split(" ");
    	  	    	        for(int i=0;i<paths.length;i++){
    	  	    	        	System.out.println(paths[i]);
    	  	    	        }
    	  	    	        process(paths);
    	  	    	        /*
    	  	    	        // update
                        	Configuration conf = new Configuration();
                    		conf.set("fs.default.name","hdfs://node3:9000");
                    		CopyFile(conf,paths[0]);
                        	//CopyFile(conf,"/home/hadoop/paperTestData/outputab");
                    		//CopyFile(conf,"/home/hadoop/paperTestData/testData200");
                        	System.out.println("Finished update");
                        	output.write("Finished update to Cluster2\n");
    	  	    	        output.flush();
    	  	    	        
    	  	    	        output.write("Start process devided data in Cluster2\n");
  	  	    	       		output.flush();
  	  	    	       		// FPG
  	  	    	       		callHadoopTest();
  	  	    	       		System.out.println("Finished FPG");
  	  	    	       		output.write("Finished process in Cluster2\n");
  	  	    	       		output.flush();
  	  	    	       		
  	  	    	       		scpToCluster1();
  	  	    	       		output.write("output to C1 finished\n");
  	  	    	       		output.flush();*/
  	  	    	       		output.write("3\n");
  	  	    	       		output.flush();
    	  	    	        
                        }else{
                        	System.out.println("接收訊息內容>>" + str);
                        }
                    }
                        	
                } 
	       
	    	} catch (Exception e) {
	    	}
	    }
	}
}