package coordination;

import java.io.*;

public class callHadoopTest {
   public static String JobID;
   public callHadoopTest(String JID){
	   this.JobID = JID;
	   call c=new call(JobID);
	   c.start();
     
   }
 

   class call extends Thread{
	   BufferedReader br;
	   public String JobID;
	   public call(String JID){this.JobID=JID;}
   
	   public void run() {
		   try {
    	        Thread callhdp = new Thread(new Runnable() {
    	            public void run() {
    	            		
    	            		System.out.println("---------------- "+JobID+" C1------------------");
    	            		System.out.println("Starting process dataset in wc...");
    	            		
							try {
								long startTime = System.currentTimeMillis();
								Process rmwc;
								rmwc = Runtime.getRuntime().exec("hadoop fs -rmr dataFrequencies"+JobID);
								rmwc.waitFor();
	    	            		Process pwc = Runtime.getRuntime().exec("hadoop jar /opt/hadoop/WordCountN.jar WordCountN FPG/testData"+JobID+" dataFrequencies"+JobID);
	    	            		pwc.waitFor();
	    	            		long endTime = System.currentTimeMillis();
	    	            		System.out.println("Total cost of "+JobID+" wc is " + (endTime - startTime)/1000 + " s...");
	    	            		System.out.println("-----------------C1 "+JobID+" wc-----------------");
	    	            		
	    	            		Process rmfpg = Runtime.getRuntime().exec("hadoop fs -rmr FPGrowthFinalOutput"+JobID);
	    	            		rmfpg.waitFor();
	    	            		
	    	            		long startTime2 = System.currentTimeMillis();
	    	            		System.out.println("Starting process dataset in FPG...");
	    	            		Process pfp = Runtime.getRuntime().exec("hadoop jar /opt/hadoop/FPGrowth.jar FPGrowth FPG/testData"+JobID+" FPGrowthFinalOutput"+JobID+" "+JobID);
	    	            		pfp.waitFor();
	    	            		long endTime2 = System.currentTimeMillis();
	    	            		System.out.println("Total cost of FPG is " + (endTime2 - startTime2)/1000 + " s...");
	    	            		System.out.println("------------------C1 "+JobID+" FPG----------------");

	    	            		Thread.sleep(1000*30);

	    	            		long startTime3 = System.currentTimeMillis();
	    	            		System.out.println("Starting download output...");
	    	            		Process pfdfp = Runtime.getRuntime().exec("hadoop fs -get FPGrowthFinalOutput"+JobID+" /home/hadoop/FPGrowthFinalOutput/C1"+JobID);
	    	            		pfp.waitFor();
	    	            		long endTime3 = System.currentTimeMillis();
	    	            		System.out.println("Total cost of download output is " + (endTime3 - startTime3)/1000 + " s...");
	    	            		System.out.println("------------------C1 "+JobID+" DW----------------");
	    	            		System.out.println("C1 Job "+JobID+" FINISHED TIME IS : "+System.currentTimeMillis());
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
    	            		
    	            	}
    	            
    	        });
    	        callhdp.start();
    	        
    	       
    	    } catch (Exception e) {
    	    }
    	}
   }
   
}