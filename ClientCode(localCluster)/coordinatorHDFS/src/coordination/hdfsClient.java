package coordination;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
public class hdfsClient {
 
   /*public static void main(String[] args) {
     
	   hdfsClient p=new hdfsClient();
   }*/
   
   public hdfsClient(){
	   System.out.println("\n----------------SSSSSSS-------------------\n");
	   client c=new client();
	   c.start();
   }
 

   class client extends Thread{
	   BufferedReader br;

	   public client(){

	   }
       
	   public void run() {
		   try {
    	        Socket socket = new Socket();
    	        socket.connect(new InetSocketAddress("120.107.172.194", 8888));
    	        System.out.println("\n----------------Connected Successed-------------------\n");
    	        PrintWriter output = new PrintWriter(socket.getOutputStream(), true);
    	        br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    	        BufferedReader buff = new BufferedReader(new InputStreamReader(System.in));
    	        
    	        Thread msgReceiver = new Thread(new Runnable() {
    	            public void run() {
    	                String str;
    	                try {
    	                    while ((str = br.readLine()) != null) {
    	                        if (str.length() > 0){
    	                        	 System.out.println("\nmessage from Cluster2>>" + str);
    	                        	 if(str.equals("quit")){
    	                        		 System.out.println("Connect Close");
    	                        		 break;
    	                        	 }else if (str.equals("3")){
    	                        		 
    	                        	 }
    	                        }
    	                        
    	                    }
    	                } catch (Exception e) {
    	                    e.printStackTrace();
    	                }
    	            }
    	        });
    	        msgReceiver.start();
    	        System.out.println("send update command to Cluster2");
    	        output.write("1\n");
	            output.flush();

	            /*filePathOut.writeObject(pinC2);
	            filePathOut.flush();
	            filePathOut.close();
	            filePathOut = null;*/
    	        /*while (true) {
    	        	System.out.println("訊息內容>>");
    	            String str = buff.readLine();
    	            output.write(str + "\n");
    	            output.flush();
    	            if(str.equals("quit")){
    	            	System.out.println("Connect Close");
    	            	msgReceiver.stop();
    	            	socket.close();
    	            	break;
    	            }
    	        }*/
    	    } catch (Exception e) {
    	    }
    	}
   }
   
}