package coordination;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class capacityScheduler {
	ArrayList<String> pinC1 = new ArrayList<String>();
	ArrayList<String> pinC2 = new ArrayList<String>();
	ArrayList<FileStatus> remainList = new ArrayList<FileStatus>();
	ArrayList<FileStatus> mergeList = new ArrayList<FileStatus>();
	/**********************************************************
	 * one block size = 64*1024*1024
	 * C1 = 64*1024*1024*2*3
	 * C2 = 64*1024*1024*2*2
	 * Capality C1+C2 = (64*1024*1024*2*3)+(64*1024*1024*2*2)
	 **********************************************************/
	static int fileID=0;
	
	final double C1 = 384;
	final double C2 = 256;
	final double CCC = C1 + C2;
	
	final double capacityC1 = 0.6;
	final double capacityC2 = 0.4;
	
	public capacityScheduler(String[] filePath,long[] fileSize) throws IOException, InterruptedException{
		//CSchedule(filePath,fileSize);
		NCSchedule(filePath,fileSize);
	}
	

	public void CSchedule(String[] filePath,long[] fileSize) throws IOException, InterruptedException{
		
		
		for(int i=0;i<filePath.length;i++){
			long devideSize = (long) (fileSize[i] * capacityC1);
			devideFiles(filePath[i],devideSize,i);
			pinC1.add("/home/hadoop/paperTestData/"+i+"aa");
			pinC2.add("/home/hadoop/paperTestData/"+i+"ab");
		}
		
		FileWriter fw = new FileWriter("/home/hadoop/paperTestData/C2Path");
		BufferedWriter bw = new BufferedWriter(fw);
		for(int j=0;j<pinC2.size();j++){
			//bw.write("test");
			bw.write(pinC2.get(j)+" ");
		}bw.close();
	}
	
	public void NCSchedule(String[] filePath,long[] fileSize) throws IOException, InterruptedException{
		
		int mergeCount=0;
		for(int i=0;i<filePath.length;i++){
			FileStatus md = new FileStatus(filePath[i],fileSize[i]);
			remainList.add(md);
		}
		
		long startTimeMerge = System.currentTimeMillis();
		FileStatus fs = mergeFiles(remainList,mergeCount);
		long endTimeMerge = System.currentTimeMillis();
		System.out.println("Total cost of MERGE TEST is " + (endTimeMerge - startTimeMerge)/1000 + " s...");
		remainList.clear();
		System.out.println("mrege in "+fs.size);
		mergeCount++;
		// for 300m single
		//pinC1.add(fs.path);
		
		long devideSize = (long) (fs.size * capacityC1);
		devideFiles(fs.path,devideSize,fileID);
		pinC1.add("/home/hadoop/paperTestData/"+fileID+"aa");
		pinC2.add("/home/hadoop/paperTestData/"+fileID+"ab");
		fileID++;
		
		
		// final
		FileWriter fw = new FileWriter("/home/hadoop/paperTestData/C2Path");
		BufferedWriter bw = new BufferedWriter(fw);
		for(int j=0;j<pinC2.size();j++){
			//bw.write("test");
			bw.write(pinC2.get(j)+" ");
		}bw.close();
		
	}
	
	
	public void devideFiles(String devidePath,long devideSize,int fileID) throws IOException, InterruptedException{
		System.out.println("---------------- CS devide "+ fileID + " start ------------------");
		String command = "split --line-bytes=";
		command += devideSize;
		command += " "+devidePath+" ";
		command +="/home/hadoop/paperTestData/";
		command += String.valueOf(fileID);
		try {
			System.out.println("Strat devide file and divide size is "+ devideSize+" file ID is " + fileID);
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
			System.out.println("---------------- CS devide "+fileID+" end ------------------");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public FileStatus mergeFiles(ArrayList<FileStatus> mergePath,int fileID) throws IOException{
		String command = "cat ";
		long reSize = 0;
		FileStatus resultFile = null;
		for(int i=0;i < mergePath.size();i++){
			command += mergePath.get(i).path+" ";
			reSize += mergePath.get(i).size;
		}
		command+=("> /home/hadoop/paperTestData/mergeFile"+String.valueOf(fileID));

		//System.out.println(command);
		
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
			resultFile = new FileStatus("/home/hadoop/paperTestData/mergeFile"+String.valueOf(fileID),reSize);

		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return resultFile;
	}
	
}
