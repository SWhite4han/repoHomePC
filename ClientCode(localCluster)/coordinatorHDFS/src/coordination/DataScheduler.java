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

public class DataScheduler {
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
	
	final long C1d = 384*1048576;
	final long C2d = 256*1048576;
	final long CCCd = C1d + C2d;
	
	public DataScheduler(String[] filePath,long[] fileSize) throws IOException, InterruptedException{
		newSchedule(filePath,fileSize);
		
	}
	
	public void newSchedule(String[] filePath,long[] fileSize) throws IOException, InterruptedException{
		// First divide
		System.out.println("-------------- First Divide --------------- \n");
		schedule(filePath,fileSize);
		
		
		System.out.println("----------------- re file ------------------");
		for(int j=0;j<remainList.size();j++){
			System.out.println("re file "+remainList.get(j).path+"\n");
		}
		System.out.println("----------------- p in C1 file ------------------");
		for(int j=0;j<pinC1.size();j++){
			System.out.println("re file "+pinC1.get(j)+"\n");
		}
		System.out.println("----------------- p in C2 file ------------------");
		for(int j=0;j<pinC2.size();j++){
			System.out.println("re file "+pinC2.get(j)+"\n");
		}
		
		class metaComparator implements Comparator<FileStatus>{
			@Override
			public int compare(FileStatus o1, FileStatus o2) {
				return (int) (o1.size - o2.size);
			}
		}
		Collections.sort(remainList,new metaComparator());
		
		// for test
		System.out.println("----------------- re file after sorting ------------------");
		for(int j=0;j<remainList.size();j++){
			System.out.println("re file "+remainList.get(j).path+"\n");
			System.out.println("re file "+remainList.get(j).size+"\n");
		}
		
		double count=0;
		int mergeCount=100;
		// schedule remainList data
		/*while(!remainList.isEmpty()){
			System.out.println("I am coming while");
			DecimalFormat df = new DecimalFormat("#.00");
			count = Double.parseDouble(df.format((double)remainList.get(remainList.size()-1).size / 1048576)); // =max
			mergeList.add(remainList.get(remainList.size()-1));	// take last num
			remainList.remove(remainList.size()-1);
			
			for(int j=0;j<remainList.size();j++){
				
				String fileSizeString = df.format((double) remainList.get(j).size / 1048576);	// format size
				double fs = Double.parseDouble(fileSizeString);

				if((count + fs) <= CCC){
					count+=fs;
					mergeList.add(remainList.get(j));
					remainList.remove(j);
					j--;
				}
			}
			System.out.println("----------------- merge files "+ mergeCount +" ------------------");
			for(int j=0;j<mergeList.size();j++){
				System.out.println("merge file path "+mergeList.get(j).path+"\n");
				System.out.println("merge file size "+mergeList.get(j).size+"\n");
			}
			
			if(mergeList.size()==1){
				
				if(mergeList.get(0).size > C1d){
					devideFiles(mergeList.get(0).path,C1d,fileID);
					pinC1.add("/home/hadoop/paperTestData/"+fileID+"aa");
					pinC2.add("/home/hadoop/paperTestData/"+fileID+"ab");
					fileID++;
					mergeList.clear();
				}else{
					pinC1.add(mergeList.get(0).path);
					mergeList.clear();
				}
				
			}else{
				FileStatus fs = mergeFiles(mergeList,mergeCount);
				mergeCount++;
				mergeList.clear();
				if(fs.size > C1d){
					devideFiles(fs.path,C1d,fileID);
					pinC1.add("/home/hadoop/paperTestData/"+fileID+"aa");
					pinC2.add("/home/hadoop/paperTestData/"+fileID+"ab");
					fileID++;
					mergeList.clear();
				}else{
					pinC1.add(fs.path);
					mergeList.clear();
				}
			}
			
		}*/
		
		// new scheduler
		// merge All Files
		long startTimeMerge = System.currentTimeMillis();
		FileStatus fs = mergeFiles(remainList,mergeCount);
		long endTimeMerge = System.currentTimeMillis();
		System.out.println("Total cost of MERGE TEST is " + (endTimeMerge - startTimeMerge)/1000 + " s...");
		System.out.println("------------------ C1  MERGE  TEST ----------------");
		remainList.clear();
		System.out.println("mrege in "+fs.size);
		
		DecimalFormat df = new DecimalFormat("#.00");
		count = Double.parseDouble(df.format((double)fs.size / 1048576));
		long round = (long) (count/CCC);
		long remainS = (long) (count%CCC);
		System.out.println("round is "+round+" ; remain is "+ remainS + " ; devide in"+CCC+ "; fileID is "+fileID);
		devideFiles(fs.path,CCCd,fileID);
		if (remainS != 0){
			round++;
		}
		String[] devidePath = new String[(int)round];
		long[] devideSize = new long[(int)round];
		int ascii = 97;
		for(int i = 0;(int)i<round;i++){
			if(i!=(round-1)){
				devideSize[i] = CCCd;
			}else{
				devideSize[i] = (remainS*1048576);
			}
			devidePath[i] = ("/home/hadoop/paperTestData/"+fileID+"a"+(char)ascii);
			ascii++;
		}
		fileID++;
		mergeCount++;
		System.out.println("-------------- Secound Divide --------------- \n");
		schedule(devidePath,devideSize);
		
		if(!remainList.isEmpty()){
			System.out.println("remainSize is : "+remainList.get(0).size);
			if(remainList.get(0).size > C1d){
				System.out.println("if");
				//if(remainList.get(0).size < CCCd){
					devideFiles(remainList.get(0).path,C1d,fileID);
					pinC1.add("/home/hadoop/paperTestData/"+fileID+"aa");
					pinC2.add("/home/hadoop/paperTestData/"+fileID+"ab");
				//}
			}else{
				System.out.println("else");
				pinC1.add(remainList.get(0).path);
			}
			
			
		}
		
		
		System.out.println("----------------- p in C1 file Again ------------------");
		for(int j=0;j<pinC1.size();j++){
			System.out.println("re file "+pinC1.get(j)+"\n");
		}
		System.out.println("----------------- p in C2 file Again ------------------");
		for(int j=0;j<pinC2.size();j++){
			System.out.println("re file "+pinC2.get(j)+"\n");
		}
		

		
		FileWriter fw = new FileWriter("/home/hadoop/paperTestData/C2Path");
		BufferedWriter bw = new BufferedWriter(fw);
		for(int j=0;j<pinC2.size();j++){
			//bw.write("test");
			bw.write(pinC2.get(j)+" ");
		}bw.close();		
	}
	
	public void schedule(String[] filePath,long[] fileSize) throws IOException, InterruptedException{
		
		for(int i=0;i<filePath.length;i++){
			System.out.println("schedule file "+filePath[i]);
			DecimalFormat df = new DecimalFormat("#.00");
			String fileSizeString = "";
			fileSizeString = df.format((double) fileSize[i] / 1048576);	// format size
			double fs = Double.parseDouble(fileSizeString);
			
			if(fs < CCC){
				System.out.println("********* file < 640m");
				FileStatus md = new FileStatus(filePath[i],fileSize[i]);
				remainList.add(md);
			}else if(fs%CCC == 0 || ((fs>CCC-1)&&(fs<CCC+1))){
				System.out.println("********* file = 640m+-1");
				devideFiles(filePath[i],C1d,fileID);
				pinC1.add("/home/hadoop/paperTestData/"+fileID+"aa");
				pinC2.add("/home/hadoop/paperTestData/"+fileID+"ab");
				fileID++;
			}else if(fs > CCC && fs < (CCC*2)){
				System.out.println("********* 640m*2 > file >640m ");
				double round = fs/CCC;
				double remain = fs%CCC;
				devideFiles(filePath[i],CCCd,fileID);
				FileStatus md = new FileStatus("/home/hadoop/paperTestData/"+fileID+"ab",(fileSize[i]-CCCd));
				remainList.add(md);
				fileID++;
				devideFiles("/home/hadoop/paperTestData/"+(fileID-1)+"aa",C1d,fileID);
				pinC1.add("/home/hadoop/paperTestData/"+fileID+"aa");
				pinC2.add("/home/hadoop/paperTestData/"+fileID+"ab");
				fileID++;
			}else{
				System.out.println("********* IF 4");
			}
		}
	}
	
	public void devideFiles(String devidePath,long devideSize,int fileID) throws IOException, InterruptedException{
		
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
			//System.out.println(command);
			dataOut.writeBytes(command);
			//dataOut.writeBytes("split --line-bytes=60000000 /home/hadoop/paperTestData/final20030 /home/hadoop/paperTestData/op");
			dataOut.flush();
			dataOut.close();
			ps.waitFor();
			while ((line = in.readLine()) != null){
				System.out.println(line);
			}
			System.out.println("---------------- devide end ------------------");
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
