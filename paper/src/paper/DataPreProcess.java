package paper;
import java.io.*;

public class DataPreProcess {

	public static String processingFile(String sourceFilePath , String targetDirectoryPath ) throws Exception{
		File sourceFile = new File(sourceFilePath);
		File targetFile = new File(targetDirectoryPath);
		String fileName = "ppOutPut" ;
		if(!sourceFile.exists()||sourceFile.isDirectory()){
			System.out.println("文件不存在");
			return fileName;
		}
		if(targetFile.exists()){
			if(!targetFile.isDirectory()){
				System.out.println("目標文件夾錯誤,不是一個文件夾");
				return fileName;
			}
		}else{
			targetFile.mkdirs();
		}
		try{
			BufferedReader br = new BufferedReader(new FileReader(sourceFile));
			BufferedWriter bw = null;
			String str = "";
			int lastIndex = 0;			
			String tempData = br.readLine();
			
			while(tempData!=null)
			{
				
				String[] tempSplit = tempData.split(" ");
				//System.out.println(tempSplit[0] + " " + tempSplit[1]);	//for test
				
				if (lastIndex == Integer.parseInt(tempSplit[0]))
				{
					str += (tempSplit[1]+" ");
				}else if(Integer.parseInt(tempSplit[0]) == lastIndex+1){
					str += ("\r\n"+tempSplit[1]+" ");
				}
				lastIndex = Integer.parseInt(tempSplit[0]);
				tempData = br.readLine();	//讀下一行
			}
			
			//System.out.println("data=\n" + str);
			bw = new BufferedWriter(new FileWriter(new File(targetFile.getAbsolutePath()+"/"+fileName+"_"+sourceFile.getName())));
			bw.write(str);
			bw.close();
			br.close();
			return fileName+"_"+sourceFile.getName();
			/*
			BufferedReader br = new BufferedReader(new FileReader(sourceFile));
			BufferedWriter bw = null;
			String str = "";
			String tempData = br.readLine();
			int i=1,s=0;
			while(tempData!=null){
				str += tempData+"\r\n";
				if(i%rows==0){
					bw = new BufferedWriter(new FileWriter(new File(targetFile.getAbsolutePath()+"/"+s+"_"+sourceFile.getName())));
					bw.write(str);
					bw.close();
					str = "";
					s += 1;
				}
				i++;
				tempData = br.readLine();
			}
			if((i-1)%rows!=0){
				bw = new BufferedWriter(new FileWriter(new File(targetFile.getAbsolutePath()+"/"+s+"_"+sourceFile.getName())));
				bw.write(str);
				bw.close();
				br.close();
				s += 1;
			}
			System.out.println("文件分割結束,共分割成了"+s+"個文件");
			*/
		}catch(Exception e){}
		return fileName;
	}
	
	
	public static void main(String[] args) throws Exception {
		String fileName = DataPreProcess.processingFile("D:/test/1123.txt", "D:/test/PreProcess/");
		System.out.println("輸出檔案 D:/test/PreProcess/" + fileName);
	}

}
