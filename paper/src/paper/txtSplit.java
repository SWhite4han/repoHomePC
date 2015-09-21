package paper;

import java.io.*;
public class txtSplit{
/** 
 *根據需求,直接調用靜態方法start來執行操作
 *參數: 
 * rows為多少行一個文件int類型
 * sourceFilePath為源文件路徑String類型
 * targetDirectoryPath為文件分割後存放的目標目錄String類型
 * ---分割後的文件名 ​​為索引號(從0開始)加'_'加源文件名​​,例如源文件名 ​​為test.txt,則分割後文件名 ​​為0_test .txt,以此類推
 */ 
	public static void start(int rows,String sourceFilePath,String targetDirectoryPath){
		File sourceFile = new File(sourceFilePath);
		File targetFile = new File(targetDirectoryPath);
		if(!sourceFile.exists()||rows<=0||sourceFile.isDirectory()){
			System.out.println("源文件不存在或者輸入了錯誤的行數");
			return;
		}
		if(targetFile.exists()){
			if(!targetFile.isDirectory()){
				System.out.println("目標文件夾錯誤,不是一個文件夾");
				return;
			}
		}else{
			targetFile.mkdirs();
		}
		try{
			long strLength = sourceFile.length();
			long size = 500 * 1024;
			//System.out.println(strLength/size);
			int dataNum = (int)strLength/(int)size;
			if(strLength/size != 0 ){
				dataNum = ((int)strLength/(int)size + 1);
			}
			rows = rows/dataNum ;
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
		}catch(Exception e){}
	}
 //Test
 
	public static void main(String args[]){
		txtSplit.start(97236,"D:/test/PreProcess/ppOutPut_1123.txt","D:/test/PreProcess/ppOutPut_out2/");
	}

}