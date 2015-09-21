/*****************************************
 * Used to get File's sizes and paths
 * Ver1.2 modify at (15/06/06)
 * Paper Code
 * coding by White
 *****************************************/
package coordination;

import java.io.File;
import java.text.DecimalFormat;
import java.util.Scanner;
import java.io.FileInputStream;

import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.filechooser.FileNameExtensionFilter;


public class GetFileSize
{
	// for getSize()  get file size
	public long getFileSizes(File f) throws Exception{
		long s=0;
		if (f.exists()) {
			FileInputStream fis = null;
			fis = new FileInputStream(f);
			s= fis.available();
		} else {
			f.createNewFile();
			System.out.println("File does not exist");
		}
		return s;
	}
	// Iterator
	// for getSize()  get director size
	public long getFileSize(File f)throws Exception
	{
		long size = 0;
		File flist[] = f.listFiles();
		for (int i = 0; i < flist.length; i++)
		{
			if (flist[i].isDirectory())
			{
				size = size + getFileSize(flist[i]);
			} else
			{
				size = size + flist[i].length();
			}
		}
		return size;
	}
	// for getSize()  transform sizes in K M G and print sizes
	public String FormetFileSize(long fileS) {
		DecimalFormat df = new DecimalFormat("#.00");
		String fileSizeString = "";
		if (fileS < 1024) {
			fileSizeString = df.format((double) fileS) + "B";
		} else if (fileS < 1048576) {
			fileSizeString = df.format((double) fileS / 1024) + "K";
		} else if (fileS < 1073741824) {
			fileSizeString = df.format((double) fileS / 1048576) + "M";
		} else {
			fileSizeString = df.format((double) fileS / 1073741824) + "G";
		}
		return fileSizeString;
	}
	
		
	// for getSize()  iterator to get how many files in director
	public long getlist(File f){
		long size = 0;
		File flist[] = f.listFiles();
		size=flist.length;
		for (int i = 0; i < flist.length; i++) {
			if (flist[i].isDirectory()) {
				size = size + getlist(flist[i]);
				size--;
			}
		}
		return size;

	}
	// for getSize() read file and return path[] to getSize()
	public String[] getPaths(){
		String input = JOptionPane.showInputDialog( "How many file do you want to exe?" );
		int fileNum = Integer.parseInt(input);
		//System.out.println("Please input"+fileNum+"path:");
		String[] filePaths = new String[fileNum];

		// use JFileChooser to be file chooser UI
		JFileChooser chooser = new JFileChooser();
		//chooser.setFileFilter(new FileNameExtensionFilter("Text file", "txt"));
        chooser.setCurrentDirectory(new File("D://test//"));
        chooser.setDialogTitle("Choose file location:");
        //chooser.setFileHidingEnabled(false);
 
        chooser.setDialogType(JFileChooser.OPEN_DIALOG);
        chooser.setApproveButtonText("Choose");
        
        // use JFileChooser read file paths
        for(int i=0;i<fileNum;i++){
        	int confirm = chooser.showOpenDialog(null);
        	if (confirm == chooser.APPROVE_OPTION)
        	{
        		//JOptionPane.showMessageDialog(null, "Your file: " + chooser.getSelectedFile() + "\n,in " + chooser.getCurrentDirectory() + ".", "Message", JOptionPane.INFORMATION_MESSAGE);
        		JOptionPane.showMessageDialog(null, "OK!", "Message", JOptionPane.INFORMATION_MESSAGE);
        		filePaths[i] = chooser.getSelectedFile().toString();
        		//chooser.getSize();
        		System.out.println("file " + (i+1) +" is at:" + filePaths[i]);
        	}
        }
        //end test
        /*
		for(int i=0;i<fileNum;i++){
			Scanner pathIn = new Scanner(System.in);
			System.out.print("�п�J���|" + (i+1) +":");
			filePaths[i] = pathIn.next();�o�ɮפj�p
			System.out.println("���|" + (i+1) +"��:" + filePaths[i]);
		}*/
		System.out.println("----------------------------");
		return filePaths;
	}
	
	public long[] getSize(String[] paths){
		//GetFileSize g = new GetFileSize();
		
		long[] tempSize = new long[paths.length];
		long startTime = System.currentTimeMillis();
		for(int i=0;i<paths.length;i++){
			try
			{
				long l = 0;
				//String path = "D:\\test\\test000.txt";
				String path = paths[i];
				File ff = new File(path);
				if (ff.isDirectory()) { //path is a director
					System.out.println("file numbers " + getlist(ff));
					System.out.println("your input path is a director");
					l = getFileSize(ff);
					System.out.println(path + "dir Size is:" + FormetFileSize(l));
					tempSize[i] =l ;	
				} else {
					//System.out.println("file");
					//System.out.println("your input path is linked to a file");
					l = getFileSizes(ff);
					System.out.println(path + "\nthe file size is: " + FormetFileSize(l));
					tempSize[i] = l;
				}
				

			} catch (Exception e)
			{
				e.printStackTrace();
			}
		}	
		long endTime = System.currentTimeMillis();
		System.out.println("Total cost of read file is " + (endTime - startTime) + " ms...");
		System.out.println("----------------------------------");
		return tempSize;
	}
	
	
}