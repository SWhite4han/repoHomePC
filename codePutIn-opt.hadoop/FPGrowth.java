import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;

class TreeNode implements Comparable<TreeNode> {
    private String name; 
    private int count; 
    private TreeNode parent; 
    private List<TreeNode> children; 
    private TreeNode nextHomonym;
  
    public TreeNode() {}
  
    public TreeNode(String name) {
        this.name = name;
    }
  
    public String getName() {
        return name;
    }
  
    public void setName(String name) {
        this.name = name;
    }
  
    public int getCount() {
        return count;
    }
  
    public void setCount(int count) {
        this.count = count;
    }
  
    public TreeNode getParent() {
        return parent;
    }
  
    public void setParent(TreeNode parent) {
        this.parent = parent;
    }
  
    public List<TreeNode> getChildren() {
        return children;
    }
  
    public void addChild(TreeNode child) {
        if (this.getChildren() == null) {
            List<TreeNode> list = new ArrayList<TreeNode>();
            list.add(child);
            this.setChildren(list);
        } else {
            this.getChildren().add(child);
        }
    }
  
    public TreeNode findChild(String name) {
        List<TreeNode> children = this.getChildren();
        if (children != null) {
            for (TreeNode child : children) {
                if (child.getName().equals(name)) {
                    return child;
                }
            }
        }
        return null;
    }
  
    public void setChildren(List<TreeNode> children) {
        this.children = children;
    }
  
    public void printChildrenName() {
        List<TreeNode> children = this.getChildren();
        if (children != null) {
            for (TreeNode child : children) {
                System.out.print(child.getName() + " ");
            }
        } else {
            System.out.print("null");
        }
    }
  
    public TreeNode getNextHomonym() {
        return nextHomonym;
    }
  
    public void setNextHomonym(TreeNode nextHomonym) {
        this.nextHomonym = nextHomonym;
    }
  
    public void countIncrement(int n) {
        this.count += n;
    }
  
    @Override
    public int compareTo(TreeNode arg0) {
        int count0 = arg0.getCount();
        return count0 - this.count;
    }
}

public class FPGrowth {
  private static final int minSuport = 10; //****************************************************support count set**********************************************************
  
  class MyInputFormat extends FileInputFormat<LongWritable,Text>  {
    protected boolean isSpitable(JobContext context, Path file) {
	CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
	return codec == null;
    }
    
    public RecordReader<LongWritable,Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
	return new SplitRecordReader();
    }
  }

  class SplitRecordReader extends RecordReader<LongWritable,Text> { 
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader in; //defination class
    private int maxLineLength;
    private LongWritable key = null;
    private Text value = null;

    //private byte[] separator = "END\n".getBytes(); 

    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
	FileSplit split = (FileSplit) genericSplit;
	Configuration conf = context.getConfiguration();
	this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength",Integer.MAX_VALUE);
	
	start = split.getStart();
	end = start + split.getLength();
	final Path file = split.getPath();
	compressionCodecs = new CompressionCodecFactory(conf);
 	final CompressionCodec codec = compressionCodecs.getCodec(file);

	FileSystem fs = file.getFileSystem(conf);
	FSDataInputStream fileIn = fs.open(split.getPath());

	boolean skipFirstLine = false;

	if(codec != null) {
            in = new LineReader(codec.createInputStream(fileIn),conf);
	    end = Long.MAX_VALUE;
	} else {
	    if(start != 0) {
                skipFirstLine = true;
                //this.start -= separator.length;
		--start;
		fileIn.seek(start);
	    }

	    in = new LineReader(fileIn,conf);	
	}
	

	if(skipFirstLine) {
	   start += in.readLine(new Text(),0,(int)Math.min((long)Integer.MAX_VALUE,end-start));
	}

	this.pos = start;	
     }

     public boolean nextKeyValue() throws IOException {
	if(key == null) {
	    key = new LongWritable();
	}
 
	key.set(pos);
	
	if(value == null) {
	   value = new Text();
	}

	int newSize = 0;

        while( pos < end ) {
	   newSize = in.readLine(value,maxLineLength,Math.max((int)Math.min(Integer.MAX_VALUE,end-pos),maxLineLength));

	   if(newSize == 0) {
	       break;
	   }
	
	   pos += newSize;

	   if(newSize < maxLineLength) {
	      break;
	   }
        }

       if(newSize == 0) {
	   key = null;
	   value = null;
  	   return false;
       } else {
	  return true;
       }
    } 

    public LongWritable getCurrentKey() {
	return key;
    }
	
    public Text getCurrentValue() {
	return value;
    }
	
    public float getProgress() {
	if(start == end) {
	    return 0.0f;
	} else {
	    return Math.min(1.0f,(pos - start) / (float)(end-start));
	}
    }

    public synchronized void close() throws IOException {
	if(in != null) {
	    in.close();
	}
    }     
  }
  
  public static class FPMapper extends Mapper<LongWritable, Text, Text, Text> {
    LinkedHashMap<String, Integer> freq = new LinkedHashMap<String, Integer>(); // 頻繁1項集
    ArrayList<TreeNode> itemCount = new ArrayList<TreeNode>();
    private Text JOBID = new Text();

    @Override
    public void setup(Context context) throws IOException {
        // 從HDFS文件讀入頻繁1項集
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        //Path freqFile = new Path("/user/hadoop/dataFrequencies/part-r-00000");
        JOBID = new Text(conf.get("JOBID"));
        Path freqFile = new Path("/user/hadoop/dataFrequencies"+JOBID+"/part-r-00000");

        FSDataInputStream fileIn = fs.open(freqFile);
        LineReader in = new LineReader(fileIn, conf);
        Text line = new Text();
        while (in.readLine(line) > 0) {
            String[] arr = line.toString().split("	"); // part-00000空格大小
            if (arr.length == 2) {
                int count = Integer.parseInt(arr[1]);
                // 只讀取詞頻大於最小支持度的
                if (count >= minSuport) {
                     String word = arr[0];
                     freq.put(word, count);

                     TreeNode node = new TreeNode(word);
                     node.setCount(count);  
                     itemCount.add(node);
                }
            }
        }
        Collections.sort(itemCount);
        in.close();
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	//testKeyValue(key, value, context);

        // read record from HDFS
        String[] str = value.toString().split(",");  // *******************change split sign******************************
        List<String> record = new LinkedList<String>(); 
        for(String w : str) {
	    record.add(w);
        }

        // sort record
        Map<String, Integer> map = new HashMap<String, Integer>();
        
        for (String item : record) {
            // 由於itemCount已經是按降序排列的
            for (int i = 0; i < itemCount.size(); i++) {
                TreeNode tnode = itemCount.get(i);
                if (tnode.getName().equals(item)) {
                    map.put(item, i);
                }
            }
        }

        ArrayList<Entry<String, Integer>> al = new ArrayList<Entry<String, Integer>>(map.entrySet());
        Collections.sort(al, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Entry<String, Integer> arg0, Entry<String, Integer> arg1) {
                // 降序排列
                return arg0.getValue() - arg1.getValue();
            }
        });

        List<String> list = new LinkedList<String>();
        for (Entry<String, Integer> entry : al) {
            list.add(entry.getKey());
        }

        // bulid header table & FP tree
	ArrayList<TreeNode> HeaderTable = buildHeaderTableMap(list);
	TreeNode treeRoot = buildFPTreeMap(list, HeaderTable);

	// 如果FP-Tree為空則返回
        /*if (treeRoot.getChildren()==null || treeRoot.getChildren().size() == 0)
            return;
        
        List<String> newlist = new ArrayList<String>();
        for (TreeNode header : HeaderTable) {
	    String path = "";
	    path += header.getName() + " ";
	    TreeNode backnode = header.getNextHomonym();
	    while (backnode != null) {
		TreeNode parent = backnode;
		while ((parent = parent.getParent()).getName() != null) {
		    path += parent.getName() + " ";
		}
		backnode = backnode.getNextHomonym();
	    }

	    StringBuffer buffer = new StringBuffer(path);
	    buffer.reverse();
	    if(buffer.length() != 2 && buffer.length() != 3) {
		newlist.add(buffer.toString().trim());
	    }
	}  

        // output
        for(String s:newlist) {
	    context.write(new Text(Character.toString(s.charAt(s.length() - 1)) + freq.get(Character.toString(s.charAt(s.length() - 1))).toString()), new Text(s));			
	}*/

        // output
        List<String> newlist = new ArrayList<String>();
        newlist.add(list.get(0));

        for (int i = 1; i < list.size(); i++) {
            // 去除list中的重複項
            if (!list.get(i).equals(list.get(i - 1))) {
                newlist.add(list.get(i));
            }
        }

        for (int i = 1; i < newlist.size(); i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j <= i; j++) {
                sb.append(newlist.get(j) + " ");
            }
            
            context.write(new Text(newlist.get(i) + freq.get(newlist.get(i)).toString()), new Text(sb.toString()));
        }
            
        // 清除資料
        record.clear();
        list.clear();
        map.clear();
        al.clear();
        newlist.clear();
    }

    public void testKeyValue(LongWritable key, Text value, Context context) throws IOException {
        String outputPath = "hdfs://node1:9000/user/hadoop/result/readFileResult.txt";
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path outputFile = new Path(outputPath);

	FSDataOutputStream fsStream1 = fs.create(outputFile);
	OutputStreamWriter osr = new OutputStreamWriter(fsStream1);
	BufferedWriter bw = new BufferedWriter(osr);
	
	try {
            bw.write(key + " " + value);    
	    bw.write("\n");
            bw.flush();
  
        } finally {
	    fsStream1.close();
            bw.close();			
        }
    } 

    public ArrayList<TreeNode> buildHeaderTableMap(List<String> transRecords) {
	ArrayList<TreeNode> F1 = null;
	if (transRecords.size() > 0) {
	    F1 = new ArrayList<TreeNode>();
	    Map<String, TreeNode> map = new HashMap<String, TreeNode>();

	    // 計算事務數據庫中各項的支持度
	    for (String item : transRecords) {
	        if (!map.keySet().contains(item)) {
		    TreeNode node = new TreeNode(item);
		    node.setCount(1);
		    map.put(item, node);
		} else {
		    map.get(item).countIncrement(1);
		}
	    }

	    // 把支持度大於（或等於）minSup的項加入到F1中
	    Set<String> names = map.keySet();
	    for (String name : names) {
		TreeNode tnode = map.get(name);
		F1.add(tnode);
	    }
	    Collections.sort(F1);
	    return F1;
	} else {
	    return null;
	}
    } 

    public TreeNode buildFPTreeMap(List<String> transRecord, ArrayList<TreeNode> F1) {
	TreeNode root = new TreeNode(); // 創建樹的根節點

	LinkedList<String> record = transformer(transRecord); 
	TreeNode subTreeRoot = root;
	TreeNode tmpRoot = null;

	while (!record.isEmpty() && (tmpRoot = subTreeRoot.findChild(record.peek())) != null) {
	    tmpRoot.countIncrement(1);
	    subTreeRoot = tmpRoot;
	    record.poll();
	}
	addNodesMap(subTreeRoot, record, F1);

	return root;
    } 

    public LinkedList<String> transformer(List<String> transRecord) {
	LinkedList<String> rest = new LinkedList<String>();

	for (String item : transRecord) {
	    rest.add(item);
	}
		
	return rest;
    }

    public void addNodesMap(TreeNode ancestor, LinkedList<String> record, ArrayList<TreeNode> F1) {
	if (record.size() > 0) {
	    while (record.size() > 0) {
		String item = record.poll();
		TreeNode leafnode = new TreeNode(item);
		leafnode.setCount(1);
		leafnode.setParent(ancestor);
		ancestor.addChild(leafnode);
		for (TreeNode f1 : F1) {
		    if (f1.getName().equals(item)) {
			while (f1.getNextHomonym() != null) {
			    f1 = f1.getNextHomonym();
			}
			f1.setNextHomonym(leafnode);
			break;
		    }
		}

		addNodesMap(leafnode, record, F1);
	   }
       }
    } 
  }

  public static class FPReducer extends Reducer<Text, Text, Text, Text> { 
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<List<String>> transRecords = new LinkedList<List<String>>(); // 事務數據庫
        boolean isNotCBPPatternHT = true;
        boolean isNotCBPPatternS = true; 
        String headerNode = key.toString();

        // 從reduce讀入資料            
        while (values.iterator().hasNext()) {
            String[] arr = values.iterator().next().toString().split(" ");
            LinkedList<String> list = new LinkedList<String>();
            for (String ele : arr)
               list.add(ele);
            transRecords.add(list);
        } 

        // test 
        //testReducerInput(key, transRecords, context);

        FPGrowth(transRecords, null, isNotCBPPatternHT, isNotCBPPatternS, headerNode, context);
    }

    public void testReducerInput(Text key, List<List<String>> transRecords, Context context) throws IOException, InterruptedException {
        // test transRecords
        for (List<String> record : transRecords) {
            StringBuilder sb2 = new StringBuilder();
            for (String item : record) {    
                sb2.append(item + " ");    
            }
            context.write(new Text(key), new Text(sb2.toString()));
        }
    }

    public void FPGrowth(List<List<String>> transRecords, List<String> postPattern, boolean isNotCBPPatternHT, boolean isNotCBPPatternS, String headerNode, Context context) throws IOException, InterruptedException {
        // 構建項頭表，同時也是頻繁1項集
        ArrayList<TreeNode> HeaderTable;

        if(isNotCBPPatternHT) {
            HeaderTable = buildHeaderTable(headerNode);
            isNotCBPPatternHT = false;
        } else {
            HeaderTable = buildCBPHeaderTable(transRecords);
        }
        
        // 構建FP-Tree
        TreeNode treeRoot = buildFPTree(transRecords, HeaderTable, isNotCBPPatternS);
        isNotCBPPatternS = false; 

        // 如果FP-Tree為空則返回
        if (treeRoot.getChildren()==null || treeRoot.getChildren().size() == 0)
            return;
        
        // 輸出項頭表的每一項+postPattern
        if (postPattern!=null) {
            for (TreeNode header : HeaderTable) {
                String outStr = header.getName();
                int count = header.getCount();
                for (String ele : postPattern)
                    outStr += "\t" + ele;
                context.write(new Text(Integer.toString(count)), new Text(outStr));
            }
        }
        
        // 找到項頭表的每一項的條件模式基，進入遞歸迭代
        for (TreeNode header : HeaderTable) {
            // 後綴模式增加一項
            List<String> newPostPattern = new LinkedList<String>();
            newPostPattern.add(header.getName());
            if (postPattern != null)
                newPostPattern.addAll(postPattern);
            // 尋找header的條件模式基CPB，放入newTransRecords中
            List<List<String>> newTransRecords = new LinkedList<List<String>>();
            TreeNode backnode = header.getNextHomonym();
            while (backnode != null) {
                int counter = backnode.getCount();
                List<String> prenodes = new ArrayList<String>();
                TreeNode parent = backnode;
                // 遍歷backnode的祖先節點，放到prenodes中
                while ((parent = parent.getParent()).getName() != null) {
                    prenodes.add(parent.getName());
                }
                while (counter-- > 0) {
                    newTransRecords.add(prenodes);
                }
                backnode = backnode.getNextHomonym();
            }
            // 遞歸迭代
            FPGrowth(newTransRecords, newPostPattern, isNotCBPPatternHT, isNotCBPPatternS, headerNode, context);
        }
    }

    public ArrayList<TreeNode> buildHeaderTable(String headerNode) {
        ArrayList<TreeNode> F1 = new ArrayList<TreeNode>();
        TreeNode node = new TreeNode(Character.toString(headerNode.charAt(0)));
        node.setCount(Integer.valueOf(Character.toString(headerNode.charAt(1))));  
        F1.add(node);
        return F1;   
    }

    public ArrayList<TreeNode> buildCBPHeaderTable(List<List<String>> transRecords) {
        ArrayList<TreeNode> F1 = null;
        if (transRecords.size() > 0) {
            F1 = new ArrayList<TreeNode>();
            Map<String, TreeNode> map = new HashMap<String, TreeNode>();
            
            // 計算事務數據庫中各項的支持度
            for (List<String> record : transRecords) {
                for (String item : record) {
                    if (!map.keySet().contains(item)) {
                        TreeNode node = new TreeNode(item);
                        node.setCount(1);
                        map.put(item, node);
                    } else {
                        map.get(item).countIncrement(1);
                    }
                }
            }
            
            // 把支持度大於（或等於）minSup的項加入到F1中
            Set<String> names = map.keySet();
            for (String name : names) {
                TreeNode tnode = map.get(name);
                if (tnode.getCount() >= minSuport) {
                    F1.add(tnode);
                }
            }
            Collections.sort(F1);
            return F1;
        } else {
            return null;
        }
    }

    public TreeNode buildFPTree(List<List<String>> transRecords, ArrayList<TreeNode> F1, boolean isNotCBPPatternS) {
        TreeNode root = new TreeNode(); // 創建樹的根節點
        LinkedList<String> record;
        for (List<String> transRecord : transRecords) {
            if(isNotCBPPatternS) {
                record = sortByF1(transRecord); 
            } else {
                record = sortByF1CBP(transRecord, F1);
            }
            TreeNode subTreeRoot = root;
            TreeNode tmpRoot = null;
            if (root.getChildren() != null) {
                while (!record.isEmpty() && (tmpRoot = subTreeRoot.findChild(record.peek())) != null) {
                    tmpRoot.countIncrement(1);
                    subTreeRoot = tmpRoot;
                    record.poll();
                }
            }
            addNodes(subTreeRoot, record, F1);
        }

        return root;
    }

    public LinkedList<String> sortByF1(List<String> transRecord) {
        LinkedList<String> rest = new LinkedList<String>();
        
        for (String item : transRecord) {
            rest.add(item);
        }
        
        return rest;
    }

    public LinkedList<String> sortByF1CBP(List<String> transRecord,ArrayList<TreeNode> F1) {
        Map<String, Integer> map = new HashMap<String, Integer>();
        
        for (String item : transRecord) {
            // 由於F1已經是按降序排列的
            for (int i = 0; i < F1.size(); i++) {
                TreeNode tnode = F1.get(i);
                if (tnode.getName().equals(item)) {
                    map.put(item, i);
                }
            }
        }
        ArrayList<Entry<String, Integer>> al = new ArrayList<Entry<String, Integer>>(
                map.entrySet());
        Collections.sort(al, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Entry<String, Integer> arg0,
                    Entry<String, Integer> arg1) {
                // 降序排列
                return arg0.getValue() - arg1.getValue();
            }
        });
        LinkedList<String> rest = new LinkedList<String>();
        for (Entry<String, Integer> entry : al) {
            rest.add(entry.getKey());
        }
        return rest;
    }

    public void addNodes(TreeNode ancestor, LinkedList<String> record, ArrayList<TreeNode> F1) {
        if (record.size() > 0) {
            while (record.size() > 0) {
                String item = record.poll();
                TreeNode leafnode = new TreeNode(item);
                leafnode.setCount(1);
                leafnode.setParent(ancestor);
                ancestor.addChild(leafnode);
 
                for (TreeNode f1 : F1) {
                    if (f1.getName().equals(item)) {
                        while (f1.getNextHomonym() != null) {
                            f1 = f1.getNextHomonym();
                        }
                        f1.setNextHomonym(leafnode);
                        break;
                    }
                }
 
                addNodes(leafnode, record, F1);
            }
        }
    } 
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("JOBID",args[2]);
    Job job = new Job(conf, "FP Growth");
    job.setJarByClass(FPGrowth.class);
    job.setMapperClass(FPMapper.class);
    //job.setCombinerClass(FPReducer.class);
    job.setReducerClass(FPReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(2);   //for test
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

