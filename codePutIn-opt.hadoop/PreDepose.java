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


public class PreDepose {
  public static class FPMapper extends Mapper<LongWritable, Text, Text, Text> {
    int NITEMS = 2000; // change item count ***************************************
    String beforeNumber = "0";
    List<String> transation = new LinkedList<String>();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arr = value.toString().split(" "); 
        String number = arr[0];
        String item = arr[1];

	boolean flag = false;

	flag = deterDuplicate(transation, item); 
       
	if (number.equals(beforeNumber)) {
	    if(flag){
	         item = deposeDuplicate(transation, item);
		 if(!item.equals("empty")) {
		     transation.add(item);
		 }	
	     } else {
	         transation.add(item);
	     }	
	     flag = false;
			
	 } else {
	     beforeNumber = number;

	     StringBuilder sb = new StringBuilder();
	     for (int i = 0; i < transation.size(); i++) {
		 sb.append(transation.get(i) + ",");
	     }
		
	     context.write(new Text(""), new Text(sb.toString()));

	     transation.clear();
	     transation.add(item);
	}	
    }

    private boolean deterDuplicate(List<String> duplicate, String record) {
		boolean flag = false;

		for(int i = 0; i < duplicate.size(); i++) {
			if(record.equals(duplicate.get(i))) {
				flag = true;
			}
		}
		return flag;
    }

    private String deposeDuplicate(List<String> duplicate, String record) {
		String result = null;
		boolean flag = false;
		int value;

		result = record;

		do {
			value = Integer.parseInt(result);
			value = value + 1;
			result = Integer.toString(value);
			flag = deterDuplicate(duplicate, result);
		} while((flag) && (value < NITEMS));

		if(value >= NITEMS) {
			result = "empty";
		}

		return result;
    }
  }

  public static class FPReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        while (values.iterator().hasNext()) {
            context.write(new Text(values.iterator().next()), new Text(""));  
        }           
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "PreDepose");
    job.setJarByClass(PreDepose.class);
    job.setMapperClass(FPMapper.class);
    job.setReducerClass(FPReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

