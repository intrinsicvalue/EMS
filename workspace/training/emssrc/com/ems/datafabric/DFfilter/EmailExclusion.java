package com.ems.datafabric.DFfilter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EmailExclusion extends Configured implements Tool {

  public static class EmailExclusionMapper
       extends Mapper<Object, Text, NullWritable, Text> {

    private ArrayList<Pattern> badEmailPatternList = null; //to store compiled Regex patterns
    
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
          
          badEmailPatternList = new ArrayList<Pattern>();

          FileReader fr = new FileReader("./bademailpattern.csv");
          BufferedReader d = new BufferedReader(fr);
         
          String line;                  
          while ((line = d.readLine()) != null) {
              String[] toks = new String[2];
              toks = line.split(" ");
              
              //Once on each data node- create and compile Regex patterns from bad email patterns file
              Pattern emailPattern = Pattern.compile("\\A" + toks[0].toUpperCase().trim().replace(".", "\\.").replace("%", ".*") + "\\Z");
              badEmailPatternList.add(emailPattern);
              
          }
    
          fr.close();
          super.setup(context);
           
    }
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
    	
    	Configuration conf = context.getConfiguration();
    	String param = conf.get("position");
    	
    	//incur method call overhead once for each email 
    	String emailString = getElement(value.toString(), Integer.parseInt(param.toString()), "|" ).toUpperCase();
        
    	for (int i=0; i<badEmailPatternList.size(); i++){
       
        	Matcher m = badEmailPatternList.get(i).matcher(emailString);
       	 	if (m.matches())
       	 		return;      	        	
  
        }
        
        context.write(NullWritable.get(), value);
    	   
    }
    
    public String getElement(String str,int position, String delimiter)
    
    {
           String element=null;
           StringTokenizer strToken=new StringTokenizer(str,delimiter);
           for(int i=0; i<= position && strToken.hasMoreTokens();i++)
                  element=strToken.nextToken();
                  return element;
                  
    }
    
  }
   

 public static void main(String[] args) throws Exception {
	 
	int res = ToolRunner.run(new Configuration(), new EmailExclusion(), args);
	System.exit(res);
	
}
	 
public int run(String[] args) throws Exception {
	 
	Configuration conf = this.getConf();
	conf.set("position", args[2]);
	//conf.setBoolean("mapred.compress.map.output", true);
	//conf.setClass("mapred.map.output.compression.codec", Lz4Codec.class, CompressionCodec.class);
	Job job = Job.getInstance(conf, "emailexclusion");
	job.addCacheFile(new Path("./datafabric/configuration/bademailpattern.csv").toUri());
	job.setJarByClass(EmailExclusion.class);
    job.setMapperClass(EmailExclusionMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }
 }
