package org.commoncrawl.examples;

// Java classes
import java.lang.Math;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;

// Apache Project classes
import org.apache.log4j.Logger;

// Hadoop classes
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonArray;

import com.google.common.net.InternetDomainName;



/**
 * An example showing how to use the Common Crawl 'textData' files to efficiently
 * work with Common Crawl corpus text content.
 * 
 * @author Tolixx
 */
public class UniqueReducer extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(UniqueReducer.class);
	public static class UniqueMapper
	extends    MapReduceBase 
	implements Mapper<Text, Text, Text, LongWritable> {

    // create a counter group for Mapper-specific statistics
		private final String _counterGroup = "Custom Mapper Counters";
		private Reporter reporter = null;

		public void map(Text key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
		throws IOException {
        	output.collect ( key, new LongWritable(Integer.parseInt(value.toString()))); //--- integer
        }
    }

    /**
   * Implmentation of Tool.run() method, which builds and runs the Hadoop job.
   *
   * @param  args command line parameters, less common Hadoop job parameters stripped
   *              out and interpreted by the Tool class.  
   * @return      0 if the Hadoop job completes successfully, 1 if not. 
   */
    @Override
    public int run(String[] args)
    throws Exception {

    	String outputPath = null;
    	String configFile = null;
    // String inputPath  = null;

    // Read the command line arguments.
    	if (args.length <  1)
    		throw new IllegalArgumentException("Example JAR must be passed an output path.");

    	outputPath = args[0];

    	if (args.length >= 2)
    		configFile = args[1];


    	if (configFile != null) {
    		LOG.info("adding config parameters from '"+ configFile + "'");
    		this.getConf().addResource(configFile);
    	}

    //setConfiguration();
    // Creates a new job configuration for this Hadoop job

    	JobConf job = new JobConf(this.getConf());

    	job.setJarByClass(UniqueReducer.class);
    	FileSystem fs;
    	String inputPath = null;

    //--- set the separator here ( purpose testing here ) ---
    	this.getConf().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t" );


    	inputPath = "s3n://tolixuniq/emr/ExampleBackwards/1350433107105/part-00002";
    	FileInputFormat.addInputPath(job, new Path(inputPath));



    	LOG.info ( "We just added inputPath : " + inputPath );



    	inputPath = "s3n:///tolixuniq/emr/ExampleBackwards/1350433107095/part-00012";
    	FileInputFormat.addInputPath(job, new Path(inputPath));



    	LOG.info ( "We just added inputPath : " + inputPath );


    	fs = FileSystem.get(new URI("s3n://aws-publicdatasets"), job);
    	LOG.info("clearing the output path at '" + outputPath + "'");

    	fs = FileSystem.get(new URI(outputPath), job);

    	if (fs.exists(new Path(outputPath)))
    		fs.delete(new Path(outputPath), true);



    	FileOutputFormat.setOutputPath(job, new Path(outputPath));
    	FileOutputFormat.setCompressOutput(job, false);

    // Set which InputFormat class to use.
    	job.setInputFormat(KeyValueTextInputFormat.class);

    // Set which OutputFormat class toString use.
    	job.setOutputFormat(TextOutputFormat.class);

    // Set the output data types.
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(LongWritable.class);
    //job.setOutputValueClass(Text.class);

    //--- how to ---

    // Set which Mapper and Reducer classes to use. 
    	job.setMapperClass(UniqueReducer.UniqueMapper.class);
    	job.setReducerClass(LongSumReducer.class); 

    	if (JobClient.runJob(job).isSuccessful())
    		return 0;
    	else
    		return 1;
    }

  /**
   * Main entry point that uses the {@link ToolRunner} class to run the example
   * Hadoop job.
   */
  public static void main(String[] args)
  throws Exception {

  	int res = ToolRunner.run(new Configuration(), new UniqueReducer(), args);
  	System.exit(res);
  }

}