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
import java.util.Iterator;
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


import org.apache.hadoop.mapred.Reducer;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonArray;

import com.google.common.net.InternetDomainName;


public class LinkProcessor extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(LinkProcessor.class);

	public static class LinksMapper
	extends    MapReduceBase 
	implements Mapper<Text, Text, Text, IntegerPair> {

		private final String _counterGroup = "Custom Mapper Counters";
		private Reporter reporter = null;

		public void map(Text key, Text value, OutputCollector<Text, IntegerPair> output, Reporter reporter)
		throws IOException {

			String url = key.toString();
			String json = value.toString();

			try {


				reporter.incrCounter(this._counterGroup, "inputStream", 1);

				this.reporter = reporter; 
				String baseDomain = getDomainName (url);

				if ( baseDomain == null ) {
            		return; //--- skip this record here ---
            	}



            	JsonArray contentLinks = getAllLinks ( json );
            	if ( contentLinks == null ) {
            		return; 
            	}

            	reporter.incrCounter(this._counterGroup, "validAllLinks", 1);

            	int linksCount = contentLinks.size();
            	reporter.incrCounter(this._counterGroup, "totalLinkCount", linksCount);
            	
            	JsonObject link;
            	String  href;
            	String  domain;
            	int     totalLinks = 0;

            	output.collect ( new Text(url), new IntegerPair(0,0));


            	for (int i = 0; i < linksCount; i++) {
            		link = contentLinks.get(i).getAsJsonObject();
            		href = getHref ( link );
            		reporter.incrCounter(this._counterGroup, "tryToGetHrefs", linksCount);
            		if ( href != null ) {
            			reporter.incrCounter(this._counterGroup, "validHrefs", linksCount);
            			domain = getDomainName ( href );

            			if ( domain != null ) {

            				if ( href.indexOf("?") == - 1 ) {
            					URI uri = new URI(href);
            					String path = uri.getPath();

            					if ( path.equals("") ) {
            						reporter.incrCounter(this._counterGroup, "added slashes to empty path", 1);
            						href = href + "/";
            					}

            					if ( path.indexOf(".") == -1 ) {
            						if ( path.lastIndexOf('/') != path.length() - 1 ) {
            							reporter.incrCounter(this._counterGroup, "added slashes", 1);
            							href = href + "/";  
            						}
            					}
            				}

            				if ( domain.equalsIgnoreCase(baseDomain) ) {
            					output.collect ( new Text(href), new IntegerPair(1,0));
            					reporter.incrCounter(this._counterGroup, "internal links", 1);
            				} else {
            					output.collect ( new Text(href), new IntegerPair(0,1));
            					reporter.incrCounter(this._counterGroup, "external links", 1);
            				}    
            			}
            		}
            	}

            } catch ( Exception ex ) {
            	LOG.error("Caught Exception", ex);
            	reporter.incrCounter(this._counterGroup, "Exceptions", 1);
            }
        }

        protected String getDomainName ( String url ) {
        	try {
        		URI uri = new URI(url);

        		String host = uri.getHost();
        		String scheme = uri.getScheme();

        		if (host == null) {
        			return null;
        		}

        		if ( !scheme.equalsIgnoreCase("http") && !scheme.equalsIgnoreCase("https") ) {
        			reporter.incrCounter(this._counterGroup, "scheme."+scheme, 1);
        			return null;
        		}

        		InternetDomainName domainObj = InternetDomainName.from(host);
        		String baseDomain = domainObj.topPrivateDomain().name();

        		return baseDomain;
        	} catch ( Exception ex ) {
        		reporter.incrCounter(this._counterGroup, "getDomainName.Exceptions", 1);
        		return null;
        	}
        } 

        protected JsonArray getAllLinks ( String json ) {
        	try {

        		reporter.incrCounter(this._counterGroup, "startToParser", 1);

        		JsonParser jsonParser = new JsonParser();
        		JsonObject jsonObj = jsonParser.parse(json).getAsJsonObject();

        		reporter.incrCounter(this._counterGroup, "getAsJsonObject", 1);

        		if (jsonObj.has("content") == false) {
        			reporter.incrCounter(this._counterGroup, "Content Missing", 1);
        			return null ;
        		}

        		JsonObject jsonContent = jsonObj.getAsJsonObject("content");
        		reporter.incrCounter(this._counterGroup, "Content.Parsed", 1);

        		if (jsonContent.has("links") == false) {
        			reporter.incrCounter(this._counterGroup, "Links Missing", 1);
        			return null;
        		}

        		JsonArray contentLinks = jsonContent.getAsJsonArray("links");
        		return contentLinks;
        	} catch ( Exception ex ) {
        		reporter.incrCounter(this._counterGroup, "getAllLinks.Exceptions", 1);
        		return null;
        	}
        }  

        protected String getHref ( JsonObject link ) {
        	try {
        		if (link.has("type") == true) {
        			String linktype = link.get("type").getAsString();
        			if (linktype.equalsIgnoreCase("a")) {
        				if (link.has("href") == true) {
        					reporter.incrCounter(this._counterGroup, "ahref processed", 1);
        					String linkhref = link.get("href").getAsString();
        					return linkhref;
        				}  
        			}
        		}
        	} catch ( Exception ex ) {
        		reporter.incrCounter(this._counterGroup, "getHref.Exceptions", 1);
        	}

        	return null;        
        }
    }


    public static class LinksReducer  extends MapReduceBase implements 
    Reducer<Text,IntegerPair, Text, IntegerPair> {


    	public void reduce(Text key, Iterator<IntegerPair> values, OutputCollector<Text,IntegerPair>  output, Reporter reporter)
    	throws IOException {

    		Integer first  = 0;
    		Integer second = 0;

    		while (values.hasNext()) {
    			IntegerPair val = values.next();
    			first += val.first();
    			second += val.second();
    		}

    		output.collect(key, new IntegerPair(first,second));
    	}
    } 


    /* REPLACE MAPPER just map as is */
    public static class ReplaceMapper
	extends    MapReduceBase 
	implements Mapper<Text, Text, Text, Text> {

		private final String _counterGroup = "Custom Mapper Counters";
		private Reporter reporter = null;

		public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException {
			output.collect ( key, value );
		}
	}


	public static class ReplaceReducer  extends MapReduceBase implements 
    Reducer<Text,Text, Text, Text> {

    	private final String _counterGroup = "Replace Reducer Counters";

    	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text>  output, Reporter reporter)
    	throws IOException {

    		Boolean towrite = false; //--- 
    		Boolean replaced = false;

    		String  url = key.toString();
    		String  theData = null;

    		while (values.hasNext()) {
    			Text val = values.next();
				String data = val.toString(); //--- string representation ---

				if ( data.indexOf ( "http") == 0 ) {
					replaced = true;
					url = data;
				} else {
					String[] rawData = data.split("\t");
					if ( rawData.length == 2 ) {
						towrite = true;
						theData = data;
					}
				}

    		}

    		if ( towrite ) {
    			reporter.incrCounter(this._counterGroup, "reduceroutput", 1);
    			output.collect ( new Text(url), new Text(theData) );
    			if ( replaced ) {
    				reporter.incrCounter(this._counterGroup, "reducerreplaced", 1);
    			}

    		}
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

    	if (args.length <  1)
    		throw new IllegalArgumentException("Example JAR must be passed an output path.");

    	outputPath = args[0];

    	if (args.length >= 2)
    		configFile = args[1];


    	if (configFile != null) {
    		LOG.info("adding config parameters from '"+ configFile + "'");
    		this.getConf().addResource(configFile);
    	}


    	String  redirectSource = "s3n://linksresults/redirects/*"; //-- mix to replace ---  

        String  firstInput = "s3n://linksresults/results/000001.gz";
    	
    	String  firstOutput = "s3n://parsedlinks/output/reduced/";
    	String  secondOutput = "s3n://parsedlinks/output/replaced/";


    	{
    		LOG.info ( "Starting first job" ) ;
    		JobConf job = new JobConf(this.getConf());

    		job.setJarByClass(ExampleMetadataDomainPageCount.class);
    		FileSystem fs;

    		

    		FileInputFormat.addInputPath(job, new Path(firstInput));
    		LOG.info("clearing the output path at '" + firstOutput + "'");
    		fs = FileSystem.get(new URI(firstOutput), job);

    		if (fs.exists(new Path(firstOutput)))
    			fs.delete(new Path(firstOutput), true);


    		FileOutputFormat.setOutputPath(job, new Path(firstOutput));
    		FileOutputFormat.setCompressOutput(job, false);

    		job.setInputFormat(KeyValueTextInputFormat.class);
    		job.setOutputFormat(TextOutputFormat.class);
    		job.setOutputKeyClass(Text.class);
    		job.setOutputValueClass(IntegerPair.class);


    		job.setMapperClass(LinkProcessor.LinksMapper.class);
    		job.setReducerClass(LinkProcessor.LinksReducer.class); 

    		if (JobClient.runJob(job).isSuccessful()) {
    			LOG.info ( "1 : JOB complete successfully" );
    		}
    	}  

    	
    	{
    		LOG.info ( "2 : Starting REPLACE job" ) ;
    		JobConf job = new JobConf(this.getConf());

    		job.setJarByClass(ExampleMetadataDomainPageCount.class);
    		FileSystem fs;

    		

    		FileInputFormat.addInputPath(job, new Path(firstOutput + "*"));
    		FileInputFormat.addInputPath(job, new Path(redirectSource) );

    		LOG.info("clearing the output path at '" + firstOutput + "'");
    		fs = FileSystem.get(new URI(secondOutput), job);

    		if (fs.exists(new Path(secondOutput)))
    			fs.delete(new Path(secondOutput), true);


    		FileOutputFormat.setOutputPath(job, new Path(secondOutput));
    		FileOutputFormat.setCompressOutput(job, false);

    		job.setInputFormat(KeyValueTextInputFormat.class);
    		job.setOutputFormat(TextOutputFormat.class);
    		job.setOutputKeyClass(Text.class);
    		job.setOutputValueClass(Text.class);


    		job.setMapperClass(LinkProcessor.ReplaceMapper.class);
    		job.setReducerClass(LinkProcessor.ReplaceReducer.class); 

    		if (JobClient.runJob(job).isSuccessful()) {
    			LOG.info ( "2 : Replace JOB complete successfully" );
    		}
    	}

    	{

    	}

    	return 0; 
    } 


  /**
   * Main entry point that uses the {@link ToolRunner} class to run the example
   * Hadoop job.
   */
  public static void main(String[] args)
  throws Exception {
  	int res = ToolRunner.run(new Configuration(), new LinkProcessor(), args);
  	System.exit(res);
  }
}


