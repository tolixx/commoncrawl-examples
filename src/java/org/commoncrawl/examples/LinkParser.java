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


import org.apache.hadoop.mapred.Reducer;


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
public class LinkParser extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(LinkParser.class);

	public static class LinkParserMapper
      extends    MapReduceBase 
      implements Mapper<Text, Text, Text, IntegerPair> {

    // create a counter group for Mapper-specific statistics
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

                //Map<String,Integer> linkMap = new HashMap<String,Integer>();
                
                for (int i = 0; i < linksCount; i++) {
                    link = contentLinks.get(i).getAsJsonObject();
                    href = getHref ( link );
                    reporter.incrCounter(this._counterGroup, "tryToGetHrefs", linksCount);
                    if ( href != null ) {
                    	reporter.incrCounter(this._counterGroup, "validHrefs", linksCount);
                    	domain = getDomainName ( href );

                    	if ( domain != null ) {
                            if ( domain.equalsIgnoreCase(baseDomain) ) {
                    		    output.collect ( new Text(href), new IntegerPair(1,0));
                            } else {
                                output.collect ( new Text(href), new IntegerPair(0,1));
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


  public static class SampleFilter
      implements PathFilter {

    private static int count =         0;
    private static int max   = 999999999;

    public boolean accept(Path path) {

      if (!path.getName().startsWith("metadata-"))
        return false;

      SampleFilter.count++;

      if (SampleFilter.count > SampleFilter.max)
        return false;

      return true;
    }
  }


  public static class LinksReduce  extends MapReduceBase implements 
    Reducer<Text,IntegerPair, Text, IntegerPair> {


        public void reduce(Text key, Iterator<IntegerPair> values, OutputCollector<Text,IntegerPair>  output, Reporter reporter)
        throws IOException, InterruptedException {

            Integer first = 0;
            Integer second = 0;
            for (IntegerPair val : values) {
                first += val.first();
                second += val.second();
            }
            output.collect(key, new IntegerPair(first,second));
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

    String baseInputPath = "s3n://aws-publicdatasets/common-crawl/parse-output/segment";
    
    if (configFile != null) {
      LOG.info("adding config parameters from '"+ configFile + "'");
      this.getConf().addResource(configFile);
    }
    
    //setConfiguration();
    // Creates a new job configuration for this Hadoop job

    JobConf job = new JobConf(this.getConf());

    job.setJarByClass(ExampleMetadataDomainPageCount.class);
    FileSystem fs;

    fs = FileSystem.get(new URI("s3n://aws-publicdatasets"), job);
    int counter = 0;
    int used = 0;


    String segmentId;
    String lastSegment = "";


    String segmentInfo = this.getConf().get("segment.info");
    LOG.info ( "SegmentInfo readed : " + segmentInfo ); //--- si ---

    String parts[] = segmentInfo.split("/"); //--- 1/2
    
    int segmentNum = 0;
    int segmentAll = 0;


    if ( parts.length == 2 ) {
        segmentNum = Integer.valueOf(parts[0]);
        segmentAll = Integer.valueOf(parts[1]);
    } 

    
    String inputPath = null;
    

    inputPath = "s3n://linksresults/results/000001.gz"; //--- all the files here ---

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
    //job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputFormat(KeyValueTextInputFormat.class);

    // Set which OutputFormat class toString use.
    job.setOutputFormat(TextOutputFormat.class);

    // Set the output data types.
    job.setOutputKeyClass(Text.class);
    //job.setOutputValueClass(LongWritable.class);
    job.setOutputValueClass(IntegerPair.class);

    //--- how to ---

    // Set which Mapper and Reducer classes to use. 
    job.setMapperClass(LinkParser.LinkParserMapper.class);
    job.setReducerClass(LinkParser.LinksReduce.class); 

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
    int res = ToolRunner.run(new Configuration(), new LinkParser(), args);
    System.exit(res);
  }
}
