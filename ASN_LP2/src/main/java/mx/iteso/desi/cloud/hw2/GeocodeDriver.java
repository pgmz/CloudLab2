package mx.iteso.desi.cloud.hw2;

import mx.iteso.desi.cloud.GeocodeWritable;
import mx.iteso.desi.cloud.Triple;

import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GeocodeDriver {

  public static void main(String[] args) throws Exception {
  
    if (args.length != 3) {
    	System.err.println("Usage: GeocodeDriver <input path> <output path>");
      System.exit(-1);
    }
    
    /* TODO: Your driver code here */
		System.err.println("Running GeocodeDriver: input path -> " + args[0] + " and output path -> " + args[1]);
		Configuration conf = new Configuration();

    //set which cities to select on Mapper
    FileSystem fs = FileSystem.get(conf);
    FSDataInputStream inputStream = fs.open(new Path(args[2]));

    //this string will be stored in conf, so later, we are able to recover from context.
    String totalCoordinates = "";

    //append all "city,lat,long" values
    String line;
    while((line = inputStream.readLine()) != null){
      totalCoordinates = totalCoordinates + line + ";";
    }

    conf.set("city", totalCoordinates);

		Job job = Job.getInstance(conf, "Geocode");
    job.setJarByClass(GeocodeDriver.class);
    job.setMapperClass(GeocodeMapper.class);
    job.setReducerClass(GeocodeReducer.class);
		      
		job.setOutputKeyClass(Text.class);    
		job.setOutputValueClass(GeocodeWritable.class);
    
		/* Let Hadoop review the datasets */
		FileInputFormat.setInputDirRecursive(job, true);		
		FileInputFormat.addInputPath(job, new Path(args[0]));   
	 	FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
