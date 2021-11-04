import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*This class is responsible for running map reduce job*/
public class CovidCaseNumberJobRunner {
	public int run(String[] args) throws Exception  {

		if(args.length !=2) {

			System.err.println("Usage: CovidCaseNumberDriver <input path> <outputpath>");
            // example: hadoop jar ./maxt.jar MaxTemperatureJobRunner /input /output
			System.exit(-1);

		}

Job job = new Job();
		
		job.setJarByClass(CovidCaseNumberJobRunner.class);

		job.setJobName("Max Temperature");

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		job.setMapperClass(CovidCaseNumberMapper.class);

		job.setReducerClass(CovidCaseNumberReducer.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0:1); 

		
		
		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		CovidCaseNumberJobRunner driver = new CovidCaseNumberJobRunner();
		driver.run(args);


	}

}

