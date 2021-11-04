import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CovidCaseNumberMapper extends  Mapper<LongWritable, Text, Text, Text> {

    Text textValue = new Text();
    
	@Override
	 public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
	
		String line = value.toString();
		
		String[] tokens = line.split(",", -1);
		
		String location = tokens[1];
		
		if(tokens[3].isEmpty()){
			tokens[3] = "0";
		}
		if(tokens[4].isEmpty()){
			tokens[4] = "0";
		}
		
		if(!tokens[0].contains("continent")){
	        int new_case = Integer.parseInt(tokens[3]);
	        int new_death = Integer.parseInt(tokens[4]);
	        
	        String new_case_death = tokens[3] + "," + tokens[4];
	        
	        
			String continent = tokens[0];
		
			if (!continent.isEmpty()) {
				textValue.set(new_case_death);
				context.write(new Text(location), textValue);
		
			}
		}
	
	}
}
