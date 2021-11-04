import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CovidCaseNumberReducer extends Reducer<Text, Text, Text, Text> {

	Text textValue = new Text();
	@Override

	public void reduce(Text key, Iterable<Text> values,  Context context)  throws IOException, InterruptedException {

		int sumNewCases = 0;
		int sumNewdeaths = 0;
		String[] cases;
		
		for (Text value : values) {
			String line = value.toString();
	        String[] field = line.split(",");
	        
//			cases = Integer.toString(value.get()).split("101",-1);
			
			sumNewCases = sumNewCases + Integer.parseInt(field[0]);
			sumNewdeaths = sumNewdeaths + Integer.parseInt(field[1]);
		}
		if (sumNewCases >= 1000000){
		
			String v = String.valueOf(sumNewCases) + "\t" + String.valueOf(sumNewdeaths);
			textValue.set(v);
			
			context.write(key, textValue);
		}

	}

}
