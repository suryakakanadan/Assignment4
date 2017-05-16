package hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class CleanUpMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\\|");		
		String company = new String(lineArray[0]);
		String product = new String(lineArray[1]);
		if(!company.equals("NA") && !product.equals("NA")){
		context.write(value,null);
		}
	}
}
