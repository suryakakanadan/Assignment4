package hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Ass4Task3Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\\|");
		
		String company = new String(lineArray[0]);
		String product = new String(lineArray[1]);
		String state = new String(lineArray[3]);
		if(!company.equals("NA") && !product.equals("NA") && company.equals("Onida")){
		context.write(new Text(state),new IntWritable(1));
		}
	}
}
