import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimSumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		String[] parse = line.split("\t");
		String author = parse[0];
		double mult = Double.parseDouble(parse[1]);
		
		context.write(new Text(author), new DoubleWritable(mult));
	}

}
