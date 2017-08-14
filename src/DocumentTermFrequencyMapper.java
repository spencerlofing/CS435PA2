import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DocumentTermFrequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		String[] lineParse = line.split("\t");
		String word = lineParse[0];
		String author = lineParse[1];
		Integer count = Integer.parseInt(lineParse[2]);
		context.write(new Text(word), new IntWritable(count));
		
	}
}
