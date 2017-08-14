import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AugmentedTFMapper extends Mapper<LongWritable, Text, Text, WordCount> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		String[] lineParse = line.split("\t");
		String word = lineParse[0];
		String author = lineParse[1];
		Double count = Double.parseDouble(lineParse[2]);
		WordCount wc = new WordCount(word, count);
		context.write(new Text(author), wc);
	}
}
