import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrainAAVMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		String[] parse = line.split("\t");
		
		if(parse.length == 3)
		{
			String author = parse[0];
			String word = parse[1];
			String TF = parse[2];
			context.write(new Text(word), new Text(author + "\t" + TF));
		}
		else
		{
			String word = parse[0];
			String IDF = parse[1];
			String authors = parse[2];
			String authorCount = parse[3];
			context.write(new Text(word), new Text(IDF + "\t" + authors + "\t" + authorCount));
		}
	}

}
