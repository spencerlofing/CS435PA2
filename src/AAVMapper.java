import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AAVMapper extends Mapper <LongWritable, Text, Text, Text>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		String[] parse = line.split("\t");
		//input from AugTF
		if(parse.length == 4)
		{
			String author = parse[0];
			String word = parse[1];
		//	String tfind = parse[2];
			String TF = parse[3];
			context.write(new Text(word), new Text(author + TF));
		}
		//input from TrainIDFOutput
		else if(parse.length == 2)
		{
			String word = parse[0];
			String IDF = parse[1];
			context.write(new Text(word), new Text(IDF));
		}
	}

}
