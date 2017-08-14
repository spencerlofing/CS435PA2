import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimSquareSumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		String[] parse = line.split("\t");
		String author;
		double square;
		if(parse.length == 2)
		{
			author = " ";
			square = Double.parseDouble(parse[1]) * Double.parseDouble(parse[1]);
		}
		else
		{
			author = parse[0];
			square = Double.parseDouble(parse[2]) * Double.parseDouble(parse[2]);
		}
		context.write(new Text(author), new DoubleWritable(square));
	}

}
