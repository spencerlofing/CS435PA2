import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SimMagMultReducer extends Reducer<Text, Text, Text, DoubleWritable>{
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		double sampleTFIDF = 0;
		ArrayList<String> cache = new ArrayList<String>();
		for(Text val : values)
		{
			//find the sample author's TFIDF
			String line = val.toString();
			String[] parse = line.split("\t");
			if(parse[0].equals(" "))
			{
				sampleTFIDF = Double.parseDouble(parse[1]);
			}
			else
			{
				cache.add(line);
				//context.write(new Text("here1"), new DoubleWritable(1));
			}
		}
		for(String line : cache)
		{
			String[] parse = line.split("\t");
			String author = parse[0];
			double TFIDF = Double.parseDouble(parse[1]);
			double multiplier = sampleTFIDF * TFIDF;
			context.write(new Text(author + "\t" + "denominator"), new DoubleWritable(multiplier));
		}
		//context.write(new Text("here2"), new DoubleWritable(2));
	}

}
