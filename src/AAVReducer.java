import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AAVReducer extends Reducer<Text, Text, Text, DoubleWritable>{
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		double IDF = 1; 
		boolean existsInSampleAuthor = false;
		ArrayList<String> cache = new ArrayList<String>();
		//loop through and find IDF
		for(Text val : values)
		{
			String line = val.toString();
			String[] parse = line.split("\t");
			//AugTF for online
			if(parse.length == 1)
			{
				IDF = Double.parseDouble(parse[0]);
			}
			//AugTF for offline
			else if(parse.length == 2)
			{
				//context.write(new Text("here2"), new DoubleWritable(2));
				String author = parse[0];
				String AugTF = parse[1];
				existsInSampleAuthor = true;
				cache.add(key.toString() + "\t" + AugTF);
			}
		}
		if(existsInSampleAuthor == false)
		{
			String line = key.toString() + "\t" + 0.5;
			cache.add(line);
		}
		for(String line : cache)
		{
			//context.write(new Text("here3"), new DoubleWritable(3));
			String[] parse = line.split("\t");
			String word = parse[0];
			double TF = Double.parseDouble(parse[1]);
			double TFIDF = TF * IDF; 
			context.write(new Text(word), new DoubleWritable(TFIDF));
		}
		
	}
}
