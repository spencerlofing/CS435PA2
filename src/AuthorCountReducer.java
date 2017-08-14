import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AuthorCountReducer extends Reducer<Text, Text, Text, IntWritable> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		LinkedList<String> authors = new LinkedList<String>();
		for(Text val : values)
		{
			if(!authors.contains(val.toString()))
			{
				authors.add(val.toString());
			}
		}
		context.write(new Text("Author Count: " + authors), new IntWritable(authors.size()));
	}
}
