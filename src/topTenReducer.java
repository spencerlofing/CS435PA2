import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class topTenReducer extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		PriorityQueue<authorSimilarity> heap = new PriorityQueue<authorSimilarity>();
		for(Text val : values)
		{
			String line = val.toString();
			String[] parse = line.split("\t");
			String author = parse[0];
			Double similarity = Double.parseDouble(parse[1]);
			authorSimilarity as = new authorSimilarity(author, similarity);
			heap.add(as);
		}
		int numAuthors = heap.size();
		for(int i = 0; i < 10 && i < numAuthors; i++)
		{
			authorSimilarity as = heap.remove();
			context.write(new Text(as.getAuthor()), new Text(as.getSimilarity().toString()));
		}
	}
}
