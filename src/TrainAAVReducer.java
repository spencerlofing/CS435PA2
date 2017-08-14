import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TrainAAVReducer extends Reducer<Text, Text, Text, DoubleWritable>{
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		double IDF = 1;
		double authorCount = 0; 
		//boolean existsInSampleAuthor = false;
		ArrayList<String> cache = new ArrayList<String>();
		ArrayList<String> totalAuthors = new ArrayList<String>();
		ArrayList<String> theseAuthors = new ArrayList<String>();
		//loop through and find IDF
		for(Text val : values)
		{
			String line = val.toString();
			String[] parse = line.split("\t");
			//IDF input for offline
			if(parse.length == 3)
			{
				//context.write(new Text("here0"), new DoubleWritable(0));
				//String word = parse[0];
				IDF = Double.parseDouble(parse[0]);
				String authors = parse[1];
				authors = authors.replaceAll("\\]", "").replaceAll("\\[", "").replaceAll("Author Count: ", "");
				String[] authorParse = authors.split(", ");
				for(String author : authorParse)
				{
					//context.write(new Text("here1"), new DoubleWritable(1));
					totalAuthors.add(author);
				}
				authorCount = Double.parseDouble(parse[2]);
			}
			//AugTF for offline
			else if(parse.length == 2)
			{
				//context.write(new Text("here2"), new DoubleWritable(2));
				String author = parse[0];
				String AugTF = parse[1];
				cache.add(line);
				theseAuthors.add(author);
			}
		}
		for(String line : cache)
		{
			//context.write(new Text("here3"), new DoubleWritable(3));
			String[] parse = line.split("\t");
			double TF;
			String newKey;
			//offline
			String author = parse[0];
			TF = Double.parseDouble(parse[1]);
			newKey = author + "\t" + key.toString();
			double TFIDF = TF * IDF; 
			context.write(new Text(newKey), new DoubleWritable(TFIDF));
		}
		for(String author : totalAuthors)
		{
			//context.write(new Text("here4"), new DoubleWritable(4));
			if(!theseAuthors.contains(author))
			{
				double TF = 0.5;
				double TFIDF = TF * IDF;
				context.write(new Text(author + "\t" + key.toString()), new DoubleWritable(TFIDF));
			}
		}
	}
}
