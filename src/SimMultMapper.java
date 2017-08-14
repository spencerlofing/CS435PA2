import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimMultMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		String[] parse = line.split("\t");
		//sample author
		if(parse.length == 2)
		{
			String word = parse[0];
			String TFIDF = parse[1];
			context.write(new Text(word), new Text(TFIDF));
			//context.write(new Text("here0"), new Text("there0"));
		}
		//training authors
		else
		{
			String author = parse[0];
			String word = parse[1];
			String TFIDF = parse[2];
			//context.write(new Text("here"), new Text("there"));
			context.write(new Text(word), new Text(author + "\t" + TFIDF));
		}
		//context write out <word, {(author), TF-IDF}>
	}

}
