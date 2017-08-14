import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AuthorCountMapper extends Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		//parse the line by the defined separator
		String[] lineParse = line.split("<===>");
		//get the author from the first position in this string array
		String author = lineParse[0];
		String[] authorFullName = author.split(" ");
		String authorLastName = authorFullName[authorFullName.length - 1];
		//parse the actual text by spaces from the third position in the string array
		//will need to perform other parsing because of punctuation
		String writing = lineParse[2];
		context.write(new Text(" "), new Text(authorLastName));
	}
}
