import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AuthorIDFReducer extends Reducer<Text, Text, Text, Text>{
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		String authors = " ";
		ArrayList<String> lines = new ArrayList<String>();
		for(Text val : values)
		{
			String[] parse = val.toString().split("\t");
			String[] authorParse = parse[0].split("\\[");
			if(authorParse[0].equals("Author Count: "))
			{
				authors = parse[0] + "\t" + parse[1];
			}
			else
			{
				lines.add(val.toString());
			}
		}
		for(String line : lines)
		{
			context.write(new Text(line), new Text(authors));
		}
	}
}
