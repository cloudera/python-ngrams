import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;


public class NgramsMapper extends Mapper<LongWritable, Text, TextTriple, IntWritable> {
	
	private Logger LOG = Logger.getLogger(getClass());
	
	private int expectedTokens;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		String inputFile = ((FileSplit) context.getInputSplit()).getPath().getName();
		LOG.info("inputFile: " + inputFile);
		Pattern c = Pattern.compile("([\\d]+)gram");
		Matcher m = c.matcher(inputFile);
		m.find();
		expectedTokens = Integer.parseInt(m.group(1));
		return;
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] data = value.toString().split("\\t");
		
		if (data.length < 3) {
			return;
		}
		
		String[] ngram = data[0].split("\\s+");
		String year = data[1];
		IntWritable count = new IntWritable(Integer.parseInt(data[2]));
		
		if (ngram.length != this.expectedTokens) {
			return;
		}
		
		// build keyOut
		List<String> triple = new ArrayList<String>(3);
		triple.add(ngram[0]);
		triple.add(ngram[expectedTokens - 1]);
		Collections.sort(triple);
		triple.add(year);
		TextTriple keyOut = new TextTriple(triple);
		
		context.write(keyOut, count);
	}
}
