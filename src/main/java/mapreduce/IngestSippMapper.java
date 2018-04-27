package mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.google.common.base.Strings;


public class IngestSippMapper extends Mapper<LongWritable, Text, Text, Mutation> {

	private static final SplittableRandom RANDOM = new SplittableRandom(System.currentTimeMillis());
	private String inputFileName = null;
	private String wave = null;
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Mutation>.Context context)
			throws IOException, InterruptedException {

		// Value indicates a file
		// get the file
		String line = value.toString();
		if(Strings.isNullOrEmpty(line))
			return;
		
		URI[] cacheFiles = context.getCacheFiles();
		Configuration conf = context.getConfiguration();
		List<String> attributes = null;
		for (URI uri : cacheFiles) {
			if(uri.getPath().contains(conf.get("attributes.filename"))) {
				attributes = loadAttributes(uri, context);
			}
		}
		
		if (attributes == null || attributes.size() == 0) {
			throw new RuntimeException("Invalid attributes files or not attributes could be loaded");
		}
		
		
		Mutation mutation = null;
		String[] fields = line.split(",");
		String ssuid = "";
		for(int i = 0; i < fields.length; i++) {
			if(attributes.get(i).equalsIgnoreCase("ssuid")) {
				ssuid = fields[i].trim();
				mutation = new Mutation(RANDOM.nextInt(1, 4) + "_wave" + this.wave + "_" + ssuid);
				break;
			}
		}
			
		for(int i = 0; i < fields.length; i++) {
			mutation.put("attributes", attributes.get(i), fields[i].trim());
		}
		
		context.write(new Text(conf.get("sipp.table.name")), mutation);
					
	}
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Mutation>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		this.inputFileName = fileSplit.getPath().toString();
		String[] fields = inputFileName.split("wave=");
		if (fields.length > 0)
			this.wave = fields[0];
		else
			this.wave = context.getConfiguration().get("wave");
		
	}
	
	private List<String> loadAttributes(URI attributesFile, Mapper<LongWritable, Text, Text, Mutation>.Context context) throws IOException {
		
		final List<String> attributes = new ArrayList<String>();
		final FileSystem fs = FileSystem.get(context.getConfiguration());
		Path path = new Path(attributesFile.getPath());
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
				
		String line;
		while((line = reader.readLine()) != null) {
			attributes.add(line.trim());
		}
		
		reader.close();
		
		return attributes;
	}
	
}