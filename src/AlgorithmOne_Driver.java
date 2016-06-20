import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Michael Oertel and Aldo D'Eramo on 03/06/16.
 */
public class AlgorithmOne_Driver extends Configured implements Tool {
	
	static int printUsage() {
		System.out.println("AlgorithmOne_Driver [-m <maps>] [-r <reduces>] <input> <temp> <output> <stripes/pairs>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Method Name: main Return type: none Purpose:Read the arguments from
	 * command line and run the Job till completion
	 */
	public static void main(String[] args) throws Exception {

		long startTime = System.currentTimeMillis();

		ToolRunner.run(new Configuration(), new AlgorithmOne_Driver(), args);

		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("Elapsed time: " + totalTime / 1000 + " seconds");
	}

	public static void deleteFolder(File folder) {
		File[] files = folder.listFiles();
		if (files != null) { // some JVMs return null for empty dirs
			for (File f : files) {
				if (f.isDirectory()) {
					deleteFolder(f);
				} else {
					f.delete();
				}
			}
		}
		folder.delete();
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		List<String> otherArgs = new ArrayList<String>();

		/* Read arguments */
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					conf.setInt("mapreduce.job.maps", Integer.parseInt(args[++i]));
				} else if ("-r".equals(args[i])) {
					conf.setInt("mapreduce.job.reduces", Integer.parseInt(args[++i]));
				} else {
					otherArgs.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				System.exit(printUsage());
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " + args[i - 1]);
				System.exit(printUsage());
			}
		}
		// Make sure there are exactly 4 parameters left.
		if (otherArgs.size() != 4) {
			System.out.println("ERROR: Wrong number of parameters: " + otherArgs.size() + " instead of 4.");
			System.exit(printUsage());
		}

		File tmp = new File(otherArgs.get(1));
		File out = new File(otherArgs.get(2));

		if (tmp.exists() && tmp.isDirectory()) {
			deleteFolder(tmp);
		} else { System.err.println("WARN: temp folder \""+tmp+"\"doesn't exists");}
		if (out.exists() && out.isDirectory()) {
			deleteFolder(out);
		} else { System.err.println("WARN: output folder \""+out+"\"doesn't exists");}


/*
 * Round 1
 */
		FileSystem fs = FileSystem.get(conf);
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(AlgorithmOne_Driver.class);

	/*
	 * Approccio Stripes
	 */
		if (otherArgs.get(3).equals("stripes")) {
			job1.setMapperClass(Mapper_1_stripes.class);
			job1.setCombinerClass(Combiner_1.class);
			job1.setReducerClass(Reducer_1_stripes.class);
	
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(MapWritable.class);
	
			job1.setOutputKeyClass(WordPair.class);
			job1.setOutputValueClass(IntWritable.class);
		}
		
	/*
	 * Approccio Pairs
	 */
		if (otherArgs.get(3).equals("pairs")) {
			job1.setMapperClass(Mapper_1_pairs.class);
			job1.setCombinerClass(Reducer_1_pairs.class);
			job1.setReducerClass(Reducer_1_pairs.class);
			
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(IntWritable.class);
	
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
		}

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(job1, new Path(otherArgs.get(0)));
		TextOutputFormat.setOutputPath(job1, new Path(otherArgs.get(1)));

		job1.waitForCompletion(true);

/*
 * Round 2
 */
		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(AlgorithmOne_Driver.class);

		job2.setMapperClass(Mapper_2.class);
		job2.setReducerClass(Reducer_2.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(job2, new Path(otherArgs.get(1)));
		TextOutputFormat.setOutputPath(job2, new Path(otherArgs.get(2)));

		return job2.waitForCompletion(true) ? 0 : 1;
	}
}
