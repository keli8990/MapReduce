package join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.FileStatus;
/**
 * This is a sample program to chain the place filter job and replicated join job.
 * 
 * @author Ying Zhou
 *
 */

public class JobChainDriver {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 0) {
			System.err.println("Usage: JobChainDriver <inPlace> <inPhoto> <out>");
			System.exit(2);
		}
		
		// pass a parameter to mapper class
		
		Path tmpFilterOut = new Path("tmpFilterOut"); // a temporary output path for the first job
		
	
	
		Job placeFilterJob = new Job(conf, "Place Filter");
		placeFilterJob.setJarByClass(JobChainDriver.class);///////////
		placeFilterJob.setNumReduceTasks(0);
		placeFilterJob.setMapperClass(PlaceFilterMapper.class);
		placeFilterJob.setOutputKeyClass(Text.class);
		placeFilterJob.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(placeFilterJob, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(placeFilterJob, tmpFilterOut);
		placeFilterJob.waitForCompletion(true);

		Job joinJob = new Job(conf, "Replication Join");
		DistributedCache.addCacheFile(new Path("tmpFilterOut/part-m-00000").toUri(),joinJob.getConfiguration());
		joinJob.setJarByClass(JobChainDriver.class);///////////////////////////
		joinJob.setNumReduceTasks(3);
		joinJob.setMapperClass(ReplicateJoinMapper.class);
		joinJob.setReducerClass(JoinAddReducer.class);

		joinJob.setOutputKeyClass(Text.class);
		joinJob.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(joinJob, new Path(otherArgs[1]));
		TextOutputFormat.setOutputPath(joinJob,new Path(otherArgs[2]));
		joinJob.setPartitionerClass(WordPartitioner.class);
		joinJob.waitForCompletion(true);
		// remove the temporary path
		//FileSystem.get(conf).delete(tmpFilterOut, true);



		Path tmpSortedLocalityOut = new Path(otherArgs[2]); // a temporary output path for the first job
		
		//Path tmpFinalOut = new Path(otherArgs[5]);

		//Path tmpFinalOut = new Path("tmpFinalOut");
		Job sortPhotoJob = new Job(conf, "Sort by Photo number");
		//FileSystem fileSystem = FileSystem.get(conf);
       		 //Path pathPattern = new Path(tmpSortedLocalityOut,"part-r-[0-9]*");
       		 //FileStatus[] list = fileSystem.globStatus(pathPattern);
       		 //for (FileStatus status : list) {
           	 //DistributedCache.addCacheFile(status.getPath().toUri(), sortPhotoJob.getConfiguration());
       		// }
		sortPhotoJob.setJarByClass(JobChainDriver.class);
		sortPhotoJob.setNumReduceTasks(3);
		sortPhotoJob.setMapperClass(PhotoSortMapper.class);
		sortPhotoJob.setMapOutputKeyClass(IntWritable.class);
		sortPhotoJob.setMapOutputValueClass(Text.class);
		sortPhotoJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
		sortPhotoJob.setReducerClass(PhotoSortReducer.class);
		sortPhotoJob.setOutputKeyClass(Text.class);
		sortPhotoJob.setOutputValueClass(IntWritable.class);
		TextInputFormat.addInputPath(sortPhotoJob, tmpSortedLocalityOut);
		TextOutputFormat.setOutputPath(sortPhotoJob,new Path(otherArgs[3]) );
		//TextOutputFormat.setOutputPath(sortPhotoJob, otherArgs[3]);
		sortPhotoJob.setPartitionerClass(SortPartitioner.class);
		sortPhotoJob.waitForCompletion(true);

		/*Job topJob = new Job(conf, "Number Filter");
		topJob.setJarByClass(JobChainDriver.class);
		topJob.setMapperClass(FinalPhotoMapper.class);
		topJob.setNumReduceTasks(0);
		topJob.setOutputKeyClass(Text.class);
		topJob.setOutputValueClass(IntWritable.class);
		TextInputFormat.addInputPath(topJob, tmpFinalOut);
		TextOutputFormat.setOutputPath(topJob, new Path(otherArgs[3]));
		topJob.waitForCompletion(true);*/
	
		/*Tag*/
		//String tmp_place_path = otherArgs[2];
		//Path tmpFinalOut = new Path("tmpFinalOut");

		Job placeIdJob = new Job(conf, "Place ID Filter");
		DistributedCache.addCacheFile(new Path("tmpFilterOut/part-m-00000").toUri(),placeIdJob.getConfiguration());	
		DistributedCache.addCacheFile(new Path("2-photo-assign/part-r-00002").toUri(),placeIdJob.getConfiguration());
		

		placeIdJob.setJarByClass(JobChainDriver.class);
		placeIdJob.setNumReduceTasks(1);
		placeIdJob.setMapperClass(PlaceIdMapper.class);
		
		//placeIdJob.setSortComparatorClass(IntWritableDecreasingComparator.class);		

		//placeIdJob.setMapOutputKeyClass(Text.class);
		//placeIdJob.setMapOutputValueClass(Text.class);
		//placeIdJob.setSortComparatorClass(FreDecreasingComparator.class);		

		placeIdJob.setReducerClass(TagReducer.class);
		
		placeIdJob.setOutputKeyClass(Text.class);
		placeIdJob.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(placeIdJob, new Path(otherArgs[1]));
		TextOutputFormat.setOutputPath(placeIdJob, new Path(otherArgs[4]));
		placeIdJob.waitForCompletion(true);	
		
		Job topJob = new Job(conf, "Place sort");
		//Path tmpFinalOut = new Path(otherArgs[4]); 
		topJob.setJarByClass(JobChainDriver.class);
		topJob.setNumReduceTasks(1);
		topJob.setMapperClass(FinalPhotoMapper.class);
		topJob.setMapOutputKeyClass(IntWritable.class);
		topJob.setMapOutputValueClass(Text.class);
		topJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
		
		topJob.setReducerClass(OrderReducer.class);
		
		topJob.setOutputKeyClass(Text.class);
		topJob.setOutputValueClass(IntWritable.class);
		TextInputFormat.addInputPath(topJob,new Path("3-photo-assign") );
		TextOutputFormat.setOutputPath(topJob, new Path(otherArgs[5]));
		topJob.waitForCompletion(true);
		

		
		
	}
}
