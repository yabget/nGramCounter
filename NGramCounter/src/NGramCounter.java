/**
 * Created by ydubale on 2/22/15.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class NGramCounter {

    /**
     * Makes filename the key to the Map Function, value is the line
     */
    public static class FileNameKeyRecordReader extends KeyValueLineRecordReader {

        private final LineRecordReader lineRecordReader = new LineRecordReader();

        private Text key = new Text();
        private Text value = new Text();

        private String fileName = new String();

        public FileNameKeyRecordReader(Configuration conf, FileSplit split) throws IOException {
            super(conf);
            fileName = split.getPath().getName();
        }

        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
            this.lineRecordReader.initialize(genericSplit, context);
        }

        public synchronized boolean nextKeyValue() throws IOException {
            if(this.lineRecordReader.nextKeyValue()) {
                this.key = new Text(fileName); //Set the key to the fileName
                this.value = this.lineRecordReader.getCurrentValue(); //Value is the line of file
                return true;
            }
            return false;
        }

        public Text getCurrentKey(){
            return this.key;
        }

        public Text getCurrentValue(){
            return this.value;
        }
    }

    /**
     * Creates and returns a FileNameKeyRecordReader, Job is not splitable
     */
    public static class FileNameKeyInputFormat extends FileInputFormat<Text, Text> {

        public FileNameKeyInputFormat(){
            super();
        }

        @Override
        protected boolean isSplitable(JobContext context, Path filename){
            return false;
        }

        @Override
        public RecordReader<Text, Text> createRecordReader(
                InputSplit inputSplit, TaskAttemptContext taskAttemptContext
        ) throws IOException, InterruptedException {

            taskAttemptContext.setStatus(inputSplit.toString());
            return new FileNameKeyRecordReader(taskAttemptContext.getConfiguration(), (FileSplit) inputSplit);
        }
    }


}
