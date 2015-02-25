/**
 * Created by ydubale on 2/22/15.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class NGramCounter {

    // Custom File Input Format
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

    // Custom Writables
    /**
     * Can Use Key/Value (IntWritable[])
     */
    public static class IntArrayWritable implements WritableComparable {

        private IntWritable[] values;

        public IntArrayWritable(){
            values = new IntWritable[0];
        }

        public IntArrayWritable(IntWritable[] values){
            this.values = values;
        }

        public IntWritable[] get(){
            return values;
        }

        @Override
        public int compareTo(Object o) {
            if(!(o instanceof IntArrayWritable)){
                throw new ClassCastException();
            }
            IntWritable[] newValues = (IntWritable[])o;

            int compareToVal = -1;
            for(int i = 0; i < this.values.length; ++i){
                compareToVal = newValues[i].compareTo(this.values[i]);
            }
            return compareToVal;
        }

        public String toString(){
            String outp = "";
            for(IntWritable intWritable : this.values){
                outp += intWritable + "\t";
            }
            return outp;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(this.values.length);

            for(IntWritable intWritable : values){
                intWritable.write(dataOutput);
            }
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.values = new IntWritable[dataInput.readInt()];

            for(int i = 0; i < this.values.length; ++i){
                IntWritable value = new IntWritable();
                value.readFields(dataInput);
                this.values[i] = value;
            }

        }
    }

    /**
     * Can Use Key/Value (Text, IntWritable)
     */
    public static class TextIntWritable implements WritableComparable<TextIntWritable> {

        private Text text;
        private IntWritable intWritable;

        public TextIntWritable(){
            text = new Text();
            intWritable = new IntWritable();
        }

        public TextIntWritable(Text text, IntWritable intWritable) {
            this.text = text;
            this.intWritable = intWritable;
        }

        public Text getText(){
            return text;
        }

        public IntWritable getIntWritable(){
            return intWritable;
        }

        public String toString(){
            return text + "\t" + intWritable;
        }

        @Override
        public int compareTo(TextIntWritable newTI) {
            if(newTI.getIntWritable().compareTo(this.intWritable) == 0){
                return -(newTI.getText().compareTo(this.text));
            }
            return -(newTI.getIntWritable().compareTo(this.intWritable));
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            text.write(dataOutput);
            intWritable.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            text = new Text();
            text.readFields(dataInput);

            intWritable = new IntWritable();
            intWritable.readFields(dataInput);
        }

    }

    // MapReduce methods

    public static class NGramMapper extends Mapper<Text, Text, TextIntWritable, IntArrayWritable> {

        private static ConcurrentHashMap<Text, IntWritable> fileYear = new ConcurrentHashMap<Text, IntWritable>();
        private final static IntWritable one = new IntWritable(1);
        private static IntWritable year = new IntWritable(9999);

        private Text word = new Text();
        private IntArrayWritable vals;

        private boolean startParsing = false; //Start parsing file for words

        private TextIntWritable keyVals;

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            String currLine = value.toString();
            fileYear.put(key, year);
            if(!startParsing){
                String[] metaD = currLine.split(":");
                if(metaD.length == 2){
                    if(metaD[0].equals("Release Date")){
                        //Found Release date: ****
                        try {
                            //Get year from June 25, 2008 [EBook #11]
                            String yearS = metaD[1].split(",")[1].replace(" ", "").split("\\[")[0];

                            year = new IntWritable(Integer.valueOf(yearS));
                            fileYear.put(key, year);
                        }
                        catch (ArrayIndexOutOfBoundsException outBound){
                            System.out.println("[ERROR] FILE: " + key + " Line: " + currLine);
                        }
                    }
                }
                if(currLine.contains("*** START OF")){
                    startParsing = true;
                }
                return;
            }

            for(String token : currLine.split("\\s")){
                if(token.equals(null)){
                    continue;
                }

                //Remove everything but letters and numbers
                word.set(token.replaceAll("[^a-zA-Z1-9 ]", "").toLowerCase());

                IntWritable[] countVolume = {one, one}; // Year Count Volumes

                vals = new IntArrayWritable(countVolume);

                keyVals = new TextIntWritable(word, fileYear.get(key));
                context.write(keyVals, vals);
            }
        }
    }

    public static class CombineNGramCountReducer extends Reducer<TextIntWritable, IntArrayWritable, TextIntWritable, IntArrayWritable> {

        private IntArrayWritable result;

        public void reduce(TextIntWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
            int occurrences = 0;

            for(IntArrayWritable val : values){
                IntWritable count = (val.get())[0];
                occurrences += count.get();
            }

            IntWritable numOccurrences = new IntWritable(occurrences);
            IntWritable[] out = {numOccurrences, new IntWritable(1)};

            result = new IntArrayWritable(out);

            TextIntWritable wordYear = new TextIntWritable(key.getText(), key.getIntWritable());

            context.write(wordYear, result);
        }
    }

    public static class NGramReducer extends Reducer<TextIntWritable,IntArrayWritable, TextIntWritable,IntArrayWritable> {

        private IntArrayWritable result;

        public void reduce(TextIntWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
            int occurSum = 0;
            int volSum = 0;

            for(IntArrayWritable val : values){
                IntWritable countOccur = (val.get())[0];
                IntWritable countVolu = (val.get())[1];

                occurSum += countOccur.get();
                volSum += countVolu.get();
            }

            IntWritable finalOccur = new IntWritable(occurSum);
            IntWritable finalVol = new IntWritable(volSum);

            IntWritable[] out = {finalOccur, finalVol};

            result = new IntArrayWritable(out);

            TextIntWritable wordYear = new TextIntWritable(key.getText(), key.getIntWritable());

            context.write(wordYear, result);
        }
    }

    public static void main(String[] args) throws Exception {

            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "word count");

            job.setJarByClass(NGramCounter.class);

            job.setMapperClass(NGramMapper.class);

            job.setCombinerClass(CombineNGramCountReducer.class);

            job.setReducerClass(NGramReducer.class);

            job.setMapOutputKeyClass(TextIntWritable.class);

            job.setMapOutputValueClass(IntArrayWritable.class);

            job.setOutputKeyClass(TextIntWritable.class);

            job.setOutputValueClass(IntArrayWritable.class);

            job.setInputFormatClass(FileNameKeyInputFormat.class);

            Path inputPath = new Path(args[0]);

            FileInputFormat.setInputPaths(job, inputPath);

            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
