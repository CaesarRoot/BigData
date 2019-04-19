package Lab3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class invertedIndex
{
    private static Configuration conf2 = new Configuration();

    public static class Map extends Mapper<Object,Text,Text,Text>
    {
        private Text valueInfo = new Text();
        private Text keyInfo = new Text();
        public void map(Object key, Text value,Context context)
                throws IOException, InterruptedException
        {
            /// split the file into small piece
            FileSplit split;
            split = (FileSplit) context.getInputSplit();
            /// split the input into token
            StringTokenizer stk = new StringTokenizer(value.toString());
            /// get the filename
            String fileName = split.getPath().getName();
            int splitIndex = fileName.indexOf(".");
            if(splitIndex >= 0 && splitIndex < fileName.length())
                fileName = fileName.substring(0, splitIndex);
            while (stk.hasMoreElements())
            {
                /// the key is <word:fileName, "1">
                keyInfo.set(stk.nextToken() + ":" + fileName);
                valueInfo.set("1");
                context.write(keyInfo, valueInfo);
            }
        }
    }

    public static class Combiner extends Reducer<Text,Text,Text,Text>
    {
        /// to calc the average count
        Text info = new Text();
        public void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException
        {
            int sum = 0;
            for (Text value : values)
            {
                sum += Integer.parseInt(value.toString());
            }
            int splitIndex = key.toString().indexOf(":");
            /// the key is <word, fileName:num>
            key.set(key.toString().substring(0,splitIndex));
            info.set(key.toString().substring(splitIndex+1) + ":" + sum);
            context.write(key, info);
        }
    }

    public static class Reduce extends Reducer<Text,Text,Text,Text>
    {
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values,Context contex) throws IOException, InterruptedException
        {
            //fileList is to record the file
            String fileList = new String();
            double wordNum = 0 , fileNum = 0;
            for (Text value : values)
            {
                fileNum++;
                /// value is <fileName:num>, so it's property to add and is directly what we want
                fileList += value.toString() + ";";
                int splitIndex = value.toString().indexOf(":");
                wordNum += Integer.parseInt(value.toString().substring(splitIndex+1));
            }
            double avgCount = wordNum / fileNum;

            key.set(key.toString() + '\t' + String.format("%.2f", avgCount));
            result.set(fileList);
            contex.write(key, result);
        }
    }

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException
    {
        Configuration conf = new Configuration();//配置对象

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.err.println(otherArgs.length);
            System.err.print("Usage: Lab3.jar <in> <out>\n");
            System.exit(2);
        }

        Job job = new Job(conf,"InvertedIndex");//新建job
        job.setJarByClass(invertedIndex.class);//job类

        job.setMapperClass(Map.class);//map设置
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(Combiner.class);//combiner设置

        job.setReducerClass(Reduce.class);//reduce设置
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //FileInputFormat.addInputPath(job, new Path("/data/wuxia_novels/"));//路径设置
        //FileOutputFormat.setOutputPath(job, new Path("/user/2016st28/exp2/"));
        FileInputFormat.addInputPath(job, new Path(args[0]));//路径设置
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}