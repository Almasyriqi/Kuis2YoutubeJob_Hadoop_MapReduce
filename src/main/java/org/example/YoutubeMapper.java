package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import static java.lang.String.format;
public class YoutubeMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

    private Text channel_title = new Text();
    private Text data = new Text();
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        String line = value.toString();

        // Pecah dulu berdasarkan ',;'
        String[] split = line.split(",;");
        System.out.println("Jumlah Split: " + split.length);

        // cek apakah terdapat data view, like, dislike, dan comment
        if(split.length > 10) {
            String channel = split[3];
            channel = channel.trim();
            channel = '"' + channel + '"';
            System.out.println("Channel title : " + channel);

            // Cari data views, like, dislike, comment
            String view = split[7];
            String like = split[8];
            String dislike = split[9];
            String comment = split[10];

            // Laporkan ke kolektor
            this.channel_title.set(format("%s,",channel));
            this.data.set(format("%s,%s,%s,%s", view, like, dislike, comment));
            output.collect(this.channel_title, this.data);
        }
    }
}
