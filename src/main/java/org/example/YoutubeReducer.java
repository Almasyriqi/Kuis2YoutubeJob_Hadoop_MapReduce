package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;
import static java.lang.String.format;

public class YoutubeReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    private Text channel_title = new Text();
    private Text data = new Text();
    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
                       Reporter reporter) throws IOException {
        System.out.println("----------------");
        System.out.println("Ini dari reducer");
        System.out.println("Key: " + key.toString());
        System.out.println("----------------");

        int viewSum = 0, likeSum = 0, dislikeSum = 0, commentSum = 0;
        while (values.hasNext()){
            Text value = values.next();

            // pecah berdasarkan ","
            String hasil = value.toString();
            String [] split = hasil.split(",");

            int view = Integer.parseInt(split[0]);
            int like = Integer.parseInt(split[1]);
            int dislike = Integer.parseInt(split[2]);
            int comment = Integer.parseInt(split[3]);

            System.out.println("Isi dari values saat ini: " + value.toString());

            viewSum += view;
            likeSum += like;
            dislikeSum += dislike;
            commentSum += comment;
        }
        this.data.set(format("%s,%s,%s,%s", viewSum, likeSum, dislikeSum, commentSum));
        output.collect(key, this.data);
    }
}
