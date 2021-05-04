package com.yeshaswini.womendirector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class WomenDirectorReducer extends Reducer<Text, Text, Text, Text> {
    private ArrayList<Text> listN = new ArrayList<Text>();
    private ArrayList<Text> listR= new ArrayList<Text>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        listN.clear();
        listR.clear();

        for(Text text : values){
            if(text.charAt(0) == 'N'){
                listN.add(new Text(text.toString().substring(1)));
            } else if(text.charAt(0) == 'R'){
                listR.add(new Text(text.toString().substring(1)));
            }
        }
        executeJoinLogic(context);
    }

    public void executeJoinLogic(Context context) throws IOException, InterruptedException {
        String joinType = context.getConfiguration().get("join.type");

        //INNER JOIN OPERATION
        if (joinType.equalsIgnoreCase("inner")) {
            if (!listN.isEmpty() && !listR.isEmpty()) {
                for (Text movieName : listN) {
                    for (Text womenDirector : listR) {
                        context.write(movieName, womenDirector);
                    }
                }
            }
        }
    }
}
