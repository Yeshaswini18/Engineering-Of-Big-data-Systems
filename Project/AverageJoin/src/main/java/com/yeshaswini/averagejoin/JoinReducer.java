package com.yeshaswini.averagejoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {
    private ArrayList<Text> listT = new ArrayList<Text>();
    private ArrayList<Text> listR= new ArrayList<Text>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        listT.clear();
        listR.clear();
        double sum = 0;
        int count = 0;
        for(Text text : values){
            if(text.charAt(0) == 'T'){
                listT.add(new Text(text.toString().substring(1)));
            } else if(text.charAt(0) == 'R'){
                listR.add(new Text(text.toString().substring(1)));
            }
        }

        Text movieName = new Text();


        for (int i=0; i<listR.size(); i++ ) {
            count += 1;
            sum += Double.parseDouble(String.valueOf(listR.get(i)));
        }


        if (count != 0 && listT.size()>0) {
            double average = sum/count;
            movieName.set(listT.get(0));
            if (average >= 2){
                context.write(movieName, new Text(String.valueOf(average)));
            }
        }

        //executeJoinLogic(context);
    }

//    private void executeJoinLogic(Context context) throws IOException, InterruptedException {
//        String joinType = context.getConfiguration().get("join.type");
//
//        //INNER JOIN OPERATION
//        if (joinType.equalsIgnoreCase("inner")) {
//            if (!listT.isEmpty() && !listR.isEmpty()) {
//                for (Text movieName : listT) {
//                    for (Text Rating : listR) {
//                        context.write(movieName, Rating);
//                    }
//                }
//            }
//        }
//    }
}
