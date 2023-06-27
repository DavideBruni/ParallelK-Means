package it.unipi.hadoop;

import java.util.ArrayList;
import java.util.List;

public class Point {

    private List<Double> values = new ArrayList<>();

    public Point(String str){
        String[] values_str = str.split(",");   // array di double in formato stringa
        for (String value_str: values_str) {            // popolo l'arraylist
            values.add(Double.parseDouble(value_str));
        }
    }

    public List<Double> getValues() {
        return values;
    }
}
