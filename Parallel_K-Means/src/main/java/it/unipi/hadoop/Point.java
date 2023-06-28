package it.unipi.hadoop;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Point implements WritableComparable<Point> {

    private List<Double> values = new ArrayList<>();

    public Point(){}

    public Point(String str){
        String[] values_str = str.split(",");   // array di double in formato stringa
        for (String value_str: values_str) {            // popolo l'arraylist
            values.add(Double.parseDouble(value_str));
        }
    }

    public List<Double> getValues() {
        return values;
    }

    public void setValues(List<Double> values){
        this.values = values;
    }

    @Override
    public int compareTo(Point o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
