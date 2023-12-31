package it.unipi.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Point implements Writable{

    private List<Double> values = new ArrayList<>();

    public Point(){}

    /**
     * Constructor for the Point class.
     * Parses a string representation of a point and initializes the values list.
     *
     * @param str The string representation of the point.
     */
    public Point(String str){
        String[] values_str = str.split(",");       // array of doubles with a string format, with comma as separator
        for (String value_str: values_str) {            // arraylist population
            values.add(Double.parseDouble(value_str));
        }
    }


    /**
     * Retrieves the list of values representing the coordinates of the point.
     *
     * @return The list of values.
     */
    public List<Double> getValues() {
        return values;
    }


    /**
     * Sets the list of values representing the coordinates of the point.
     *
     * @param values The list of values to set.
     */
    public void setValues(List<Double> values){
        this.values = values;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        int size = values.size();
        dataOutput.writeInt(size);          // Writes the size of the list

        for (Double value : values) {
            dataOutput.writeDouble(value);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        values.clear();                     // Removes any existing values in the list
        int size = dataInput.readInt();
        for (int i = 0; i < size; i++) {
            double value = dataInput.readDouble();
            values.add(value);
        }
    }
}
