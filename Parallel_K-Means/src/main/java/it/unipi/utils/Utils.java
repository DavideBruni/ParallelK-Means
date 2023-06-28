package it.unipi.utils;

import java.util.List;

public class Utils {
    public static List<Double> sum(List<Double> sum, List<Double> values) {
        if(sum.isEmpty()){
            return values;
        }else{
            for (Double dir_sum:sum) {
                int index = sum.indexOf(dir_sum);
                dir_sum += values.get(index);
                sum.set(index,dir_sum);
            }
            return sum;
        }
    }
}
