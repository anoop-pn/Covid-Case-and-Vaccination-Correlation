package edu.ucr.cs.cs226.aputt003;

import org.apache.spark.sql.*;
import java.util.*;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.correlation.SpearmansCorrelation;

public class Correlation {
    private void computeCorrelation(Dataset<Row> caseVaccinationDF) {
        List<Row> l1 = caseVaccinationDF.select("count").collectAsList();
        List<Row> l2 = caseVaccinationDF.select("cumulative_fully_vaccinated").collectAsList();
        List<Double> ll1 = new ArrayList<>();
        for (Row r : l1) {
            ll1.add((double) r.getLong(0));
        }
        System.out.println("l1 size: " + l1.size());
        System.out.println("ll1 size: " + ll1.size());
        List<Double> ll2 = new ArrayList<>();
        for (Row r : l2) {
            ll2.add(Double.parseDouble(r.getString(0)));
        }
        System.out.println("l2 size: " + l2.size());
        System.out.println("ll2 size: " + ll2.size());

        double[] ld1 = ll1.stream().mapToDouble(Double::doubleValue).toArray();
        double[] ld2 = ll2.stream().mapToDouble(Double::doubleValue).toArray();

        System.out.println("ld1 size: " + ld1.length);
        System.out.println("ld2 size: " + ld2.length);

        for (int i = 1; i < ld1.length; i++) {
            ld1[i] += ld1[i - 1];
        }

        Double coef = new PearsonsCorrelation().correlation(ld1, ld2);
        System.out.println("Correlation Pearson: " + coef);

        Double spearman = new SpearmansCorrelation().correlation(ld1, ld2);
        System.out.println("Correlation Spearman: " + spearman);
    }

}
