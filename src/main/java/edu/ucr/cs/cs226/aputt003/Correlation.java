package edu.ucr.cs.cs226.aputt003;

import org.apache.spark.sql.*;
import java.util.*;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.correlation.SpearmansCorrelation;

public class Correlation {
    public void computeCorrelation(Dataset<Row> caseVaccinationDF) {

        List<Row> monthlyCaseCountList = caseVaccinationDF.select("count").collectAsList();
        List<Row> cumulativeVaccineCountList = caseVaccinationDF.select("cumulative_fully_vaccinated").collectAsList();

        List<Double> monthlyCaseCountListDouble = new ArrayList<>();
        for (Row r : monthlyCaseCountList) {
            monthlyCaseCountListDouble.add((double) r.getLong(0));
        }
        System.out.println("monthlyCaseCountList size: " + monthlyCaseCountList.size());
        System.out.println("monthlyCaseCountListDouble size: " + monthlyCaseCountListDouble.size());

        List<Double> cumulativeVaccineCountListDouble = new ArrayList<>();
        for (Row r : cumulativeVaccineCountList) {
            cumulativeVaccineCountListDouble.add(Double.parseDouble(r.getString(0)));
        }
        System.out.println("cumulativeVaccineCountList size: " + cumulativeVaccineCountList.size());
        System.out.println("cumulativeVaccineCountListDouble size: " + cumulativeVaccineCountListDouble.size());

        double[] monthlyCaseCountListDoubleArray = monthlyCaseCountListDouble.stream().mapToDouble(Double::doubleValue)
                .toArray();
        double[] cumulativeVaccineCountListDoubleArray = cumulativeVaccineCountListDouble.stream()
                .mapToDouble(Double::doubleValue).toArray();

        System.out.println("ld1 size: " + monthlyCaseCountListDoubleArray.length);
        System.out.println("ld2 size: " + cumulativeVaccineCountListDoubleArray.length);

        // computing cumulative monthly case count.
        for (int i = 1; i < monthlyCaseCountListDoubleArray.length; i++) {
            monthlyCaseCountListDoubleArray[i] += monthlyCaseCountListDoubleArray[i - 1];
        }

        Double pearson = new PearsonsCorrelation().correlation(monthlyCaseCountListDoubleArray,
                cumulativeVaccineCountListDoubleArray);
        System.out.println("Correlation Pearson: " + pearson);

        Double spearman = new SpearmansCorrelation().correlation(monthlyCaseCountListDoubleArray,
                cumulativeVaccineCountListDoubleArray);
        System.out.println("Correlation Spearman: " + spearman);
    }

}
