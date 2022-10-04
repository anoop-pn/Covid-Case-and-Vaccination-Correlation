package edu.ucr.cs.cs226.aputt003;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.correlation.SpearmansCorrelation;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.mortbay.log.Log;

import static org.apache.spark.sql.functions.*;

/**
 * Hello world!
 *
 */
public class App {

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local").set("spark.testing.memory",
                "2147480000");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("test")
                .getOrCreate();
        // spark.sparkContext().setLogLevel("ERROR");

        Correlation correlation = new Correlation();
        StructType schema1 = new StructType()
                .add("case_month", "string")
                .add("res_state", "string")
                .add("state_fips_code", "string")
                .add("res_county", "string")
                .add("county_fips_code", "string")
                .add("age_group", "string")
                .add("sex", "string")
                .add("race", "string")
                .add("ethnicity", "string")
                .add("case_positive_specimen_interval", "string")
                .add("case_onset_interval", "string")
                .add("process", "string")
                .add("exposure_yn", "string")
                .add("current_status", "string")
                .add("symptom_status", "string")
                .add("hospital_yn", "string")
                .add("icu_yn", "string")
                .add("death_yn", "string")
                .add("underlying_conditions_yn", "string");

        StructType schema2 = new StructType()
                .add("county", "string")
                .add("county_type", "string")
                .add("demographic_category", "string")
                .add("demographic_value", "string")
                .add("est_population", "string")
                .add("est_age_12plus_pop", "string")
                .add("est_age_5plus_pop", "string")
                .add("administered_date", "string")
                .add("partially_vaccinated", "string")
                .add("total_partially_vaccinated", "string")
                .add("fully_vaccinated", "string")
                .add("cumulative_fully_vaccinated", "string")
                .add("at_least_one_dose", "string")
                .add("cumulative_at_least_one_dose", "string")
                .add("cumulative_unvax_total_pop", "string")
                .add("cumulative_unvax_12plus_pop", "string")
                .add("cumulative_unvax_5plus_pop", "string")
                .add("administered_month", "string");

        Dataset<Row> df1 = spark.read()
                .option("mode", "DROPMALFORMED")
                .schema(schema1)
                .csv("COVID-19_Case_Surveillance_Public_Use_Data_with_Geography_clean_Nov29.csv");

        Dataset<Row> df2 = spark.read()
                .option("mode", "DROPMALFORMED")
                .schema(schema2)
                .csv("vaccine_data_dec9_cleaned_3.csv");

        List<String> validDates = new ArrayList<String>(Arrays.asList("2020-07-31",
                "2020-08-31",
                "2020-09-30",
                "2020-10-31",
                "2020-11-30",
                "2020-12-31",
                "2021-01-31",
                "2021-02-28",
                "2021-03-31",
                "2021-04-30",
                "2021-05-31",
                "2021-06-30",
                "2021-07-31",
                "2021-08-31",
                "2021-09-30",
                "2021-10-31",
                "2021-11-30",
                "2021-12-31"));

        // ----------------------------------SAFE CODE FOR
        // AGE--------------------------------------------------------
        HashMap<String, List<List<String>>> lineChartMapAge = new HashMap<>();
        Set<String> distinctCounties = new HashSet<>();
        List<Row> countyList = df2.select("county").distinct().toDF().collectAsList();
        for (Row r : countyList) {
            distinctCounties.add(r.getString(0));
        }

        for (String county : distinctCounties) {
            Dataset<Row> vaccineAgeDF = df2.filter("demographic_category == 'Age Group'")
                    .filter("county =='" + county + "'")
                    .select("county", "cumulative_fully_vaccinated", "administered_date", "administered_month",
                            "demographic_category", "demographic_value")
                    .filter(df2.col("administered_date").isInCollection(validDates))
                    .sort(df2.col("administered_month").desc())
                    .groupBy("county", "demographic_category", "demographic_value")
                    .df();

            System.out.println(vaccineAgeDF.getRows(20, 100));
            System.out.println("vaccineAgeDF DF1!!!!!!!!!!");

            Dataset<Row> caseAgeDF = df1.select("res_county", "case_month", "age_group")
                    .filter("res_state=='CA'")
                    .filter("res_county =='" + county + "'")
                    .groupBy("res_county", "case_month", "age_group")
                    .count()
                    .sort("case_month")
                    .toDF();

            System.out.println(caseAgeDF.getRows(20, 100));
            System.out.println("caseAgeDF  DF2 !!!!!!!!!!");

            Dataset<Row> joinedAgeDF = caseAgeDF
                    .join(vaccineAgeDF)
                    .where(caseAgeDF.col("case_month").equalTo(vaccineAgeDF.col("administered_month"))
                            .and(caseAgeDF.col("res_county").equalTo(vaccineAgeDF.col("county")))
                            .and(caseAgeDF.col("age_group").equalTo(vaccineAgeDF.col("demographic_value"))))
                    .sort(col("case_month").desc())
                    .select("res_county", "case_month", "age_group", "count", "cumulative_fully_vaccinated");

            System.out.println(joinedAgeDF.getRows(50, 100));
            System.out.println("joined AGE !!!!!!!!!!");

            List<List<String>> lineitem = new ArrayList<>();
            List<Row> lineChartRow = joinedAgeDF.toDF().collectAsList();
            Set<String> distinctAgeGroup = new HashSet<>();
            for (Row r : lineChartRow) {
                List<String> l = new ArrayList<>();
                l.add(r.getString(0));
                l.add(r.getString(1));
                l.add(r.getString(2));
                l.add(String.valueOf(r.getLong(3)));
                l.add(r.getString(4));
                distinctAgeGroup.add(r.getString(2));
                lineitem.add(l);
            }
            for (String s : distinctAgeGroup) {
                List<List<String>> mapList = new ArrayList<>();
                List<List<String>> t = lineitem.stream().filter(c -> c.get(2).equals(s)).collect(Collectors.toList());
                for (List<String> el : t) {
                    el.remove(0);
                    el.remove(1);
                    mapList.add(el);
                }
                lineChartMapAge.put(county + "_" + s, mapList);
            }
            System.out.println("lineChart HAshMAP:   " + lineChartMapAge.toString());
            for (HashMap.Entry<String, List<List<String>>> e : lineChartMapAge.entrySet()) {
                LineChart lineChart = new LineChart(e.getKey().split("_")[0], e.getKey(), e.getValue());
                System.out.println(e.getKey());
                System.out.println("Correlation_Co-Efficients age wise For: " + county);
                correlation.computeCorrelation(joinedAgeDF);
            }
            lineChartMapAge.clear();
        }

        // ---------------------------------------------------------RACE---------------------------------------
        HashMap<String, List<List<String>>> lineChartMapRace = new HashMap<>();

        for (String county : distinctCountiesDummy) {
            Dataset<Row> vaccineRaceDF = df2.filter("demographic_category == 'Race/Ethnicity'")
                    .filter("county =='" + county + "'")
                    .select("county", "cumulative_fully_vaccinated", "administered_date", "administered_month",
                            "demographic_category", "demographic_value")
                    .filter(df2.col("administered_date").isInCollection(validDates))
                    .sort(df2.col("administered_month").desc())
                    .groupBy("county", "demographic_category", "demographic_value")
                    .df();

            System.out.println(vaccineRaceDF.describe().toString());
            vaccineRaceDF.printSchema();

            System.out.println(vaccineRaceDF.getRows(20, 100));
            System.out.println(" vaccineRaceDF DF1!!!!!!!!!!");

            Dataset<Row> caseRaceDF = df1.select("res_county", "case_month", "race")
                    .filter("res_state=='CA'")
                    .filter("res_county =='" + county + "'")
                    .groupBy("res_county", "case_month", "race")
                    .count()
                    .sort("case_month")
                    .toDF();

            System.out.println(caseRaceDF.getRows(20, 100));
            System.out.println(" caseRaceDF DF2 !!!!!!!!!!");

            caseRaceDF.printSchema();

            Dataset<Row> joinedRaceDF = caseRaceDF
                    .join(vaccineRaceDF)
                    .where(caseRaceDF.col("case_month").equalTo(vaccineRaceDF.col("administered_month"))
                            .and(caseRaceDF.col("res_county").equalTo(vaccineRaceDF.col("county")))
                            .and(caseRaceDF.col("race").equalTo(vaccineRaceDF.col("demographic_value"))))
                    .sort(col("case_month").desc())
                    .select("res_county", "case_month", "race", "count", "cumulative_fully_vaccinated");

            System.out.println("joined joinedRaceDF DF--------");
            System.out.println(joinedRaceDF.getRows(50, 100));
            joinedRaceDF.describe();
            joinedRaceDF.printSchema();

            List<List<String>> lineitem = new ArrayList<>();
            List<Row> lineChartRow = joinedRaceDF.toDF().collectAsList();
            Set<String> distinctRaceGroup = new HashSet<>();
            for (Row r : lineChartRow) {
                List<String> l = new ArrayList<>();
                l.add(r.getString(0));
                l.add(r.getString(1));
                l.add(r.getString(2));
                l.add(String.valueOf(r.getLong(3)));
                l.add(r.getString(4));
                distinctRaceGroup.add(r.getString(2));
                lineitem.add(l);
            }
            for (String s : distinctRaceGroup) {
                List<List<String>> mapList = new ArrayList<>();
                List<List<String>> t = lineitem.stream().filter(c -> c.get(2).equals(s)).collect(Collectors.toList());
                for (List<String> el : t) {
                    el.remove(0);
                    el.remove(1);
                    mapList.add(el);
                }
                lineChartMapRace.put(county + "_" + s, mapList);
            }
            System.out.println("lineChart HAshMAP:   " + lineChartMapRace.toString());
            for (HashMap.Entry<String, List<List<String>>> e : lineChartMapRace.entrySet()) {
                LineChart lineChart = new LineChart(county, e.getKey(), e.getValue());
                System.out.println(e.getKey());
            }
            lineChartMapRace.clear();
            System.out.println("Correlation_Co-Efficients race wiseFor: " + county);
            correlation.computeCorrelation(joinedRaceDF);
        }
    }

}
// TODO
// Describe Chart
// spark-submit build with dependencies and fix it
// command line arguments for files
// Correlation Co-Efficient in Report
// Code Refactor
// Replace distinctCountiesDummy with distinctCounties before submitting
// ReadMe file for the project with clear instructions