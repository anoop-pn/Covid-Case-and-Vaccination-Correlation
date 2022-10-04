package edu.ucr.cs.cs226.aputt003;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.List;

import javax.swing.JFrame;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.block.BlockBorder;
import org.jfree.chart.labels.StandardXYToolTipGenerator;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.StandardXYItemRenderer;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.time.Month;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;

@SuppressWarnings("serial")
public class LineChart extends JFrame {

    public LineChart(String county, String name, List<List<String>> list) {
        initUI(county, name, list);
    }

    private void initUI(String county, String name, List<List<String>> list) {
        TimeSeriesCollection[] dataset = createDataset(list);
        JFreeChart chart = createChart(county, name, dataset);
        // Saving the output graph as file
        try {
            if (!Files.exists(Paths.get("output/"))) {
                Files.createDirectory(Paths.get("output/"));
            }
            if (!Files.exists(Paths.get("output/" + county))) {
                Files.createDirectory(Paths.get("output/" + county + ""));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                Files.deleteIfExists(Paths.get("output/" + county + "/" + name + ".jpg"));
                System.out.println("output/" + county + "");
                System.out.println("output/" + county + "/" + name + ".jpg");
                ChartUtils.saveChartAsJPEG(new File("output/" + county + "/" + name + ".jpg"), chart, 1200, 700);
            } catch (IOException e) {
                // e.printStackTrace();
            }
        }
    }

    private TimeSeriesCollection[] createDataset(List<List<String>> list) {

        TimeSeries series1 = new TimeSeries("Covid");
        TimeSeries series2 = new TimeSeries("Vaccine");

        for (List<String> eachEntry : list) {
            int month = Integer.parseInt((eachEntry.get(0)).split("-")[1]);
            int year = Integer.parseInt((eachEntry.get(0)).split("-")[0]);
            series1.addOrUpdate(new Month(month, year), Double.parseDouble(eachEntry.get(1)));
            series2.addOrUpdate(new Month(month, year), Double.parseDouble(eachEntry.get(2)));
        }

        TimeSeriesCollection[] dataset = new TimeSeriesCollection[2];
        dataset[0] = new TimeSeriesCollection();
        dataset[0].addSeries(series1);
        dataset[1] = new TimeSeriesCollection();
        dataset[1].addSeries(series2);

        return dataset;
    }

    private JFreeChart createChart(String county, String name, TimeSeriesCollection[] dataset) {

        JFreeChart chart = ChartFactory.createTimeSeriesChart(
                "Correlation between Covid cases and Vaccination",
                "Time frame",
                "Covid cases",
                dataset[0],
                true,
                true,
                false);

        // seconday - y axis at right
        final XYPlot plot = chart.getXYPlot();
        final NumberAxis axis2 = new NumberAxis("Vaccine count");
        axis2.setAutoRangeIncludesZero(false);
        plot.setRangeAxis(1, axis2);
        plot.setDataset(1, dataset[1]);
        plot.mapDatasetToRangeAxis(1, 1);

        plot.setBackgroundPaint(Color.white);
        plot.setRangeGridlinesVisible(true);
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setDomainGridlinesVisible(true);
        plot.setDomainGridlinePaint(Color.BLACK);

        final XYItemRenderer renderer = plot.getRenderer();
        renderer.setDefaultToolTipGenerator(StandardXYToolTipGenerator.getTimeSeriesInstance());
        if (renderer instanceof StandardXYItemRenderer) {
            final StandardXYItemRenderer rr = (StandardXYItemRenderer) renderer;
            rr.setPlotImages(true);
            rr.setBaseShapesFilled(true);
        }

        final StandardXYItemRenderer renderer2 = new StandardXYItemRenderer();
        renderer2.setPlotImages(true);
        renderer2.setSeriesPaint(0, Color.BLUE);
        renderer2.setSeriesStroke(0, new BasicStroke(2.0f));
        renderer2.setDefaultToolTipGenerator(StandardXYToolTipGenerator.getTimeSeriesInstance());
        plot.setRenderer(1, renderer2);

        final DateAxis axis = (DateAxis) plot.getDomainAxis();
        axis.setDateFormatOverride(new SimpleDateFormat("MMM-yyyy"));

        chart.getLegend().setFrame(BlockBorder.NONE);

        chart.setTitle(new TextTitle("Correlation between Covid cases vs Vaccination for " + county,
                new Font("Serif", java.awt.Font.BOLD, 18)));

        return chart;
    }
}