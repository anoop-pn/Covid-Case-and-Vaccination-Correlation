// package edu.ucr.cs.cs226.aputt003;
// import java.awt.Color;
// import java.awt.GradientPaint;
// import java.awt.Paint;
// import java.util.ArrayList;
// import java.util.HashMap;
// import java.util.Map.Entry;

// import org.jfree.chart.ChartFactory;
// import org.jfree.chart.ChartPanel;
// import org.jfree.chart.JFreeChart;
// import org.jfree.chart.LegendItem;
// import org.jfree.chart.LegendItemCollection;
// import org.jfree.chart.axis.SubCategoryAxis;
// import org.jfree.chart.plot.CategoryPlot;
// import org.jfree.chart.plot.PlotOrientation;
// import org.jfree.chart.renderer.category.GroupedStackedBarRenderer;
// import org.jfree.data.KeyToGroupMap;
// import org.jfree.data.category.CategoryDataset;
// import org.jfree.data.category.DefaultCategoryDataset;
// import org.jfree.chart.ui.ApplicationFrame;
// import org.jfree.chart.ui.GradientPaintTransformType;
// import org.jfree.chart.ui.StandardGradientPaintTransformer;

// @SuppressWarnings("serial")
// public class MultiStackedBarChart extends ApplicationFrame {
// public MultiStackedBarChart(String title, HashMap<String, HashMap<String,
// HashMap<String, String>>> map) {
// super("Covid & Vaccine MultiStackChart");

// ArrayList<String> groupMapUtil = new ArrayList<>();

// final CategoryDataset dataset = createDataset(title, map, groupMapUtil);
// final JFreeChart chart = createChart(dataset, groupMapUtil);

// final ChartPanel chartPanel = new ChartPanel(chart);
// chartPanel.setPreferredSize(new java.awt.Dimension(590, 350));
// setContentPane(chartPanel);
// }

// private CategoryDataset createDataset(String title, HashMap<String,
// HashMap<String, HashMap<String, String>>> map, ArrayList<String>
// groupMapUtil) {

// DefaultCategoryDataset result = new DefaultCategoryDataset();
// HashMap<String, HashMap<String, String>> typeMap = new HashMap<>();

// for(String type : map.keySet()) {
// typeMap = map.get(type);
// for(Entry<String, HashMap<String, String>> eachMonth: typeMap.entrySet()) {
// groupMapUtil.clear();
// String month = eachMonth.getKey();
// HashMap<String, String> dataInBar = eachMonth.getValue();

// for(Entry<String, String> eachBarVal: dataInBar.entrySet()) {
// String semiBar = eachBarVal.getKey();
// double value = Double.parseDouble(eachBarVal.getValue());

// result.addValue(value, type + " ("+semiBar+")", month);
// groupMapUtil.add(semiBar);
// }
// }
// }
// return result;
// }

// private JFreeChart createChart(final CategoryDataset dataset,
// ArrayList<String> groupMapUtil) {

// final JFreeChart chart = ChartFactory.createStackedBarChart(
// "Race Ethnicity Covid-Vaccine Data", // chart title
// "Category", // domain axis label
// "Count ->", // range axis label
// dataset, // data
// PlotOrientation.VERTICAL, // the plot orientation
// true, // legend
// true, // tooltips
// false // urls
// );

// GroupedStackedBarRenderer renderer = new GroupedStackedBarRenderer();
// KeyToGroupMap map = new KeyToGroupMap("G1");

// for(int i=1;i<3;i++) {
// String type = "Covid";
// if(i==2) {
// type = "Vaccine";
// }
// for(String semiBar: groupMapUtil) {
// map.mapKeyToGroup(type+" ("+semiBar+")", "G"+i);
// }
// }

// renderer.setSeriesToGroupMap(map);

// renderer.setItemMargin(0.0);

// Color[] color = new Color[8];
// color[0] = new Color(0x22, 0x22, 0xFF);
// color[1] = new Color(0x22, 0xFF, 0x22);
// color[2] = new Color(0xFF, 0x22, 0x22);
// color[3] = new Color(0xFF, 0xFF, 0x22);

// Paint p1 = new GradientPaint(
// 0.0f, 0.0f, color[0], 0.0f, 0.0f, new Color(0x88, 0x88, 0xFF)
// );
// renderer.setSeriesPaint(0, p1);
// renderer.setSeriesPaint(4, p1);
// renderer.setSeriesPaint(8, p1);

// Paint p2 = new GradientPaint(
// 0.0f, 0.0f, color[1], 0.0f, 0.0f, new Color(0x88, 0xFF, 0x88)
// );
// renderer.setSeriesPaint(1, p2);
// renderer.setSeriesPaint(5, p2);
// renderer.setSeriesPaint(9, p2);

// Paint p3 = new GradientPaint(
// 0.0f, 0.0f, color[2], 0.0f, 0.0f, new Color(0xFF, 0x88, 0x88)
// );
// renderer.setSeriesPaint(2, p3);
// renderer.setSeriesPaint(6, p3);
// renderer.setSeriesPaint(10, p3);

// Paint p4 = new GradientPaint(
// 0.0f, 0.0f, color[3], 0.0f, 0.0f, new Color(0xFF, 0xFF, 0x88)
// );
// renderer.setSeriesPaint(3, p4);
// renderer.setSeriesPaint(7, p4);
// renderer.setSeriesPaint(11, p4);

// renderer.setGradientPaintTransformer(
// new StandardGradientPaintTransformer(GradientPaintTransformType.HORIZONTAL)
// );

// SubCategoryAxis domainAxis = new SubCategoryAxis("Time Frame ->");
// domainAxis.setCategoryMargin(0.08);
// domainAxis.addSubCategory("Covid");
// domainAxis.addSubCategory("Vaccine");

// CategoryPlot plot = (CategoryPlot) chart.getPlot();
// plot.setDomainAxis(domainAxis);
// plot.setRenderer(renderer);
// plot.setFixedLegendItems(createLegendItems(groupMapUtil, color));
// return chart;

// }

// private LegendItemCollection createLegendItems(ArrayList<String>
// groupMapUtil, Color[] color) {
// LegendItemCollection result = new LegendItemCollection();
// int i=0;
// for(String semiBar: groupMapUtil) {
// if(i<5) {
// LegendItem item = new LegendItem(semiBar, color[i]);
// result.add(item);
// }
// i++;
// }

// return result;
// }

// public static void main(final String[] args) {

// String title = "Race Data:Covid & Vaccine";

// HashMap<String, HashMap<String, HashMap<String, String>>> map = new
// HashMap<>();
// HashMap<String, HashMap<String, String>> month = new HashMap<>();

// HashMap<String, String> semiBarValuePair = new HashMap<>();

// semiBarValuePair.put("18-24", "562");
// semiBarValuePair.put("25-40", "262");
// semiBarValuePair.put("41-60", "662");
// semiBarValuePair.put("61-80", "962");

// HashMap<String, String> semiBarValuePair2 = new HashMap<>();

// semiBarValuePair2.put("18-24", "562");
// semiBarValuePair2.put("25-40", "262");
// semiBarValuePair2.put("41-60", "662");
// semiBarValuePair2.put("61-80", "962");

// month.put("Jan 2020", semiBarValuePair);
// month.put("Mar 2020", semiBarValuePair2);

// map.put("Covid", month);

// HashMap<String, HashMap<String, String>> month2 = new HashMap<>();

// HashMap<String, String> semiBarValuePair3 = new HashMap<>();

// semiBarValuePair3.put("18-24", "262");
// semiBarValuePair3.put("25-40", "562");
// semiBarValuePair3.put("41-60", "362");
// semiBarValuePair3.put("61-80", "762");

// HashMap<String, String> semiBarValuePair4 = new HashMap<>();

// semiBarValuePair4.put("18-24", "062");
// semiBarValuePair4.put("25-40", "462");
// semiBarValuePair4.put("41-60", "962");
// semiBarValuePair4.put("61-80", "862");

// month2.put("Jan 2020", semiBarValuePair3);
// month2.put("Mar 2020", semiBarValuePair4);

// map.put("Vaccine", month2);

// final MultiStackedBarChart demo = new MultiStackedBarChart(title, map);
// demo.pack();
// demo.setVisible(true);
// }

// }