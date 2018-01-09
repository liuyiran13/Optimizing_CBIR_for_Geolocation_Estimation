package MyAPI.General;

import java.awt.Color;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartFrame;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.RectangleInsets;

public class General_Chart {
	
	public static void drawLineChart(String chartTitle, String X_label, String Y_label, String[] titles, float[][] lines, float[] x){
		if (x==null) {//default use point-ind in lines as x, so each line should have same length
			x=General.makeRange(new float[]{0,lines[0].length-1,1});//0~max
		}
		drawLineChart(chartTitle, X_label, Y_label, titles, make_SameXValues_LineData(lines, x));
	}
	
	public static void drawLineChart(String chartTitle, String X_label, String Y_label, String[] titles, float[][][] lines){
		General.Assert(titles.length==lines.length, "err, titles.length should == lines.length == line number");
		//creat dataset
		XYSeriesCollection dataset = new XYSeriesCollection();
		for (int i = 0; i < titles.length; i++) {
			//process oneLine
			XYSeries oneLine = new XYSeries(titles[i]);
			for (float[] onePoint : lines[i]) {
				oneLine.add(onePoint[0], onePoint[1]);//add one point
			}
			dataset.addSeries(oneLine);
		}
		//creat chart
		JFreeChart chart = ChartFactory.createXYLineChart(
				chartTitle, // chart title
				X_label, // x axis label
				Y_label, // y axis label
				dataset, // data
				PlotOrientation.VERTICAL,
				true, // include legend
				true, // tooltips
				false // urls
				);
		// set the background color for the chart...
		chart.setBackgroundPaint(Color.white);
		// get a reference to the plot for further customisation...
		XYPlot plot = (XYPlot) chart.getPlot();
		plot.setBackgroundPaint(Color.lightGray);
		plot.setAxisOffset(new RectangleInsets(5.0, 5.0, 5.0, 5.0));
		plot.setDomainGridlinePaint(Color.white);
		plot.setRangeGridlinePaint(Color.white);
		// change the auto tick unit selection to integer units only...
		NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
		rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
		// create and display a frame...
		ChartFrame frame = new ChartFrame(chartTitle, chart);
		frame.pack();
		frame.setVisible(true);
	}

	public static float[][][] make_SameXValues_LineData(float[][] lines, float[] x) {
		float[][][] res=new float[lines.length][lines[0].length][2];
		for (int i = 0; i < lines.length; i++) {
			//one line
			General.Assert(lines[i].length==x.length, "err, lines[i].length should ==x.length, lines[i].length:"+lines[i].length+", x.length:"+x.length);
			for (int j = 0; j < lines[i].length; j++) {
				res[i][j]=new float[]{x[j],lines[i][j]}; //each point
			}
		}
		return res;
	}
	
	public static void main(String[] args) throws Exception {
		float[][] lines=new float[][]{General.makeRange(new float[]{0,1,(float) 0.1}), General.makeRange(new float[]{1,0,(float) -0.1})};
		float[][][] data=make_SameXValues_LineData(lines, General.makeRange(new float[]{0,1,(float) 0.1}));
		drawLineChart("example", "X", "Y", new String[]{"line1","line2"},data);
		
	}
}
