package Lire;
//package utils.extract;
//
//import java.io.File;
//import java.util.ArrayList;
//import java.util.List;
//
//import net.semanticmetadata.lire.imageanalysis.bovw.SurfFeatureHistogramBuilder;
//
//import org.apache.lucene.analysis.SimpleAnalyzer;
//import org.apache.lucene.index.IndexReader;
//import org.apache.lucene.index.IndexWriter;
//import org.apache.lucene.store.FSDirectory;
//
//public class ExtractImageFeaturesThreaded {
//
//	public static String DATAPATH = "O:\\Work\\PhD\\Datasets\\MIRFLICKR25K\\";
//
//	private static List<String> images = new ArrayList<String>();
//
//	private static String testFilesPath = DATAPATH + "images/";
//	private static String indexPath = DATAPATH + "index/";
//
//	private static int nrThreads = 2;
//	private static int nrClusters = 4000;
//	private static int nrSamples = 1000;
//
//	private static void readImages(File dir) {
//		String pattern = ".jpg";
//		File listFile[] = dir.listFiles();
//		if (listFile != null) {
//
//			for (int i = 0; i < listFile.length; i++) {
//				// if (listFile[i].isDirectory()) {
//				if (!listFile[i].isDirectory()) {
//					if (listFile[i].getName().endsWith(pattern)) {
//						images.add(listFile[i].getName());
//					}
//				}
//			}
//		}
//	}
//
//	public static void main(String[] args) {
//		try {
//			DATAPATH = args[0];
//			testFilesPath = args[1];
//			nrThreads = Integer.parseInt(args[2]);
//			nrClusters = Integer.parseInt(args[3]);
//			nrSamples = Integer.parseInt(args[4]);
//
//			indexPath = DATAPATH + "index/";
//
//			System.out.println("start");
//
//			readImages(new File(testFilesPath));
//			System.out.println("image paths read");
//
//			List<List<String>> splits = new ArrayList<List<String>>(nrThreads);
//			for (int t = 0; t < nrThreads; t++) {
//				splits.add(new ArrayList<String>());
//			}
//			int batchSize = (int) Math.floor((double) images.size()
//					/ (double) nrThreads) + 1;
//			int remaining = images.size();
//			for (int t = 0; t < nrThreads; t++) {
//				List<String> currentQueries = splits.get(t);
//				for (int i = 0; i < Math.min(batchSize, remaining); i++) {
//					currentQueries.add(images.get(t * batchSize + i));
//				}
//				remaining -= batchSize;
//			}
//			System.out.println("separated");
//
//			IndexWriter iw = new IndexWriter(FSDirectory.open(new File(
//					indexPath)), new SimpleAnalyzer(), true,
//					IndexWriter.MaxFieldLength.UNLIMITED);
//			System.out.println("index created");
//
//			Thread[] threads = new ThreadedExtractImageFeatures[nrThreads];
//			for (int i = 0; i < nrThreads; i++) {
//				threads[i] = new ThreadedExtractImageFeatures(i + 1,
//						splits.get(i), iw, testFilesPath, DATAPATH
//								+ "mirflickr08_meta/meta/");
//				threads[i].start();
//			}
//
//			for (int i = 0; i < nrThreads; i++) {
//				try {
//					threads[i].join();
//				} catch (InterruptedException ignore) {
//				}
//			}
//
//			iw.optimize();
//			iw.close();
//
//			System.out
//					.println("indexing done -- create bovw codebook and clusters");
//
//			try {
//				// create the visual words.
//				IndexReader ir = IndexReader.open(
//						FSDirectory.open(new File(indexPath)), true);
//				// create a BoVW indexer
//				SurfFeatureHistogramBuilder sh = new SurfFeatureHistogramBuilder(
//						ir, nrSamples, nrClusters);
//				// progress monitoring is optional and opens a window showing
//				// you the progress.
//				// sh.setProgressMonitor(new ProgressMonitor(null, "", "", 0,
//				// 100));
//				sh.index();
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//
//			System.out.println("done");
//
//			/*
//			 * IndexReader reader = IndexReader.open(FSDirectory.open(new
//			 * File(indexPath)));
//			 * 
//			 * for (int i = 0; i < reader.numDocs(); i++) { Document d =
//			 * reader.document(i);
//			 * System.out.println(d.getField(DocumentBuilder.
//			 * FIELD_NAME_IDENTIFIER).stringValue() + ": ");
//			 * 
//			 * Field f = d.getField(DocumentBuilder.FIELD_NAME_COLORLAYOUT); for
//			 * (byte b : f.getBinaryValue()) { System.out.print(b + " "); }
//			 * System.out.println();
//			 * 
//			 * 
//			 * }
//			 */
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//}
