package Lire;
//package utils.extract;
//
//import java.io.FileInputStream;
//import java.util.List;
//
//import net.semanticmetadata.lire.DocumentBuilderFactory;
//import net.semanticmetadata.lire.impl.ChainedDocumentBuilder;
//import net.semanticmetadata.lire.impl.SurfDocumentBuilder;
//
//import org.apache.lucene.document.Document;
//import org.apache.lucene.index.IndexWriter;
//
//public class ThreadedExtractImageFeatures extends Thread {
//
//	private int thread;
//	private List<String> images = null;
//	private IndexWriter iw = null;
//	private String testFilesPath = null;
//	private String metaDir = null;
//
//	public ThreadedExtractImageFeatures(int thread, List<String> images,
//			IndexWriter iw, String testFilesPath, String metaDir) {
//		this.thread = thread;
//		this.images = images;
//		this.iw = iw;
//		this.testFilesPath = testFilesPath;
//		this.metaDir = metaDir;
//	}
//
//	@Override
//	public void run() {
//		System.out.println("#" + thread + ": started (" + images.size() + ")");
//
//		long begin = System.currentTimeMillis();
//
//		// DocumentBuilder builder =
//		// DocumentBuilderFactory.getFullDocumentBuilder();
//
//		ChainedDocumentBuilder builder = new ChainedDocumentBuilder();
//		builder.addBuilder(new SurfDocumentBuilder());
//		builder.addBuilder(DocumentBuilderFactory.getFullDocumentBuilder());
//
//		int cnt = 0;
//		for (String identifier : images) {
//			try {
//				// Build the Lucene Documents
//				Document doc = builder.createDocument(new FileInputStream(
//						testFilesPath + identifier), identifier);
//
//				/*
//				 * String imnr = identifier.substring(2,
//				 * identifier.indexOf('.'));
//				 * 
//				 * String tags = ""; try { BufferedReader intags = new
//				 * BufferedReader(new
//				 * FileReader(metaDir+"tags/tags"+imnr+".txt")); String strtags
//				 * = ""; while ((strtags = intags.readLine()) != null) { tags +=
//				 * strtags + " "; } intags.close(); } catch (Exception e) {}
//				 * 
//				 * String desc = ""; try { BufferedReader inlicense = new
//				 * BufferedReader(new
//				 * FileReader(metaDir+"license/license"+imnr+".txt")); String
//				 * line = ""; while ((line = inlicense.readLine()) != null) { if
//				 * (line.startsWith("Picture title")) { desc =
//				 * line.substring(line.indexOf(":")+1, line.length()); break; }
//				 * } inlicense.close(); } catch (Exception e) {}
//				 * 
//				 * doc.add(new Field("tags", tags.trim(), Field.Store.YES,
//				 * Field.Index.ANALYZED));
//				 * 
//				 * doc.add(new Field("description", desc.trim(),
//				 * Field.Store.YES, Field.Index.ANALYZED));
//				 */
//
//				// Add the Documents to the index
//				iw.addDocument(doc);
//
//				// System.out.println(doc.getField(DocumentBuilder.FIELD_NAME_COLORLAYOUT).stringValue());
//				/*
//				 * for (Iterator<Fieldable> iterator =
//				 * doc.getFields().iterator(); iterator.hasNext(); ) { Field f =
//				 * (Field) iterator.next(); if
//				 * (f.name().equals(DocumentBuilder.FIELD_NAME_IDENTIFIER)) {
//				 * System.out.println("\t"+f.name() + ": " + f.stringValue()); }
//				 * else { System.out.print("\t"+f.name() + ": " +
//				 * f.getBinaryLength() + " "); for (byte b : f.getBinaryValue())
//				 * { System.out.print(b + " "); } System.out.println(); } }
//				 * System.out.println();
//				 */
//				cnt++;
//
//				System.out.println("#" + thread + ": " + cnt + "/"
//						+ images.size());// + ": " + ((end-begin)/100) +
//											// " [s]; " +
//											// ((double)(end-begin)/(double)cnt)
//											// + " [ms] per image");
//
//				/*
//				 * if ((cnt%2500)==0) { long end = System.currentTimeMillis();
//				 * System.out.println("#"+thread + ": " + cnt + "/" +
//				 * images.size() + ": " + ((end-begin)/100) + " [s]; " +
//				 * ((double)(end-begin)/(double)cnt) + " [ms] per image"); }
//				 */
//
//			} catch (Exception e) {
//				e.printStackTrace();
//				System.out.println(identifier);
//			}
//		}
//
//		long end = System.currentTimeMillis();
//		System.out.println((end - begin) + " [ms]; "
//				+ ((double) (end - begin) / (double) images.size())
//				+ " [ms] per image");
//
//	}
//}
