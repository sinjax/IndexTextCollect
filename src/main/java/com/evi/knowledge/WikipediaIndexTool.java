package com.evi.knowledge;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.benchmark.byTask.feeds.NoMoreDataException;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.evi.knowledge.lucene.LuceneIndexCreator;

public class WikipediaIndexTool implements Closeable {
	@Option(name = "-dump", usage = "The wikipedia dump file", required = true)
	String wikipediaLocation;

	@Option(name = "-index", usage = "Where to put the wikipedia index, defaults to <dump>-lucene")
	String outputLucene = null;

	@Option(name = "-limit", usage = "How many documents to process")
	int limit = -1;
	
	@Option(name = "-append", usage = "Add to an existing index, default behaviour fails")
	boolean append = false;
	
	@Option(name = "-delete", usage = "Delete the existing index, default behaviour fails")
	boolean delete = false;
	
	@Option(name = "-commit-batch-size", usage = "How many documents to index before a commit")
	private int commitBatchSize = 100000;
	
	@Option(name = "-ignore-redirect", usage = "don't add the redirect pages")
	private boolean ignoreRedirects = false;
	
	private File wikipediaFile;

	private File luceneIndex;

	private LuceneIndexCreator luceneIndexWriter;

	private SimpleWikipediaSource wikipediaSource;



	public WikipediaIndexTool(String[] args) throws Exception {
		CmdLineParser parser = new CmdLineParser(this);
		try {
			parser.parseArgument(args);
			prepare();
			validate();
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
			parser.printUsage(System.err);
			throw e;
		} 
	}

	private void validate() throws IOException {

		if (!wikipediaFile.exists()) {
			throw new IOException();
		}

	}

	private void prepare() throws IOException {
		if(outputLucene == null){
			outputLucene = wikipediaLocation + "-lucene";
		}
		
		File file = new File(outputLucene);
		if(file.exists()){
			FileUtils.deleteDirectory(file);
		}
		this.luceneIndex = file;
		luceneIndexWriter = new LuceneIndexCreator(this.luceneIndex);
		this.wikipediaFile = new File(this.wikipediaLocation);
		this.wikipediaSource = new SimpleWikipediaSource(this.wikipediaFile);
	}

	public static void main(String[] args) throws IOException {
		try (WikipediaIndexTool tool = new WikipediaIndexTool(args)) {
			tool.start();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	private void start() {
		int done = 0;
		int redirects = 0;
		try {
			while (limit == -1 || done < limit) {
				try {
					done++;
					boolean redirect = false;
					Document doc = this.wikipediaSource.getNextDocument();
					IndexableField body = doc.getField("body");
					String bodyStr = body.stringValue();
					if(bodyStr.length() > 9){						
						String subBody = bodyStr.substring(0,9);
						if(subBody.toLowerCase().startsWith("#redirect")){
							redirects ++;
							redirect = true;
						}
					}
					
					if(!ignoreRedirects || !redirect){
						this.luceneIndexWriter.writeDocument(doc);
					} else{
						done--;
						redirects--; 
					}
					if(done % 1000 == 0 && !redirect){
						System.out.printf("Seen %d, redirects %d, sample: %s\n",done, redirects, doc.getField("title").stringValue());
					}
					if(done % commitBatchSize == 0 && !redirect){
						this.luceneIndexWriter.commit();
					}
				} catch (NoMoreDataException e) {
					System.err.println("Done!");
					break;
				}
			}
			this.luceneIndexWriter.commit();
		} catch (IOException e) {
			System.err.println("Failed to read wiki xml: " + e.getMessage());
		}
	}

	@Override
	public void close() throws IOException {
		if(luceneIndexWriter!=null)
			this.luceneIndexWriter.close();
	}
}
