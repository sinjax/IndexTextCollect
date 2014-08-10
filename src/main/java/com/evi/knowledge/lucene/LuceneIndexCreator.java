package com.evi.knowledge.lucene;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;

public class LuceneIndexCreator implements Closeable{

	private SimpleFSDirectory directory;
	private IndexWriter writer;
	private IndexWriterConfig conf;
	private Map<String, Analyzer> maps = new HashMap<String,Analyzer>();
	

	public LuceneIndexCreator(File file) throws IOException {
		this.directory = new SimpleFSDirectory(file);
		PerFieldAnalyzerWrapper fieldAnalyzer = new PerFieldAnalyzerWrapper(new KeywordAnalyzer(),maps);
		maps.put("body", new StandardAnalyzer(Version.LUCENE_46));
		this.conf = new IndexWriterConfig(Version.LUCENE_46, fieldAnalyzer);
		this.writer = new IndexWriter(directory, conf);
	}
	
	public void writeDocument(Document doc) throws IOException{
		this.writer.addDocument(doc);
	}
	
	@Override
	public void close() throws IOException {
		this.writer.close();
	}

	public void commit() throws IOException {
		System.out.println("Commiting lucene index");
		this.writer.commit();
	}
	
}
