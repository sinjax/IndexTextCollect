package com.evi.knowledge.lucene;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
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
	

	public LuceneIndexCreator(File file) throws IOException {
		this.directory = new SimpleFSDirectory(file);
		this.conf = new IndexWriterConfig(Version.LUCENE_46, new KeywordAnalyzer());
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
