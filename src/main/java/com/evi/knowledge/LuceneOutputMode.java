package com.evi.knowledge;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.kohsuke.args4j.Option;

public abstract class LuceneOutputMode {
	
	@Option(name="-of",usage="output file, defaults to stdout (-)")
	public String outputFile = "-";
	
	protected PrintWriter pw;
	
	public void prepare(IndexReader searcher, List<String> fields) throws IOException{
		if(outputFile.equals("-")){
			this.pw = new PrintWriter(System.out);
		} else{
			this.pw = new PrintWriter(new File(outputFile), "UTF-8");
		}
	}
	public abstract void write(int docid) throws IOException;
}
