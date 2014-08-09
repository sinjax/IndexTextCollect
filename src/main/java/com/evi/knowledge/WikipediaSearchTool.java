package com.evi.knowledge;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.lucene.benchmark.byTask.feeds.NoMoreDataException;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.AutomatonProvider;
import org.apache.lucene.util.automaton.RegExp;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineOptionsProvider;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ProxyOptionHandler;

import com.evi.knowledge.lucene.LuceneIndexCreator;

public class WikipediaSearchTool implements Closeable {
	@Option(name = "-index", required=true, usage = "Where to put the wikipedia index, defaults to <dump>-lucene")
	String inputLucene = null;
	
	@Option(name = "-limit", usage = "How many to find")
	int limit = 100;
	
	enum OutputMode implements CmdLineOptionsProvider{
		JSON {
			@Override
			public LuceneOutputMode getOptions() {
				return new JSONLuceneOutputMode();
			}
		};

		@Override
		public abstract LuceneOutputMode getOptions();
		
	}
	
	@Option(name="-om",usage="Output mode",handler=ProxyOptionHandler.class)
	public OutputMode outputMode = OutputMode.JSON;
	public LuceneOutputMode outputModeOp = OutputMode.JSON.getOptions();
	
	@Option(name="-q", usage="Query string",required=true)
	private String queryStr;
	
	@Option(name="-f", usage="Field to search within", required=true)
	private String searchField;
	
	@Option(name="-t", usage="Output fields")
	private List<String> outputFields;

	private File luceneIndex;

	private IndexSearcher searcher;

	private IndexReader ir;

	
	
	public WikipediaSearchTool(String[] args) throws Exception {
		CmdLineParser parser = new CmdLineParser(this);
		try {
			parser.parseArgument(args);
			prepare();
			validate();
		} catch (NullPointerException e){
			e.printStackTrace();
			throw e;
		}
		catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
			parser.printUsage(System.err);
			throw e;
		}
	}

	private void validate() throws IOException {
		if (!luceneIndex.exists()) {
			throw new IOException();
		}
	}

	private void prepare() throws IOException {
		this.luceneIndex = new File(inputLucene);
		this.ir = DirectoryReader.open(new SimpleFSDirectory(this.luceneIndex));
		this.searcher = new IndexSearcher(ir);
		if(outputFields == null || outputFields.size() == 0){
			this.outputFields = new ArrayList<String>();
			outputFields.add(this.searchField);
		}
		this.outputModeOp.prepare(ir, this.outputFields);
		
	}

	public static void main(String[] args) throws Exception {
		try (WikipediaSearchTool tool = new WikipediaSearchTool(args)) {
			tool.start();
		} catch(Exception e){
			
		}
	}

	private void start() throws IOException, ParseException {
		QueryParser parser = new QueryParser(Version.LUCENE_46, searchField, null);
		parser.setLowercaseExpandedTerms(false);
		Query query = parser.parse(queryStr);
		TopDocs hits = this.searcher.search(query , limit);
		System.out.printf("Found %d results\n",hits.scoreDocs.length);
		for (ScoreDoc sd : hits.scoreDocs) {
			this.outputModeOp.write(sd.doc);
		}
		
	}

	@Override
	public void close() throws IOException {
	}
}
