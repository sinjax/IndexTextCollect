package com.evi.knowledge.terrier;

import gnu.trove.TIntIntHashMap;
import gnu.trove.TIntIntIterator;
import gnu.trove.TObjectIntHashMap;
import gnu.trove.TObjectIntProcedure;

import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.terrier.indexing.Document;
import org.terrier.querying.parser.MultiTermQuery;
import org.terrier.querying.parser.Query;
import org.terrier.querying.parser.SingleTermQuery;

public class MapDocument implements Document{
	private TIntIntIterator es;
	private int count;
	protected Map<String,String> props = new HashMap<String, String>();
	private TIntIntHashMap map;
	
	public MapDocument(String docno, TIntIntHashMap map) {
		this.map = map;
		reset();
		this.props.put("docno", docno);
	}
	@Override
	public String getNextTerm() {
		while(this.count >= this.es.value()){
			this.es.advance();
			this.count = 0;
		}
		this.count ++;
		return this.es.key() + "";
	}
	
	@Override
	public Set<String> getFields() {
		return null;
	}

	@Override
	public boolean endOfDocument() { 
				return this.count>=this.es.value() && this.es.hasNext();
	}

	@Override
	public Reader getReader() {
		return null;
	}

	@Override
	public String getProperty(String name) {
		return props.get(name);
	}

	@Override
	public Map<String, String> getAllProperties() {
		return props;
	}
	public Query asQuery() {
		TObjectIntHashMap<String> counts = new TObjectIntHashMap<String>();		
		while (!endOfDocument()){
			counts.adjustOrPutValue(getNextTerm(), 1, 1);
		}
		reset();
		MultiTermQuery mtq = new MultiTermQuery();
		counts.forEachEntry(new TObjectIntProcedure<String>() {
			@Override
			public boolean execute(String term, int count) {
				SingleTermQuery stq = new SingleTermQuery(term);
				stq.setWeight(count);
				mtq.add(stq);
				return true;
			}
		});
		return mtq;
	}
	
	public void reset() {
		this.es = map.iterator();
		this.es.advance();
	}

}
