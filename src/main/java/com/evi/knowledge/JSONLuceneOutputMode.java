package com.evi.knowledge;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JSONLuceneOutputMode extends LuceneOutputMode {

	private IndexReader reader;
	private List<String> fields;
	Gson gson = new GsonBuilder().disableHtmlEscaping().create();
	@Override
	public void prepare(IndexReader reader, List<String> fields) throws IOException{
		super.prepare(reader, fields);
		this.reader = reader;
		this.fields = fields;
		
	}

	@Override
	public void write(int docid) throws IOException {
		Document doc = this.reader.document(docid);
		Map<String,String> vals = fromDoc(doc);
		vals.put("docId", "" + docid);
		String json = gson.toJson(vals);
		
		this.pw.println(json);
		this.pw.flush();
	}

	private Map<String, String> fromDoc(Document doc) {
		Map<String, String> ret = new HashMap<String, String>();
		if(fields == null){
			fields = fromFieldList(doc.getFields());
		}
		for (String name : this.fields) {
			ret.put(name, doc.getField(name).stringValue());
		}
		
		return ret;
	}

	private List<String> fromFieldList(List<IndexableField> fieldList) {
		List<String> ret = new ArrayList<String>();
		
		for (IndexableField indexableField : fieldList) {
			 ret.add(indexableField.name());
		}
		return ret;
	}

}
