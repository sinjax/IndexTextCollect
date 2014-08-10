package com.evi.knowledge.lucene;

import java.util.Date;

import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexableField;

import sun.security.provider.certpath.IndexedCollectionCertStore;

public class LuceneUtils {
	public static Field createTextField(String name, String value){
		return newField(name, value, TextField.TYPE_STORED);
	}
	
	public static Field createNotStoredTextField(String name, String value){
		return newField(name, value, TextField.TYPE_NOT_STORED);
	}
	
	public static Field newField(String name, String value, FieldType type) {
		return new Field(name, value, type);
	}

	public static Field createDateField(String name, Date date) {
		
		Field f = new Field("modified",
		        DateTools.dateToString(date, DateTools.Resolution.MINUTE),
		        StringField.TYPE_STORED);
		
		return f;
	}

	public static IndexableField createStringField(String name, String value) {
		return newField(name, value, StringField.TYPE_STORED);
	}
}
