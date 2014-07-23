package info.boytsov.lucene;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.surround.parser.CharStream;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Version;

public class SearchIndex {
	public static final String FIELD_CONTENTS = "body";
	public static void main(String[] args) throws IOException, ParseException {
		Directory directory = FSDirectory.open(new File(args[0]));
		IndexReader indexReader = DirectoryReader.open(directory);
		
		IndexSearcher indexSearcher = new IndexSearcher(indexReader);
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
		QueryParser queryParser = new QueryParser(Version.LUCENE_46,FIELD_CONTENTS,analyzer);
//		Query query = queryParser.parse("body:\"strings like\"");
		Query query = new TermQuery(new Term("url", "McFly"));
		TopDocs hits = indexSearcher.search(query, 100);
		int i = 0;
		for (ScoreDoc sd: hits.scoreDocs) {
			System.out.println("Index: " + i);
			System.out.println(indexReader.document(sd.doc).getField("doctitle"));
			System.out.println(indexReader.document(sd.doc).getField("docid"));
			System.out.println(indexReader.document(sd.doc).getField("body"));
			System.out.println(indexReader.document(sd.doc).getField("url"));
			i++;
		}
	}
}
