package com.evi.knowledge.terrier;

import gnu.trove.TIntIntHashMap;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.terrier.indexing.Collection;
import org.terrier.indexing.CollectionDocumentList;
import org.terrier.indexing.Document;
import org.terrier.matching.ResultSet;
import org.terrier.matching.models.TF_IDF;
import org.terrier.querying.Manager;
import org.terrier.querying.SearchRequest;
import org.terrier.querying.parser.Query;
import org.terrier.structures.DocumentIndexEntry;
import org.terrier.structures.Index;
import org.terrier.structures.indexing.singlepass.BasicSinglePassIndexer;
import org.terrier.utility.ApplicationSetup;


public class TerrierIndexer {
	private static final int R_IND_LEN = 1000;

	public static void main(String[] args) throws IOException {
		BasicTerrierConfig.configure();
		
		File tmp = File.createTempFile("index", ".dir");		
		tmp.delete();
		tmp.mkdirs();
		BasicSinglePassIndexer indexer = new BasicSinglePassIndexer(tmp.getAbsolutePath(), "things");
		List<Document> _docs = createDocs();
		Collection collection = new CollectionDocumentList(_docs.toArray(new Document[_docs.size()]), "id");
		indexer.createInvertedIndex(new Collection[]{collection});
		Index idx = Index.createIndex(tmp.getAbsolutePath(), "things");
		Manager manager = new Manager(idx);
		manager.setProperty("ignore.low.idf.terms", "false");
		SearchRequest request = manager.newSearchRequest("foo");
		
		request.setQuery(createMapDocument("query", "bee", R_IND_LEN).asQuery());
		
		request.addMatchingModel("Matching", "TF_IDF");
		manager.runPreProcessing(request);
		manager.runMatching(request);
		manager.runPostProcessing(request);
		manager.runPostFilters(request);

		ResultSet res = request.getResultSet();
		for (int r = 0 ; r < res.getDocids().length; r++) {
			int docid = res.getDocids()[r];
			String dn = idx.getMetaIndex().getItem("docno", docid);
			System.out.println(dn + ": " + res.getScores()[r]);
		}
		
	}

	private static List<Document> createDocs() {
		List<Document> ret = Arrays.asList(
			createMapDocument("cat_bee","cat cat cat bee",R_IND_LEN),
			createMapDocument("cat_tree","cat tree tree tree",R_IND_LEN),
			createMapDocument("tree_dog","tree dog tree dog",R_IND_LEN)
		);
		return ret;
	}

	private static MapDocument createMapDocument(String name, String string, int len) {
		TIntIntHashMap map = new TIntIntHashMap();
		for (String s : string.split(" ")) {
			Random rng = new Random(s.hashCode());
			map.adjustOrPutValue(rng.nextInt(len), 1, 1);
			map.adjustOrPutValue(rng.nextInt(len), 1, 1);
			map.adjustOrPutValue(rng.nextInt(len), 1, 1);
			map.adjustOrPutValue(rng.nextInt(len), 1, 1);
		}
		return new MapDocument(name,map);
	}
}
