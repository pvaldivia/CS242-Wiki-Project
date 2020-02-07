package edu.ucr.cs.cs242.group14;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class LucenePartA {
	static JSONParser parser = new JSONParser();

	public static void buildIndex(String indexPath, String dataPath) throws IOException, org.json.simple.parser.ParseException {
		long startTime = System.currentTimeMillis();
		
		// necessary variables for lucene indexing
    	Analyzer analyzer = new StandardAnalyzer();
		Directory directory = FSDirectory.open(Paths.get(indexPath));
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		IndexWriter indexWriter = new IndexWriter(directory,config);

		int fileNumber = 1;
		File file = new File(dataPath + "/wiki_" + fileNumber + ".json");
		
		// read in each file then add each json object in the file into the indexer
		while(file.exists()) {
			BufferedReader bufferedReader = new BufferedReader(new FileReader(file)); 
			for(String line; (line = bufferedReader.readLine()) != null; ) {
				JSONObject json = (JSONObject) parser.parse(line);
				Document doc = new Document();
				
				//adding data to documents. Use TextField to store and index. use StoredField to just store.
				doc.add(new Field("title", json.get("title").toString(), TextField.TYPE_STORED));
				doc.add(new Field("lastEdit", json.get("lastEdit").toString(), TextField.TYPE_STORED));
				doc.add(new Field("url", json.get("url").toString(), StringField.TYPE_STORED));
				doc.add(new Field("facts", json.get("facts").toString(), TextField.TYPE_STORED));
				doc.add(new Field("outGoingLinks", json.get("outGoingLinks").toString(), StoredField.TYPE));
				
				// make string of every element in the json array
				JSONArray outGoingLinksList = (JSONArray) json.get("outGoingLinksList");
				@SuppressWarnings("unchecked")
				Iterator<String> iterator =  outGoingLinksList.iterator();
				String outGoingLinksTemp = "";
				while (iterator.hasNext()) {
					outGoingLinksTemp += iterator.next() + " , ";
				}
				doc.add(new Field("outGoingLinksList", outGoingLinksTemp, StoredField.TYPE));
				
				
				doc.add(new Field("description", json.get("description").toString(), TextField.TYPE_STORED));
									
				indexWriter.addDocument(doc);
			}
			bufferedReader.close();
			System.out.println("file " + fileNumber);
			fileNumber += 1;
			file = new File(dataPath + "/wiki_" + fileNumber + ".txt");
		}

		indexWriter.close();
		
		long endTime = System.currentTimeMillis();
		System.out.println("Time to build index with " + fileNumber + " files: " + (endTime - startTime) + " ms");
	}
	
	public static ScoreDoc[] searchIndex(String indexPath, String userQuery, int numHits) throws IOException, ParseException {
		// necessary variables for lucene indexer
		Analyzer analyzer = new StandardAnalyzer();
		Directory directory = FSDirectory.open(Paths.get(indexPath));
		DirectoryReader indexReader = DirectoryReader.open(directory);
		IndexSearcher indexSearcher = new IndexSearcher(indexReader);
		
		String[] fields = {"title", "lastEdit", "url" , "facts", "description"};
		Map<String, Float> boosts = new HashMap<>();
		
		// index fields and their result weights
		boosts.put(fields[0], 0.2f);
		boosts.put(fields[1], 0.1f);
		boosts.put(fields[2], 0.2f);
		boosts.put(fields[3], 0.2f);
		boosts.put(fields[4], 0.3f);
		
		MultiFieldQueryParser parser = new MultiFieldQueryParser(fields, analyzer, boosts);
		Query query = parser.parse(userQuery);
		System.out.println(query.toString());
		int topHitCount = numHits;
		ScoreDoc[] hits = indexSearcher.search(query, topHitCount).scoreDocs;
		
		return hits;
	}
	
	public static void printResults(String indexPath, ScoreDoc[] hits) throws IOException {
		//necessary variables for lucene indexer
		Directory directory = FSDirectory.open(Paths.get(indexPath));
		DirectoryReader indexReader = DirectoryReader.open(directory);
		IndexSearcher indexSearcher = new IndexSearcher(indexReader);
		
		// loop through array with all the results obtained from the search function
		for (int rank = 0; rank < hits.length; ++rank) {
			Document hitDoc = indexSearcher.doc(hits[rank].doc);
			System.out.println((rank + 1) + " (score:" + hits[rank].score + ") --> " + 
								" title: " + hitDoc.get("title") + 
								" lastEdit: " + hitDoc.get("lastEdit") + 
								" url: " + hitDoc.get("url") +
								" facts: " + hitDoc.get("facts") + 
								" outgoingLinksList: " + hitDoc.get("outGoingLinksList") + 
								" description: " + hitDoc.get("description"));
		}
	}
	
    public static void main(String[] args) {
    	
    	// check for valid number of arguments
    	if (args.length != 4) {
    		System.out.println("Must have four arguments <directory for index to be stored. Example: ./index) <directory to data. Example: ./data> <string to search in index. surounded with \"\"> <number of document hits to return>");
    	}
    	else {
    		String indexPath = args[0];
    		String dataPath = args[1];
    		String userQuery = args[2];
    		int numHits = Integer.parseInt(args[3]);
    		
    		// run index builder function
        	try {
        		buildIndex(indexPath, dataPath);
        	} catch (IOException | org.json.simple.parser.ParseException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
        	
        	// run index searcher function
        	ScoreDoc[] hits = null;
        	try {
        		hits = searchIndex(indexPath, userQuery, numHits);
        	} catch (IOException | ParseException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}

        	// print the results of the index search
    		try {
    			printResults(indexPath, hits);
    		} catch (IOException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
    	}
    }
}