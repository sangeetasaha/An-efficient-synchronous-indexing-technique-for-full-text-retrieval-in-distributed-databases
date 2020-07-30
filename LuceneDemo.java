package database.assignment2.lucene;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;

/**
 * 
 * @author Sangeeta_Saha
 *
 */
class LuceneDemo
{  
	//Path where lucene index directory will be created
	static String indexDir = "sudo mkdir /home/Sangeeta/Downloads/Lucene_Demo/Lucene_Demo/index";
	final static File INDEX_DIR = new File(indexDir);

	public static void main(String args[])
	{  
		try
		{  
			//Connect to postgreSQL database
			Class.forName("org.postgresql.Driver"); 

			//Local machine connection
			//Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/Lucene/Demo", "postgres", "postgres");  

			//Remote machine connection
			Connection con = DriverManager.getConnection("jdbc:postgresql://10.53.150.151:5432/Lucene/Demo", "postgres", "postgres");  

			//Insert rows to database
			Statement stmt = con.createStatement();  

			/*
			 * Searching a random word in database that does not exist so that all rows have to be checked
			 * and we get the worst or maximum time
			 */
			String searchStr = "" + Math.random();
			System.out.println("Searching for word " + searchStr + ".....");

			//Huge string inserted
			String str = "QueryParser Class is the basic Class defined in Lucene Core particularly" + 
					" specialized for direct use for parsing queries and maintaining the queries. Different methods are available in the " +
					"QueryParser Class so that we can easily go with the searching tasks using a wide range of searching options provided by" +
					" the Lucene. The next part of the test considers the network impact of the distributed soltuion. We evaluated the " +
					"bandwidth consumption on the implemented solution on 4 nodes. We  that the total consumed bandwidth is the sum of " +
					"the bandwidth use on the three links of the topology. We took advantage from the search indexes use to afford a " +
					"search enhancement feature through building new search operators and providing these new search functions as " +
					"reusable facilities. Through the same search interface the end users may query a distributed database in a " +
					"full-text mode and get faster and richer responses than the basic SQL. The search time benefit was proven by manual" + 
					" and automated stress tests in the same platform with two different methods. QueryParser Class is the basic Class " +
					"defined in Lucene Core particularly specialized for direct use for parsing queries and maintaining the queries. " +
					"Different methods are available in the QueryParser Class so that we can easily go with the searching tasks using a " +
					"wide range of searching options provided by the Lucene. The next part of the test considers the network impact of " +
					"the distributed soltuion. We evaluated the bandwidth consumption on the implemented" + 
					" solution on 4 nodes. We  that the total consumed bandwidth is the sum of the bandwidth use on the three links of " +
					"the topology. We took advantage from the search indexes use to afford a search enhancement feature through building " +
					"new search operators and providing these new search functions as reusable facilities. Through the same search " +
					"interface the end users may query a distributed database in a full-text mode and get faster and richer responses " +
					"than the basic SQL. The search time benefit was proven by manual and automated stress tests in the same platform " +
					"with two different methods.QueryParser Class is the basic Class defined in Lucene Core particularly" + 
					" specialized for direct use for parsing queries and maintaining the queries. Different methods are available in " +
					"the QueryParser Class so that we can easily go with the searching tasks using a wide range of searching " +
					"options provided by the Lucene. The next part of the test considers the network impact of the distributed " +
					"soltuion. We evaluated the bandwidth consumption on the implemented solution on 4 nodes. We  that the total " +
					"consumed bandwidth is the sum of the bandwidth use on the three links of the topology. We took advantage from " +
					"the search indexes use to afford a search enhancement feature through building new search " +
					"operators and providing these new search functions as reusable facilities. Through the same search interface " +
					"the end users may query a distributed database in a full-text mode and get faster and richer responses than the " +
					"basic SQL. The search time benefit was proven by manual and automated stress tests in the same platform with " +
					"two different methods. QueryParser Class is the basic Class defined in Lucene Core particularly";	

			//Without any indexing
			/*for(int i=0; i<31189; i++)
			{
				String sql = "INSERT INTO \"Demo\".\"Non_Indexed_Table\" VALUES (" + i + ", '" + str + "')";
				stmt.executeUpdate(sql);
			}*/

			int count = 0;

			long startTime = System.currentTimeMillis();
			ResultSet rs1 = stmt.executeQuery("select * from \"Demo\".\"Non_Indexed_Table\" where text like '%" + searchStr + "%'");

			while (rs1.next()) 
			{
				count++;
				//System.out.println(rs1.getString(2));
			}

			long endTime = System.currentTimeMillis();
			System.out.println("Time without any indexing : " + (endTime - startTime) +" ms and hits : " + count);

			//With normal Primary Key indexing
			/*for(int j=0; j<31189; j++)
			{
				String sql = "INSERT INTO \"Demo\".\"Indexed_Table\" VALUES (" + j + ", '" + j + " " + str + "')";
				stmt.executeUpdate(sql);
			}*/

			count = 0;
			long startTime2 = System.currentTimeMillis();
			ResultSet rs2 = stmt.executeQuery("select * from \"Demo\".\"Indexed_Table\" where text like '%" + searchStr + "%'");

			while (rs2.next()) 
			{
				count++;
				//System.out.println(rs1.getString(2));
			}

			long endTime2 = System.currentTimeMillis();
			System.out.println("Time with normal indexing : " + (endTime2 - startTime2) +" ms and hits : " + count);

			//With Lucene indexing
			try 
			{
				//Index database table in lucene index directory
				StandardAnalyzer analyzer = new StandardAnalyzer();
				/*IndexWriter writer = new IndexWriter(INDEX_DIR, analyzer, true);
				indexDocs(writer, con);
				writer.optimize();
				writer.close();*/

				//Search in lucene index directory
				count = 0;
				long startTime1 = System.currentTimeMillis();
				Searcher searcher = new IndexSearcher(IndexReader.open(indexDir));
				Query query = new QueryParser("text", analyzer).parse(searchStr);
				Hits hits = searcher.search(query);
				long endTime1 = System.currentTimeMillis();

				for (int i=0; i <hits.length(); i++)
				{
					//String id = hits.doc(i).get("id");
					count++;
					//System.out.println(id);
				}
				System.out.println("Time with lucene indexing : " + (endTime1 - startTime1) +" ms and hits : " + count);

			} 
			catch (Exception e)
			{
				e.printStackTrace();
			} 

			con.close();  
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}  
	}  

	static void indexDocs(IndexWriter writer, Connection conn) throws Exception 
	{
		String sql = "select id, text from \"Demo\".\"Non_Indexed_Table\"";
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);

		while (rs.next())
		{
			Document d = new Document();
			d.add(new Field("id", rs.getString("id"), Field.Store.YES, Field.Index.NO));
			d.add(new Field("text", rs.getString("text"), Field.Store.NO, Field.Index.TOKENIZED));
			writer.addDocument(d);
		}
	}

}  
