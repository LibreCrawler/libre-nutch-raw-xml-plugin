package org.nsidc.nutch.index.fetchdate;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Nutch;
import org.junit.Before;
import org.junit.Test;

public class FetchDateIndexingFilterTest {
	
	private FetchDateIndexingFilter filter;
	private final Long fetchTstamp = 1342120971583L;// 2012-07-12T13:22:051Z
	private NutchDocument doc;
	private CrawlDatum crawlDatum;
	private MapWritable metaData;
	private Text writableFetchTimeKey;	
	
	@Before
	public void setUp(){
		filter = new FetchDateIndexingFilter();		
		doc = new NutchDocument();
		crawlDatum = mock(CrawlDatum.class);
		metaData = mock(MapWritable.class);		
		writableFetchTimeKey = new Text(Nutch.FETCH_TIME_KEY);
		LongWritable date = new LongWritable(fetchTstamp);
		when(crawlDatum.getMetaData()).thenReturn(metaData);		
		when (metaData.get(writableFetchTimeKey)).thenReturn(date);
	}
	

	@Test
	public void testIndexingFilterAdds_fetchDate_Field_when_is_provided() throws Exception {

		// act
		filter.filter(doc, null, new Text("dummy.html"), crawlDatum, null);
		// assert
		assertThat(doc.getFieldNames(), hasItem("first_fetch_date"));
		String firstFetchDate = doc.getField("first_fetch_date").getValues().get(0).toString();
		assertThat(firstFetchDate, equalTo("2012-07-12T13:22:051Z"));
	}
	

	
}
