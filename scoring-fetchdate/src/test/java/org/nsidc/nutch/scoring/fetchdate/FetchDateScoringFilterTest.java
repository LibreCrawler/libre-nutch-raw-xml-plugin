package org.nsidc.nutch.scoring.fetchdate;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.scoring.ScoringFilterException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class FetchDateScoringFilterTest {

	TestSetup setup;	
	
	@Before
	public void initialize_the_filter_and_mock_the_crawl_datum_and_its_metadata() {
		setup = new TestSetup();
	}
	
	@Test
	public void filter_should_add_fetch_time_key_to_crawl_datum_metadata() throws ScoringFilterException {
		setup.runFilter();

		ArgumentCaptor<Text> keyCaptor = ArgumentCaptor.forClass(Text.class);
		verify(setup.mockMetadata, atLeastOnce()).put(keyCaptor.capture(), isA(Writable.class));
		Text fetchTimeKey = keyCaptor.getValue();
		
		assertThat(fetchTimeKey.toString(), equalTo(Nutch.FETCH_TIME_KEY));
	}

	@Test
	public void filter_should_add_fetch_time_to_crawl_datum_metadata() throws ScoringFilterException {
		setup.runFilter();

		ArgumentCaptor<LongWritable> timeCaptor = ArgumentCaptor.forClass(LongWritable.class);
		verify(setup.mockMetadata, atLeastOnce()).put(isA(Writable.class), timeCaptor.capture());
		LongWritable fetchTimeValue = timeCaptor.getValue();
		
		assertThat(fetchTimeValue.get(), equalTo(setup.fakeFetchTimeValue));
	}

	@Test
	public void filter_should_not_write_fetch_time_if_metadata_has_a_fetch_time_key() throws ScoringFilterException {		
		when(setup.mockMetadata.get(new Text(Nutch.FETCH_TIME_KEY))).thenReturn(new Text("fakeFetchTimeKey"));
		setup.runFilter();

		verify(setup.mockMetadata, never()).put(isA(Writable.class), isA(Writable.class));
	}

	@Test
	public void filter_should_write_fetch_time_if_metadata_does_not_have_a_fetch_time_key() throws ScoringFilterException {
		when(setup.mockMetadata.get(new Text(Nutch.FETCH_TIME_KEY))).thenReturn(null);
		setup.runFilter();

		verify(setup.mockMetadata).put(isA(Writable.class), isA(Writable.class));
	}

	private class TestSetup {
		long fakeFetchTimeValue;
		FetchDateScoringFilter filter;
		MapWritable mockMetadata;
		CrawlDatum mockCrawlDatum;
		
		TestSetup() {
			this.fakeFetchTimeValue = 1341949016654l;
			
			this.filter = new FetchDateScoringFilter();
			
			this.mockMetadata = mock(MapWritable.class);
			
			this.mockCrawlDatum = mock(CrawlDatum.class);
			when(this.mockCrawlDatum.getFetchTime()).thenReturn(this.fakeFetchTimeValue);
			when(this.mockCrawlDatum.getMetaData()).thenReturn(this.mockMetadata);
		}

		float runFilter() throws ScoringFilterException {
			return this.filter.generatorSortValue(new Text("fakeURL"), this.mockCrawlDatum, 1.0f);
		}

	}

}