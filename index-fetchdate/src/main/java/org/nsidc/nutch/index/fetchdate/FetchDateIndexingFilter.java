package org.nsidc.nutch.index.fetchdate;

import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Parse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds the first fetch date from the
 * org.nsidc.nutch.parse.scoring.FetchDateScoringFilter to a Nutch document
 * ready to be indexed in Solr
 */
public class FetchDateIndexingFilter implements IndexingFilter {

	private Configuration conf;
	private static final Logger LOG = LoggerFactory
			.getLogger(FetchDateIndexingFilter.class);
	private static final Text WRITABLE_FETCH_KEY = new Text(
			Nutch.FETCH_TIME_KEY);

	public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
			CrawlDatum datum, Inlinks inlinks) throws IndexingException {

		MapWritable metadata = datum.getMetaData();
		Writable date = metadata.get(WRITABLE_FETCH_KEY);
		Long fetchTimeStamp = Long.parseLong(date.toString());
		java.util.Date fetchDate = new java.util.Date(fetchTimeStamp);
		// formats the date to be Solr schema compatible
		String formatedDate = (new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:sss'Z'"))
				.format(fetchDate);
		if (fetchDate != null) {
			LOG.info("First date obtained for " + url.toString() + " is: "
					+ formatedDate);
			doc.add("first_fetch_date", formatedDate);
		}
		return doc;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}
}
