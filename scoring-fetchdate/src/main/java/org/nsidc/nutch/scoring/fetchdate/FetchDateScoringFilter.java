package org.nsidc.nutch.scoring.fetchdate;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.ScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetchDateScoringFilter implements ScoringFilter {

	Configuration conf;
	private static final Logger LOG = LoggerFactory.getLogger(FetchDateScoringFilter.class);
	
	@Override
	public float generatorSortValue(Text url, CrawlDatum datum, float initSort)
			throws ScoringFilterException {
		
		LongWritable writableFetchTime = new LongWritable(datum.getFetchTime());
		Text writableFetchTimeKey = new Text(Nutch.FETCH_TIME_KEY);
		MapWritable metadata = datum.getMetaData();
		String urlString = url.toString();	
		
		
		if (metadata.get(writableFetchTimeKey) == null) {
			LOG.info("Adding fetch time value to the metadata: " + writableFetchTime.toString() + " " + urlString);
			metadata.put(writableFetchTimeKey, writableFetchTime);
		} else {
			LOG.info("Metadata already has fetch time value");
		}
		
		return initSort;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public void injectedScore(Text url, CrawlDatum datum)
			throws ScoringFilterException {
	}

	@Override
	public void initialScore(Text url, CrawlDatum datum)
			throws ScoringFilterException {
	}

	@Override
	public void passScoreBeforeParsing(Text url, CrawlDatum datum,
			Content content) throws ScoringFilterException {
	}

	@Override
	public void passScoreAfterParsing(Text url, Content content, Parse parse)
			throws ScoringFilterException {
	}

	@Override
	public CrawlDatum distributeScoreToOutlinks(Text fromUrl,
			ParseData parseData, Collection<Entry<Text, CrawlDatum>> targets,
			CrawlDatum adjust, int allCount) throws ScoringFilterException {
		return adjust;
	}

	@Override
	public void updateDbScore(Text url, CrawlDatum old, CrawlDatum datum,
			List<CrawlDatum> inlinked) throws ScoringFilterException {
	}

	@Override
	public float indexerScore(Text url, NutchDocument doc, CrawlDatum dbDatum,
			CrawlDatum fetchDatum, Parse parse, Inlinks inlinks, float initScore)
			throws ScoringFilterException {
		return initScore;
	}

}
