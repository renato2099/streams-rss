package org.apache.streams.examples.rss;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.streams.console.ConsolePersistWriter;
import org.apache.streams.core.StreamBuilder;
import org.apache.streams.core.StreamsDatum;
import org.apache.streams.local.builders.LocalStreamBuilder;
import org.apache.streams.rss.FeedDetails;
import org.apache.streams.rss.RssStreamConfiguration;
import org.apache.streams.rss.processor.RssTypeConverter;
import org.apache.streams.rss.provider.RssStreamProvider;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 *
 */
public class RssTest 
{
    public static void main( String[] args )
    {
        RssStreamConfiguration configuration = new RssStreamConfiguration();
        List<FeedDetails> feeds = new ArrayList<FeedDetails>();
        FeedDetails feed1 = new FeedDetails();
        feed1.setUrl("http://www.thelocal.ch/feeds/rss.php");
        feeds.add(feed1);
        configuration.setFeeds(feeds);

        RssStreamProvider provider = new RssStreamProvider(configuration, ObjectNode.class);
        provider.setConfig(configuration);
        
        Queue<StreamsDatum> persistQueue = new LinkedBlockingQueue<StreamsDatum>();
        
        ConsolePersistWriter console = new ConsolePersistWriter(persistQueue);
        
        //RssTypeConverter converter = new RssTypeConverter();
        RssTypeConverter converter = new RssTypeConverter();
        converter.
        StreamBuilder builder = new LocalStreamBuilder(persistQueue);
        builder.newPerpetualStream("provider", provider);
        builder.addStreamsProcessor("converter", converter, 1, "provider");
        //builder.addStreamsPersistWriter("writer", console, 1, "converter");
        builder.start();
    }
}
