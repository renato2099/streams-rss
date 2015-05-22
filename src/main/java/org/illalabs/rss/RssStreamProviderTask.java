/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.illalabs.rss;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedInput;

/**
 * A {@link java.lang.Runnable} task that queues rss feed data.
 *
 * <code>RssStreamProviderTask</code> reads the content of an rss feed and
 * queues the articles from the feed inform of a Datum The task can filter
 * articles by a published date.
 *
 */
public class RssStreamProviderTask implements Runnable {

    private static final int MAX_RETRIES = 5;
    private final static Logger LOGGER = LoggerFactory
            .getLogger(RssStreamProviderTask.class);
    private static final int DEFAULT_TIME_OUT = 10000; // 10 seconds
    private static final String RSS_KEY = "rssFeed";
    private static final String URI_KEY = "uri";
    private static final String LINK_KEY = "link";
    private static final String DATE_KEY = "publishedDate";

    /**
     * Map that contains the Set of previously seen articles by an rss feed.
     */
    @VisibleForTesting
    protected static final Map<String, Set<String>> PREVIOUSLY_SEEN = new ConcurrentHashMap<>();

    private BlockingQueue<Datum> dataQueue;
    private int timeOut;
    private SyndEntrySerializer serializer;
    private DateTime publishedSince;
    private FeedDetails feedDetails;
    private BlockingQueue<FeedDetails> rssQueue;

    /**
     * Non-perpetual mode, no date filter
     * 
     * @param queue
     * @param rssFeed
     * @param timeOut
     */
    public RssStreamProviderTask(BlockingQueue<Datum> queue, String rssFeed,
            int timeOut) {
        this(queue, rssFeed, new DateTime().minusYears(30), timeOut);
    }

    /**
     * Non-perpetual mode, time out of 10 sec
     * 
     * @param queue
     * @param rssFeed
     * @param publishedSince
     */
    public RssStreamProviderTask(BlockingQueue<Datum> queue, String rssFeed,
            DateTime publishedSince) {
        this(queue, rssFeed, publishedSince, DEFAULT_TIME_OUT);
    }

    /**
     * RssStreamProviderTask that reads an rss feed url and queues the resulting
     * articles as StreamsDatums with the documents being object nodes.
     * 
     * @param queue
     *            Queue to push data to
     * @param rssFeed
     *            url of rss feed to read
     * @param publishedSince
     *            DateTime to filter articles by, will queue articles with
     *            published times after this
     * @param timeOut
     *            url connection timeout in milliseconds
     */
    public RssStreamProviderTask(BlockingQueue<Datum> queue, String rssFeed,
            DateTime publishedSince, int timeOut) {
        this.dataQueue = queue;
        this.timeOut = timeOut;
        this.publishedSince = publishedSince;
        this.serializer = new SyndEntrySerializer();
    }

    public RssStreamProviderTask(BlockingQueue<FeedDetails> rssQueue,
            BlockingQueue<Datum> dQueue, String string,
            DateTime publishedSince, int timeOut) {
        this.dataQueue = dQueue;
        this.rssQueue = rssQueue;
        this.timeOut = timeOut;
        this.publishedSince = publishedSince;
        this.serializer = new SyndEntrySerializer();
    }

    @Override
    public void run() {
        boolean work = true;
        int retries = 0;
        try {
            while (work) {
                feedDetails = this.rssQueue.poll();
                //If there is nothing, then wait for some time
                if (retries < MAX_RETRIES && this.feedDetails == null) {
                    this.waiting(DEFAULT_TIME_OUT / (ThreadLocalRandom.current().nextInt(MAX_RETRIES)+1));
                    retries ++;
                }
                else if (this.feedDetails != null){
                    // if enough time has passed, then read again
                    long waitTime = (System.currentTimeMillis() - this.feedDetails
                            .getLastPolled()) / 1000;
                    if (waitTime > this.feedDetails.getPollIntervalMillis()) {
                        Set<String> batch = queueFeedEntries(new URL(
                                feedDetails.getUrl()));
                        // do something with batch
                        System.out.println("Batch " + feedDetails.getUrl()
                                + " " + this.feedDetails.getLastPolled() + "");
                        PREVIOUSLY_SEEN.put(feedDetails.getUrl(), batch);
                        work = false;
                        this.feedDetails.setLastPolled(System
                                .currentTimeMillis());
                    } else {
                        LOGGER.info(this.feedDetails.getUrl()
                                + " has been already polled.");
                        this.waiting(waitTime);
                    }
                    // Put back the item we just worked with
                    this.rssQueue.put(this.feedDetails);
                } else {
                    // if we waited, and there is nothing to work on
                    work = false;
                }
            }
            LOGGER.info("Worker finished");

        } catch (IOException | FeedException | InterruptedException e) {
            LOGGER.warn("Exception while reading rss stream, {} : {}",
                    feedDetails, e);
        }
    }

    /**
     * Safe waiting
     * 
     * @param waitTime
     * @throws InterruptedException
     */
    private void waiting(long waitTime) throws InterruptedException {
        LOGGER.info("Waiting for " + waitTime + " mlsecs.");
        synchronized (this) {
            this.wait(waitTime);
        }
    }

    /**
     * Reads the url and queues the data
     * 
     * @param feedUrl
     *            rss feed url
     * @return set of all article urls that were read from the feed
     * @throws IOException
     *             when it cannot connect to the url or the url is malformed
     * @throws FeedException
     *             when it cannot reed the feed.
     */
    @VisibleForTesting
    protected Set<String> queueFeedEntries(URL feedUrl) throws IOException,
            FeedException {
        Set<String> batch = Sets.newConcurrentHashSet();
        URLConnection connection = feedUrl.openConnection();
        connection.setConnectTimeout(this.timeOut);
        connection.setConnectTimeout(this.timeOut);
        SyndFeedInput input = new SyndFeedInput();
        SyndFeed feed = input.build(new InputStreamReader(connection
                .getInputStream()));
        for (Object entryObj : feed.getEntries()) {
            SyndEntry entry = (SyndEntry) entryObj;
            ObjectNode nodeEntry = this.serializer.deserialize(entry);
            nodeEntry.put(RSS_KEY, this.feedDetails.getUrl());
            String entryId = determineId(nodeEntry);
            batch.add(entryId);
            Datum datum = new Datum(nodeEntry, entryId, DateTime.now());
            try {
                JsonNode published = nodeEntry.get(DATE_KEY);
                if (published != null) {
                    try {
                        DateTime date = RFC3339Utils.parseToUTC(published
                                .asText());
                        if (date.isAfter(this.publishedSince)
                                && (!seenBefore(entryId,
                                        this.feedDetails.getUrl()))) {
                            this.dataQueue.put(datum);
                            LOGGER.debug("Added entry, {}, to provider queue.",
                                    entryId);
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        LOGGER.trace("Failed to parse date from object node, attempting to add node to queue by default.");
                        if (!seenBefore(entryId, this.feedDetails.getUrl())) {
                            this.dataQueue.put(datum);
                            LOGGER.debug("Added entry, {}, to provider queue.",
                                    entryId);
                        }
                    }
                } else {
                    LOGGER.debug("No published date present, attempting to add node to queue by default.");
                    if (!seenBefore(entryId, this.feedDetails.getUrl())) {
                        this.dataQueue.put(datum);
                        LOGGER.debug("Added entry, {}, to provider queue.",
                                entryId);
                    }
                }
            } catch (InterruptedException ie) {
                LOGGER.error("Interupted Exception.");
                Thread.currentThread().interrupt();
            }
        }
        return batch;
    }

    /**
     * Returns a link to the article to use as the id
     * 
     * @param node
     * @return
     */
    private String determineId(ObjectNode node) {
        String id = null;
        if (node.get(URI_KEY) != null
                && !node.get(URI_KEY).textValue().equals("")) {
            id = node.get(URI_KEY).textValue();
        } else if (node.get(LINK_KEY) != null
                && !node.get(LINK_KEY).textValue().equals("")) {
            id = node.get(LINK_KEY).textValue();
        }
        return id;
    }

    /**
     * Returns false if the artile was previously seen in another task for this
     * feed
     * 
     * @param id
     * @param rssFeed
     * @return
     */
    private boolean seenBefore(String id, String rssFeed) {
        Set<String> previousBatch = PREVIOUSLY_SEEN.get(rssFeed);
        if (previousBatch == null) {
            return false;
        }
        return previousBatch.contains(id);
    }

}