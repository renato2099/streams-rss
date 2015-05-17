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
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.syndication.io.FeedException;

/**
 * Spawns threads to check on the last time a RSS has been read.
 */
public class RssMaster {
    private final static Logger LOGGER = LoggerFactory
            .getLogger(RssMaster.class);
    public final static Long DEFAULT_WAIT = 1000L;
    private static ExecutorService execService;
    public static BlockingQueue<FeedDetails> rssQueue;

    public static void main(String[] args) throws MalformedURLException,
            IOException, FeedException {
        // create thread pool
        execService = Executors.newFixedThreadPool(3);
        // load rss file into queue
        rssQueue = loadRssFile(args[0]);
        // give the rssQueue to the pool
        BlockingQueue<Datum> filledQueue = new LinkedBlockingQueue<>();
        DateTime publishedSince = new DateTime().withYear(2014)
                .withDayOfMonth(5).withMonthOfYear(9)
                .withZone(DateTimeZone.UTC);
        // this will finish as soon as all threads are working
        // while(true) { // this might be a more suitable option
        while(!rssQueue.isEmpty()) {
            execService.execute(new RssStreamProviderTask(rssQueue, filledQueue,
                    "url", publishedSince, 10000));
        }
        execService.shutdown();

    }

    private static BlockingQueue<FeedDetails> loadRssFile(String rssPath) {
        BlockingQueue<FeedDetails> rssQueue = new LinkedBlockingQueue<>();
        try {
            List<String> readAllLines = Files.readAllLines(FileSystems
                    .getDefault().getPath(rssPath), Charset.defaultCharset());
            for (String line : readAllLines) {
                String[] split = line.split(",");
                rssQueue.put(new FeedDetails(split[0], split[1]));
            }
        } catch (IOException | InterruptedException e) {
            LOGGER.error("Error while reading RSS list file.");
            e.printStackTrace();
        }
        return rssQueue;
    }
}
