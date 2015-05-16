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
        while(!rssQueue.isEmpty()) {
            execService.execute(new RssStreamProviderTask(rssQueue, filledQueue,
                    "fake url", publishedSince, 10000));
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
