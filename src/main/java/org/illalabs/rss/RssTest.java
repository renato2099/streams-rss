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

import com.sun.syndication.io.FeedException;

/**
 *
 */
public class RssTest {
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

        // RssStreamProviderTask task = new RssStreamProviderTask(filledQueue,
        // "fake url", publishedSince, 10000);
        // Set<String> batch = task.queueFeedEntries(new
        // URL("http://diariocorreo.pe/rss/"));
        // System.out.println(filledQueue);
        // System.out.println(batch);
        // assertEquals( 15, queue.size());
        // assertEquals( 20 , batch.size());
        // assertTrue( queue.size() < batch.size());
        // RssStreamProviderTask.PREVIOUSLY_SEEN.put("fake url", batch);
        // Test that it will not out put previously seen articles
        // filledQueue.clear();
        // batch = task.queueFeedEntries(new
        // URL("resource:///test_rss_xml/economist1.xml"));
        // assertEquals( 0, queue.size());
        // assertEquals( 20 , batch.size());
        // assertTrue( queue.size() < batch.size());
        // RssStreamProviderTask.PREVIOUSLY_SEEN.put("fake url", batch);

        // batch = task.queueFeedEntries(new
        // URL("resource:///test_rss_xml/economist2.xml"));
        // assertTrue( queue.size() < batch.size());
        // assertEquals("Expected queue size to be 0", 3, queue.size());
        // assertEquals("Expected batch size to be 0", 25, batch.size());
    }

    private static BlockingQueue<FeedDetails> loadRssFile(String rssPath) {
        BlockingQueue<FeedDetails> rssQueue = new LinkedBlockingQueue<>();
        try {
            List<String> readAllLines = Files.readAllLines(FileSystems
                    .getDefault().getPath(rssPath), Charset.defaultCharset());
            for (String line : readAllLines) {
                String[] split = line.split(",");
                rssQueue.add(new FeedDetails(split[0], split[1]));
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return rssQueue;
    }
}
