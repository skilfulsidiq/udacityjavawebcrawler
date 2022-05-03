package com.udacity.webcrawler;

import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.RecursiveTask;
import java.util.regex.Pattern;

public class ParallelInternalCrawler extends RecursiveTask<Void> {

    private final String url;
    private final Instant deadline;
    private final int maxDepth;
    private final ConcurrentMap<String, Integer> counts;
    private final ConcurrentSkipListSet<String> visitedUrls;
    private final List<Pattern> ignoredUrls;
    private final PageParserFactory parserFactory;
    private final Clock clock;
    // Builder pattern
    private ParallelInternalCrawler(Builder builder) {
        this.url = builder.url;
        this.maxDepth=builder.maxDepth;
        this.deadline=builder.deadline;
        this.counts=builder.counts;
        this.visitedUrls=builder.visitedUrls;
        this.ignoredUrls = builder.ignoredUrls;
        this.parserFactory=  builder.pageParserFactory;
        this.clock = builder.clock;
    }
    public static final class Builder{
        private String url;
        private Instant deadline;
        private int maxDepth;
        private ConcurrentMap<String, Integer> counts;
        private ConcurrentSkipListSet<String> visitedUrls;
        private PageParserFactory pageParserFactory;
        private Clock clock;
        private List<Pattern> ignoredUrls;


        public Builder setUrl(String url){
            this.url=url;
            return this;
        }

        public Builder setDeadline(Instant deadline) {
            this.deadline = deadline;
            return this;
        }

        public Builder setClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder setCounts(ConcurrentMap<String, Integer> counts) {
            this.counts = counts;
            return this;
        }

        public Builder setIgnoredUrls(List<Pattern> ignoredUrls) {
            this.ignoredUrls = ignoredUrls;
            return this;
        }

        public Builder setMaxDepth(int maxDepth) {
            this.maxDepth = maxDepth;
            return this;
        }

        public Builder setPageParserFactory(PageParserFactory pageParserFactory) {
            this.pageParserFactory = pageParserFactory;
            return this;
        }

        public Builder setVisitedUrls(ConcurrentSkipListSet<String> visitedUrls) {
            this.visitedUrls = visitedUrls;
            return this;
        }

        public ParallelInternalCrawler build(){
            return new ParallelInternalCrawler(this);
        }
    }




    @Override
    protected Void compute() {

        if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
            return null;
        }
        for (Pattern pattern : ignoredUrls) {
            if (pattern.matcher(url).matches()) {
                return null;
            }
        }
        if (visitedUrls.contains(url)) {
            return null;
        }
        visitedUrls.add(url);
        PageParser.Result result = parserFactory.get(url).parse();
        for (ConcurrentMap.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
            if (counts.containsKey(e.getKey())) {
                counts.put(e.getKey(), e.getValue() + counts.get(e.getKey()));
            } else {
                counts.put(e.getKey(), e.getValue());
            }
        }
        List<ParallelInternalCrawler> subtasks =new ArrayList<>();
        ParallelInternalCrawler.Builder builder =  new Builder()
                .setCounts(counts)
                .setClock(clock)
                .setDeadline(deadline)
                .setIgnoredUrls(ignoredUrls)
                .setPageParserFactory(parserFactory)
                .setVisitedUrls(visitedUrls);

        for (String link : result.getLinks()) {
            subtasks.add(builder.setUrl(link).setMaxDepth(maxDepth-1).build());
        }
        invokeAll(subtasks);

        return null;
    }
}
