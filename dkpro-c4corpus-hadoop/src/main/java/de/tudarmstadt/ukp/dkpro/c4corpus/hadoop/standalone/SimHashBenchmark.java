package de.tudarmstadt.ukp.dkpro.c4corpus.hadoop.standalone;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.IntSummaryStatistics;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;
import com.google.common.collect.Multisets;

import de.tudarmstadt.ukp.dkpro.c4corpus.boilerplate.BoilerPlateRemoval;
import de.tudarmstadt.ukp.dkpro.c4corpus.boilerplate.impl.JusTextBoilerplateRemoval;
import de.tudarmstadt.ukp.dkpro.c4corpus.deduplication.impl.SimHashUtils;
import de.tudarmstadt.ukp.dkpro.c4corpus.hadoop.CharsetDetector;
import de.tudarmstadt.ukp.dkpro.c4corpus.hadoop.impl.ICUCharsetDetectorWrapper;
import de.tudarmstadt.ukp.dkpro.c4corpus.hadoop.io.WARCFileReader;
import de.tudarmstadt.ukp.dkpro.c4corpus.warc.io.WARCRecord;

public class SimHashBenchmark
{
    public static final Logger logger = Logger.getLogger(SimHashBenchmark.class);

    private static final boolean TEST_ORIGINAL = true;
    private static final int ITERATIONS = 10000000;
    private static final int SIZE = 10000;
    private static final int HAMMING_THRESHOLD = 3; // less than 3 bits different
    private static final Set<String> ALLOWED_CONTENT_TYPES = new HashSet<>(
            Arrays.asList("text/plain", "text/html", "application/xhtml+xml"));

    private static void timeHamming()
    {
        long[] hashes = new long[SIZE];
        Random rg = new Random(1000); // control seed for repeatable results
        for (int i = 0; i < SIZE; i++) {
            hashes[i] = rg.nextLong();
        }
        long startTime = System.nanoTime();
        @SuppressWarnings("unused")
        int totalDist = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            int j = i % SIZE;
            int k = (i + 13) % SIZE;
            @SuppressWarnings("deprecation")
            int dist = SimHashUtils.diffOfBits(hashes[j], hashes[k]);
            totalDist += dist;
        }
        long endTime = System.nanoTime();
        System.out.printf(Locale.US, "Old Hamming distance - %d iterations in %.4f msec.%n",
                ITERATIONS, (endTime - startTime) / 1000000f);
        startTime = System.nanoTime();
        for (int i = 0; i < ITERATIONS; i++) {
            int j = i % SIZE;
            int k = (i + 13) % SIZE;
            int dist = Long.bitCount(hashes[j] ^ hashes[k]);
            totalDist += dist;
        }
        endTime = System.nanoTime();
        System.out.printf(Locale.US, "New Hamming distance - %d iterations in %.4f msec.%n",
                ITERATIONS, (endTime - startTime) / 1000000f);
        startTime = System.nanoTime();
        for (int i = 0; i < ITERATIONS; i++) {
            @SuppressWarnings("unused")
            int j = i % SIZE;
            int k = (i + 13) % SIZE;
            totalDist += k;
        }
        endTime = System.nanoTime();
        System.out.printf(Locale.US, "Timing loop (control) - %d iterations in %.4f msec.%n",
                ITERATIONS, (endTime - startTime) / 1000000f);
    }

    private static void timeTextProcessing(String testFilename)
    {
        Configuration conf = new Configuration();
        Path inputPath = new Path(testFilename);
        WARCFileReader fileReader;
        try {
            fileReader = new WARCFileReader(conf, inputPath);
        }
        catch (IOException e1) {
            System.err.println("Error opening " + testFilename);
            e1.printStackTrace();
            return;
        }

        BoilerPlateRemoval boilerPlateRemoval = new JusTextBoilerplateRemoval();
        final CharsetDetector charsetDetector = new ICUCharsetDetectorWrapper();

        long startTime = System.currentTimeMillis();
        long totalBytes = 0;
        long totalTextChar = 0;
        int counter = 0;
        int recordsRead = 0;
        int emptyText = 0;
        int badContentType = 0;

        ListMultimap<String, Long> hashCounts;
        ListMultimap<Long, Long> hashCounts2;
        if (TEST_ORIGINAL) {
            hashCounts = ArrayListMultimap.create();
        }
        else {
            hashCounts2 = ArrayListMultimap.create();
        }

        while (true) {
            try {
                WARCRecord wc = fileReader.read();
                counter++;
                if (counter % 1000 == 0) {
                    System.out.printf(Locale.ENGLISH,
                            "~%.1f entries per second, %d of %d records processed, %d empty responses, %d skipped content types%n",
                            counter * 1000f / (double) (System.currentTimeMillis() - startTime),
                            recordsRead, counter, emptyText, badContentType);
//                    if (counter >= 5000) {
//                        break;
//                    }
                }
                if (!wc.isContentApplicationHttpResponse()) {
                    continue;
                }

                String headers = wc.getHTTPHeaders();
                String contentType = WARCRecord.extractHTTPHeaderContentType(headers);
                if (!ALLOWED_CONTENT_TYPES.contains(contentType)) {
                    // System.out.println("Skipping content type " + contentType);
                    badContentType++;
                    continue;
                }

                byte[] bytes = wc.getContent();
                totalBytes += bytes.length;
                Charset charset;
                // Only use first 8K because that's all ICU will use
                if (TEST_ORIGINAL || bytes.length < 8000) {
                    charset = charsetDetector.detectCharset(bytes);
                }
                else {
                    charset = charsetDetector.detectCharset(
                            Arrays.copyOfRange(bytes, 0, Math.min(8000, bytes.length)));
                }
                String html = new String(bytes, charset);

                // String headerCharset = WARCRecord.extractHTTPHeaderCharset(headers);
                // if (charset == null || !charset.name().toLowerCase().equals(headerCharset)) {
                // System.out.printf("Mismatched charsets: header = %s, detected = %s%n",
                // headerCharset, charset.name());
                // }

                // strip HTTP header - separated from body by blank line
                html = html.substring(html.indexOf("\r\n\r\n") + 4);

                String plainText = boilerPlateRemoval.getPlainText(html, null);
                if (plainText == null || "".equals(plainText)) {
                    emptyText++;
                    continue;
                }
                int length = plainText.length();
                totalTextChar += length;

                String recordID = wc.getHeader().getRecordID();
                String digest = wc.getHeader().getField("WARC-Payload-Digest");

                long simhash;
                if (TEST_ORIGINAL) {
                    simhash = SimHashUtils.getSimHash(plainText);
                    Set<String> slices1 = SimHashUtils.computeHashIndex(simhash);
                    for (String slice : slices1) {
                        hashCounts.put(slice, simhash);
                    }
                }
                else {
                    simhash = SimHashUtils.getSimHash2(plainText);
                    for (long slice : SimHashUtils.sliceHash(simhash)) {
                        hashCounts2.put(slice, simhash);
                    }
                }
                // String precis = plainText.substring(0, Math.min(plainText.length(), 100))
                // .replace("\n", "\\n").replace("\t", "\\t");
                // System.out.printf("%016x %s %s %s%n", simhash, digest, recordID, precis);
                recordsRead++;
            }
            catch (IOException e) {
                if (e instanceof EOFException) {
                    break;
                }
                System.err.println("I/O error reading WARC " + testFilename);
                e.printStackTrace();
            }
        }

        long endTime;

        int nearDuplicates = 0;
        int totalSetCount = 0;
        int exactDuplicates = 0;
        int maxSetSize = 0;
        int dupSets = 0;
        
        if (TEST_ORIGINAL) {
            System.out.printf("%nMost frequent old hash slices & counts - before dedupe%n");
            printHashStatsOld(hashCounts);
            for (List<Long> values : Multimaps.asMap(hashCounts).values()) {
                int dupsInSet = 0;
                Collections.sort(values);
                Long last = null;
                Iterator<Long> iter = values.iterator();
                while (iter.hasNext()) {
                    Long value = iter.next();
                    if (value.equals(last)) {
                        iter.remove();
                        exactDuplicates++;
                        dupsInSet++;
                    } else {
                        last = value;
                        if (dupsInSet > 0) {
                            dupSets++;
                            if (dupsInSet > maxSetSize) {
                                maxSetSize = dupsInSet;
                                System.out.printf("New max set size=%d, value=%016x%n", maxSetSize,
                                        value);
                            }
                            dupsInSet = 0;
                        }
                    }
                }
                // Handle dup set that goes all the way to end of list
                if (dupsInSet > 0) {
                    dupSets++;
                    if (dupsInSet > maxSetSize) {
                        maxSetSize = dupsInSet;
                    }
                    dupsInSet = 0;
                }

                BitSet duplicates = new BitSet(values.size());
                int dupSetCount = 0;
                for (int i = 0; i < values.size(); i++) {
                    boolean dupFound = false;
                    for (int j = i + 1; j < values.size(); j++) {
                        // Skip bitmask check optimization to mimic original algorithm
//                        if (duplicates.get(j)) {
//                            continue;
//                        }
                        long a = values.get(i);
                        long b = values.get(j);
                        @SuppressWarnings("deprecation")
                        int dist = SimHashUtils.diffOfBits(a, b);
                        if (dist < HAMMING_THRESHOLD) {
                            duplicates.set(j);
                            dupFound = true;
                            // System.out.printf("Near duplicate %d distance = %d, %016X %016X%n",
                            // duplicates, dist, a, b);
                            // System.out.printf(" a: %64s%n", Long.toBinaryString(a).replace(' ',
                            // '0'));
                            // System.out.printf(" b: %64s%n", Long.toBinaryString(b).replace(' ',
                            // '0'));
                            // System.out.printf("a^b: %64s%n", Long.toBinaryString(a ^
                            // b).replace('0', ' '));
                        }
                    }
                    if (dupFound) {
                        dupSetCount++;
                    }
                }
                int dupCount = duplicates.cardinality();
                nearDuplicates += dupCount;
                totalSetCount += dupSetCount;

                // if (dupCount > 0) {
                // System.out.printf("%d duplicates in %d sets for %d entries in bucket%n",
                // dupCount, dupSetCount, values.size());
                // }
            }

            endTime = System.currentTimeMillis();

            System.out.printf("Exact duplicates = %d in %d sets, max set size = %d %n",
                    exactDuplicates, dupSets, maxSetSize);
            System.out.printf("Near duplicates = %d in %d sets %n",
                            nearDuplicates, totalSetCount);
            System.out.printf("Most frequent old hash slices & counts%n");
            printHashStatsOld(hashCounts);
        }
        else {
            System.out.printf("%nMost frequent new hash slices & counts - before dedupe%n");
            printHashStatsNew(hashCounts2);

            for (List<Long> values : Multimaps.asMap(hashCounts2).values()) {
                int dupsInSet = 0;
                Collections.sort(values);
                Long last = null;
                Iterator<Long> iter = values.iterator();
                while (iter.hasNext()) {
                    Long value = iter.next();
                    if (value.equals(last)) {
                        iter.remove();
                        exactDuplicates++;
                        dupsInSet++;
                    } else {
                        last = value;
                        if (dupsInSet > 0) {
                            dupSets++;
                            if (dupsInSet > maxSetSize) {
                                maxSetSize = dupsInSet;
                                System.out.printf("New max set size=%d, value=%016x%n", maxSetSize,
                                        value);
                            }
                            dupsInSet = 0;
                        }
                    }
                }
                // Handle dup set that goes all the way to end of list
                if (dupsInSet > 0) {
                    dupSets++;
                    if (dupsInSet > maxSetSize) {
                        maxSetSize = dupsInSet;
                    }
                    dupsInSet = 0;
                }

                BitSet duplicates = new BitSet(values.size());
                int dupSetCount = 0;
                for (int i = 0; i < values.size(); i++) {
                    boolean dupFound = false;
                    for (int j = i + 1; j < values.size(); j++) {
                        // Skip bitmask check optimization to mimic original algorithm
                        long a = values.get(i);
                        long b = values.get(j);
                        int dist = Long.bitCount(a ^ b);
                        if (dist < HAMMING_THRESHOLD) {
                            duplicates.set(j);
                            dupFound = true;
                            // System.out.printf("Near duplicate %d distance = %d, %016X %016X%n",
                            // duplicates, dist, a, b);
                            // System.out.printf(" a: %64s%n", Long.toBinaryString(a).replace(' ',
                            // '0'));
                            // System.out.printf(" b: %64s%n", Long.toBinaryString(b).replace(' ',
                            // '0'));
                            // System.out.printf("a^b: %64s%n", Long.toBinaryString(a ^
                            // b).replace('0', ' '));
                        }
                    }
                    if (dupFound) {
                        dupSetCount++;
                    }
                }
                int dupCount = duplicates.cardinality();
                nearDuplicates += dupCount;
                totalSetCount += dupSetCount;
                // if (dupCount > 0) {
                // System.out.printf("%d duplicates in %d sets for %d entries in bucket%n",
                // dupCount, dupSetCount, values.size());
                // }
            }

            endTime = System.currentTimeMillis();

            System.out.printf("Exact duplicates = %d in %d sets, max set size = %d %n",
                    exactDuplicates, dupSets, maxSetSize);
            System.out.printf("Near duplicates = %d in %d sets %n",
                            nearDuplicates, totalSetCount);
            System.out.printf("%nMost frequent new hash slices & counts - exact dupes removed%n");
            printHashStatsNew(hashCounts2);
        }

        System.out.printf("Processed %d records in %.1f seconds%n", recordsRead,
                (endTime - startTime) / 1000f);
        System.out.printf("Processed %.1f MB, avg %.1f Kbytes/record, avg %.1f Kchar/record%n", totalBytes / 1000000f,
                totalBytes / 1000f / recordsRead, totalTextChar / 1000f / recordsRead);

    }


    private static void printHashStatsOld(ListMultimap<String, Long> hashCounts)
    {
        for (Entry<String> e : Multisets.copyHighestCountFirst(hashCounts.keys()).entrySet()
                .asList().subList(0, 10)) {
            int count = e.getCount();
            System.out.printf("%5d %s%n", count, e.getElement());
        }
        
        int[] counts = hashCounts.keys().entrySet().parallelStream().mapToInt(Multiset.Entry<String>::getCount).toArray();
        IntAccumulator stats = hashCounts.keys().entrySet().parallelStream()
                .collect(IntAccumulator.summarizingIntStdDev(Multiset.Entry<String>::getCount));
        System.out.printf(
                "Old hash slice counts - avg = %.1f, stddev=%.3f (%.3f), min = %d, max = %d, number of slices = %d%n",
                stats.getAverage(), stats.getSampleStdDev(), stddev(counts), stats.getMin(),
                stats.getMax(), stats.getCount());
    }

    private static void printHashStatsNew(ListMultimap<Long, Long> hashCounts2)
    {
        for (Entry<Long> e : Multisets.copyHighestCountFirst(hashCounts2.keys()).entrySet()
                .asList().subList(0, 10)) {
            int count = e.getCount();
            System.out.printf("%5d %016x%n", count, e.getElement());
        }
        
        int[] counts = hashCounts2.keys().entrySet().parallelStream().mapToInt(Multiset.Entry<Long>::getCount).toArray();
        IntAccumulator stats = hashCounts2.keys().entrySet().parallelStream()
                .collect(IntAccumulator.summarizingIntStdDev(Multiset.Entry<Long>::getCount));
        System.out.printf(
                "New hash slice counts - avg = %.1f, stddev=%.3f (%.3f), min = %d, max = %d, number of slices = %d%n",
                stats.getAverage(), stats.getSampleStdDev(), stddev(counts), stats.getMin(),
                stats.getMax(), stats.getCount());
    }
    
    private static double stddev(int[] samples) {
        long sum = 0;
        double m2 = 0;
        for (int i : samples) {
            sum += i;
        }
        double mean = sum * 1.0 / samples.length;
        for (int i : samples) {
            double delta = i - mean;
            m2 += delta * delta;
        }
        return m2 / (samples.length);
    }

    public static void main(String[] args)
    {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.WARN); // turn down chatty Hadoop logging

        timeHamming();

        String testFilename = "/Users/tfmorris/Downloads/commoncrawl/crawls/CC-MAIN-2015-40/CC-MAIN-20151124205408-00105-ip-10-71-132-137.ec2.internal.warc.gz";
        if (args.length < 1 || args[0] == null) {
            System.out.println("No input test filename - using default");
        }

        timeTextProcessing(testFilename);
    }

    private class DocInfo implements Comparable<DocInfo>{
        private String docId;
        private String digest;
        private long hash;
        private int length;

        DocInfo(String docId, String digest, long hash, int length) {
            this.docId = docId;
            this.digest = digest;
            this.hash = hash;
            this.length = length;
        }

        @Override
        public int compareTo(DocInfo other)
        {
            if (this.hash == other.hash) {
                // Inverted comparison to return biggest first
                return other.length - this.length;
            }
            return (int)(this.hash - other.hash);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof DocInfo)) {
                return false;
            }
            DocInfo docInfo = (DocInfo) other;
            return this.hash == docInfo.hash && this.length == docInfo.length;
        }

    }
    
    /**
     * Online accumulator which extends the Java 8 IntSummaryStatistics class
     * to also do variance and standard deviation using Welford's algorithm.
     * 
     * @author Tom Morris <tfmorris@gmail.com>
     *
     */
    static class IntAccumulator extends IntSummaryStatistics {
        private double mean = 0.0; // our online mean estimate
        private double m2 = 0.0;

        @Override
        public void accept(int value)
        {
            super.accept(value);
            double delta = value - mean;
            mean += delta / this.getCount(); // getCount() too inefficient?
            m2 += delta * (value - mean);
        }

        @Override
        public void combine(IntSummaryStatistics other) {
            // TODO: What's the right answer here? Just throw or attempt to cast?
            combine((IntAccumulator) other);
        };

        public void combine(IntAccumulator other)
        {
            long count = getCount(); // get the old count before we combined
            long otherCount = other.getCount();
            double totalCount = count + otherCount;
            super.combine(other);
            // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
            double delta = other.getMeanEstimate() - mean;
//            mean += delta * (otherCount / totalCount);
            mean = (mean * count + other.getMeanEstimate() * otherCount) / totalCount;
            m2 += other.getSquareSum() + ((delta * delta) * count * otherCount / totalCount);
        }

        private double getSquareSum()
        {
            return m2;
        }

        /**
         * Returns the online version of the mean which may be less accurate,
         * but won't overflow like the version kept by {@link #getAverage()}.
         * 
         * @return 
         */
        public double getMeanEstimate() {
            return mean;
        }

        public double getSampleVariance() {
            long count = getCount();
            if (count < 2) {
                return 0.0;
            } else {
                return m2 / (getCount() - 1); // sample variance N-1
            }
        }

        public double getSampleStdDev() {
            return Math.sqrt(getSampleVariance());
        }

        public static <T>
        Collector<T, ?, IntAccumulator> summarizingIntStdDev(ToIntFunction<? super T> mapper) {
            return new CollectorImpl<T, IntAccumulator, IntAccumulator>(
                    IntAccumulator::new,
                    (r, t) -> r.accept(mapper.applyAsInt(t)),
                    (l, r) -> { l.combine(r); return l; }, CollectorImpl.CH_ID);
    }

    }
    
    /**
     * Private copy of {@link Collectors.CollectorImpl} that we can use to get
     * around visibility restrictions.
     *
     * @param <T>
     * @param <A>
     * @param <R>
     */
    static class CollectorImpl<T, A, R> implements Collector<T, A, R> {
        static final Set<Collector.Characteristics> CH_ID
        = Collections.unmodifiableSet(EnumSet.of(Collector.Characteristics.IDENTITY_FINISH));

        private final Supplier<A> supplier;
        private final BiConsumer<A, T> accumulator;
        private final BinaryOperator<A> combiner;
        private final Function<A, R> finisher;
        private final Set<Characteristics> characteristics;

        CollectorImpl(Supplier<A> supplier,
                      BiConsumer<A, T> accumulator,
                      BinaryOperator<A> combiner,
                      Function<A,R> finisher,
                      Set<Characteristics> characteristics) {
            this.supplier = supplier;
            this.accumulator = accumulator;
            this.combiner = combiner;
            this.finisher = finisher;
            this.characteristics = characteristics;
        }

        CollectorImpl(Supplier<A> supplier,
                      BiConsumer<A, T> accumulator,
                      BinaryOperator<A> combiner,
                      Set<Characteristics> characteristics) {
            this(supplier, accumulator, combiner, castingIdentity(), characteristics);
        }

        @Override
        public BiConsumer<A, T> accumulator() {
            return accumulator;
        }

        @Override
        public Supplier<A> supplier() {
            return supplier;
        }

        @Override
        public BinaryOperator<A> combiner() {
            return combiner;
        }

        @Override
        public Function<A, R> finisher() {
            return finisher;
        }

        @Override
        public Set<Characteristics> characteristics() {
            return characteristics;
        }
        
        @SuppressWarnings("unchecked")
        private static <I, R> Function<I, R> castingIdentity() {
            return i -> (R) i;
        }
    }


}
