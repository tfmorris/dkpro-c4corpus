/*
 * Copyright 2016
 * Ubiquitous Knowledge Processing (UKP) Lab
 * Technische Universität Darmstadt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.tudarmstadt.ukp.dkpro.c4corpus.hadoop.full;

import de.tudarmstadt.ukp.dkpro.c4corpus.boilerplate.BoilerPlateRemoval;
import de.tudarmstadt.ukp.dkpro.c4corpus.boilerplate.impl.JusTextBoilerplateRemoval;
import de.tudarmstadt.ukp.dkpro.c4corpus.deduplication.impl.ParallelDocumentDeDuplication;
import de.tudarmstadt.ukp.dkpro.c4corpus.deduplication.impl.SimHashUtils;
import de.tudarmstadt.ukp.dkpro.c4corpus.hadoop.CharsetDetector;
import de.tudarmstadt.ukp.dkpro.c4corpus.hadoop.LanguageIdentifier;
import de.tudarmstadt.ukp.dkpro.c4corpus.hadoop.deduplication.DocumentInfo;
import de.tudarmstadt.ukp.dkpro.c4corpus.hadoop.impl.CybozuLanguageIdentifier;
import de.tudarmstadt.ukp.dkpro.c4corpus.hadoop.impl.ICUCharsetDetectorWrapper;
import de.tudarmstadt.ukp.dkpro.c4corpus.hadoop.io.WARCInputFormat;
import de.tudarmstadt.ukp.dkpro.c4corpus.hadoop.io.WARCOutputFormat;
import de.tudarmstadt.ukp.dkpro.c4corpus.hadoop.io.WARCWritable;
import de.tudarmstadt.ukp.dkpro.c4corpus.license.LicenseDetector;
import de.tudarmstadt.ukp.dkpro.c4corpus.license.impl.FastRegexLicenceDetector;
import de.tudarmstadt.ukp.dkpro.c4corpus.warc.io.WARCRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Single Map-Reduce task for performing license identification, boilerplate
 * removal, language identification and sim hashing. Only non-empty texts after
 * boilerplate removal are kept.
 * <br>
 * Configuration parameters
 * {@code c4corpus.keepminimalhtml} - boolean (keep minimal html in boilerplate removal?)
 *
 * @author Omnia Zayed
 * @author Ivan Habernal
 */
public class Phase1FullJob
        extends Configured
        implements Tool
{
    public static enum C4_COUNTER
    {
        M1_WARC_RESPONSE_TOO_BIG,
        M2_WARC_NOT_HTTP_RESPONSE,
        M3_NO_HTTP_HEADER,
        M4_WARC_WRONG_CONTENT_TYPE,
        M5_WARC_EMPTY_TEXT,
        M6_WARC_OUTPUT_RECORDS,
        R_SIMHASH_HASH_DIFFERENT_LANGUAGE,
        R_SIMHASH_EXACT_DUPLICATE,
        R_SIMHASH_NEAR_DUPLICATE,
        R_SIMHASH_CANDIDATE_NOT_DUPLICATE,
        R_SIMHASH_NEAR_DUPLICATE_DIFF_LANG,
        R_SIMHASH_HASH_DIFFERENT_LENGTH,
        R_SIMHASH_COMPARISONS, 
        R_TOO_MANY_COMPARISONS_ABORT,
    };

    public static enum C4_HAMMING_DIST
    {
        D00, D01, D02, D03, D04, D05, D06, D07, D08, D09, D10, D11, D12, D13, D14, D15, D16, D17, D18, D19, D20, D21, D22, D23, D24, D25, D26, D27, D28, D29, D30, D31, D32
    }

    public static final long COMPARISON_LIMIT = 100*1000*1000; // enough for 30K docs (after exact matches filtered)

    public static C4_HAMMING_DIST[] HAMMING_ENUMS = C4_HAMMING_DIST.values();

    @Override
    public int run(String[] args)
            throws Exception
    {
        Job job = Job.getInstance(getConf());
        // set from the command line

        job.setJarByClass(Phase1FullJob.class);
        job.setJobName(Phase1FullJob.class.getName());

        // mapper
        job.setMapperClass(WARCToTextMapper.class);

        // reducer
        job.setReducerClass(SimhashSimilarityReducer.class);

        // input-output is warc
        job.setInputFormatClass(WARCInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // mapper output data
        job.setMapOutputKeyClass(LongWritable2.class);
        job.setMapOutputValueClass(Text.class);

        // set output compression to GZip
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        // Side channel (non-reducer) output for mapper to write text-only WARC to
        MultipleOutputs.addNamedOutput(job, "textWARC", WARCOutputFormat.class, NullWritable.class, WARCWritable.class);

        if (args.length < 2) {
            throw new IllegalArgumentException(
                    "Two command parameters required: input paths & output directory");
        }
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args)
            throws Exception
    {
        ToolRunner.run(new Phase1FullJob(), args);
    }

    /**
     * Our version of LongWritable with a new hashCode() implementation which
     * produces less uneven output to reducers. Rather than just truncating to
     * the lower order 32-bits, it folds the high & low 32-bits together and
     * uses the combined value.
     *
     */
    public static class LongWritable2 extends LongWritable {

        public LongWritable2() {
            super();
        }

        public LongWritable2(long value)
        {
            super(value);
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.io.LongWritable#hashCode()
         * 
         * This implementation produces more balanced reducer load when 
         * simhash slice has all the lower bits masked to zero.
         */
        @Override
        public int hashCode()
        {
            Long value = super.get();
            return (int) ((value & 0xffffffff) ^ (value >> 32));
        }
    }

    public static class WARCToTextMapper
            extends Mapper<LongWritable, WARCWritable, LongWritable2, Text>
    {

        private final static CharsetDetector CHARSET_DETECTOR = new ICUCharsetDetectorWrapper();
        private final static LicenseDetector LICENSE_DETECTOR = new FastRegexLicenceDetector();
        private final static BoilerPlateRemoval BOILER_PLATE_REMOVAL = new JusTextBoilerplateRemoval();
        private final static LanguageIdentifier LANGUAGE_IDENTIFIER = new CybozuLanguageIdentifier();

        private long recordCounter = 0;

        private long sizeCounter = 0;

        // logger
        private static final Log LOG = LogFactory.getLog(WARCToTextMapper.class);

        // utf-8 charset
        private static final Charset UTF8_CHARSET = Charset.forName("utf-8");

        // only meaningful html pages
        private static final Set<String> ALLOWED_CONTENT_TYPES = new HashSet<>(
                Arrays.asList("text/html", "application/xhtml+xml"));

        // mapper parameter
        private boolean keepMinimalHTML;
        private MultipleOutputs<LongWritable2, Text> mos;
        private long startTime;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException
        {
            super.setup(context);
            startTime = System.currentTimeMillis();
            mos = new MultipleOutputs<LongWritable2, Text>(context);

            // parameterize the mapper
            this.keepMinimalHTML = context.getConfiguration()
                    .getBoolean("c4corpus.keepminimalhtml", false);
        }

        /**
         * Checks whether the given WARC record should be ignored; this applies for documents
         * longer than 10 MB and documents that are not text/html
         *
         * @param value WARC record
         * @return true if ignored, false otherwise
         * @throws IOException I/O exception
         */
        protected static boolean ignoreWARCRecord(Context context, WARCWritable value)
                throws IOException
        {
            // avoid documents bigger than 10 MB as in ClueWeb12
            int contentLength = value.getRecord().getHeader().getContentLength();
            if (contentLength >= 10000000) {
                // This never gets triggered because CommonCrawl caps at 1 MB currently
                context.getCounter(C4_COUNTER.M1_WARC_RESPONSE_TOO_BIG).increment(1);
                return true;
            }

            // we're only interested in processing the responses, not requests or metadata
            if (!value.getRecord().isContentApplicationHttpResponse()) {
                context.getCounter(C4_COUNTER.M2_WARC_NOT_HTTP_RESPONSE).increment(1);
                return true;
            }

            // HTTP header in CommonCrawl is delimited by newline
            String httpHeaderText = value.getRecord().getHTTPHeaders();

            // we're only interested in text/html
            if (httpHeaderText == null) {
                context.getCounter(C4_COUNTER.M3_NO_HTTP_HEADER).increment(1);
                return true;
            }

            String contentType = WARCRecord.extractHTTPHeaderContentType(httpHeaderText);
            if (!ALLOWED_CONTENT_TYPES.contains(contentType)) {
                context.getCounter(C4_COUNTER.M4_WARC_WRONG_CONTENT_TYPE).increment(1);
                return true;
            }

            // we accept the page
            return false;
        }

        /**
         * Extracts HTML from the CommonCrawl WARC record with correctly identified encoding and
         * stripped the leading HTTP header
         *
         * @param value WARC record
         * @return HTML as string
         */
        protected String extractHTML(WARCWritable value)
        {
            // detect charset
            byte[] bytes = value.getRecord().getContent();
            Charset charset = CHARSET_DETECTOR.detectCharset(bytes);

            String html = new String(bytes, charset);

            // strip HTTP header
            return html.substring(html.indexOf("\r\n\r\n") + 4);
        }

        @Override
        protected void map(LongWritable key, WARCWritable value, Context context)
                throws IOException, InterruptedException
        {
            // check first if it's worth processing
            if (ignoreWARCRecord(context, value)) {
                return;
            }

            // extract HTML
            String html = extractHTML(value);

            // license detection
            String license = LICENSE_DETECTOR.detectLicence(html);

            // boilerplate removal
            String plainText;
            if (this.keepMinimalHTML) {
                plainText = BOILER_PLATE_REMOVAL.getMinimalHtml(html, null);
            }
            else {
                plainText = BOILER_PLATE_REMOVAL.getPlainText(html, null);
            }

            // skip empty documents
            if (plainText.isEmpty()) {
                context.getCounter(C4_COUNTER.M5_WARC_EMPTY_TEXT).increment(1);
                return;
            }

            // keeping the location and ID of the original file in HDFS in header meta-data
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            final String origFile = inputSplit.getPath().toString();

            // language identification
            final String language = LANGUAGE_IDENTIFIER.identifyLanguage(plainText);

            // compute simhash
            long docSimHash = SimHashUtils.getSimHash2(plainText);

            WARCRecord.Header header = value.getRecord().getHeader();

            // original warc split location
            header.setField(WARCRecord.WARCRecordFieldConstants.ORIGINAL_LOCATION, origFile);

            // set the license to the metadata
            header.setField(WARCRecord.WARCRecordFieldConstants.LICENSE, license);

            //set the language to meta data
            header.setField(WARCRecord.WARCRecordFieldConstants.LANGUAGE, language);

            // add info about boilerplate removal
            String noBoilerplate = Boolean.TRUE.toString();
            header.setField(WARCRecord.WARCRecordFieldConstants.NO_BOILERPLATE, noBoilerplate);

            // minimal html tag
            String minimalHtml = Boolean.valueOf(this.keepMinimalHTML).toString();
            header.setField(WARCRecord.WARCRecordFieldConstants.MINIMAL_HTML, minimalHtml);

            // add simhash
            header.setField(WARCRecord.WARCRecordFieldConstants.SIMHASH, Long.toString(docSimHash));

            // replace the content with the plain text
            value.getRecord().setContent(plainText);

            // warning: never call getBytes() without specifying charset; will behave
            // differently on different computers (due to default locales!!!)
            byte[] plainTextBytes = plainText.getBytes(UTF8_CHARSET);
            header.setField("Content-Length", String.valueOf(plainTextBytes.length));

            // Our primary output (although smaller in size) goes to the primary output so it gets
            // fed to the reducers
            String metadata = new DocumentInfo(header.getRecordID(), plainTextBytes.length,
                    docSimHash, language, license, noBoilerplate, minimalHtml).toString();
            for (long slice : SimHashUtils.sliceHash(docSimHash)) {
                context.write(new LongWritable2(slice), new Text(metadata));
            }

            // Our text-only WARC goes to a non-reduce mapper output on the side
            String segmentDir = inputSplit.getPath().getParent().getName();
            String warcName = inputSplit.getPath().getName().replace(".warc.gz", "");
            mos.write("textWARC",NullWritable.get(), value, segmentDir + "/" + warcName);

            context.getCounter(C4_COUNTER.M6_WARC_OUTPUT_RECORDS).increment(1);
            // collect some stats to logs
            recordCounter++;
            sizeCounter += plainText.length();
            if ((recordCounter % 1000) == 0) {
                LOG.info(String.format("Processed %d records, total length %d characters",
                        recordCounter, sizeCounter));
            }
        }

        @Override
        protected void cleanup(
                Mapper<LongWritable, WARCWritable, LongWritable2, Text>.Context context)
            throws IOException, InterruptedException
        {
            mos.close();
            super.cleanup(context);
            long elapsedMillis = System.currentTimeMillis() - startTime;
            LOG.info(String.format("Mapper complete - %d records totaling %.2f MB in %.2f minutes",
                    recordCounter, sizeCounter / 1000000., elapsedMillis / 1000. / 60));
        }
    }

    /**
     * Coalesce hashes with matching slices
     */
    public static class SimhashSimilarityReducer
        extends Reducer<LongWritable, Text, LongWritable, Text>
    {
        private long keyCount = 0;
        private long maxDocs = 0;
        private long maxDocsKey = -1L;
        private int maxSimilarDocs = 0;
        private long maxSimilarDocsKey = -1L;
        private boolean keySeen = false;
        private long totalComparisons = 0;
        private long startTime;
        private static final Log LOG = LogFactory.getLog(SimhashSimilarityReducer.class);

        @Override
        protected void setup(
                org.apache.hadoop.mapreduce.Reducer<LongWritable, Text, LongWritable, Text>.Context context)
            throws IOException, InterruptedException
        {
            super.setup(context);
            LOG.info("Reducer setup starting");
            keySeen = false;
        };

        @Override
        protected void cleanup(
                org.apache.hadoop.mapreduce.Reducer<LongWritable, Text, LongWritable, Text>.Context context)
            throws IOException, InterruptedException
        {
            long elapsedMillis = System.currentTimeMillis() - startTime;
            LOG.info(String.format(
                    "Reducer complete - %d msec elapsed, key count=%d, max values = %d, key for max values = %016x, total comparisons = %d",
                    elapsedMillis, keyCount, maxDocs, maxDocsKey, totalComparisons));
            super.cleanup(context);
        };

        /**
         * TODO: Compare this to
         * {@link ParallelDocumentDeDuplication#selectIDsToDelete(List)} to make
         * sure we haven't left anything out
         */
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
        {
            keyCount++;
            if (!keySeen) {
                LOG.info(String.format("First key seen - key=%016x", maxDocsKey));
                keySeen = true;
                startTime = System.currentTimeMillis();
            }

            // Collect all docs with same Simhash slice value (our key)
            List<DocumentInfo> docs = new ArrayList<DocumentInfo>();
            // TODO: Do we need to cap this for memory reasons? We've seen at least 723K docs in 10% sample run
            for (Text docString : values) {
                DocumentInfo doc = new DocumentInfo(docString.toString());
                docs.add(doc);
            }

            int docCount = docs.size(); // save starting count
            if (docCount > maxDocs) {
                maxDocs = docs.size();
                maxDocsKey = key.get();
                LOG.debug(String.format("New max document count - key=%016x, count=%d", maxDocsKey, maxDocs));
            }

            // Sort by full Simhash value and descending size
            Collections.sort(docs, new Comparator<DocumentInfo>()
            {
                @Override
                public int compare(DocumentInfo o1, DocumentInfo o2)
                {
                    if (o1.getDocSimHash() == o2.getDocSimHash()) {
                        // biggest first
                        return o2.getDocLength().compareTo(o1.getDocLength());
                    }
                    else {
                        return o1.getDocSimHash().compareTo(o2.getDocSimHash());
                    }
                }
            });

            // Remove all exact matches and non-candidates so we don't need 
            // to consider them in the O(n^2) phase
            DocumentInfo head = null;
            long headHash = 0;
            Iterator<DocumentInfo> docIterator = docs.iterator();
            while (docIterator.hasNext()) {
                DocumentInfo doc = docIterator.next();
                if (head == null || doc.getDocSimHash().get() != headHash) {
                    head = doc;
                    headHash = doc.getDocSimHash().get();
                }
                else if (!doc.getDocLang().equals(head.getDocLang())) {
                    // False positive due to hash collision
                    context.getCounter(C4_COUNTER.R_SIMHASH_HASH_DIFFERENT_LANGUAGE).increment(1);
                    docIterator.remove();
                }
                else if (!doc.getDocLength().equals(head.getDocLength())) {
                    // False positive due to hash collision
                    context.getCounter(C4_COUNTER.R_SIMHASH_HASH_DIFFERENT_LENGTH).increment(1);
                    docIterator.remove();
                }
                else if (doc.getDocSimHash().get() == headHash) { // TODO: Didn't we already test this?
                    context.getCounter(C4_COUNTER.R_SIMHASH_EXACT_DUPLICATE).increment(1);
                    // TODO: Do we want to split output to separate files by match type?
                    context.write(doc.getDocSimHash(),
                            new Text("exact|" + head.toString() + "|" + doc.toString()));
                    docIterator.remove();
                }
            }

            // See how many docs we have left after exact dupes were removed
            if (docs.size() > maxSimilarDocs) {
                maxSimilarDocs = docs.size();
                maxSimilarDocsKey = key.get();
                LOG.debug(String.format("New max similar document count - key=%016x, count=%d",
                        maxSimilarDocsKey, maxSimilarDocs));
            }

            // Pairwise comparison O(n^2) of similarity for remaining candidates
            // TODO: re-slice into smaller blocks if too many candidates? (slices only match 16 bits out of 64)
            BitSet duplicates = new BitSet(docs.size());
            long comparisons = 0;
            for (int i = 0; i < docs.size(); i++) {
                if (comparisons > COMPARISON_LIMIT) {
                    LOG.warn(String.format("Exceeded comparison limit %d for key %016x after %5d docs. N=%6d for N^2 phase",
                            comparisons, key.get(), i, docs.size()));
                    context.getCounter(C4_COUNTER.R_TOO_MANY_COMPARISONS_ABORT).increment(1);
                    break;
                }
                if (duplicates.get(i)) {
                    continue; // already a duplicate - no need to process again
                }
                head = docs.get(i);
                headHash = head.getDocSimHash().get();
                for (int j = i + 1; j < docs.size(); j++) {
                    if (duplicates.get(i)) {
                        continue;
                    }
                    DocumentInfo doc = docs.get(j);
                    int hammingDist = Long.bitCount(doc.getDocSimHash().get() ^ headHash);
                    comparisons++;
                    if (hammingDist < SimHashUtils.HAMMING_DISTANCE_THRESHOLD) {
                        // Skip if it has a different language (false positive)
                        if (!doc.getDocLang().equals(head.getDocLang())) {
                            context.getCounter(C4_COUNTER.R_SIMHASH_NEAR_DUPLICATE_DIFF_LANG)
                                    .increment(1);
                            continue;
                        }

                        // TODO: Do we want to consider length and disqualify near matches with very
                        // different lengths?

                        duplicates.set(j);
                        context.getCounter(C4_COUNTER.R_SIMHASH_NEAR_DUPLICATE).increment(1);
                        String output = String.format("near%3d|%s|%s", hammingDist, head.toString(),
                                doc.toString());
                        // TODO: Do we want to split output to separate files by match type?
                        context.write(doc.getDocSimHash(), new Text(output));
                    }
                    else {
                        context.getCounter(C4_COUNTER.R_SIMHASH_CANDIDATE_NOT_DUPLICATE).increment(1);
                        // Keep histogram of Hamming distances 
                        if (hammingDist < HAMMING_ENUMS.length) {
                            context.getCounter(HAMMING_ENUMS[hammingDist]).increment(1);
                        }
                    }
                }
            }
            context.getCounter(C4_COUNTER.R_SIMHASH_COMPARISONS).increment(comparisons);
            totalComparisons += comparisons;
        }
    }
}
