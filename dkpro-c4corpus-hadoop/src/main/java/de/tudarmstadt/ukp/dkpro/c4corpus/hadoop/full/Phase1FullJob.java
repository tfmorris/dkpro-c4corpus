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
        WARC_RESPONSE_TOO_BIG,
        WARC_NOT_HTTP_RESPONSE,
        WARC_WRONG_CONTENT_TYPE,
        WARC_EMPTY_TEXT,
        WARC_OUTPUT_RECORDS,
        SIMHASH_HASH_DIFFERENT_LANGUAGE,
        SIMHASH_EXACT_DUPLICATE,
        SIMHASH_NEAR_DUPLICATE,
        SIMHASH_UNIQ_SLICES,
        SIMHASH_CANDIDATE_NOT_DUPLICATE,
    };

    @Override
    public int run(String[] args)
            throws Exception
    {
        Job job = Job.getInstance(getConf());
        // set from the command line

        job.setJarByClass(Phase1FullJob.class);
        job.setJobName(Phase1FullJob.class.getName());

        // mapper
        job.setMapperClass(MapperClass.class);

        // reducer
        job.setReducerClass(SimhashSimilarityReducer.class);

        // input-output is warc
        job.setInputFormatClass(WARCInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // mapper output data
        job.setMapOutputKeyClass(LongWritable.class);
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

    public static class MapperClass
            extends Mapper<LongWritable, WARCWritable, LongWritable, Text>
    {

        private final static CharsetDetector CHARSET_DETECTOR = new ICUCharsetDetectorWrapper();
        private final static LicenseDetector LICENSE_DETECTOR = new FastRegexLicenceDetector();
        private final static BoilerPlateRemoval BOILER_PLATE_REMOVAL = new JusTextBoilerplateRemoval();
        private final static LanguageIdentifier LANGUAGE_IDENTIFIER = new CybozuLanguageIdentifier();

        private long recordCounter = 0;

        private long sizeCounter = 0;

        // logger
        private static final Log LOG = LogFactory.getLog(MapperClass.class);

        // utf-8 charset
        private static final Charset UTF8_CHARSET = Charset.forName("utf-8");

        // only meaningful html pages
        private static final Set<String> ALLOWED_CONTENT_TYPES = new HashSet<>(
                Arrays.asList("text/html", "application/xhtml+xml"));

        // mapper parameter
        private boolean keepMinimalHTML;
        private MultipleOutputs<LongWritable, Text> mos;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException
        {
            super.setup(context);
            mos = new MultipleOutputs<LongWritable, Text>(context);

            // parametrize the mapper
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
                context.getCounter(C4_COUNTER.WARC_RESPONSE_TOO_BIG).increment(1);
                return true;
            }

            // we're only interested in processing the responses, not requests or metadata
            if (!value.getRecord().isContentApplicationHttpResponse()) {
                context.getCounter(C4_COUNTER.WARC_NOT_HTTP_RESPONSE).increment(1);
                return true;
            }

            // HTTP header in CommonCrawl is delimited by newline
            String httpHeaderText = value.getRecord().getHTTPHeaders();

            // we're only interested in text/html
            if (httpHeaderText == null) {
                return true;
            }

            String contentType = WARCRecord.extractHTTPHeaderContentType(httpHeaderText);
            if (!ALLOWED_CONTENT_TYPES.contains(contentType)) {
                context.getCounter(C4_COUNTER.WARC_WRONG_CONTENT_TYPE).increment(1);
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
                context.getCounter(C4_COUNTER.WARC_EMPTY_TEXT).increment(1);
                return;
            }

            // keeping the location and ID of the original file in HDFS in header meta-data
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            final String origFile = inputSplit.getPath().toString();

            // language identification
            final String language = LANGUAGE_IDENTIFIER.identifyLanguage(plainText);

            // compute simhash
            long docSimHash = SimHashUtils.getSimHash(plainText);

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
                    docSimHash, language).toString();
            for (long slice : SimHashUtils.sliceHash(docSimHash)) {
                context.write(new LongWritable(slice), new Text(metadata));
            }

            // Our text-only WARC goes to a non-reduce mapper output on the side
            String baseName = inputSplit.getPath().getName().replace(".warc.gz", "");
            mos.write("textWARC",NullWritable.get(), value, baseName);

            context.getCounter(C4_COUNTER.WARC_OUTPUT_RECORDS).increment(1);
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
                Mapper<LongWritable, WARCWritable, LongWritable, Text>.Context context)
            throws IOException, InterruptedException
        {
            mos.close();
            super.cleanup(context);
        }
    }

    /**
     * Coalesce hashes with matching slices
     */
    public static class SimhashSimilarityReducer
        extends Reducer<LongWritable, Text, LongWritable, Text>
    {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
        {
            // Collect all docs with same Simhash slice value (our key)
            List<DocumentInfo> docs = new ArrayList<DocumentInfo>();
            for (Text docString : values) {
                DocumentInfo doc = new DocumentInfo(docString.toString());
                docs.add(doc);
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

            // Remove all exact matches and non-candidates so we don't need to consider them in the
            // O(n^2) phase
            DocumentInfo head = null;
            long headHash = 0;
            Iterator<DocumentInfo> docIterator = docs.iterator();
            while (docIterator.hasNext()) {
                DocumentInfo doc = docIterator.next();
                if (head == null || !doc.getDocSimHash().equals(head.getDocSimHash())) {
                    head = doc;
                    headHash = doc.getDocSimHash().get();
                    context.getCounter(C4_COUNTER.SIMHASH_UNIQ_SLICES).increment(1);
                }
                else if (!doc.getDocLang().equals(head.getDocLang())) {
                    context.getCounter(C4_COUNTER.SIMHASH_HASH_DIFFERENT_LANGUAGE).increment(1);
                    docIterator.remove();
                }
                else if (doc.getDocSimHash().get() == headHash) {
                    context.getCounter(C4_COUNTER.SIMHASH_EXACT_DUPLICATE).increment(1);
                    context.write(doc.getDocSimHash(),
                            new Text("exact|" + head.toString() + "|" + doc.toString()));
                    docIterator.remove();
                }
            }

            // Pairwise comparison O(n^2) of similarity for remaining candidates
            for (int i = 0; i < docs.size(); i++) {
                head = docs.get(i);
                headHash = head.getDocSimHash().get();
                // TODO: Handle scanning duplicates multiple times?
                for (int j = i + 1; j < docs.size(); j++) {
                    DocumentInfo doc = docs.get(j);
                    long hammingDist = Long.bitCount(doc.getDocSimHash().get() ^ headHash);
                    if (hammingDist < SimHashUtils.HAMMING_DISTANCE_THRESHOLD) {
                        // TODO: Do we want to consider length and disqualify near matches with very
                        // different lengths?
                        context.getCounter(C4_COUNTER.SIMHASH_NEAR_DUPLICATE).increment(1);
                        String output = String.format("near%3d|%s|%s", hammingDist, head.toString(),
                                doc.toString());
                        // TODO: Do we want to split output to separate files by match type?
                        context.write(doc.getDocSimHash(), new Text(output));
                    }
                    else {
                        context.getCounter(C4_COUNTER.SIMHASH_CANDIDATE_NOT_DUPLICATE).increment(1);
                        // We output these for statistical purposes, but this can be skipped
                        String output = String.format("nomatch%3d|%s|%s", hammingDist,
                                head.toString(), doc.toString());
                        context.write(doc.getDocSimHash(), new Text(output));
                    }
                }
            }
        }
    }
}
