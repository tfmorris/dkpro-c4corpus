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

import de.tudarmstadt.ukp.dkpro.c4corpus.deduplication.impl.SimHashUtils;
import de.tudarmstadt.ukp.dkpro.c4corpus.hadoop.deduplication.DocumentInfo;
import de.tudarmstadt.ukp.dkpro.c4corpus.hadoop.io.WARCInputFormat;
import de.tudarmstadt.ukp.dkpro.c4corpus.hadoop.io.WARCOutputFormat;
import de.tudarmstadt.ukp.dkpro.c4corpus.hadoop.io.WARCWritable;
import de.tudarmstadt.ukp.dkpro.c4corpus.warc.io.WARCRecord;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;

/**
 * Accepts two sets of inputs: 1) text documents containing lists of duplicates and near duplicates
 * to be filtered (deleted) and 2) a set of input WARCs.
 * <ul>
 * <li> arg0 text files containing list of document pairs
 * <li> arg1 text-only WARCs to be filtered
 * <li> arg2 is the output
 * </ul>
 * 
 * Because of the way the pairs of documents are generated, there is duplication in the lists
 * so they need to be sorted and deduplicated as part of the map process.
 * 
 * Input format is:
 * <pre>
 * {simhash} exact|{headDocString>}|{duplicateDocString}
 * {simhash} near {hammingDistance}|{headDocString>}|{duplicateDocString}
 * </pre>
 * There is additional information included to allow analysis of the duplicates, but all we need at
 * this stage is the document ID from the duplicateDocString.
 * 
 * <br>
 * Based on and replaces the old Phase 4 & Phase 5 jobs.
 *
 * @author Omnia Zayed
 * @author Ivan Habernal
 */
public class Phase2RemoveDuplicatesUsingReduceSideJoins
        extends Configured
        implements Tool
{
    
    public enum C4P2_COUNTER {
        REDUCE_OUTPUT_RECORDS, REDUCE_NULL_LANGUAGE, REDUCE_FILTERED_DUPLICATES, MAP_NEAR, MAP_EXACT
    }


    @Override
    public int run(String[] args)
            throws Exception
    {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(Phase2RemoveDuplicatesUsingReduceSideJoins.class);
        job.setJobName(Phase2RemoveDuplicatesUsingReduceSideJoins.class.getName());

        //first input the look up text file of ids to be deleted
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,
                JoinTextMapper.class);
        //second input the list of WARCs (check comma separated availability)  *.warc.gz
        MultipleInputs.addInputPath(job, new Path(args[1]), WARCInputFormat.class,
                JoinWARCMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WARCWritable.class);

        job.setReducerClass(DuplicateReducer.class);

        job.setOutputFormatClass(WARCOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(WARCWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args)
            throws Exception
    {
        ToolRunner.run(new Phase2RemoveDuplicatesUsingReduceSideJoins(), args);
    }


    public static class JoinWARCMapper
            extends Mapper<LongWritable, WARCWritable, Text, WARCWritable>
    {

        @Override
        protected void map(LongWritable key, WARCWritable value, Context context)
                throws IOException, InterruptedException
        {
            WARCRecord.Header header = value.getRecord().getHeader();
            String warcId = header.getRecordID();
            String license = header.getField(WARCRecord.WARCRecordFieldConstants.LICENSE);
            String language = header.getField(WARCRecord.WARCRecordFieldConstants.LANGUAGE);
            String noBoilerplate = header
                    .getField(WARCRecord.WARCRecordFieldConstants.NO_BOILERPLATE);
            String minimalHtml = header.getField(WARCRecord.WARCRecordFieldConstants.MINIMAL_HTML);
            String reduceKey = WARCWriterReducerClass.createKey(license, language, noBoilerplate, minimalHtml, warcId) ;
            context.write(new Text(reduceKey), value);
        }
    }

    public static class JoinTextMapper
            extends Mapper<LongWritable, Text, Text, WARCWritable>
    {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String[] valuePieces = value.toString().split("\\t")[1].split("\\|");
            String matchType = valuePieces[0]; // exact or near %3d with Hamming distance
            int matchDistance = 0;
            if (matchType.startsWith("near")) {
                matchDistance = Integer.parseInt(matchType.split("\\s+")[1]);
                context.getCounter(C4P2_COUNTER.MAP_NEAR).increment(1);
            } else if ("exact".equals(matchType)) {
                context.getCounter(C4P2_COUNTER.MAP_EXACT).increment(1);
            }

            // Upstream job may include some non-match types for analysis purposes, so be careful to filter here
            if ("exact".equals(matchType) 
                    || (matchType.startsWith("near") && (matchDistance < SimHashUtils.HAMMING_DISTANCE_THRESHOLD))) {
                String headDoc = valuePieces[1]; // This one doesn't get deleted. It was best candidate if a near match.

                DocumentInfo duplicate = new DocumentInfo(valuePieces[2]);
                String warcId = duplicate.getDocID().toString();
                String license = duplicate.getLicense().toString();
                String language = duplicate.getDocLang().toString();
                String noBoilerplate = duplicate.getNoBoilerPlate().toString();
                String minimalHtml = duplicate.getMinimalHtml().toString();
                String reduceKey = WARCWriterReducerClass.createKey(license, language, noBoilerplate, minimalHtml, warcId) ;


                // create a new dummy WARC record to be merged in the reducer
                // will indicate that this document should be removed as a duplicate or near duplicate
                DataInputStream stream = new DataInputStream(new ByteArrayInputStream(
                        ("WARC/1.0\r\n" + "WARC-Type: warcinfo\r\n"
                                + "WARC-Date: 2014-03-18T17:47:38Z\r\n" + "WARC-Record-ID: " + warcId
                                + "\r\n" + "Content-Length: 19\r\n"
                                + "Content-Type: application/warc-fields\r\n"
                                + "WARC-Filename: split.00_20150305143820.warc.gz\r\n" + "\r\n"
                                + "robots: classic\r\n" + "\r\n" + "\r\n" + "\r\n").getBytes("UTF-8")));
                WARCRecord record = new WARCRecord(stream);

                WARCWritable dummyWARC = new WARCWritable(record);

                context.write(new Text(reduceKey), dummyWARC);
            }
        }
    }

    /**
     * Accept a set of WARCs keyed by their lang_license_etc file prefix with
     * their WARC ID appended.
     * 
     * This key serves the dual purposes of sorting the documents into
     * language/license/etc bins and filtering out duplicates (those which
     * appear more than once in the streams, indicating that they were on the
     * list of duplicates).
     *
     */
    public static class DuplicateReducer
            extends Reducer<Text, WARCWritable, NullWritable, WARCWritable>
    {
        private MultipleOutputs<NullWritable, WARCWritable> multipleOutputs;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException
        {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<WARCWritable> values, Context context)
                throws IOException, InterruptedException
        {
            Iterator<WARCWritable> iterator = values.iterator();
            WARCWritable warcWritable = iterator.next();
            // If there's a second value, it means we have a duplicate and this should be skipped for output
            if (!iterator.hasNext()) {
                if (warcWritable.getRecord().getHeader().getField(WARCRecord.WARCRecordFieldConstants.LANGUAGE) != null) {
                    WARCWriterReducerClass.writeSingleWARCWritableToOutput(warcWritable, multipleOutputs);
                    context.getCounter(C4P2_COUNTER.REDUCE_OUTPUT_RECORDS).increment(1);
                } else {
                    context.getCounter(C4P2_COUNTER.REDUCE_NULL_LANGUAGE).increment(1);
                }
            } else {
                context.getCounter(C4P2_COUNTER.REDUCE_FILTERED_DUPLICATES).increment(1);
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException
        {
            multipleOutputs.close();
        }

    }

    
}
