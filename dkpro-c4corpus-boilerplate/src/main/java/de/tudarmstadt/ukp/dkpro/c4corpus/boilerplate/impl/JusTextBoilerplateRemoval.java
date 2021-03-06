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
package de.tudarmstadt.ukp.dkpro.c4corpus.boilerplate.impl;

import de.tudarmstadt.ukp.dkpro.c4corpus.boilerplate.BoilerPlateRemoval;
import de.tudarmstadt.ukp.dkpro.c4corpus.boilerplate.impl.Paragraph.PARAGRAPH_TYPE;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Entities.EscapeMode;
import org.jsoup.nodes.Node;
import org.jsoup.safety.Cleaner;
import org.jsoup.safety.Whitelist;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Re-implementing the jusText Python boilerplate removal algorithm (Pomikalek,
 * 2011)
 * <br>
 * References:
 * <br>
 * Pomikalek, J. (2011). Removing boilerplate and duplicate content from web corpora.
 * Ph.D. thesis, Masaryk university, Faculty of informatics, Brno, Czech Republic.
 * <br>
 * http://corpus.tools/wiki/Justext/Algorithm
 * https://github.com/miso-belica/jusText/blob/dev/doc/algorithm.rst
 *
 * @author Omnia Zayed
 */
public class JusTextBoilerplateRemoval
        implements BoilerPlateRemoval
{

    static final double MAX_LINK_DENSITY_DEFAULT = 0.20;
    static final int LENGTH_LOW_DEFAULT = 70;
    static final int LENGTH_HIGH_DEFAULT = 200;
    static final double STOPWORDS_LOW_DEFAULT = 0.30;
    static final double STOPWORDS_HIGH_DEFAULT = 0.32;

    // Short and near-good headings within MAX_HEADING_DISTANCE characters before
    // a good paragraph are classified as good unless --no-headings is specified.
    static final int MAX_HEADING_DISTANCE_DEFAULT = 200;

    //to optimize the time complexity of getNeighbour method
    Pair prevNeighbourCache;
    Pair nextNeighbourCache;

    //To optimize getting next neighbours (when doing re-classification of short & near-good)
    static final int LOOP_THRESHOLD_OF_NEIGHBOURS = 10;
    // for storing stopwords (key = lang, value = stopword set); new languages are added on-demand
    Map<Locale, Set<String>> lazyStopwordMap = new HashMap<>();

    /**
     * covert html to a jsoup document
     */
    private Document convertHtmlToDoc(String html)
    {
        Document document;
        try {
            document = Jsoup.parse(html);
            document = new Cleaner(
                    Whitelist.relaxed()
                        .removeTags("img", "head", "script", "style", ".hidden", "embedded", "#comment")
                        .removeAttributes("a", "href")) // URL absolutization takes forever!
                            .clean(document);
            document.outputSettings().charset("UTF-8");
            document.outputSettings().escapeMode(EscapeMode.xhtml);
        }
        catch (Throwable ex) {
            System.err.println(
                    "Exception raised when parsing HTML with JSoup; returning empty document");
            System.err.println("Stack trace:");
            ex.printStackTrace(System.err);
            System.err.println("----- Original HTML begin -----");
            System.err.println(html);
            System.err.println("----- Original HTML end -----");

            // create empty dummy document
            document = new Document("www.example.com");
        }

        return document;
    }

    /**
     * Initialize the Paragraph explorer class in order to convert a document to
     * a list of blocks (paragraphs)
     */
    private ArrayList<Paragraph> makeParagraphs(Node node)
    {
        ParagraphsExplorer pe = new ParagraphsExplorer();
        node.traverse(pe); //begin the traversal of the doc
        return pe.getParagraphs();
    }

    /**
     * Context-free paragraph classification. Assigns each block (paragraph) to one of four classes:
     * <li>bad – boilerplate blocks
     * <li>good – main content blocks
     * <li>short – too short to make a reliable decision about the class
     * <li>near-good – somewhere in-between short and good
     */
    private void classifyContextFree(List<Paragraph> paragraphs, Set<String> stopListLower, Locale locale,
            int lengthLow, int lengthHigh, double stopwordsLow,
            double stopwordsHigh, double maxLinkDensity)
    {
        for (Paragraph paragraph : paragraphs) {
            int length = paragraph.getRawText().length();
            float stopWordDensity = paragraph.stopwords_density(stopListLower, locale);
            double link_density = paragraph.calcLinksDensity();

            if ("select".equalsIgnoreCase(paragraph.getTagName())) {
                paragraph.setContextFreeClass(PARAGRAPH_TYPE.BAD);
            }
            else if (paragraph.getRawText().contains("\u00a9")) { // copyright symbol
                paragraph.setContextFreeClass(PARAGRAPH_TYPE.BAD);
            }
            else if (link_density > maxLinkDensity) {
                paragraph.setContextFreeClass(PARAGRAPH_TYPE.BAD);
            }
            else if (length < lengthLow) {
                if (paragraph.getLinksLength() > 0 || length == 0) {
                    paragraph.setContextFreeClass(PARAGRAPH_TYPE.BAD);
                }
                else {
                    paragraph.setContextFreeClass(PARAGRAPH_TYPE.SHORT);
                }
            }
            else if (stopWordDensity >= stopwordsHigh) {
                if (length > lengthHigh) {
                    paragraph.setContextFreeClass(PARAGRAPH_TYPE.GOOD);
                }
                else {
                    paragraph.setContextFreeClass(PARAGRAPH_TYPE.NEAR_GOOD);
                }
            }
            else if (stopWordDensity >= stopwordsLow) {
                paragraph.setContextFreeClass(PARAGRAPH_TYPE.NEAR_GOOD);
            }
            else {
                paragraph.setContextFreeClass(PARAGRAPH_TYPE.BAD);
            }
        }
    }

    /**
     * Optimization of the original getNeighbour method to reduce the time
     * complexity. used to save the nearest previous neighbour.
     */
    private PARAGRAPH_TYPE getPrevNeighbourOptimized(int i, List<Paragraph> paragraphs,
            boolean ignoreNearGood, int inc, int boundary)
    {
        while (i + inc != boundary) {
            i += inc;
            PARAGRAPH_TYPE c = paragraphs.get(i).getClassType();
            if (c == PARAGRAPH_TYPE.GOOD || c == PARAGRAPH_TYPE.BAD) {
                prevNeighbourCache = new Pair(i, c);
                return c;
            }
            if (c == PARAGRAPH_TYPE.NEAR_GOOD && !ignoreNearGood) {
                return c;
            }
            if (prevNeighbourCache != null
                    && i > prevNeighbourCache.getID() && c == PARAGRAPH_TYPE.SHORT) {
                //render the prev class
                return prevNeighbourCache.getClassType();
            }
        }
        return PARAGRAPH_TYPE.BAD;
    }

    /**
     * Optimization of the original getNeighbour method to reduce the time
     * complexity. used to save the nearest next neighbour.
     */
    private PARAGRAPH_TYPE getNextNeighbourOptimized(int i, List<Paragraph> paragraphs,
            boolean ignoreNeargood, int inc, int boundary)
    {
        int counter = 0; // to avoid infinite loop in case the whole document is short
        while (i + inc != boundary && counter < LOOP_THRESHOLD_OF_NEIGHBOURS) {
            i += inc;
            PARAGRAPH_TYPE c = paragraphs.get(i).getClassType();
            if (c == PARAGRAPH_TYPE.GOOD || c == PARAGRAPH_TYPE.BAD) {
                //newly visited paragraph
                nextNeighbourCache = new Pair(i, c);
                return c;
            }
            if (c == PARAGRAPH_TYPE.NEAR_GOOD && !ignoreNeargood) {
                return c;

            }
            if (nextNeighbourCache != null
                    && i < nextNeighbourCache.getID() && c == PARAGRAPH_TYPE.SHORT) {
                //render the prev class if this paragraph was visited before                 
                return nextNeighbourCache.getClassType();
            }

            //corner case if the whole document is initially short and no bad
            //or good classes at the end of the paragraphs.
            if (nextNeighbourCache == null && i == boundary - 1) {
                nextNeighbourCache = new Pair(i, PARAGRAPH_TYPE.BAD);
                return nextNeighbourCache.getClassType();
            }
            counter++;
        }
        return PARAGRAPH_TYPE.BAD;
    }

    /**
     * optimized version to be used only if the class is short and
     * ignoreNeargood is false.
     */
    private PARAGRAPH_TYPE getPrevNeighbourOptimized(int i, List<Paragraph> paragraphs,
            boolean ignoreNeargood)
    {
        return getPrevNeighbourOptimized(i, paragraphs, ignoreNeargood, -1, -1);
    }

    /**
     * optimized version to be used only if the class is short and
     * ignoreNeargood is false.
     */
    private PARAGRAPH_TYPE getNextNeighbourOptimized(int i, List<Paragraph> paragraphs,
            boolean ignoreNeargood)
    {
        return getNextNeighbourOptimized(i, paragraphs, ignoreNeargood, 1, paragraphs.size());
    }

    /*
     * Context-sensitive paragraph classification. Assumes that context free classification of
     * paragraphs has already been called. The purpose is to re classify neargood and short
     * paragraphs according to the classes of the surrounding blocks.
     * <p>
     * The algorithm adds two stages of processing for the header blocks. The first stage
     * (preprocessing) is executed after context-free classification and before context-sensitive
     * classification. The second stage (postprocessing) is performed after the context-sensitive
     * classification:
     * <ol>
     * <li>context-free classification [done before this method is called]
     * <li>preprocessing of header blocks
     * <li>context-sensitive classification
     * <li>postprocessing of header blocks
     * </ol>
     * 
     * NOTE: Normally we'd use List<Paragraph> in the definition, but this method makes extensive
     * use of List.get() which is very inefficient with other list implementations such as LinkedList.
     * This could be rewritten to use ListIterators to generalize it, but I don't see the point.
     */
    private void reclassifyContextSensitive(ArrayList<Paragraph> paragraphs, int maxHeadingDistance)
    {
        // Default classification is the same as the context-free classification
        for (Paragraph p : paragraphs) {
            p.setClassType(p.getContextFreeClass());
        }

        /* 
         * re-classify good headings - from the description of the original Python implementation:
         * <p>
         * "2. The preprocessing looks
         * for short header blocks which precede good blocks and at the same time there is no more
         * than MAX_HEADING_DISTANCE characters between the header block and the good block. The
         * context-free class of such header blocks is changed from short to near-good. The purpose
         * of this is to preserve short blocks between the heading and the good text which might
         * otherwise be removed (classified as bad) by the context-sensitive classification."
         */
        for (int i = 0; i < paragraphs.size(); i++) {
            Paragraph paragraph = paragraphs.get(i);
            if (paragraph.isHeading() && paragraph.getClassType() == PARAGRAPH_TYPE.SHORT) {
                int j = i + 1;
                int distance = 0;
                while (j < paragraphs.size() && distance <= maxHeadingDistance) {
                    if (paragraphs.get(j).getClassType() == PARAGRAPH_TYPE.GOOD) {
                        paragraph.setClassType(PARAGRAPH_TYPE.NEAR_GOOD);
                        break;
                    }
                    distance += paragraphs.get(j).getRawText().length();
                    j += 1;
                }
            }
        }

        //re-classify short
        //a new data structure is used for storage as we don't want to mess the
        //original classification. It will be used by other parts of the code later.       
        // TODO: Is a LinkedHashMap needed here?
        Map<Integer, PARAGRAPH_TYPE> newClasses = new LinkedHashMap<>();

        for (int i = 0; i < paragraphs.size(); i++) {
            if (paragraphs.get(i).getClassType() == Paragraph.PARAGRAPH_TYPE.SHORT) {
                PARAGRAPH_TYPE prevNeighbour = getPrevNeighbourOptimized(i, paragraphs, true); // ignore_neargood
                PARAGRAPH_TYPE nextNeighbour = getNextNeighbourOptimized(i, paragraphs, true); // ignore_neargood

                // TODO: Is a LinkedHashSet needed here?
                Set<PARAGRAPH_TYPE> neighbours = new LinkedHashSet<>();
                neighbours.add(prevNeighbour);
                neighbours.add(nextNeighbour);

                if (neighbours.size() == 1 && neighbours.contains(PARAGRAPH_TYPE.GOOD)) {
                    newClasses.put(i, PARAGRAPH_TYPE.GOOD);
                } else if (neighbours.size() == 1 && neighbours.contains(PARAGRAPH_TYPE.BAD)) {
                    newClasses.put(i, PARAGRAPH_TYPE.BAD);
                } // it must be set(['good', 'bad'])
                else if ((prevNeighbour == PARAGRAPH_TYPE.BAD
                        && getPrevNeighbourOptimized(i, paragraphs, false) == PARAGRAPH_TYPE.NEAR_GOOD)
                        || (nextNeighbour == PARAGRAPH_TYPE.BAD
                                && getNextNeighbourOptimized(i, paragraphs, false) == PARAGRAPH_TYPE.NEAR_GOOD)) {
                    newClasses.put(i, PARAGRAPH_TYPE.GOOD);
                } else {
                    newClasses.put(i, PARAGRAPH_TYPE.BAD);
                }
            }
        }

        //set the final class type with the new classes
        for (Integer i : newClasses.keySet()) {
            paragraphs.get(i).setClassType(newClasses.get(i));
        }

        // revise neargood
        for (int i = 0; i < paragraphs.size(); i++) {
            Paragraph paragraph = paragraphs.get(i);
            if (paragraph.getClassType() == PARAGRAPH_TYPE.NEAR_GOOD) {
                PARAGRAPH_TYPE prevNeighbour = getPrevNeighbourOptimized(i, paragraphs, true);
                PARAGRAPH_TYPE nextNeighbour = getNextNeighbourOptimized(i, paragraphs, true);
                if (prevNeighbour == PARAGRAPH_TYPE.BAD && nextNeighbour == PARAGRAPH_TYPE.BAD) {
                    paragraph.setClassType(PARAGRAPH_TYPE.BAD);
                } else {
                    paragraph.setClassType(PARAGRAPH_TYPE.GOOD);
                }
            }
        }

        /*
         * re-classify more good headings - post-processing
         * <p> 
         * "4. The postprocessing again looks for header blocks
         * which precede good blocks and are no further than MAX_HEADING_DISTANCE away. This time,
         * the matched headers are classified as good if their context-free class was other than
         * bad. In other words, the bad headings remain bad, but some short and near-good headings
         * can be classified as good if they precede good blocks, even though they would normally be
         * classified as bad by the context-sensitive classification (e.g. they are surrounded by
         * bad blocks). This stage preserves the 'non-bad' headings of good blocks."
         * 
         */
        for (int i = 0; i < paragraphs.size(); i++) {
            Paragraph paragraph = paragraphs.get(i);
            if (paragraph.isHeading() && paragraph.getClassType() == PARAGRAPH_TYPE.BAD
                    && paragraph.getContextFreeClass() != PARAGRAPH_TYPE.BAD) {
                int j = i + 1;
                int distance = 0;
                while (j < paragraphs.size() && distance <= maxHeadingDistance) {
                    if (paragraphs.get(j).getClassType() == PARAGRAPH_TYPE.GOOD) {
                        paragraph.setClassType(PARAGRAPH_TYPE.GOOD);
                        break;
                    }
                    distance += paragraphs.get(j).getRawText().length();
                    j += 1;
                }
            }
        }
    }

    /**
     * Converts an HTML page into a list of classified paragraphs. Each
     * paragraph is represented as instance of class "Paragraph"
     */
    private List<Paragraph> classify(String htmlText, Set<String> stopwordsSet, Locale locale, int lengthLow,
            int lengthHigh, double stopwordsLow, double stopwordsHigh, double maxLinkDensity,
            int maxHeadingDistance)
    {

        //language-independent mode
        if (stopwordsSet.isEmpty()) {
            //empty stop list, switch to language-independent mode
            stopwordsHigh = 0;
            stopwordsLow = 0;
        }

        Document jSoupDoc = convertHtmlToDoc(htmlText);
        ArrayList<Paragraph> paragraphs = makeParagraphs(jSoupDoc);
        //context-free classification
        classifyContextFree(paragraphs, stopwordsSet, locale, lengthLow, lengthHigh,
                stopwordsLow, stopwordsHigh, maxLinkDensity);
        //context-sensitive classification.
        reclassifyContextSensitive(paragraphs, maxHeadingDistance);

        return paragraphs;
    }

    /**
     * using defaults and allowing language-independent mode
     */
    private List<Paragraph> classify(String htmlText, Locale locale)
            throws IOException
    {

        Set<String> stopwordsSet;
        //activate the language-independent mode if language is set to null
        if (locale == null) {
            stopwordsSet = new HashSet<>();
        }
        else if (!lazyStopwordMap.containsKey(locale)) {
            List<String> stopWords = Utils.loadStopWords(locale);
            stopwordsSet = new HashSet<>(stopWords.size());
            for (String word : stopWords) {
                // We always use these lowercased, so do it at initialization
                stopwordsSet.add(word.toLowerCase(locale));
            }
            lazyStopwordMap.put(locale, stopwordsSet);
        }

        else {
            stopwordsSet = lazyStopwordMap.get(locale);
        }

        return classify(htmlText, stopwordsSet, locale, 
                JusTextBoilerplateRemoval.LENGTH_LOW_DEFAULT,
                JusTextBoilerplateRemoval.LENGTH_HIGH_DEFAULT,
                JusTextBoilerplateRemoval.STOPWORDS_LOW_DEFAULT,
                JusTextBoilerplateRemoval.STOPWORDS_HIGH_DEFAULT,
                JusTextBoilerplateRemoval.MAX_LINK_DENSITY_DEFAULT,
                JusTextBoilerplateRemoval.MAX_HEADING_DISTANCE_DEFAULT
        );
    }

    @Override
    public String getPlainText(String html, Locale locale)
            throws IOException
    {

        List<Paragraph> paragraphs = classify(html, locale);

        StringBuilder sb = new StringBuilder();
        for (Paragraph p : paragraphs) {
            if (!p.isBoilerplate()) {
                // extract raw text
                String rawText = Utils.normalize(p.getRawText());
                rawText = StringEscapeUtils.unescapeHtml(rawText).trim();

                sb.append(rawText);
                sb.append("\n");
            }
        }

        return sb.toString().trim();
    }

    @Override
    public String getMinimalHtml(String html, Locale locale)
            throws IOException
    {

        List<Paragraph> paragraphs = classify(html, locale);

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        for (Paragraph p : paragraphs) {
            if (!p.isBoilerplate()) {
                String tag = p.getTagName();

                //edited as sometimes  the tag is empty because it is div or br
                if (tag.trim().isEmpty()) {
                    tag = "p";
                }
                // extract raw text
                String rawText = Utils.normalize(p.getRawText());

                // for non-empty text and non-empty tags, print the output
                if (!tag.trim().isEmpty() && !rawText.isEmpty()) {
                    pw.printf("<%s>%s</%s>%n", tag, rawText, tag);
                }
            }
        }

        IOUtils.closeQuietly(pw);
        return sw.toString();
    }
}
