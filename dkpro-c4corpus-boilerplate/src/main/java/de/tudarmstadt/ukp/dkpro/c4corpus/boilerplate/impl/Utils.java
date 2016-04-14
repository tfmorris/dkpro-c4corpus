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

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Some helper functions for cleaning web text
 *
 * @author Omnia Zayed
 * @author Ivan Habernal
 */
public class Utils
{

    /**
     * Translates multiple whitespace into single space character. If there is
     * at least one new line character chunk is replaced by single LF (Unix new
     * line) character.
     *
     * @param text text
     * @return normalized text
     */
    public static String normalizeBreaks(String text)
    {
        //replace extra <br> (sometimes the paragraph contains <br><br>,
        //the first one will be use as new paragraph marker but the second 
        //one must be removed)
        text = BREAK_TAG.matcher(text).replaceAll(""); // or [\\s+&&[^\n])]

        return text;
    }
    private static final Pattern BREAK_TAG = Pattern.compile("<br>");

    /**
     * This implements the HTML white space normalization as specified in the
     * rendering rules:
     * https://www.w3.org/TR/html4/struct/text.html#h-9.1
     * https://www.w3.org/TR/CSS2/text.html#white-space-model
     * 
     * @param text
     * @return
     */
    public static String normalizeWhitespace(String text) {
        return text;
    }

    /**
     * Loads the stop-words list of a given language
     *
     * @param locale locale
     * @return set of stop words
     * @throws IOException exception
     */
    public static Set<String> loadStopWords(Locale locale)
            throws IOException
    {

        String streamName = "/stoplists/" + locale.getLanguage() + ".txt";
        InputStream stream = Utils.class.getResourceAsStream(streamName);

        if (stream == null) {
            throw new IOException("Stream " + streamName + " not found");
        }

        List<String> stopList = IOUtils.readLines(stream);
        return new HashSet<>(stopList);

    }

    private static final String WHITESPACE_CHARS = "" + "\\u0009" // CHARACTER TABULATION
            //				+ "\\u000A" // LINE FEED (LF)
            + "\\u000B" // LINE TABULATION
            //				+ "\\u000C" // FORM FEED (FF)
            //				+ "\\u000D" // CARRIAGE RETURN (CR)
            + "\\u0009" // horizontal tab
            + "\\u0010" // Data Link Escape
            + "\\u0020" // SPACE
            + "\\u0085" // NEXT LINE (NEL)
            + "\\u00A0" // NO-BREAK SPACE
            + "\\u1680" // OGHAM SPACE MARK
            + "\\u180E" // MONGOLIAN VOWEL SEPARATOR
            + "\\u2000" // EN QUAD
            + "\\u2001" // EM QUAD
            + "\\u2002" // EN SPACE
            + "\\u2003" // EM SPACE
            + "\\u2004" // THREE-PER-EM SPACE
            + "\\u2005" // FOUR-PER-EM SPACE
            + "\\u2006" // SIX-PER-EM SPACE
            + "\\u2007" // FIGURE SPACE
            + "\\u2008" // PUNCTUATION SPACE
            + "\\u2009" // THIN SPACE
            + "\\u200A" // HAIR SPACE
            + "\\u2028" // LINE SEPARATOR
            + "\\u2029" // PARAGRAPH SEPARATOR
            + "\\u202F" // NARROW NO-BREAK SPACE
            + "\\u205F" // MEDIUM MATHEMATICAL SPACE
            + "\\u3000";

    private static final String WHITESPACE_CHAR_CLASS = "[" + WHITESPACE_CHARS + "]";
    private static final Pattern MULTIPLE_WHITESPACE = Pattern.compile(WHITESPACE_CHAR_CLASS + "+");
    private static final Pattern CONTROL_CHARACTERS = Pattern.compile("[\\p{Cntrl}&&[^\\r\\n]]"); // except CR & LF
    // One or more newlines optionally preceded or trailed by whitespace become
    private static final Pattern LEADING_TRAILING_WHITESPACE = Pattern.compile("\\n+" + WHITESPACE_CHAR_CLASS + "*|" + WHITESPACE_CHAR_CLASS + "*\\n+");
    private static final Pattern ONE_OR_MORE__DASHES = Pattern.compile("[" + "" + "\\u2012" // figure dash
            + "\\u2013" // en dash
            + "\\u2014" // em dash
            + "\\u2015" // horizontal bar
            + "\\u2053" // swung dash
            +"]+");
    private static final Pattern DOUBLE_QUOTES = Pattern.compile("[“”«»„‟]");
    private static final Pattern SINGLE_QUOTES = Pattern.compile("[‘’‚‛‹›`]");
    
    /**
     * Normalizes the given string - unifying whitespaces, quotations, and dashes
     *
     * @param text text
     * @return normalized text
     */
    public static String normalize(String text)
    {
        // first replace all control characters except newlines
        String result = CONTROL_CHARACTERS.matcher(text).replaceAll("");

        // all weird whitespaces
        result = MULTIPLE_WHITESPACE.matcher(result).replaceAll(" ");

        // trim the lines - also handles multiple newlines
        result = LEADING_TRAILING_WHITESPACE.matcher(result).replaceAll("\n");

        // dashes
        result = ONE_OR_MORE__DASHES.matcher(result).replaceAll("-");

        // ellipsis
        result = result.replace("\\u2026", "...");

        // quotation marks
        result = DOUBLE_QUOTES.matcher(result).replaceAll("\"");
        result = SINGLE_QUOTES.matcher(result).replaceAll("'");

        return result.trim();
    }

}
