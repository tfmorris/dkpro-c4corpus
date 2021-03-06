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

import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;

import java.util.LinkedList;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Data structure representing one block of text in HTML
 *
 * Based on https://github.com/duongphuhiep/justext/ by Duong Phu-Hiep
 *
 * @author Duong Phu-Hiep
 * @author Omnia Zayed
 */
public class Paragraph
        extends LinkedList<Node>
{
    private static final long serialVersionUID = 1L;
    private static final Pattern WHITESPACE = Pattern.compile("\\s+");

    public enum PARAGRAPH_TYPE {UNKNOWN, SHORT, GOOD, NEAR_GOOD, BAD};

    int charsCountInLinks = 0;
    private PARAGRAPH_TYPE classType = PARAGRAPH_TYPE.UNKNOWN;
    private PARAGRAPH_TYPE contextFreeClass = PARAGRAPH_TYPE.UNKNOWN;
    private String tagName = "";
    private String rawText = "";
    private boolean isHeading = false;

    public Paragraph(Node firstNode, boolean heading)
    {
        add(firstNode);
        Node node = firstNode;
        while (NodeHelper.isInnerText(node) || node instanceof TextNode) {
            node = node.parent();
        }
        if (node != null) {
            this.tagName = node.nodeName();
        }
        this.isHeading = heading;
        if (firstNode instanceof TextNode) {
            String nodeRawText = ((TextNode) firstNode).text();
            this.rawText = nodeRawText.trim();

            if (NodeHelper.isLink(firstNode)) {
                charsCountInLinks += nodeRawText.length();
            }
        }
    }


    public int getLinksLength()
    {
        return this.charsCountInLinks;
    }

    public PARAGRAPH_TYPE getClassType()
    {
        return this.classType;
    }

    public void setClassType(PARAGRAPH_TYPE classType)
    {
        this.classType = classType;
    }

    public PARAGRAPH_TYPE getContextFreeClass()
    {
        return this.contextFreeClass;
    }

    public void setContextFreeClass(PARAGRAPH_TYPE contextFreeClass)
    {
        this.contextFreeClass = contextFreeClass;
    }

    public String getTagName()
    {
        return this.tagName;
    }


    public void setTagName(String name)
    {
        this.tagName = name;
    }

    public boolean isHeading()
    {
        return isHeading;
    }

    public boolean isBoilerplate()
    {
        return this.getClassType() != PARAGRAPH_TYPE.GOOD;
    }

    public String getRawText()
    {
        return rawText;
    }

    public void setRawText(String rawText)
    {
        this.rawText = rawText;
    }

    /**
     * @param stopwords set of stopwords, assumed to be already lowercased
     * @param locale locale to be used when lowercasing words
     * @return number from 0.0 to 1.0 representing proportion of stop words
     */
    public float stopwords_density(Set<String> stopwords, Locale locale)
    {
        String[] words = WHITESPACE.split(this.getRawText());
        if (words.length == 0) {
            return 0;
        }
        int stopWords = 0;
        for (String word : words) {
            if (locale == null) {
                word = word.toLowerCase();
            } else {
                word = word.toLowerCase(locale);
            }
            if (stopwords.contains(word)) {
                stopWords += 1;
            }
        }

        return stopWords / (float) words.length;
    }

    /**
     * Links density is the number of characters of the sentence defining the link
     * divide by the length of the whole paragraph.
     * e.g: hi {@code <a ...>omnia</a>} this is an example
     * link density = 5 (length of omnia) / 26 (paragraph length)
     *
     * @return Links density
     */
    public float calcLinksDensity()
    {
        int textLength = this.getRawText().length();
        if (textLength == 0) {
            return 0;
        }

        return this.getLinksLength() / (float) textLength;
    }

}
