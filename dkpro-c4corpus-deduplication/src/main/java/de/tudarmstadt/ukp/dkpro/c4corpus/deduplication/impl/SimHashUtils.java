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
package de.tudarmstadt.ukp.dkpro.c4corpus.deduplication.impl;

import java.nio.charset.Charset;
import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import de.greenrobot.common.hash.FNV64;

/**
 * methods that will be used to calculate simHash
 *
 * @author Omnia Zayed
 * @author Ivan Habernal
 */
public class SimHashUtils
{

    static int HASH_LENGTH = 64;

    /**
     * char n-gram shingle size
     */
    static int CHAR_GRAM_LENGTH = 7;

    /**
     * size of the band (window) of the bits of the sim hash
     */
    static int BAND_WIDTH = 16;

    public static int HAMMING_DISTANCE_THRESHOLD = 3;

    /**
     * converts a text into a number of "characters n-grams" shingles.
     *
     * @param text plain text
     * @return shingles
     */
    public static Set<String> createCharGramsShingles(String text)
    {

        Set<String> shingles = new HashSet<>();

        for (int i = 0; i < text.length() - CHAR_GRAM_LENGTH + 1; i++) {
            // extract an ngram
            String shingle = text.substring(i, i + CHAR_GRAM_LENGTH);
            // get it's index from the dictionary
            shingles.add(shingle);
        }
        return shingles;
    }

    /**
     * hash each shingle in the given set using String.hashCode(). A different
     * hashing function could be used.
     *
     * @param shingles list of shingles
     * @return hash phrases
     */
    public static Set<Integer> hash(Set<String> shingles)
    {

        Set<Integer> hashedShingles = new LinkedHashSet<>();

        for (String shingle : shingles) {
            int hashValue = shingle.hashCode();
            hashedShingles.add(hashValue);
        }
        //also called hash phrases
        return hashedShingles;
    }

    /**
     * count the number of bits that differ between two queries as a measure of
     * dissimilarity. Also known as Hamming distance based on the bit population
     *
     * @deprecated Use {@link Long#bitCount(long)} hash1 ^ hash2
     * @param simHash1 hash 1
     * @param simHash2 hash 2
     * @return different bits
     */
    public static int diffOfBits(long simHash1, long simHash2)
    {
        long bits = simHash1 ^ simHash2;
        int count = 0;
        while (bits != 0) {
            bits &= bits - 1;
            ++count;
        }

        return count;
    }

    /**
     * divide the 64-bit hash into 4 bit ranges of 16 bits. It will be used to
     * get similar candidates.
     *
     * @param docHash hash
     * @return ranges
     */
    public static Set<String> computeHashIndex(long docHash)
    {
        int bandIndex = 0;
        //a band (window) used to store a part of the hash (represented in bits)
        BitSet bitRange = new BitSet(BAND_WIDTH);
        //pointer to each element in a single band (window)
        int bitsWidthCounter = 0;
        Set<String> bandBitset = new HashSet<>();
        //divide our HASH_LENGTH-bit hash into bit ranges of BandWidth bits
        for (int i = 0; i < HASH_LENGTH; ++i) {
            bitRange.set(bitsWidthCounter++, ((docHash >> i) & 1) == 1);
            if (bitsWidthCounter == BAND_WIDTH) {
                bandBitset.add(bandIndex + "_" + bitRange.toString());
                bitsWidthCounter = 0;
                bitRange.clear();
                bandIndex++;
            }
        }
        return bandBitset;
    }

    /**
     * Slice a 64-bit hash into four hashes, each with all but a 16-bit
     * range masked out. It will be used to get similar candidates.
     *
     * @param docHash
     * @return
     */
    public static long[] sliceHash(long docHash)
    {
        long[] result = new long[4];
        for (int i = 0; i < 4; i++) {
            result[i] = docHash & (0xffffL << (i*16));
        }
        return result;
    }


    /**
     * Compress the hashes of all the shingles of one document to a single
     * fingerprint (SimHash) This implementation is based on the algorithm
     * described here: http://www.titouangalopin.com/blog/2014-05-29-simhash
     *
     * @param hashValues hashes
     * @return simhash fingerprint
     */
    public static long simHash(Set<Integer> hashValues)
    {
        int[] v = new int[HASH_LENGTH];
        long simhash = 0;
        //For each hash, for each bit i in this hash:
        for (Integer hash : hashValues) {
            for (int i = 0; i < HASH_LENGTH; i++) {
                //hash >> i right shift which means divide hash by 2 to the power i
                // & 1 masks the result
                boolean bitSet = ((hash >> i) & 1L) == 1L;
                if (bitSet) {
                    v[i] += 1;
                }
                else {
                    v[i] -= 1;
                }
            }
        }
        //For each bit j of the global fingerprint:
        for (int i = 0; i < HASH_LENGTH; ++i) {
            if (v[i] > 0) {
                simhash |= (1L << i);
            }
        }
        return simhash;
    }

    /**
     * Compress the hashes of all the shingles of one document to a single
     * fingerprint (SimHash) This implementation is based on the algorithm
     * described here: http://www.titouangalopin.com/blog/2014-05-29-simhash
     *
     * @param hashValues
     * @return
     */
    public static long simHash(long[] hashValues)
    {
        int[] v = new int[HASH_LENGTH];
        long simhash = 0;
        for (long hash : hashValues) {
            for (int i = 0; i < HASH_LENGTH; i++) {
                // convert 0/1 bit to -1/+1 vote
                long vote = ((hash >>> i) & 1L) * 2L - 1L; // UNSIGNED right shift
                v[i] += vote;
            }
        }
        // Convert counts back to global hash
        for (int i = 0; i < HASH_LENGTH; ++i) {
            if (v[i] > 0) {
                simhash |= (1L << i);
            }
        }
        return simhash;
    }


    /**
     * Convert a string into a number of UTF-8 byte n-gram shingles and hash the shingles.
     * <p>
     * This combines the functionality of {@link #createCharGramsShingles} and {@link #hash} without
     * the overhead of the temporary storage and extra method calls, plus it uses the 64-bit FNVJ64
     * hash instead of the 32-bit result produced by {@link Object#hashCode()}.
     *
     * @param text
     *            string to be hashed
     * @param size
     *            number of bytes to use for the N-grams
     * @return array of hashes (longs)
     */
    public static long[] createCharGramShingleHashes(String text, int size)
    {
        byte[] bytes = text.getBytes(Charset.forName("UTF-8"));
        if (bytes.length < size) {
            // TODO: Do we want to pad out to min length instead of aborting?
            return new long[0];
        }

        long[] hashedShingles = new long[bytes.length - size + 1];
        FNV64 hasher = new FNV64(); // FNVJ64 doesn't distribute evenly enough

        for (int i = 0; i <= bytes.length - size; i++) {
            hasher.reset();
            hasher.update(bytes, i, size);
            hashedShingles[i] = hasher.getValue();
            // TODO: Update votes directly inline here
        }
        return hashedShingles;
    }


    /**
     * Returns simHash of the given document
     *
     * @deprecated {{@link #getSimHash2(String)} is faster and produces 64-bit instead of 32-bit
     *             hashes.
     * @param text plain text
     * @return simhash
     */
    public static long getSimHash(String text)
    {
        Set<String> shingles = SimHashUtils.createCharGramsShingles(text);
        Set<Integer> hashPhrases = SimHashUtils.hash(shingles);
        return SimHashUtils.simHash(hashPhrases);
    }

    /**
     * Returns SimHash of the given document.
     *
     * @param text text to be hashed
     * @return simhash
     */
    public static long getSimHash2(String text)
    {
        long[] hashPhrases = 
                SimHashUtils.createCharGramShingleHashes(text, CHAR_GRAM_LENGTH);
        return SimHashUtils.simHash(hashPhrases);
    }
}
