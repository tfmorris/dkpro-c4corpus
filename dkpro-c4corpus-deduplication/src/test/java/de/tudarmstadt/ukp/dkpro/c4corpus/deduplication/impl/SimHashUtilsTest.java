package de.tudarmstadt.ukp.dkpro.c4corpus.deduplication.impl;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

/**
 * Test for the static methods in SimHashUtils.
 *
 * NOTE: {@link #testHash()} and {@link #testSimHash()} have had their
 * test values just copied from the results. They have *not* been verified
 * as correct.
 *
 * @author Tom Morris <tfmorris@gmail.com>
 *
 */
public class SimHashUtilsTest {

    @Test
    public void testCreateCharGramsShingles() {
        String testString = "abcdefghi";
        String[] refShingles = { "abcdefg", "bcdefgh", "cdefghi", };
        Set<String> refSet = new HashSet<String>(Arrays.asList(refShingles));
        assertEquals(refSet, SimHashUtils.createCharGramsShingles(testString));
    }

    @Test
    public void testHash() {
        String testString = "abcdefghi";
        // FIXME: Verify these hashes are correct
        Integer[] refHashes = {-289204219, 627882918, -1206291356};
        Set<Integer> refSet = new HashSet<Integer>(Arrays.asList(refHashes));
        Set<String> shingles = SimHashUtils.createCharGramsShingles(testString);
        Set<Integer> hashes = SimHashUtils.hash(shingles);
        assertEquals(refSet, hashes);
    }

    @Test
    public void testDiffOfBits() {
        assertEquals(1, SimHashUtils.diffOfBits(0x1L, 0x0L));
    }

    @Test
    public void testComputeHashIndex() {
        long hash = 0X0800040002000100L;
        String[] refSlices = { "0_{8}", "1_{9}", "2_{10}", "3_{11}" };
        Set<String> referenceSet = new HashSet<String>(Arrays.asList(refSlices));

        Set<String> slices = SimHashUtils.computeHashIndex(hash);
        assertEquals(referenceSet, slices);
    }

    @Test
    public void testSliceHash() {
        long hash = 0X0800040002000100L;
        long[] refSlices = {
                0X0000000000000100L,
                0X0000000002000000L,
                0X0000040000000000L,
                0X0800000000000000L,
        };
        long[] slices = SimHashUtils.sliceHash(hash);
        assertArrayEquals(refSlices, slices);
    }

    @Test
    public void testSimHash() {
        Set<Integer> hashValues = SimHashUtils.hash(
                SimHashUtils.createCharGramsShingles("abcdefghi"));
        // FIXME: Verify that this simhash is correct
        assertEquals(-6032228495725610972L, SimHashUtils.simHash(hashValues));
    }

    @Test
    public void testCreateCharGramShingleHashes()
    {
        // FIXME: Verify that these hashes are correct
        long[] refHashes = {-1206291356 & 0xFFFFFFFFL, -289204219 & 0xFFFFFFFFL, 627882918 & 0xFFFFFFFFL};
        long[] testHashes = SimHashUtils.createCharGramShingleHashes("abcdefghi",
                SimHashUtils.CHAR_GRAM_LENGTH);
//        for (long hash : testHashes) {
//            System.out.println(" new " + Long.toBinaryString(hash));
//        }
        assertArrayEquals(refHashes, testHashes);
    }

    @Test
    public void testGetSimHash()
    {
        // FIXME: Verify that this simhash is correct
        long good = -6032228495725610972L;
        long test = SimHashUtils.getSimHash("abcdefghi");
//        System.out.println("old good: " + Long.toBinaryString(good));
//        System.out.println("old test: " + Long.toBinaryString(test));
        assertEquals(good, test);
    }
    
    @Test
    public void testGetSimHash2()
    {
        // FIXME: Verify that this simhash is correct
        long good = -6032228495725610972L ; // & 0xFFFFFFFFL;
        long test = SimHashUtils.getSimHash2("abcdefghi");
        test = test << 32 | test; // Old code replicates hash in upper 32-bits
//        System.out.println("new good: " + Long.toBinaryString(good));
//        System.out.println("new test: " + Long.toBinaryString(test));
        assertEquals(good, test);
    }
}
