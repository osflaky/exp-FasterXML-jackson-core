package tools.jackson.core.unittest.constraints;

import org.junit.jupiter.api.Test;

import tools.jackson.core.*;
import tools.jackson.core.async.ByteArrayFeeder;
import tools.jackson.core.exc.StreamConstraintsException;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.core.unittest.async.AsyncTestBase;
import tools.jackson.core.unittest.testutil.AsyncReaderWrapper;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Number Length Constraint Bypass in Non-Blocking (Async) JSON Parsers.
 *<p>
 * Covers two scenarios: a complete oversized number fed in one shot (with
 * {@code endOfInput()}), and digit-only input streamed across many chunks with no
 * terminator and no {@code endOfInput()} — verifying {@code maxNumberLength} is enforced
 * promptly while streaming, not only at value completion.
 */
class AsyncLargeNumberReadTest
    extends AsyncTestBase
{
    private static final int TEST_NUMBER_LENGTH = StreamReadConstraints.DEFAULT_MAX_NUM_LEN * 2;

    private final JsonFactory JSON_F = newStreamFactory();

    // // // Streaming-chunk constraints (lower limit so tests stay fast)

    private static final int MAX_NUM_LEN = 1000;
    // Chunk size kept modest so the test runs quickly under CI but still exceeds
    // maxNumberLength after the very first chunk.
    private static final int CHUNK_SIZE = 4 * 1024;
    // Hard cap on chunks fed: well past maxNumberLength but bounded so a regressed
    // build cannot OOM the CI machine.
    private static final int MAX_CHUNKS = 32;

    private final JsonFactory STRICT_F = JsonFactory.builder()
            .streamReadConstraints(StreamReadConstraints.builder()
                    .maxNumberLength(MAX_NUM_LEN)
                    .build())
            .build();

    // // // Complete-payload tests: oversized number fed in one shot with endOfInput()

    @Test
    void asyncParserFailsTooLongInt() throws Exception {
        byte[] payload = buildPayloadWithLongInteger(TEST_NUMBER_LENGTH);

        try (JsonParser p = JSON_F.createNonBlockingByteArrayParser(ObjectReadContext.empty())) {
            ByteArrayFeeder byteArrayFeeder = (ByteArrayFeeder) p;
            byteArrayFeeder.feedInput(payload, 0, payload.length);
            byteArrayFeeder.endOfInput();

            _asyncParserFailsTooLongNumber(p, JsonToken.VALUE_NUMBER_INT);
        }
    }

    @Test
    void asyncParserFailsTooLongDecimal() throws Exception {
        byte[] payload = buildPayloadWithLongDecimal(TEST_NUMBER_LENGTH);

        try (JsonParser p = JSON_F.createNonBlockingByteArrayParser(ObjectReadContext.empty())) {
            ByteArrayFeeder byteArrayFeeder = (ByteArrayFeeder) p;
            byteArrayFeeder.feedInput(payload, 0, payload.length);
            byteArrayFeeder.endOfInput();

            _asyncParserFailsTooLongNumber(p, JsonToken.VALUE_NUMBER_FLOAT);
        }
    }

    @Test
    void asyncParserFailsTooLongDecimalWithExponent() throws Exception {
        byte[] payload = buildPayloadWithLongExponent(TEST_NUMBER_LENGTH);

        try (JsonParser p = JSON_F.createNonBlockingByteArrayParser(ObjectReadContext.empty())) {
            ByteArrayFeeder byteArrayFeeder = (ByteArrayFeeder) p;
            byteArrayFeeder.feedInput(payload, 0, payload.length);
            byteArrayFeeder.endOfInput();

            _asyncParserFailsTooLongNumber(p, JsonToken.VALUE_NUMBER_FLOAT);
        }
    }

    // // // Streaming-chunk tests: enforcement must fire mid-stream, before completion

    /**
     * Streams the integer portion of a number across many chunks. Asserts that
     * {@link StreamConstraintsException} is raised promptly once the accumulated
     * digit length exceeds {@code maxNumberLength}, rather than only at value
     * completion.
     */
    @Test
    void integerPath_streamingChunks_rejectsBeyondMaxNumberLength() throws Exception {
        // CHUNK_SIZE > MAX_NUM_LEN, so failure must occur on the first chunk past
        // the limit; chunksFed == 1 in practice.
        int chunksFed = streamDigitsUntilRejected("{\"v\":", CHUNK_SIZE);
        assertTrue(chunksFed <= 2,
                "StreamConstraintsException raised too late: after " + chunksFed
                        + " chunks of " + CHUNK_SIZE
                        + " digits (maxNumberLength=" + MAX_NUM_LEN + ")");
    }

    /**
     * Companion to the integer-path test: pins the fraction-path streaming behavior
     * so a future refactor cannot regress it.
     */
    @Test
    void fractionPath_streamingChunks_rejectsBeyondMaxNumberLength() throws Exception {
        int chunksFed = streamDigitsUntilRejected("{\"v\":0.", CHUNK_SIZE);
        assertTrue(chunksFed <= 2,
                "StreamConstraintsException raised too late: after " + chunksFed
                        + " chunks of " + CHUNK_SIZE
                        + " digits (maxNumberLength=" + MAX_NUM_LEN + ")");
    }

    /**
     * Pins the headline cross-chunk scenario directly: with a chunk size well below
     * {@code maxNumberLength}, the limit is only crossed after several streaming
     * suspensions, so this exercises accumulation inside {@code _finishNumberIntegralPart}
     * (not just a single oversized first chunk). Asserts the exception fires at the
     * expected boundary — after more than one chunk, but as soon as the limit is passed.
     */
    @Test
    void integerPath_smallChunksAccumulate_rejectAtBoundary() throws Exception {
        final int smallChunk = 100; // < MAX_NUM_LEN, so several chunks are needed
        // Limit is crossed once accumulated digits exceed MAX_NUM_LEN; with 100-digit
        // chunks that is the 11th chunk (1000 ok at chunk 10, 1100 > 1000 at chunk 11).
        final int expectedFailChunk = (MAX_NUM_LEN / smallChunk) + 1;

        int chunksFed = streamDigitsUntilRejected("{\"v\":", smallChunk);
        // Must accumulate across several chunks before firing...
        assertTrue(chunksFed > 1,
                "Exception fired on a single chunk; cross-chunk accumulation not exercised");
        // ...and fire as soon as the limit is passed, not arbitrarily later.
        assertTrue(chunksFed <= expectedFailChunk + 1,
                "StreamConstraintsException raised too late: after " + chunksFed
                        + " chunks of " + smallChunk + " (expected ~" + expectedFailChunk
                        + ", maxNumberLength=" + MAX_NUM_LEN + ")");
    }

    /**
     * Guards against the validator becoming over-eager: an integer whose length is
     * just below {@code maxNumberLength}, fed across many small chunks (each smaller
     * than the limit, so accumulation crosses several streaming-suspension points),
     * must still parse cleanly to the expected value.
     */
    @Test
    void integerPath_justUnderMaxNumberLength_parsesCleanly() throws Exception {
        // One short of the limit, so the validator must NOT fire.
        final int digitCount = MAX_NUM_LEN - 1;
        final String number = digitString(digitCount);

        // Feed the whole document in small chunks (well under maxNumberLength) so the
        // integer is accumulated across multiple _finishNumberIntegralPart resumes.
        try (AsyncReaderWrapper r = asyncForBytes(STRICT_F, 100,
                utf8Bytes("{\"v\":" + number + "}"), 0)) {
            assertToken(JsonToken.START_OBJECT, r.nextToken());
            assertToken(JsonToken.PROPERTY_NAME, r.nextToken());
            assertToken(JsonToken.VALUE_NUMBER_INT, r.nextToken());
            assertEquals(number, r.currentText(),
                    "Sub-limit integer should parse without StreamConstraintsException");
            assertToken(JsonToken.END_OBJECT, r.nextToken());
        }
    }

    /**
     * Negative-number variant of the cross-chunk rejection test: a leading {@code '-'}
     * routes through {@code _startNegativeNumber()} and the {@code negMod == -1} branch
     * of {@code _finishNumberIntegralPart}, so the sign must be excluded from the
     * validated digit length. With chunk size below {@code maxNumberLength}, the limit
     * is only crossed after accumulating across several streaming suspensions.
     */
    @Test
    void negativeIntegerPath_smallChunksAccumulate_rejectAtBoundary() throws Exception {
        final int smallChunk = 100; // < MAX_NUM_LEN, so several chunks are needed
        // Sign is excluded from the validated length, so the limit is crossed once the
        // accumulated *digit* count exceeds MAX_NUM_LEN: 1000 ok at chunk 10, 1100 > 1000
        // at chunk 11 (same boundary as the positive case).
        final int expectedFailChunk = (MAX_NUM_LEN / smallChunk) + 1;

        // Note trailing '-': routes the value through the negative-number path.
        int chunksFed = streamDigitsUntilRejected("{\"v\":-", smallChunk);
        // Must accumulate across several chunks before firing...
        assertTrue(chunksFed > 1,
                "Exception fired on a single chunk; cross-chunk accumulation not exercised");
        // ...and fire at the same boundary as the positive case (sign excluded from the
        // validated length), not one chunk earlier.
        assertTrue(chunksFed >= expectedFailChunk,
                "StreamConstraintsException raised too early (sign likely counted): after "
                        + chunksFed + " chunks of " + smallChunk
                        + " (expected ~" + expectedFailChunk + ", maxNumberLength=" + MAX_NUM_LEN + ")");
        assertTrue(chunksFed <= expectedFailChunk + 1,
                "StreamConstraintsException raised too late: after " + chunksFed
                        + " chunks of " + smallChunk + " (expected ~" + expectedFailChunk
                        + ", maxNumberLength=" + MAX_NUM_LEN + ")");
    }

    // // // Helper methods

    private void _asyncParserFailsTooLongNumber(JsonParser p, JsonToken tokenMatch) throws Exception {
        boolean foundNumber = false;
        try {
            while (p.nextToken() != null) {
                if (p.currentToken() == tokenMatch) {
                    foundNumber = true;
                    String numberText = p.getString();
                    assertEquals(TEST_NUMBER_LENGTH, numberText.length(),
                            "Async parser silently accepted all " + TEST_NUMBER_LENGTH + " digits");
                }
            }
            fail("Async parser must reject a " + TEST_NUMBER_LENGTH + "-digit number (number found? "+foundNumber+")");
        } catch (StreamConstraintsException e) {
            verifyException(e, "Number value length (");
            verifyException(e, " exceeds the maximum allowed");
        }
    }

    /**
     * Feeds {@code preamble} (drained to {@code NOT_AVAILABLE}), then streams fixed-size
     * digit-only chunks with no terminator and no {@code endOfInput()}, returning the
     * number of chunks fed before {@link StreamConstraintsException} is raised. Fails the
     * test if any token other than {@code NOT_AVAILABLE} appears while streaming, or if
     * {@link #MAX_CHUNKS} chunks are accepted without the limit being enforced.
     */
    private int streamDigitsUntilRejected(String preamble, int chunkSize) throws Exception {
        try (JsonParser ap = STRICT_F.createNonBlockingByteArrayParser(ObjectReadContext.empty())) {
            ByteArrayFeeder feeder = (ByteArrayFeeder) ap;

            // Open object and field name (and number prefix); no terminator for the value.
            byte[] pre = utf8Bytes(preamble);
            feeder.feedInput(pre, 0, pre.length);
            JsonToken t;
            while ((t = ap.nextToken()) != JsonToken.NOT_AVAILABLE) {
                if (t == null) {
                    fail("Parser ended unexpectedly while draining preamble");
                }
            }

            byte[] digits = digitBytes(chunkSize);
            int chunksFed = 0;
            try {
                for (int c = 0; c < MAX_CHUNKS; c++) {
                    feeder.feedInput(digits, 0, digits.length);
                    chunksFed++;
                    assertToken(JsonToken.NOT_AVAILABLE, ap.nextToken());
                }
            } catch (StreamConstraintsException e) {
                // Expected: validator must fire on a NOT_AVAILABLE exit once the
                // accumulated digit length exceeds maxNumberLength.
                verifyException(e, "Number value length (");
                verifyException(e, " exceeds the maximum allowed");
                return chunksFed;
            }
            // Reaching here means the parser accepted chunkSize * MAX_CHUNKS digits
            // without raising — i.e. maxNumberLength was not enforced while streaming.
            fail("Async parser accepted " + (chunkSize * MAX_CHUNKS)
                    + " digits with maxNumberLength=" + MAX_NUM_LEN
                    + "; expected StreamConstraintsException");
            return -1; // unreachable
        }
    }

    private byte[] buildPayloadWithLongInteger(int numDigits) {
        return utf8Bytes("{\"v\":" + digitString(numDigits) + "}");
    }

    private byte[] buildPayloadWithLongDecimal(int numDigits) {
        return utf8Bytes("{\"v\":0." + digitString(numDigits) + "}");
    }

    private byte[] buildPayloadWithLongExponent(int numDigits) {
        return utf8Bytes("{\"v\":1.1E" + digitString(numDigits));
    }

    private static byte[] digitBytes(int count) {
        byte[] digits = new byte[count];
        for (int i = 0; i < count; i++) {
            digits[i] = (byte) ('1' + (i % 9));
        }
        return digits;
    }

    private static String digitString(int count) {
        StringBuilder sb = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            sb.append((char) ('1' + (i % 9)));
        }
        return sb.toString();
    }
}
