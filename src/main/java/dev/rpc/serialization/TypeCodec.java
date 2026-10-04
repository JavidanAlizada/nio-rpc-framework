package dev.rpc.serialization;

/**
 * Reads and writes one declared type. Codecs nest: a record codec holds its components' codecs, a list codec its
 * element codec. depth is the current nesting level, checked by the codecs that nest.
 */
interface TypeCodec {

    void write(Output out, Object value, int depth);

    Object read(Input in, int depth);

    /** Throws if going one level deeper would pass the limit. */
    static void checkDepth(int depth, int maxDepth) {
        if (depth >= maxDepth) {
            throw new SerializationException("nesting deeper than " + maxDepth + " levels");
        }
    }
}
