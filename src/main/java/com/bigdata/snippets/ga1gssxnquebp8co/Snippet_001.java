// Source: Day04-Flink四大基石2 (ga1gssxnquebp8co) snippet #1
package com.bigdata.snippets;

public class ga1gssxnquebp8coSnippet001 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        Caused by: java.lang.NullPointerException: No key set. This method should not be called outside of a keyed context.
        	at org.apache.flink.util.Preconditions.checkNotNull(Preconditions.java:76)
        	at org.apache.flink.runtime.state.heap.StateTable.checkKeyNamespacePreconditions(StateTable.java:270)
        	at org.apache.flink.runtime.state.heap.StateTable.remove(StateTable.java:276)
    }
}
