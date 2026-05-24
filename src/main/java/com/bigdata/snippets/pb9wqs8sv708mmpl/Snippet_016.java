// Source: Day05-FlinkSQL (pb9wqs8sv708mmpl) snippet #16
package com.bigdata.snippets;

public class pb9wqs8sv708mmplSnippet016 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        Exception in thread "main" org.apache.flink.table.api.TableException: Table sink '*anonymous_datastream_sink$2*' doesn't support consuming update changes which is produced by node GroupAggregate(groupBy=[word], select=[word, SUM(num) AS cnt])
        	at org.apache.flink.table.planner.plan.optimize.program.FlinkChangelogModeInferenceProgram$SatisfyModifyKindSetTraitVisitor.createNewNode(FlinkChangelogModeInferenceProgram.scala:405)
        	at org.apache.flink.table.planner.plan.optimize.program.FlinkChangelogModeInferenceProgram$SatisfyModifyKindSetTraitVisitor.visit(FlinkChangelogModeInferenceProgram.scala:185)
        	at org.apache.flink.table.planner.plan.optimize.program.FlinkChangelogModeInferenceProgram$SatisfyModifyKindSetTraitVisitor.visitChild(FlinkChangelogModeInferenceProgram.scala:366)
        	at org.apache.flink.table.planner.plan.optimize.program.FlinkChangelogModeInferenceProgram$SatisfyModifyKindSetTraitVisitor.$anonfun$visitChildren$1(FlinkChangelogModeInferenceProgram.scala:355)
        	at org.apache.flink.table.planner.plan.optimize.program.FlinkChangelogModeInferenceProgram$SatisfyModifyKindSetTraitVisitor.$anonfun$visitChildren$1$adapted(FlinkChangelogModeInferenceProgram.scala:354)
        	at scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:233)
    }
}
