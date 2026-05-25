package com.bigdata.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.auth.Subject;
import java.security.PrivilegedExceptionAction;

/**
 * Java 17.0.13+ compatibility helper for Iceberg + Hadoop demos.
 *
 * Root Cause:
 *   JDK 17.0.13 changed Subject.getSubject(AccessControlContext) to throw
 *   UnsupportedOperationException when the Security Manager is disabled.
 *   Hadoop ≤ 3.4.1 called this method directly in UserGroupInformation.getCurrentUser().
 *
 * Fix:
 *   Hadoop 3.4.3 ships SubjectUtil.current() which uses Subject.current() (Java 21 API)
 *   with fallback to the AccessController approach, wrapped in a try-catch.
 *   Upgrading to hadoop-common:3.4.3 + hadoop-hdfs-client:3.4.3 is sufficient.
 *   initSimpleAuth() still configures Hadoop for non-Kerberos mode (simple auth).
 */
public final class HadoopCompat {

    private HadoopCompat() {}

    /**
     * Call ONCE at the very start of main(), before any Hadoop / Iceberg code.
     * Sets the JDK 17.0.13 back-compat flag and configures Hadoop for simple auth.
     */
    public static void initSimpleAuth() throws Exception {
        // No JVM-level hack needed with Hadoop 3.4.3+; just set simple auth mode.
        Configuration conf = new Configuration(false);
        conf.set("hadoop.security.authentication", "simple");
        conf.set("hadoop.security.authorization",  "false");
        UserGroupInformation.setConfiguration(conf);
    }

    /**
     * @deprecated No longer needed after adding jdk.security.allowGetSubject=true.
     * Kept for reference; you may call runWithSubject(action) directly or just use
     * initSimpleAuth() and run code normally.
     */
    public static <T> T runWithSubject(PrivilegedExceptionAction<T> action) throws Exception {
        return action.run();
    }
}




