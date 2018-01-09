package MyCustomedHaoop.ValueClass;





/**
 * Copyright 2011 Michael Cutler <m@cotdp.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Extends the basic FileInputFormat class provided by Apache Hadoop to accept ZIP files. It should be noted that ZIP
 * files are not 'splittable' and each ZIP file will be processed by a single Mapper.
 * 
 * java.io.EOFException: Unexpected end of ZLIB input stream
        at java.util.zip.InflaterInputStream.fill(InflaterInputStream.java:223)
        at java.util.zip.InflaterInputStream.read(InflaterInputStream.java:141)
        at java.util.zip.ZipInputStream.read(ZipInputStream.java:154)
        at Mahout_DIY.ZipFileRecordReader.nextKeyValue(ZipFileRecordReader.java:
120)
        at org.apache.hadoop.mapred.MapTask$NewTrackingRecordReader.nextKeyValue
(MapTask.java:456)
        at org.apache.hadoop.mapreduce.MapContext.nextKeyValue(MapContext.java:6
7)
        at org.apache.hadoop.mapreduce.Mapper.run(Mapper.java:143)
        at org.apache.hadoop.mapred.MapTask.runNewMapper(MapTask.java:647)
        at org.apache.hadoop.mapred.MapTask.run(MapTask.java:323)
        at org.apache.hadoop.mapred.Child$4.run(Child.java:270)
        at java.security.AccessController.doPrivileged(Native Method)
        at javax.security.auth.Subject.doAs(Subject.java:396)
        at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInforma
tion.java:1177)

	solution: 
	Anyone that’s worked ZIP files before has probably encountered corrupt files at some point. Having worked on a Map/Reduce use-case that trawled through tens of thousands of ZIP files (of varying quality) – I have already encountered Jobs that fail because of tiny file corruptions.

	ZipFileInputFormat has a handy method “setLenient( boolean lenient )”, this defaults to false meaning any errors processing a ZIP file will be fatal to the overall Job. However if you are dealing with ZIP files of varying quality you can “setLenient( true )” which means ZIP parsing problems will be quietly ignored.

	Note: with the “setLenient( true )” ZIP files may be partially processed. Take the example of a truncated ZIP file, the contents of the ZIP archive up to the point of the file truncation will be passed to Mappers to be processed. Upon encountering the file corruption the Mapper will be informed that the file is “finished / completed”, and move on to the Reducer phase.

 */
public class ZipFileInputFormat
    extends FileInputFormat<Text, BytesWritable>
{
    /** See the comments on the setLenient() method */
    private static boolean isLenient = false;
    
    /**
     * ZIP files are not splitable
     */
    @Override
    protected boolean isSplitable( JobContext context, Path filename )
    {
        return false;
    }

    /**
     * Create the ZipFileRecordReader to parse the file
     */
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader( InputSplit split, TaskAttemptContext context )
        throws IOException, InterruptedException
    {
        return new ZipFileRecordReader();
    }
    
    /**
     * 
     * @param lenient
     */
    public static void setLenient( boolean lenient )
    {
        isLenient = lenient;
    }
    
    public static boolean getLenient()
    {
        return isLenient;
    }
}
