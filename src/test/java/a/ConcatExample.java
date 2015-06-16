/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class ConcatExample {
  Random r = new Random();

  @Test
  public void concatIsPermissive() throws IOException, URISyntaxException {
    MiniDFSCluster cluster = null;
    final Configuration conf = WebHdfsTestUtil.createConf();
    conf.set("dfs.namenode.fs-limits.min-block-size", "1000"); // Allow tiny blocks for the test
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      cluster.waitActive();
      final FileSystem webHdfs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
          WebHdfsFileSystem.SCHEME);
      final FileSystem dfs = cluster.getFileSystem();

      final FileSystem fs = dfs;  // WebHDFS has a bug in getLocatedBlocks

      Path root = new Path("/dir");
      fs.mkdirs(root);

      short origRep = 2;
      short secondRep = (short)(origRep + 1);
      Path f1 = new Path("/dir/f1");
      long size1 = writeFile(fs, f1, /* blocksize */ 4096, origRep, 5);
      long f1NumBlocks = fs.getFileBlockLocations(f1, 0, size1).length;
      assertEquals(5, f1NumBlocks);

      Path f2 = new Path("/dir/f2");
      long size2 = writeFile(fs, f2, /* blocksize (must divide 512 for checksum) */ 4096 - 512, secondRep, 4);
      long f2NumBlocks = fs.getFileBlockLocations(f2, 0, size2).length;
      assertEquals(5, f2NumBlocks);

      fs.concat(f1, new Path [] {f2});
      FileStatus[] fileStatuses = fs.listStatus(root);

      // Only one file should remain
      assertEquals(1, fileStatuses.length);
      FileStatus fileStatus = fileStatuses[0];

      // And it should be named after the first file
      assertEquals("f1", fileStatus.getPath().getName());

      // The entire file takes the replication of the first argument
      assertEquals(origRep, fileStatus.getReplication());

      // As expected, the new concated file is the length of both the previous files
      assertEquals(size1 + size2, fileStatus.getLen());

      // And we should have the same number of blocks
      assertEquals(f1NumBlocks + f2NumBlocks, fs.getFileBlockLocations(fileStatus.getPath(), 0, size1 + size2).length);
    } finally {
      if(cluster != null) {
        cluster.shutdown();
      }

    }
  }

  private long writeFile(FileSystem fs, Path p, int blockSize, short replication, int numBlocks) throws IOException {

    int bufferSize = 4096;
    FSDataOutputStream os = fs.create(p, true, bufferSize, replication, blockSize);

    int i = 0;

    byte [] data = new byte[bufferSize];
    r.nextBytes(data);
    while(i < blockSize * numBlocks) {
      os.write(data);
      i += data.length;
    }
    os.close();
    FileStatus fileStatus = fs.getFileStatus(p);
    long f1Len = fileStatus.getLen();
    //int f1NumBlocks = fs.getFileBlockLocations(fileStatus, 0, f1Len).length;

    assertEquals(i, f1Len);

    return f1Len;
  }
}
