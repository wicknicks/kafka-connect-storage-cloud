/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.s3.storage.memmgr;

import java.io.InputStream;

public class Part {

  protected final int id;
  protected final MemoryPool pool;
  private final int partSize;
  private int bytesWritten = 0;

  Part(int id, MemoryPool pool, int partSize) {
    this.id = id;
    this.partSize = partSize;
    this.pool = pool;
  }

  public void put(byte b) {
    bytesWritten++;
    pool.write(this, b);
  }

  public void put(byte[] b, int offset, int length) {
    pool.write(this, b, offset, length);
  }

  public InputStream asInputStream() {
    return pool.newStream(this);
  }

  public void clear() {
    pool.free(this);
  }

  public boolean hasRemaining() {
    return bytesWritten < partSize;
  }

  public int remaining() {
    return (partSize - bytesWritten);
  }

  public int position() {
    return bytesWritten;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Part part = (Part) o;
    return id == part.id;
  }

  @Override
  public int hashCode() {
    return id;
  }

}
