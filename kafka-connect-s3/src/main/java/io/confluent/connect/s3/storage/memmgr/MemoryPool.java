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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation of an expandable memory manager. This class is NOT thread safe.
 */
public class MemoryPool {

  // size of each buffer in the list (in bytes)
  protected final AtomicInteger idTracker = new AtomicInteger(0);
  protected final int shardSize;
  protected final LinkedList<Shard> shards = new LinkedList<>();
  protected final Set<Integer> freeShards = new HashSet<>();
  protected final Map<Part, Shard> shardTails = new HashMap<>();
  protected final Map<Integer, Shard> shardIdx = new HashMap<>();

  public MemoryPool(int shardSize) {
    this.shardSize = shardSize;
    addNewShardToPool();
  }

  private Shard addNewShardToPool() {
    Shard buff = newBuffer();
    shards.add(buff);
    shardIdx.put(buff.id, buff);
    freeShards.add(buff.id);
    return buff;
  }

  private Shard newBuffer() {
    return new Shard(idTracker.incrementAndGet(), ByteBuffer.allocate(this.shardSize));
  }

  private int nextEmpty() {
    if (freeShards.isEmpty()) {
      Shard s = addNewShardToPool();
      return s.id;
    } else {
      return freeShards.iterator().next();
    }
  }

  public void free(Part part) {
    Shard current = shardIdx.get(part.id);
    while (current != null) {
      freeShards.add(current.id);
      shardIdx.remove(current.id);
      shards.remove(current);
      int next = current.next();
      current = shardIdx.get(next);
    }
    shardTails.remove(part);
  }

  public Part allocate(int partSize) {
    int idx = nextEmpty();
    Part part = new Part(idx, this, partSize);
    shardTails.put(part, shardIdx.get(idx));
    return part;
  }

  public void write(Part part, byte b) {
    Shard shard = shardTails.get(part);
    if (shard.buffer.hasRemaining()) {
      shard.buffer.put(b);
    } else {
      int idx = nextEmpty();
      shard.next(idx);
      Shard t = shardIdx.get(idx);
      shardTails.put(part, t);
      t.buffer.put(b);
    }
  }

  public void write(Part part, byte[] b, int offset, int length) {
    Shard shard = shardTails.get(part);
    int remaining = shard.buffer.remaining();
    if (remaining == 0) {
      int idx = nextEmpty();
      shard.next(idx);
      Shard t = shardIdx.get(idx);
      shardTails.put(part, t);
      write(part, b, offset, length);
      return;
    } else if (remaining > length) {
      shard.buffer.put(b, offset, length);
      return;
    }

    // length > remaining
    shard.buffer.put(b, offset, remaining);
    length = length - remaining;
    offset = offset + length;

    // ugh, replace with while
    for (int i = 0; i < (1 + (length / shardSize)); i++) {
      int idx = nextEmpty();
      Shard t = shardIdx.get(idx);
      shardTails.put(part, t);
      t.buffer.put(b, offset, shardSize);
      length = length - shardSize;
      offset = offset + shardSize;
    }
  }

  public InputStream newStream(Part part) {
    return new ReadOnlyStream(part);
  }

  class ReadOnlyStream extends InputStream {

    private Shard current;
    private int pos = 0;
    private int limit = -1;

    public ReadOnlyStream(Part part) {
      setupNextShard(part.id);
    }

    protected void setupNextShard(int nextId) {
      this.current = shardIdx.get(nextId);
      this.limit = current.buffer.limit();
    }

    @Override
    public int read() {
      if (pos < limit) {
        return current.buffer.get(pos++);
      } else {
        if (current.next < 0) {
          return -1;
        } else {
          setupNextShard(current.next);
          if (pos < limit) {
            return current.buffer.get(pos++);
          } else {
            return -1;
          }
        }
      }
    }
  }

  static class Shard {
    final int id;
    final ByteBuffer buffer;
    int next = -1;

    Shard(int id, ByteBuffer buffer) {
      this.id = id;
      this.buffer = buffer;
    }

    public int next() {
      return next;
    }

    public void next(int next) {
      this.next = next;
    }
  }

  public static void main(String[] args) {
    ByteBuffer buffer = ByteBuffer.allocate(100);
    for (int i = 0; i < 50; i++) {
      buffer.put((byte) i);
    }

    System.out.println(buffer.hasRemaining());
    System.out.println(buffer.position());
    System.out.println(buffer.limit());
    buffer.flip();

    int ix = 0;
    do {
      System.out.println("> " + buffer.get(ix) + " " + ix);
      ix++;
    } while (ix > buffer.position());

    System.out.println(ix);
  }
}
