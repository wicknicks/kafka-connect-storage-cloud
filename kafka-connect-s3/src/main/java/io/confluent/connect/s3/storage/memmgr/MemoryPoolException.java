package io.confluent.connect.s3.storage.memmgr;

import org.apache.kafka.connect.errors.ConnectException;

public class MemoryPoolException extends ConnectException {

  public MemoryPoolException(String msg) {
    super(msg);
  }

}
