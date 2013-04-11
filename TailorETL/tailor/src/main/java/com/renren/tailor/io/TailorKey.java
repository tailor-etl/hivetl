package com.renren.tailor.io;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.WritableComparator;

public class TailorKey extends ByteWritable {
  
  private static final int LENGTH_BYTES = 4;

  boolean hashCodeValid;

  public TailorKey() {
    hashCodeValid = false;
  }

  protected int myHashCode;

  public void setHashCode(int myHashCode) {
    hashCodeValid = true;
    this.myHashCode = myHashCode;
  }

  @Override
  public int hashCode() {
    if (!hashCodeValid) {
      throw new RuntimeException("Cannot get hashCode() from deserialized "
          + TailorKey.class);
    }
    return myHashCode;
  }

  /** A Comparator optimized for HiveKey. */
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(TailorKey.class);
    }

    /**
     * Compare the buffers in serialized form.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return compareBytes(b1, s1 + LENGTH_BYTES, l1 - LENGTH_BYTES, b2, s2
          + LENGTH_BYTES, l2 - LENGTH_BYTES);
    }
  }

  static {
    WritableComparator.define(TailorKey.class, new Comparator());
  }
 
}
