package org.radargun.tpcc;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.concurrent.atomic.AtomicLong;

/**
 * // TODO: Document this
 *
 * @author pruivo
 * @since 4.0
 */
public class CounterObjectOutput implements ObjectOutput {
   private static final AtomicLong counter = new AtomicLong(0);

   private final ObjectOutputStream delegate;

   public CounterObjectOutput(ObjectOutputStream delegate) throws IOException {
      super();
      this.delegate = delegate;
   }

   public long getCounter() {
      return counter.get();
   }

   @Override
   public void writeObject(Object o) throws IOException {
      counter.incrementAndGet();
      delegate.writeObject(o);
   }

   @Override
   public void write(int i) throws IOException {
      delegate.write(i);
   }

   @Override
   public void write(byte[] bytes) throws IOException {
      delegate.write(bytes);
   }

   @Override
   public void write(byte[] bytes, int i, int i1) throws IOException {
      delegate.write(bytes, i, i1);
   }

   @Override
   public void writeBoolean(boolean b) throws IOException {
      delegate.writeBoolean(b);
   }

   @Override
   public void writeByte(int i) throws IOException {
      delegate.writeByte(i);
   }

   @Override
   public void flush() throws IOException {
      delegate.flush();
   }

   @Override
   public void close() throws IOException {
      delegate.close();
   }

   @Override
   public void writeShort(int i) throws IOException {
      delegate.writeShort(i);
   }

   @Override
   public void writeChar(int i) throws IOException {
      delegate.writeChar(i);
   }

   @Override
   public void writeInt(int i) throws IOException {
      delegate.writeInt(i);
   }

   @Override
   public void writeLong(long l) throws IOException {
      delegate.writeLong(l);
   }

   @Override
   public void writeFloat(float v) throws IOException {
      delegate.writeFloat(v);
   }

   @Override
   public void writeDouble(double v) throws IOException {
      delegate.writeDouble(v);
   }

   @Override
   public void writeBytes(String s) throws IOException {
      delegate.writeBytes(s);
   }

   @Override
   public void writeChars(String s) throws IOException {
      delegate.writeChars(s);
   }

   @Override
   public void writeUTF(String s) throws IOException {
      delegate.writeUTF(s);
   }
}
