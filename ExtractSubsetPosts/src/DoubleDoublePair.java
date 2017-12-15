import java.io.*;
import org.apache.hadoop.io.*;

public class DoubleDoublePair implements WritableComparable<DoubleDoublePair> {

   private DoubleWritable x;
   private DoubleWritable y;

   public DoubleDoublePair() {
      set(new DoubleWritable(), new DoubleWritable());
   }

   public DoubleDoublePair(double x, double y) {
      set(new DoubleWritable(x), new DoubleWritable(y));
   }

   public void set(DoubleWritable x, DoubleWritable y) {
      this.x = x;
      this.y = y;
   }

   public DoubleWritable getX() {
      return x;
   }

   public DoubleWritable getY() {
      return y;
   }

   @Override
   public void write(DataOutput out) throws IOException {
      x.write(out);
      y.write(out);
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      x.readFields(in);
      y.readFields(in);
   }

   @Override
   public int hashCode() {
      return x.hashCode() * 163 + y.hashCode();
   }

   @Override
   public boolean equals(Object o) {
      if (o instanceof DoubleDoublePair) {
         DoubleDoublePair tp = (DoubleDoublePair) o;
         return x.equals(tp.x) && y.equals(tp.y);
      }
      return false;
   }

   @Override
   public String toString() {
      return x + "\t" + y;
   }

   @Override
   public int compareTo(DoubleDoublePair tp) {
      int cmp = x.compareTo(tp.x);
      if (cmp != 0) {
         return cmp;
      }
      return y.compareTo(tp.y);
   }
}
