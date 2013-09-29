package org.radargun.tpcc;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Random;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;

/**
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 * @author Pedro Ruivo
 */
public final class TpccTools {

   public static final double WAREHOUSE_YTD = 300000.00;

   public static final int NB_MAX_DISTRICT = 10;

   public static final long NB_MAX_ITEM = 10000; // 100000

   public static final int NB_MAX_CUSTOMER = 50; // 3000

   public static final int NB_MAX_ORDER = 50; // 3000

   public static final String CHAINE_5_1 = "11111";

   public final static int MIN_C_LAST = 0;

   public final static int MAX_C_LAST = 999;

   public final static String[] C_LAST = {"BAR", "OUGHT"/*,"ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"*/};

   public final static int LIMIT_ORDER = 2101;

   public static final int NULL_NUMBER = -1;

   public final static int S_DATA_MINN = 26;

   public static int NB_WAREHOUSES = 1;

   public static long A_C_LAST = 255L;

   public static long A_OL_I_ID = 8191L;

   public static long A_C_ID = 1023L;

   public static long C_C_LAST = 0L;

   public static long C_OL_I_ID = 0L;

   public static long C_C_ID = 0L;

   private final static int DEFAULT_RADIX = 10;

   private final static int DEFAULT_MINL = 65;

   private final static int DEFAULT_MAXL = 90;

   private final static int DEFAULT_MINN = 48;

   private final static int DEFAULT_MAXN = 57;

   private final static int S_DATA_MAXN = 50;

   public final static String ORIGINAL = "ORIGINAL";

   public final static int [] NUMBER_OF_ITEMS_INTERVAL = {5,15};

   private final static int unicode[][] = {{65, 126}, {192, 259}};

   private final Random randUniform;

   private final Random randNonUniform;

   private final Random randAlea;

   private TpccTools(long seed) {
      randUniform = new Random(seed * 31);
      randNonUniform = new Random(seed * 31 * 17);
      randAlea = new Random(seed * 31 * 17 * 3);
   }

   public static TpccTools newInstance() {
      return new TpccTools(System.nanoTime());
   }

   public static TpccTools newInstance(long seed) {
      return new TpccTools(seed);
   }

   private String aleaChaine(int deb, int fin, int min, int max, int radix) {
      if (deb > fin) return null;
      String chaine = "";
      int lch = fin;

      if (deb != fin) lch = aleaNumber(deb, fin);

      for (int i = 0; i < lch; i++) {
         int random = randAlea.nextInt(max - min + 1) + min;
         char c = (char) (((byte) random) & 0xff);
         chaine += c;
      }
      return chaine;
   }


   public final String aleaChainel(int deb, int fin, int radix) {
      return aleaChaine(deb, fin, DEFAULT_MINL, DEFAULT_MAXL, radix);
   }

   public final String aleaChainel(int deb, int fin) {
      return aleaChainel(deb, fin, DEFAULT_RADIX);
   }


   public final String aleaChainec(int deb, int fin, int radix) {
      if (deb > fin) return null;
      String chaine = "";
      String str = null;

      int lch = fin;
      if (deb != fin) lch = aleaNumber(deb, fin);

      for (int i = 0; i < lch; i++) {
         int ref = randAlea.nextInt(2);
         int min = unicode[ref][0];
         int max = unicode[ref][1];
         int random = randAlea.nextInt(max - min + 1) + min;

         char c = (char) (((byte) random));
         chaine += c;
      }
      try {
         str = new String(chaine.getBytes(), "UTF-8");
      } catch (UnsupportedEncodingException e) {
         System.out.println("----------- Error " + e.getMessage());
      }
      return str;
   }

   public final String aleaChainec(int deb, int fin) {
      return aleaChainec(deb, fin, DEFAULT_RADIX);
   }

   public final String sData() {
      String alea = aleaChainec(S_DATA_MINN, S_DATA_MAXN);
      if (aleaNumber(1, 10) == 1) {
         long number = randomNumber(0, alea.length() - 8);
         alea = alea.substring(0, (int) number) + ORIGINAL + alea.substring((int) number + 8, alea.length());
      }
      return alea;
   }


   public final String aleaChainen(int deb, int fin, int radix) {
      return aleaChaine(deb, fin, DEFAULT_MINN, DEFAULT_MAXN, radix);
   }

   public final String aleaChainen(int deb, int fin) {
      return aleaChainen(deb, fin, DEFAULT_RADIX);
   }


   public final int aleaNumber(int deb, int fin) {
      return randAlea.nextInt(fin - deb + 1) + deb;
   }


   public final long aleaNumber(long deb, long fin) {
      long random = randAlea.nextLong() % (fin + 1);
      while (random < deb) random += fin - deb;
      return random;
   }

   public final float aleaFloat(float deb, float fin, int virg) {
      if (deb > fin || virg < 1) return 0;
      long pow = (long) Math.pow(10, virg);
      long amin = (long) (deb * pow);
      long amax = (long) (fin * pow);
      long random = (long) (randAlea.nextDouble() * (amax - amin) + amin);
      return (float) random / pow;
   }

   public final double aleaDouble(double deb, double fin, int virg) {
      if (deb >= fin || virg < 1) return 0.;
      long pow = (long) Math.pow(10, virg);
      long amin = (long) (deb * pow);
      long amax = (long) (fin * pow);
      long random = (long) (randAlea.nextDouble() * (amax - amin) + amin);
      return (double) random / pow;
   }

   public final long randomNumber(long min, long max) {
      return (long) (randUniform.nextDouble() * (max - min + 1) + min);
   }

   public final double doubleRandomNumber(long min, long max) {
      return randUniform.nextDouble() * (max - min + 1) + min;
   }

   public final long randomNumberForNonUniform(long min, long max) {
      return (long) (randNonUniform.nextDouble() * (max - min + 1) + min);
   }

   public final long nonUniformRandom(long type, long x, long min, long max) {
      return (((randomNumberForNonUniform(0, x) | randomNumberForNonUniform(min, max)) + type) % (max - min + 1)) + min;
   }

   public static void selectLocalWarehouse(int numberOfSlaves, int slaveIdx, List<Integer> localWarehousesList) {
      int init = slaveIdx % NB_WAREHOUSES;

      for (int i = init + 1; i <= NB_WAREHOUSES; i += numberOfSlaves) {
         localWarehousesList.add(i);
      }
   }
   
   public static final void put(CacheWrapper cacheWrapper, LocatedKey key, Object value) {
   try {
       cacheWrapper.put(null, key, value);
   } catch (Exception e) {
       if (e instanceof RuntimeException) {
       throw (RuntimeException)e;
       }
       e.printStackTrace();
   }
   }
   
   public static final <T> T get(CacheWrapper cacheWrapper, LocatedKey key) {
   try {
       return (T) cacheWrapper.get(null, key);
   } catch (Exception e) {
       if (e instanceof RuntimeException) {
       throw (RuntimeException)e;
       }
       e.printStackTrace();
       return null;
   }
   }
}
