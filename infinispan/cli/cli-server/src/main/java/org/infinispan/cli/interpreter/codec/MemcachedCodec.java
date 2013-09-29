/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.cli.interpreter.codec;

import java.nio.charset.Charset;

import org.infinispan.cli.interpreter.Interpreter;
import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.server.memcached.MemcachedValue;
import org.infinispan.util.logging.LogFactory;

/**
 *
 * MemcachedCodec.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class MemcachedCodec implements Codec {
   private static final Log log = LogFactory.getLog(Interpreter.class, Log.class);
   private Charset UTF8 = Charset.forName("UTF-8");

   @Override
   public String getName() {
      return "memcached";
   }

   @Override
   public Object encodeKey(Object key) {
      return key;
   }

   @Override
   public Object encodeValue(Object value) throws CodecException {
      if (value != null) {
         if (value instanceof MemcachedValue) {
            return value;
         } else if (value instanceof byte[]) {
            return new MemcachedValue((byte[])value, 1, 0);
         } else if (value instanceof String) {
            return new MemcachedValue(((String)value).getBytes(UTF8), 1, 0);
         } else {
            throw log.valueEncodingFailed(value.getClass().getName(), this.getName());
         }
      } else {
         return null;
      }
   }

   @Override
   public Object decodeKey(Object key) {
      return key;
   }

   @Override
   public Object decodeValue(Object value) {
      if (value != null) {
         MemcachedValue mv = (MemcachedValue)value;
         return new String(mv.data(), UTF8);
      } else {
         return null;
      }

   }

}
