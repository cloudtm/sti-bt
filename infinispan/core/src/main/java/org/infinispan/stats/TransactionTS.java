/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.stats;

/**
 * User: roberto
 * Date: 14/12/12
 * Time: 15:46
 */

public class TransactionTS {
   private long endLastTxTs;
   private long NTBC_execution_time;
   private long NTBC_count;

   public long getEndLastTxTs() {
      return endLastTxTs;
   }

   public void setEndLastTxTs(long endLastTxTs) {
      this.endLastTxTs = endLastTxTs;
   }

   public long getNTBC_execution_time() {
      return NTBC_execution_time;
   }

   public void addNTBC_execution_time(long NTBC_execution_time) {
      this.NTBC_execution_time += NTBC_execution_time;
   }

   public long getNTBC_count() {
      return NTBC_count;
   }

   public void addNTBC_count() {
      this.NTBC_count++;
   }
}
