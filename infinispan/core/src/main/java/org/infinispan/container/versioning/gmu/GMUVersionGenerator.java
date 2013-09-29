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
package org.infinispan.container.versioning.gmu;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.dataplacement.ClusterSnapshot;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface GMUVersionGenerator extends VersionGenerator {
   GMUVersion mergeAndMax(EntryVersion... entryVersions);

   GMUVersion mergeAndMin(EntryVersion... entryVersions);

   GMUVersion calculateCommitVersion(EntryVersion prepareVersion, Collection<Address> affectedOwners);

   GMUCacheEntryVersion convertVersionToWrite(EntryVersion version, int subVersion);

   GMUReadVersion convertVersionToRead(EntryVersion version);

   GMUVersion calculateMaxVersionToRead(EntryVersion transactionVersion, Collection<Address> alreadyReadFrom);

   GMUVersion calculateMinVersionToRead(EntryVersion transactionVersion, Collection<Address> alreadyReadFrom);

   GMUVersion setNodeVersion(EntryVersion version, long value);

   GMUVersion updatedVersion(EntryVersion entryVersion);

   ClusterSnapshot getClusterSnapshot(int viewId);

   Address getAddress();
}
