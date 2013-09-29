/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
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
package org.infinispan.factories;


import org.infinispan.batch.BatchContainer;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.CommandsFactoryImpl;
import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.gmu.GMUL1Manager;
import org.infinispan.container.gmu.L1GMUContainer;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.NonTransactionalInvocationContextContainer;
import org.infinispan.context.TransactionalInvocationContextContainer;
import org.infinispan.dataplacement.DataPlacementManager;
import org.infinispan.distribution.L1Manager;
import org.infinispan.distribution.L1ManagerImpl;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.ActivationManagerImpl;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.EvictionManagerImpl;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.eviction.PassivationManagerImpl;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheLoaderManagerImpl;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.CacheNotifierImpl;
import org.infinispan.reconfigurableprotocol.ProtocolTable;
import org.infinispan.reconfigurableprotocol.manager.ReconfigurableReplicationManager;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.statetransfer.StateTransferLockImpl;
import org.infinispan.transaction.TransactionCoordinator;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.transaction.gmu.manager.GarbageCollectorManager;
import org.infinispan.transaction.gmu.manager.TransactionCommitManager;
import org.infinispan.transaction.totalorder.DefaultTotalOrderManager;
import org.infinispan.transaction.totalorder.GMUTotalOrderManager;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.transaction.xa.recovery.RecoveryAdminOperations;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.locks.containers.LockContainer;
import org.infinispan.util.concurrent.locks.containers.OwnableReentrantPerEntryLockContainer;
import org.infinispan.util.concurrent.locks.containers.OwnableReentrantStripedLockContainer;
import org.infinispan.util.concurrent.locks.containers.ReentrantPerEntryLockContainer;
import org.infinispan.util.concurrent.locks.containers.ReentrantStripedLockContainer;
import org.infinispan.util.concurrent.locks.containers.readwrite.OwnableReentrantPerEntryReadWriteLockContainer;
import org.infinispan.util.concurrent.locks.containers.readwrite.OwnableReentrantStripedReadWriteLockContainer;
import org.infinispan.xsite.BackupSender;
import org.infinispan.xsite.BackupSenderImpl;

import static org.infinispan.util.Util.getInstance;

/**
 * Simple factory that just uses reflection and an empty constructor of the component type.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Pedro Ruivo
 * @since 4.0
 */
@DefaultFactoryFor(classes = {CacheNotifier.class, CommandsFactory.class,
                              CacheLoaderManager.class, InvocationContextContainer.class,
                              PassivationManager.class, ActivationManager.class,
                              BatchContainer.class, EvictionManager.class,
                              TransactionCoordinator.class, RecoveryAdminOperations.class, StateTransferLock.class,
                              ClusteringDependentLogic.class, LockContainer.class,
                              L1Manager.class, TransactionFactory.class, BackupSender.class,
                              TotalOrderManager.class, DataPlacementManager.class, L1GMUContainer.class, CommitLog.class,
                              TransactionCommitManager.class, GarbageCollectorManager.class, ReconfigurableReplicationManager.class,
                              ProtocolTable.class})
public class EmptyConstructorNamedCacheFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      Class<?> componentImpl;
      if (componentType.equals(ClusteringDependentLogic.class)) {
         CacheMode cacheMode = configuration.clustering().cacheMode();
         if (!cacheMode.isClustered()) {
            return componentType.cast(new ClusteringDependentLogic.LocalLogic());
         } else if (cacheMode.isInvalidation()) {
            return componentType.cast(new ClusteringDependentLogic.InvalidationLogic());
         } else if (cacheMode.isReplicated()) {
            return componentType.cast(new ClusteringDependentLogic.ReplicationLogic());
         } else {
            return componentType.cast(new ClusteringDependentLogic.DistributionLogic());
         }
      } else {
         boolean isTransactional = configuration.transaction().transactionMode().isTransactional();
         if (componentType.equals(InvocationContextContainer.class)) {
            componentImpl = isTransactional ? TransactionalInvocationContextContainer.class
                  : NonTransactionalInvocationContextContainer.class;
            return componentType.cast(getInstance(componentImpl));
         } else if (componentType.equals(CacheNotifier.class)) {
            return (T) new CacheNotifierImpl();
         } else if (componentType.equals(CommandsFactory.class)) {
            return (T) new CommandsFactoryImpl();
         } else if (componentType.equals(CacheLoaderManager.class)) {
            return (T) new CacheLoaderManagerImpl();
         } else if (componentType.equals(PassivationManager.class)) {
            return (T) new PassivationManagerImpl();
         } else if (componentType.equals(ActivationManager.class)) {
            return (T) new ActivationManagerImpl();
         } else if (componentType.equals(BatchContainer.class)) {
            return (T) new BatchContainer();
         } else if (componentType.equals(TransactionCoordinator.class)) {
            return (T) new TransactionCoordinator();
         } else if (componentType.equals(RecoveryAdminOperations.class)) {
            return (T) new RecoveryAdminOperations();
         } else if (componentType.equals(StateTransferLock.class)) {
            return (T) new StateTransferLockImpl();
         } else if (componentType.equals(EvictionManager.class)) {
            return (T) new EvictionManagerImpl();
         } else if (componentType.equals(LockContainer.class)) {
            boolean  notTransactional = !isTransactional;
            if (configuration.locking().isolationLevel() == IsolationLevel.SERIALIZABLE) {
               return (T) (configuration.locking().useLockStriping() ?
                                 new OwnableReentrantStripedReadWriteLockContainer(configuration.locking().concurrencyLevel()) :
                                 new OwnableReentrantPerEntryReadWriteLockContainer(configuration.locking().concurrencyLevel()));
            }
            LockContainer<?> lockContainer = configuration.locking().useLockStriping() ?
                  notTransactional ? new ReentrantStripedLockContainer(configuration.locking().concurrencyLevel())
                        : new OwnableReentrantStripedLockContainer(configuration.locking().concurrencyLevel()) :
                  notTransactional ? new ReentrantPerEntryLockContainer(configuration.locking().concurrencyLevel())
                        : new OwnableReentrantPerEntryLockContainer(configuration.locking().concurrencyLevel());
            return (T) lockContainer;
         } else if (componentType.equals(L1Manager.class)) {
            return (T) new L1ManagerImpl();
         } else if (componentType.equals(TransactionFactory.class)) {
            return (T) new TransactionFactory();
         } else if (componentType.equals(BackupSender.class)) {
            return (T) new BackupSenderImpl(globalConfiguration.sites().localSite());
         } else if (componentType.equals(TotalOrderManager.class)) {
            return (T) (configuration.locking().isolationLevel() == IsolationLevel.SERIALIZABLE ?
                              new GMUTotalOrderManager() :
                              new DefaultTotalOrderManager());
         } else if (componentType.equals(DataPlacementManager.class)){
               return (T) new DataPlacementManager();
         }
         else if (componentType.equals(L1GMUContainer.class)) {
            return (T) new L1GMUContainer();
         } else if (componentType.equals(CommitLog.class)) {
            return (T) new CommitLog();
         } else if (componentType.equals(TransactionCommitManager.class)) {
            return (T) new TransactionCommitManager();
         } else if (componentType.equals(L1Manager.class)) {
            return configuration.locking().isolationLevel() == IsolationLevel.SERIALIZABLE ?
                  (T) new GMUL1Manager() :
                  (T) new L1ManagerImpl();
         } else if (componentType.equals(GarbageCollectorManager.class)) {
            return (T) new GarbageCollectorManager();
         } else if (componentType.equals(ReconfigurableReplicationManager.class)) {
            return (T) new ReconfigurableReplicationManager();
         } else if (componentType.equals(ProtocolTable.class)) {
            return (T) new ProtocolTable();
         }
      }

      throw new ConfigurationException("Don't know how to create a " + componentType.getName());

   }
}
