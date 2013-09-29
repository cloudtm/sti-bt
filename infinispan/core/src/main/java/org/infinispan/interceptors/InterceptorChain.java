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
package org.infinispan.interceptors;

import org.infinispan.CacheException;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.config.ConfigurationException;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.base.ReconfigurableProtocolAwareWrapperInterceptor;
import org.infinispan.reconfigurableprotocol.ReconfigurableProtocol;
import org.infinispan.reconfigurableprotocol.manager.ReconfigurableReplicationManager;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.ReflectionUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Knows how to build and manage an chain of interceptors. Also in charge with invoking methods on the chain.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public class InterceptorChain {

   private static final Log log = LogFactory.getLog(InterceptorChain.class);

   public static enum InterceptorType {
      STATE_TRANSFER,
      CUSTOM_INTERCEPTOR_BEFORE_TX_INTERCEPTOR,
      CUSTOM_INTERCEPTOR_AFTER_TX_INTERCEPTOR,
      LOCKING,
      WRAPPER,
      DEADLOCK,
      CLUSTER
   }

   private final Map<InterceptorType, ReconfigurableProtocolAwareWrapperInterceptor> wrappers =
         new EnumMap<InterceptorType, ReconfigurableProtocolAwareWrapperInterceptor>(InterceptorType.class);

   private ReconfigurableReplicationManager manager;

   /**
    * reference to the first interceptor in the chain
    */
   private volatile CommandInterceptor firstInChain;

   final ReentrantLock lock = new ReentrantLock();
   final ComponentMetadataRepo componentMetadataRepo;

   /**
    * Constructs an interceptor chain having the supplied interceptor as first.
    */
   public InterceptorChain(ComponentMetadataRepo componentMetadataRepo) {
      this.componentMetadataRepo = componentMetadataRepo;
      for (InterceptorType type : InterceptorType.values()) {
         wrappers.put(type, new ReconfigurableProtocolAwareWrapperInterceptor(type));
      }
   }


   @Inject
   public void inject(ReconfigurableReplicationManager manager) {
      this.manager = manager;
   }

   @Start
   private void printChainInfo() {
      if (log.isDebugEnabled()) {
         log.debugf("Interceptor chain size: %d", size());
         log.debugf("Interceptor chain is: %s", toString());
      }
   }

   public void setFirst(CommandInterceptor commandInterceptor) {
      this.firstInChain = commandInterceptor;
   }

   private void validateCustomInterceptor(Class<? extends CommandInterceptor> i) {
      if ((!ReflectionUtil.getAllMethodsShallow(i, Inject.class).isEmpty() ||
            !ReflectionUtil.getAllMethodsShallow(i, Start.class).isEmpty() ||
            !ReflectionUtil.getAllMethodsShallow(i, Stop.class).isEmpty()) &&
            componentMetadataRepo.findComponentMetadata(i.getName()) == null) {
         log.customInterceptorExpectsInjection(i.getName());
      }      
   }
   
   /**
    * Ensures that the interceptor of type passed in isn't already added
    *
    * @param clazz type of interceptor to check for
    */
   private void assertNotAdded(Class<? extends CommandInterceptor> clazz) {
      if (containsInterceptorType(clazz))
         throw new ConfigurationException("Detected interceptor of type [" + clazz.getName() + "] being added to the interceptor chain " + System.identityHashCode(this) + " more than once!");
   }

   /**
    * Inserts the given interceptor at the specified position in the chain (o based indexing).
    *
    * @throws IllegalArgumentException if the position is invalid (e.g. 5 and there are only 2 interceptors in the
    *                                  chain)
    */
   public void addInterceptor(CommandInterceptor interceptor, int position) {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         Class<? extends CommandInterceptor> interceptorClass = interceptor.getClass();
         assertNotAdded(interceptorClass);
         validateCustomInterceptor(interceptorClass);
         if (position == 0) {
            interceptor.setNext(firstInChain);
            firstInChain = interceptor;
            return;
         }
         if (firstInChain == null) return;
         CommandInterceptor it = firstInChain;
         int index = 0;
         while (it != null) {
            if (++index == position) {
               interceptor.setNext(it.getNext());
               it.setNext(interceptor);
               return;
            }
            it = it.getNext();
         }
         throw new IllegalArgumentException("Invalid index: " + index + " !");
      } finally {
         lock.unlock();
      }
   }

   /**
    * Removes the interceptor at the given postion.
    *
    * @throws IllegalArgumentException if the position is invalid (e.g. 5 and there are only 2 interceptors in the
    *                                  chain)
    */
   public void removeInterceptor(int position) {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         if (firstInChain == null) return;
         if (position == 0) {
            firstInChain = firstInChain.getNext();
            return;
         }
         CommandInterceptor it = firstInChain;
         int index = 0;
         while (it != null) {
            if (++index == position) {
               if (it.getNext() == null) return; //nothing to remove
               it.setNext(it.getNext().getNext());
               return;
            }
            it = it.getNext();
         }
         throw new IllegalArgumentException("Invalid position: " + position + " !");
      } finally {
         lock.unlock();
      }
   }

   /**
    * Returns the number of interceptors in the chain.
    */
   public int size() {
      int size = 0;
      CommandInterceptor it = firstInChain;
      while (it != null) {
         size++;
         it = it.getNext();
      }
      return size;

   }

   /**
    * Returns an unmofiable list with all the interceptors in sequence. If first in chain is null an empty list is
    * returned.
    */
   public List<CommandInterceptor> asList() {
      if (firstInChain == null) return InfinispanCollections.emptyList();

      List<CommandInterceptor> retval = new LinkedList<CommandInterceptor>();
      CommandInterceptor tmp = firstInChain;
      do {
         retval.add(tmp);
         tmp = tmp.getNext();
      }
      while (tmp != null);
      return Collections.unmodifiableList(retval);
   }


   /**
    * Removes all the occurences of supplied interceptor type from the chain.
    */
   public void removeInterceptor(Class<? extends CommandInterceptor> clazz) {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         if (isFirstInChain(clazz)) {
            firstInChain = firstInChain.getNext();
         }
         CommandInterceptor it = firstInChain.getNext();
         CommandInterceptor prevIt = firstInChain;
         while (it != null) {
            if (it.getClass() == clazz) {
               prevIt.setNext(it.getNext());
            }
            prevIt = it;
            it = it.getNext();
         }
      } finally {
         lock.unlock();
      }
   }

   protected boolean isFirstInChain(Class<? extends CommandInterceptor> clazz) {
      return firstInChain.getClass() == clazz;
   }

   /**
    * Adds a new interceptor in list after an interceptor of a given type.
    *
    * @return true if the interceptor was added; i.e. the afterInterceptor exists
    */
   public boolean addInterceptorAfter(CommandInterceptor toAdd, Class<? extends CommandInterceptor> afterInterceptor) {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         Class<? extends CommandInterceptor> interceptorClass = toAdd.getClass();
         assertNotAdded(interceptorClass);
         validateCustomInterceptor(interceptorClass);
         CommandInterceptor it = firstInChain;
         while (it != null) {
            if (isSameClass(it, afterInterceptor, false, null)) {
               toAdd.setNext(it.getNext());
               it.setNext(toAdd);
               return true;
            }
            it = it.getNext();
         }
         return false;
      } finally {
         lock.unlock();
      }
   }

   /**
    * Adds a new interceptor in list after an interceptor of a given type.
    *
    * @return true if the interceptor was added; i.e. the afterInterceptor exists
    */
   public boolean addInterceptorBefore(CommandInterceptor toAdd, Class<? extends CommandInterceptor> beforeInterceptor) {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         Class<? extends CommandInterceptor> interceptorClass = toAdd.getClass();
         assertNotAdded(interceptorClass);
         validateCustomInterceptor(interceptorClass);

         if (firstInChain.getClass().equals(beforeInterceptor)) {
            toAdd.setNext(firstInChain);
            firstInChain = toAdd;
            return true;
         }
         CommandInterceptor it = firstInChain;
         while (it.getNext() != null) {
            if (isSameClass(it.getNext(), beforeInterceptor, false, null)) {
               toAdd.setNext(it.getNext());
               it.setNext(toAdd);
               return true;
            }
            it = it.getNext();
         }
         return false;
      } finally {
         lock.unlock();
      }
   }

   /**
    * Replaces an existing interceptor of the given type in the interceptor chain with a new interceptor instance passed as parameter.
    *
    * @param replacingInterceptor        the interceptor to add to the interceptor chain
    * @param toBeReplacedInterceptorType the type of interceptor that should be swapped with the new one
    * @return true if the interceptor was replaced
    */
   public boolean replaceInterceptor(CommandInterceptor replacingInterceptor, Class<? extends CommandInterceptor> toBeReplacedInterceptorType) {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         Class<? extends CommandInterceptor> interceptorClass = replacingInterceptor.getClass();
         assertNotAdded(interceptorClass);
         validateCustomInterceptor(interceptorClass);

         if (firstInChain.getClass().equals(toBeReplacedInterceptorType)) {
            replacingInterceptor.setNext(firstInChain.getNext());
            firstInChain = replacingInterceptor;
            return true;
         }
         CommandInterceptor it = firstInChain;
         CommandInterceptor previous = firstInChain;
         while (it.getNext() != null) {
            CommandInterceptor current = it.getNext();
            if (current.getClass().equals(toBeReplacedInterceptorType)) {
               replacingInterceptor.setNext(current.getNext());
               previous.setNext(replacingInterceptor);
               return true;
            }
            previous = current;
            it = current;
         }
         return false;
      } finally {
         lock.unlock();
      }
   }

   /**
    * Appends at the end.
    */
   public void appendInterceptor(CommandInterceptor ci, boolean isCustom) {
      if (isCustom) validateCustomInterceptor(ci.getClass());
      // Called when building interceptor chain and so concurrent start calls are protected already
      CommandInterceptor it = firstInChain;
      while (it.hasNext()) it = it.getNext();
      it.setNext(ci);
      // make sure we nullify the "next" pointer in the last interceptors.
      ci.setNext(null);
   }

   /**
    * Walks the command through the interceptor chain. The received ctx is being passed in.
    */
   public Object invoke(InvocationContext ctx, VisitableCommand command) {
      try {
         ctx.setProtocolId(manager.getCurrentProtocolId());
         return command.acceptVisitor(ctx, firstInChain);
      } catch (CacheException e) {
         if (e.getCause() instanceof InterruptedException)
            Thread.currentThread().interrupt();
         throw e;
      } catch (RuntimeException e) {
         throw e;
      } catch (Throwable t) {
         throw new CacheException(t);
      }
   }

   /**
    * @return the first interceptor in the chain.
    */

   public CommandInterceptor getFirstInChain() {
      return firstInChain;
   }

   /**
    * Mainly used by unit tests to replace the interceptor chain with the starting point passed in.
    *
    * @param interceptor interceptor to be used as the first interceptor in the chain.
    */
   public void setFirstInChain(CommandInterceptor interceptor) {
      this.firstInChain = interceptor;
   }

   /**
    * Returns all interceptors which extend the given command interceptor.
    */
   public List<CommandInterceptor> getInterceptorsWhichExtend(Class<? extends CommandInterceptor> interceptorClass) {
      List<CommandInterceptor> result = new LinkedList<CommandInterceptor>();
      for (CommandInterceptor interceptor : asList()) {
         boolean isSubclass = interceptorClass.isAssignableFrom(interceptor.getClass());
         if (isSubclass) {
            result.add(interceptor);
         }
      }
      return result;
   }

   /**
    * Returns all the interceptors that have the fully qualified name of their class equal with the supplied class
    * name.
    */
   public List<CommandInterceptor> getInterceptorsWithClass(Class clazz) {
      // Called when building interceptor chain and so concurrent start calls are protected already
      CommandInterceptor iterator = firstInChain;
      List<CommandInterceptor> result = new ArrayList<CommandInterceptor>(2);
      while (iterator != null) {
         addInterceptorsWithClass(result, iterator, clazz);
         iterator = iterator.getNext();
      }
      return result;
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      CommandInterceptor i = firstInChain;
      while (i != null) {
         sb.append("\n\t>> ");
         sb.append(i.getClass().getName());
         if (i instanceof ReconfigurableProtocolAwareWrapperInterceptor) {
            sb.append(((ReconfigurableProtocolAwareWrapperInterceptor) i).routeTableToString());
         }
         i = i.getNext();
      }
      return sb.toString();
   }

   /**
    * Checks whether the chain contains the supplied interceptor instance.
    */
   public boolean containsInstance(CommandInterceptor interceptor) {
      CommandInterceptor it = firstInChain;
      while (it != null) {
         if (it == interceptor) return true;
         it = it.getNext();
      }
      return false;
   }

   public boolean containsInterceptorType(Class<? extends CommandInterceptor> interceptorType) {
      return containsInterceptorType(interceptorType, null, false);
   }

   public boolean containsInterceptorType(Class<? extends CommandInterceptor> interceptorType, String protocolId) {
      return containsInterceptorType(interceptorType, protocolId, false);
   }

   public boolean containsInterceptorType(Class<? extends CommandInterceptor> interceptorType, String protocolId,
                                          boolean alsoMatchSubClasses) {
      // Called when building interceptor chain and so concurrent start calls are protected already
      CommandInterceptor it = firstInChain;
      while (it != null) {
         if (isSameClass(it, interceptorType, alsoMatchSubClasses, protocolId)) return true;
         it = it.getNext();
      }
      return false;
   }

   public void appendWrapper(InterceptorType type) {
      appendInterceptor(wrappers.get(type), false);
   }

   public void registerNewProtocol(ReconfigurableProtocol protocol) {
      EnumMap<InterceptorType, CommandInterceptor> newInterceptors = protocol.buildInterceptorChain();
      for (Map.Entry<InterceptorType, CommandInterceptor> entry : newInterceptors.entrySet()) {
         wrappers.get(entry.getKey()).setProtocolDependentInterceptor(protocol.getUniqueProtocolName(), entry.getValue());
      }
   }

   public final boolean isSameClass(CommandInterceptor interceptor, Class<?> clazz, boolean matchSubclass, String protocolId) {
      if (interceptor instanceof ReconfigurableProtocolAwareWrapperInterceptor) {
         return ((ReconfigurableProtocolAwareWrapperInterceptor) interceptor).isSameClass(clazz, matchSubclass, protocolId, this);
      } else {
         return matchSubclass ? clazz.isAssignableFrom(interceptor.getClass()) : interceptor.getClass().equals(clazz);
      }
   }

   public final void addInterceptorsWithClass(List<CommandInterceptor> list, CommandInterceptor ci, Class clazz) {
      if (ci.getClass().equals(clazz)) {
         list.add(ci);
      } else if (ci instanceof ReconfigurableProtocolAwareWrapperInterceptor) {
         ((ReconfigurableProtocolAwareWrapperInterceptor) ci).addInterceptorsWithClass(list, clazz, this);
      }
   }
}
