package org.radargun.jmx;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.jmx.annotations.MBean;
import org.radargun.jmx.annotations.ManagedAttribute;
import org.radargun.jmx.annotations.ManagedOperation;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.annotation.Annotation;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class JmxRegistration {

   private static final Log log = LogFactory.getLog(JmxRegistration.class);
   private final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
   
   private static final JmxRegistration _instance = new JmxRegistration();
   
   private JmxRegistration() {
      
   }
   
   public synchronized static JmxRegistration getInstance() {
      return _instance;
   }
   
   public void processStage(Object stage) {
      Class<?> clazz = stage.getClass();

      //1) check if the MBean annotation is present in the class
      MBean mBean = getAnnotation(clazz, MBean.class);
      if (mBean == null) {
         log.trace("Object " + stage + " was not register. MBean annotation not found");
         return;
      }

      //2) take the methods annotated with ManagedAttribute and ManagerOperation
      List<Method> managedAttributeMethods = getAllMethods(clazz, ManagedAttribute.class);
      List<Method> managedOperationMethods = getAllMethods(clazz, ManagedOperation.class);

      try {
         StageMBean stageMBean = new StageMBean(stage, managedAttributeMethods, managedOperationMethods);
         mBeanServer.registerMBean(stageMBean, new ObjectName("org.radargun:stage=" + mBean.objectName()));
      } catch (Exception e) {
         log.warn("Error while registering MBean " + stage + ". " + e.getMessage());
         log.trace(e);
      }
   }

   private  <T extends Annotation> T getAnnotation(Class<?> clazz, Class<T> ann) {
      while (true) {
         // first check class
         T a = (T) clazz.getAnnotation(ann);
         if (a != null) return a;

         // check interfaces
         if (!clazz.isInterface()) {
            Class<?>[] interfaces = clazz.getInterfaces();
            for (Class<?> inter : interfaces) {
               a = getAnnotation(inter, ann);
               if (a != null) return a;
            }
         }

         // check superclasses
         Class<?> superclass = clazz.getSuperclass();
         if (superclass == null) return null; // no where else to look
         clazz = superclass;
      }
   }

   private List<Method> getAllMethods(Class<?> clazz, Class<? extends Annotation> annotationType) {
      List<Method> annotated = new LinkedList<Method>();
      inspectRecursively(clazz, annotated, annotationType);
      return annotated;
   }

   private void inspectRecursively(Class<?> clazz, List<Method> annotatedMethods, Class<? extends Annotation> annotationType) {

      for (Method method : clazz.getDeclaredMethods()) {
         // don't bother if this method has already been overridden by a subclass
         if (notFound(method, annotatedMethods) && method.isAnnotationPresent(annotationType)) {
            annotatedMethods.add(method);
         }
      }

      if (!clazz.equals(Object.class)) {
         if (!clazz.isInterface()) {
            inspectRecursively(clazz.getSuperclass(), annotatedMethods, annotationType);
            for (Class<?> ifc : clazz.getInterfaces()) inspectRecursively(ifc, annotatedMethods, annotationType);
         }
      }
   }

   private boolean notFound(Method method, Collection<Method> methods) {
      for (Method found : methods) {
         if (method.getName().equals(found.getName()) &&
               Arrays.equals(method.getParameterTypes(), found.getParameterTypes()))
            return false;
      }
      return true;
   }
}
