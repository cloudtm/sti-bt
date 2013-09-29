package org.radargun.jmx;

import org.radargun.jmx.annotations.MBean;
import org.radargun.jmx.annotations.ManagedAttribute;
import org.radargun.jmx.annotations.ManagedOperation;

import javax.management.*;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class StageMBean implements DynamicMBean {

   private final Object obj;

   private final Map<String, Method> attributes = new ConcurrentHashMap<String, Method>(64);
   private final Map<String, Method> operations = new ConcurrentHashMap<String, Method>(64);

   private final MBeanInfo mBeanInfo;

   public StageMBean(Object instance, List<Method> managedAttributeMethods, List<Method> managedOperationMethods) throws IntrospectionException {
      if (instance == null)
         throw new NullPointerException("Cannot make an MBean wrapper for null instance");

      this.obj = instance;
      Class<?> objectClass = instance.getClass();

      // Load up all fields.      
      int i = 0;
      MBeanAttributeInfo[] attInfos = new MBeanAttributeInfo[managedAttributeMethods.size()];
      for (Method method : managedAttributeMethods) {
         ManagedAttribute managedAttribute = method.getAnnotation(ManagedAttribute.class);
         attInfos[i] = new MBeanAttributeInfo(method.getName(), managedAttribute.description(), method, null);
         attributes.put(attInfos[i++].getName(), method);
      }

      // And operations      
      MBeanOperationInfo[] opInfos = new MBeanOperationInfo[managedOperationMethods.size()];
      i = 0;
      for (Method method : managedOperationMethods) {
         ManagedOperation managedOperation = method.getAnnotation(ManagedOperation.class);
         opInfos[i] = new MBeanOperationInfo(managedOperation.description(), method);
         operations.put(opInfos[i++].getName(), method);
      }

      mBeanInfo = new MBeanInfo(objectClass.getSimpleName(), objectClass.getAnnotation(MBean.class).description(),
                                attInfos, new MBeanConstructorInfo[0], opInfos, new MBeanNotificationInfo[0]);
   }

   @Override
   public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
      Attribute attr = getAttributeValue(attribute);
      if (attr == null) {
         throw new AttributeNotFoundException("Attribute " + attribute + " not found");
      }
      return attr.getValue();
   }

   @Override
   public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
      throw new UnsupportedOperationException("Operation not supported");
   }

   @Override
   public AttributeList getAttributes(String[] attributesName) {
      AttributeList list = new AttributeList(attributesName.length);

      for (String attribute : attributesName) {
         Attribute attr = getAttributeValue(attribute);
         if (attr != null) {
            list.add(attr);
         }
      }

      return list;
   }

   @Override
   public AttributeList setAttributes(AttributeList objects) {
      throw new UnsupportedOperationException("Operation not supported");
   }

   @Override
   public Object invoke(String operationName, Object[] parameters, String[] signature) throws MBeanException, ReflectionException {
      Method method = operations.get(operationName);
      if (method == null) {
         throw new MBeanException(new OperationsException("Operation " + operationName + " not found"));
      }
      try {
         return method.invoke(obj, parameters);
      } catch (Exception e) {
         throw new ReflectionException(e);
      }
   }

   @Override
   public MBeanInfo getMBeanInfo() {
      return mBeanInfo;
   }

   private Attribute getAttributeValue(String attribute) {
      Method method = attributes.get(attribute);
      if (method == null) {
         return null;
      }
      Object ret;
      try {
         ret = method.invoke(obj);
      } catch (Exception e) {
         return null;
      }
      return new Attribute(attribute, ret);
   }
}
