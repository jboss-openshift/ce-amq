/*
 * JBoss, Home of Professional Open Source
 * Copyright 2016 Red Hat Inc. and/or its affiliates and other
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

package org.jboss.ce.amq.drain;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import javax.management.AttributeList;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ActiveMQ codebase
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
class JMX {
    private static final Logger log = LoggerFactory.getLogger(JMX.class);
    private static final String queryString = "type=Broker,brokerName=%s,destinationType=%s,destinationName=%s";

    private static String BROKER_NAME;
    private static String DEFAULT_JMX_URL;
    private static String jmxUser;
    private static String jmxPassword;

    private static final String CONNECTOR_ADDRESS = "com.sun.management.jmxremote.localConnectorAddress";

    private JMXServiceURL jmxServiceUrl;
    private JMXConnector jmxConnector;
    private MBeanServerConnection jmxConnection;

    static {
        BROKER_NAME = Utils.getSystemPropertyOrEnvVar("broker.name", "localhost");
        DEFAULT_JMX_URL = Utils.getSystemPropertyOrEnvVar("activemq.jmx.url", "service:jmx:rmi:///jndi/rmi://localhost:1099/jmxrmi");
        jmxUser = Utils.getSystemPropertyOrEnvVar("activemq.jmx.user");
        jmxPassword = Utils.getSystemPropertyOrEnvVar("activemq.jmx.password");
    }

    Collection<String> queues() throws Exception {
        return destinations("Queue");
    }

    Collection<String> topics() throws Exception {
        return destinations("Topic");
    }

    boolean hasNextMessage(DestinationInfo info) throws Exception {
        String query = query(info.getType(), info.getName());
        List<ObjectInstance> mbeans = queryMBeans(createJmxConnection(), query);
        for (ObjectInstance mbean : mbeans) {
            ObjectName objectName = mbean.getObjectName();
            AttributeList attributes = createJmxConnection().getAttributes(objectName, new String[]{info.getAttribute()});
            if (attributes.size() > 0) {
                Object value = attributes.asList().get(0).getValue();
                Number number = Number.class.cast(value);
                return (number.longValue() > 0);
            }
        }
        return false;
    }

    private String query(String type, String name) {
        return String.format(queryString, BROKER_NAME, type, name);
    }

    private Collection<String> destinations(String type) throws Exception {
        String query = query(type, "*,*");
        List<ObjectInstance> mbeans = queryMBeans(createJmxConnection(), query);
        List<String> destinations = new ArrayList<>();
        for (ObjectInstance mbean : mbeans) {
            ObjectName objectName = mbean.getObjectName();
            AttributeList attributes = createJmxConnection().getAttributes(objectName, new String[]{"Name"});
            // not all mbeans are exactly queues and topics -- don't have Name (they have DestinationName, still OK?)
            if (attributes.size() > 0) {
                String name = attributes.asList().get(0).getValue().toString();
                destinations.add(name);
            }
        }
        return destinations;
    }

    // ActiveMQ impl details

    private static List<ObjectInstance> queryMBeans(MBeanServerConnection jmxConnection, String queryString) throws Exception {
        return new MBeansObjectNameQueryFilter(jmxConnection).query(queryString);
    }

    private void print(String msg) {
        log.info(msg);
    }

    private JMXServiceURL getJmxServiceUrl() {
        return jmxServiceUrl;
    }

    private void setJmxServiceUrl(JMXServiceURL jmxServiceUrl) {
        this.jmxServiceUrl = jmxServiceUrl;
    }

    private void setJmxServiceUrl(String jmxServiceUrl) throws MalformedURLException {
        setJmxServiceUrl(new JMXServiceURL(jmxServiceUrl));
    }

    private static String getJVM() {
        return System.getProperty("java.vm.specification.vendor");
    }

    private static boolean isSunJVM() {
        // need to check for Oracle as that is the name for Java7 onwards.
        return getJVM().equals("Sun Microsystems Inc.") || getJVM().startsWith("Oracle");
    }

    private MBeanServerConnection createJmxConnection() throws IOException {
        if (jmxConnection == null) {
            jmxConnection = createJmxConnector().getMBeanServerConnection();
        }
        return jmxConnection;
    }

    private JMXConnector createJmxConnector() throws IOException {
        // Reuse the previous connection
        if (jmxConnector != null) {
            jmxConnector.connect();
            return jmxConnector;
        }

        // Create a new JMX connector
        if (jmxUser != null && jmxPassword != null) {
            Map<String, Object> props = new HashMap<>();
            props.put(JMXConnector.CREDENTIALS, new String[]{jmxUser, jmxPassword});
            jmxConnector = JMXConnectorFactory.connect(useJmxServiceUrl(), props);
        } else {
            jmxConnector = JMXConnectorFactory.connect(useJmxServiceUrl());
        }
        return jmxConnector;
    }

    @SuppressWarnings("unchecked")
    private JMXServiceURL useJmxServiceUrl() throws MalformedURLException {
        if (getJmxServiceUrl() == null) {
            String jmxUrl = DEFAULT_JMX_URL;
            int connectingPid = -1;
            if (isSunJVM()) {
                try {
                    // Classes are all dynamically loaded, since they are specific to Sun VM
                    // if it fails for any reason default jmx url will be used

                    // tools.jar are not always included used by default class loader, so we
                    // will try to use custom loader that will try to load tools.jar

                    String javaHome = System.getProperty("java.home");
                    String tools = javaHome + File.separator + ".." + File.separator + "lib" + File.separator + "tools.jar";
                    URLClassLoader loader = new URLClassLoader(new URL[]{new File(tools).toURI().toURL()});

                    Class virtualMachine = Class.forName("com.sun.tools.attach.VirtualMachine", true, loader);
                    Class virtualMachineDescriptor = Class.forName("com.sun.tools.attach.VirtualMachineDescriptor", true, loader);

                    Method getVMList = virtualMachine.getMethod("list", (Class[]) null);
                    Method attachToVM = virtualMachine.getMethod("attach", String.class);
                    Method getAgentProperties = virtualMachine.getMethod("getAgentProperties", (Class[]) null);
                    Method getVMDescriptor = virtualMachineDescriptor.getMethod("displayName", (Class[]) null);
                    Method getVMId = virtualMachineDescriptor.getMethod("id", (Class[]) null);

                    List allVMs = (List) getVMList.invoke(null, (Object[]) null);

                    for (Object vmInstance : allVMs) {
                        String displayName = (String) getVMDescriptor.invoke(vmInstance, (Object[]) null);
                        if (displayName.contains("activemq.jar start")) {
                            String id = (String) getVMId.invoke(vmInstance, (Object[]) null);

                            Object vm = attachToVM.invoke(null, id);

                            Properties agentProperties = (Properties) getAgentProperties.invoke(vm, (Object[]) null);
                            String connectorAddress = agentProperties.getProperty(CONNECTOR_ADDRESS);

                            if (connectorAddress != null) {
                                jmxUrl = connectorAddress;
                                connectingPid = Integer.parseInt(id);
                                print("useJmxServiceUrl Found JMS Url: " + jmxUrl);
                                break;
                            }
                        }
                    }
                } catch (Exception ignore) {
                }
            }

            if (connectingPid != -1) {
                print("Connecting to pid: " + connectingPid);
            } else {
                print("Connecting to JMX URL: " + jmxUrl);
            }
            setJmxServiceUrl(jmxUrl);
        }

        return getJmxServiceUrl();
    }

    private static class MBeansObjectNameQueryFilter {
        static final String QUERY_DELIMETER = ",";
        static final String DEFAULT_JMX_DOMAIN = "org.apache.activemq";
        static final String QUERY_EXP_PREFIX = "MBeans.QueryExp.";

        private MBeanServerConnection jmxConnection;

        private MBeansObjectNameQueryFilter(MBeanServerConnection jmxConnection) {
            this.jmxConnection = jmxConnection;
        }

        private List<ObjectInstance> query(String query) throws Exception {
            // Converts string query to map query
            StringTokenizer tokens = new StringTokenizer(query, QUERY_DELIMETER);
            return query(Collections.list(tokens));
        }

        private List<ObjectInstance> query(List queries) throws MalformedObjectNameException, IOException {
            // Query all mbeans
            if (queries == null || queries.isEmpty()) {
                return queryMBeans(new ObjectName(DEFAULT_JMX_DOMAIN + ":*"), null);
            }

            // Constructs object name query
            String objNameQuery = "";
            String queryExp = "";
            String delimiter = "";
            for (Object query : queries) {
                String key = (String) query;
                String val = "";
                int pos = key.indexOf("=");
                if (pos >= 0) {
                    val = key.substring(pos + 1);
                    key = key.substring(0, pos);
                } else {
                    objNameQuery += delimiter + key;
                }

                if (!val.startsWith(QUERY_EXP_PREFIX)) {
                    if (!key.equals("") && !val.equals("")) {
                        objNameQuery += delimiter + key + "=" + val;
                        delimiter = ",";
                    }
                }
            }

            return queryMBeans(new ObjectName(DEFAULT_JMX_DOMAIN + ":" + objNameQuery), queryExp);
        }

        private List<ObjectInstance> queryMBeans(ObjectName objName, String queryExpStr) throws IOException {
            QueryExp queryExp = createQueryExp(queryExpStr);
            // Convert mbeans set to list to make it standard throughout the query filter
            return new ArrayList<>(jmxConnection.queryMBeans(objName, queryExp));
        }

        private QueryExp createQueryExp(@SuppressWarnings("UnusedParameters") String queryExpStr) {
            // Currently unsupported
            return null;
        }

    }

}