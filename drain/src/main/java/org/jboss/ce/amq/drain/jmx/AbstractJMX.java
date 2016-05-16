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

package org.jboss.ce.amq.drain.jmx;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
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

import org.jboss.ce.amq.drain.Utils;

/**
 * @author ActiveMQ codebase
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
abstract class AbstractJMX {
    private static String DEFAULT_JMX_URL;
    private static String jmxUser;
    private static String jmxPassword;

    private static final String PATTERN;

    private static final String CONNECTOR_ADDRESS = "com.sun.management.jmxremote.localConnectorAddress";

    private JMXServiceURL jmxServiceUrl;
    private JMXConnector jmxConnector;
    private MBeanServerConnection jmxConnection;

    static {
        DEFAULT_JMX_URL = Utils.getSystemPropertyOrEnvVar("activemq.jmx.url", "service:jmx:rmi:///jndi/rmi://localhost:1099/jmxrmi");
        jmxUser = Utils.getSystemPropertyOrEnvVar("activemq.jmx.user");
        jmxPassword = Utils.getSystemPropertyOrEnvVar("activemq.jmx.password");
        PATTERN = Utils.getSystemPropertyOrEnvVar("jmx.pattern", "activemq.jar start");
    }

    <T> T getAttribute(Class<T> type, ObjectName objectName, String attributeName) throws Exception {
        return getAttribute(type, createJmxConnection(), objectName, attributeName);
    }

    static <T> T getAttribute(Class<T> type, MBeanServerConnection connection, ObjectName objectName, String attributeName) throws Exception {
        AttributeList attributes = connection.getAttributes(objectName, new String[]{attributeName});
        if (attributes.size() > 0) {
            Object value = attributes.asList().get(0).getValue();
            return type.cast(value);
        }
        return null;
    }

    // ActiveMQ impl details

    protected List<ObjectInstance> queryMBeans(MBeanServerConnection jmxConnection, String queryString) throws Exception {
        return new MBeansObjectNameQueryFilter(this, jmxConnection).query(queryString);
    }

    protected abstract void print(String msg);

    private JMXServiceURL getJmxServiceUrl() {
        return jmxServiceUrl;
    }

    private void setJmxServiceUrl(JMXServiceURL jmxServiceUrl) {
        this.jmxServiceUrl = jmxServiceUrl;
    }

    private void setJmxServiceUrl(String jmxServiceUrl) throws MalformedURLException {
        setJmxServiceUrl(new JMXServiceURL(jmxServiceUrl));
    }

    MBeanServerConnection createJmxConnection() throws IOException {
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
            try {
                // Classes are all dynamically loaded, since they are specific to Sun VM
                // if it fails for any reason default jmx url will be used

                // tools.jar are not always included used by default class loader, so we
                // will try to use custom loader that will try to load tools.jar

                String javaHome = System.getProperty("java.home");
                print(String.format("java.home --> %s", javaHome));

                File tools = JarFinder.findJar(javaHome, "tools.jar");
                URLClassLoader loader = new URLClassLoader(new URL[]{tools.toURI().toURL()});

                Class virtualMachine = Class.forName("com.sun.tools.attach.VirtualMachine", true, loader);
                Class virtualMachineDescriptor = Class.forName("com.sun.tools.attach.VirtualMachineDescriptor", true, loader);

                Method getVMList = virtualMachine.getMethod("list", (Class[]) null);
                Method attachToVM = virtualMachine.getMethod("attach", String.class);
                Method getAgentProperties = virtualMachine.getMethod("getAgentProperties", (Class[]) null);
                Method loadAgent = virtualMachine.getMethod("loadAgent", String.class);
                Method getVMDescriptor = virtualMachineDescriptor.getMethod("displayName", (Class[]) null);
                Method getVMId = virtualMachineDescriptor.getMethod("id", (Class[]) null);

                List allVMs = (List) getVMList.invoke(null, (Object[]) null);
                print(String.format("Found %s VMs ...", allVMs.size()));

                for (Object vmInstance : allVMs) {
                    String displayName = (String) getVMDescriptor.invoke(vmInstance, (Object[]) null);
                    print(String.format("VM -- display-name: %s", displayName));
                    if (displayName.contains(PATTERN)) {
                        String id = (String) getVMId.invoke(vmInstance, (Object[]) null);

                        Object vm = attachToVM.invoke(null, id);

                        Properties agentProperties = (Properties) getAgentProperties.invoke(vm, (Object[]) null);
                        String connectorAddress = agentProperties.getProperty(CONNECTOR_ADDRESS);
                        if (connectorAddress == null) {
                            String agent = JarFinder.findJar(javaHome, "management-agent.jar").getPath();
                            loadAgent.invoke(vm, agent);
                            // re-check
                            agentProperties = (Properties) getAgentProperties.invoke(vm, (Object[]) null);
                            connectorAddress = agentProperties.getProperty(CONNECTOR_ADDRESS);
                        }

                        if (connectorAddress != null) {
                            jmxUrl = connectorAddress;
                            connectingPid = Integer.parseInt(id);
                            print("useJmxServiceUrl Found JMS Url: " + jmxUrl);
                        } else {
                            print(String.format("No %s property!?", CONNECTOR_ADDRESS));
                        }
                        break;
                    }
                }
            } catch (Exception e) {
                print("Error: " + e);
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
        static final String DEFAULT_JMX_DOMAIN = Utils.getSystemPropertyOrEnvVar("jmx.domain", "org.apache.activemq");
        static final String QUERY_EXP_PREFIX = "MBeans.QueryExp.";

        private AbstractJMX jmx;
        private MBeanServerConnection jmxConnection;

        private MBeansObjectNameQueryFilter(AbstractJMX jmx, MBeanServerConnection jmxConnection) {
            this.jmx = jmx;
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
            jmx.print(String.format("ObjectName=%s,QueryString=%s,QueryExp=%s", objName, queryExpStr, queryExp));
            // Convert mbeans set to list to make it standard throughout the query filter
            return new ArrayList<>(jmxConnection.queryMBeans(objName, queryExp));
        }

        private QueryExp createQueryExp(@SuppressWarnings("UnusedParameters") String queryExpStr) {
            // Currently unsupported
            return null;
        }

    }

}