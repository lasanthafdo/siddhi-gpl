/*
 * Copyright (C) 2015 WSO2 Inc. (http://wso2.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.wso2.siddhi.gpl.extension.pmml;

import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

public class PMMLModelProcessorTestCase {

    private volatile boolean eventArrived;

    @Before
    public void init() {
        eventArrived = false;
    }

    @Test
    public void predictFunctionTest() throws InterruptedException, URISyntaxException {

        URL resource = PMMLModelProcessorTestCase.class.getResource("/decision-tree.pmml");
        String pmmlFile = new File(resource.toURI()).getAbsolutePath();

        SiddhiManager siddhiManager = new SiddhiManager();

        String inputStream = "define stream InputStream " +
                "(root_shell double, su_attempted double, num_root double, num_file_creations double, num_shells double, num_access_files double, num_outbound_cmds double, is_host_login double, is_guest_login double, count double, srv_count double, serror_rate double, srv_serror_rate double);";

        String query = "@info(name = 'query1') " +
                "from InputStream#pmml:predict('"+ pmmlFile +"') " +
                "select * " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inputStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                if (inEvents != null) {
                    Assert.assertEquals("1", inEvents[0].getData(13));
                }
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{6, 148, 72, 35, 0, 33.6, 0.627, 50, 1, 2, 3, 4, 5});
        Thread.sleep(1000);
        junit.framework.Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void predictFunctionWithSelectedOutputAttributeTest() throws InterruptedException, URISyntaxException {

        URL resource = PMMLModelProcessorTestCase.class.getResource("/decision-tree.pmml");
        String pmmlFile = new File(resource.toURI()).getAbsolutePath();

        SiddhiManager siddhiManager = new SiddhiManager();

        String inputStream = "define stream InputStream " +
                "(root_shell double, su_attempted double, num_root double, num_file_creations double, num_shells double, num_access_files double, num_outbound_cmds double, is_host_login double, is_guest_login double, count double, srv_count double, serror_rate double, srv_serror_rate double);";

        String query = "@info(name = 'query1') " +
                "from InputStream#pmml:predict('" + pmmlFile + "', root_shell, su_attempted, num_root, num_file_creations, num_shells, num_access_files, num_outbound_cmds, is_host_login, is_guest_login, count, srv_count, serror_rate, srv_serror_rate) " +
                "select Predicted_response " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inputStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                if (inEvents != null) {
                    Assert.assertEquals("1", inEvents[0].getData(0));
                }
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{6, 148, 72, 35, 0, 33.6, 0.627, 50, 1, 2, 3, 4, 5});
        Thread.sleep(1000);
        junit.framework.Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void predictFunctionWithSelectedAttributesTest() throws InterruptedException, URISyntaxException {

        URL resource = PMMLModelProcessorTestCase.class.getResource("/decision-tree.pmml");
        String pmmlFile = new File(resource.toURI()).getAbsolutePath();

        SiddhiManager siddhiManager = new SiddhiManager();

        String inputStream = "define stream InputStream " +
                "(root_shell double, su_attempted double, num_root double, num_file_creations double, num_shells double, num_access_files double, num_outbound_cmds double, is_host_login double, is_guest_login double, count double, srv_count double, serror_rate double, srv_serror_rate double);";

        String query = "@info(name = 'query1') " +
                "from InputStream#pmml:predict('" + pmmlFile + "', root_shell, su_attempted, num_root, num_file_creations, num_shells, num_access_files, num_outbound_cmds, is_host_login, is_guest_login, count, srv_count, serror_rate, srv_serror_rate) " +
                "select * " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inputStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                if (inEvents != null) {
                    Assert.assertEquals("1", inEvents[0].getData(13));
                }
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{6, 148, 72, 35, 0, 33.6, 0.627, 50, 1, 2, 3, 4, 5});
        Thread.sleep(1000);
        junit.framework.Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }
}
