/*
 * Copyright 2017 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package azkaban.restwebapp;

import azkaban.Constants.ConfigurationKeys;
import azkaban.executor.ExecutionController;
import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.user.UserManager;
import azkaban.user.XmlUserManager;
import azkaban.utils.Props;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import java.lang.reflect.Constructor;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.log4j.Logger;

/**
 * This Guice module is currently a one place container for all bindings in the current module. This
 * is intended to help during the migration process to Guice. Once this class starts growing we can
 * move towards more modular structuring of Guice components.
 */
public class AzkabanWebServerRestModule extends AbstractModule {

  private static final Logger log = Logger.getLogger(AzkabanWebServerRestModule.class);
  private static final String USER_MANAGER_CLASS_PARAM = "user.manager.class";
  private final Props props;

  public AzkabanWebServerRestModule(final Props props) {
    this.props = props;
  }

  @Override
  protected void configure() {
    bind(ExecutorManagerAdapter.class).to(resolveExecutorManagerAdaptorClassType());
  }

  private Class<? extends ExecutorManagerAdapter> resolveExecutorManagerAdaptorClassType() {
    return this.props.getBoolean(ConfigurationKeys.AZKABAN_POLL_MODEL, false)
        ? ExecutionController.class : ExecutorManager.class;
  }

  @Inject
  @Singleton
  @Provides
  public UserManager createUserManager(final Props props) {
    final Class<?> userManagerClass = props.getClass(USER_MANAGER_CLASS_PARAM, null);
    final UserManager manager;
    if (userManagerClass != null && userManagerClass.getConstructors().length > 0) {
      log.info("Loading user manager class " + userManagerClass.getName());
      try {
        final Constructor<?> userManagerConstructor = userManagerClass.getConstructor(Props.class);
        manager = (UserManager) userManagerConstructor.newInstance(props);
      } catch (final Exception e) {
        log.error("Could not instantiate UserManager " + userManagerClass.getName());
        throw new RuntimeException(e);
      }
    } else {
      manager = new XmlUserManager(props);
    }
    return manager;
  }
}

