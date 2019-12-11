package azkaban.restwebapp;

import azkaban.AzkabanCommonModule;
import azkaban.server.AzkabanServer;
import azkaban.user.UserManager;
import azkaban.utils.Props;
import azkaban.utils.StdOutErrRedirect;
import com.google.inject.Guice;
import com.google.inject.Injector;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import static azkaban.ServiceProvider.*;

@Singleton
@SpringBootApplication
public class AzkabanWebServerRest extends AzkabanServer {
  private static final Logger log = Logger.getLogger(AzkabanWebServerRest.class);

  private final Props props;
  private final UserManager userManager;

  @Inject
  public AzkabanWebServerRest(Props props, UserManager userManager) {
    this.props = props;
    this.userManager = userManager;
  }

  public static void main(String[] args) {
    // Redirect all std out and err messages into log4j
    StdOutErrRedirect.redirectOutAndErrToLog();

    log.info("Starting Tomcat Azkaban Web Server...");
    final Props props = AzkabanServer.loadProps(args);

    if (props == null) {
      log.error("Azkaban Properties not loaded. Exiting..");
      System.exit(1);
    }

    /* Initialize Guice Injector */
    final Injector injector = Guice.createInjector(
        new AzkabanCommonModule(props),
        new AzkabanWebServerRestModule(props)
    );
    SERVICE_PROVIDER.setInjector(injector);

    launch(injector.getInstance(AzkabanWebServerRest.class));
  }

  public static void launch(AzkabanWebServerRest webServer) {
    SpringApplication app = new SpringApplication(AzkabanWebServerRest.class);
    app.setDefaultProperties(webServer.getServerProps().toProperties());

    app.run(new String[0]);
  }

  @Override
  public Props getServerProps() {
    return this.props;
  }

  @Override
  public UserManager getUserManager() {
    return this.userManager;
  }
}
