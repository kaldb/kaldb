package com.slack.astra.logstore.opensearch;

import java.util.List;
import org.opensearch.common.settings.Settings;
import org.opensearch.painless.PainlessModulePlugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.ScriptPlugin;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.ScriptService;

/**
 * The ScriptModule object appears to only be able to be instantiated once safely. This class makes
 * it a singleton, while attempting to avoid needing to pass parameters. This may eventually be
 * folded into the OpenSearchAdapter class.
 */
public class ScriptServiceProvider {

  private static class ScriptServiceHolder {
    static ScriptService scriptService = createInstance();
  }

  // The first call to this method, causes the initialization of the ScriptServiceHolder class
  // “A typical VM will synchronize field access only to initialize the class” from Effective Java
  public static ScriptService getInstance() {
    return ScriptServiceHolder.scriptService;
  }

  private static ScriptService createInstance() {
    Settings settings = AstraIndexSettings.getSharedServiceSettings();
    PluginsService pluginsService =
        new PluginsService(settings, null, null, List.of(PainlessModulePlugin.class));
    ScriptModule scriptModule =
        new ScriptModule(
            pluginsService.updatedSettings(), pluginsService.filterPlugins(ScriptPlugin.class));

    return new ScriptService(settings, scriptModule.engines, scriptModule.contexts);
  }
}
