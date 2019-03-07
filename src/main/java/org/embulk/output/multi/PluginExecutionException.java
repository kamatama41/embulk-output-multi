package org.embulk.output.multi;

class PluginExecutionException extends RuntimeException {

    private final OutputPluginDelegate plugin;

    PluginExecutionException(OutputPluginDelegate plugin, Throwable cause) {
        super(cause);
        this.plugin = plugin;
    }

    OutputPluginDelegate getPlugin() {
        return plugin;
    }
}
