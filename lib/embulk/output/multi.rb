Embulk::JavaPlugin.register_output(
  "multi", "org.embulk.output.multi.MultiOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
