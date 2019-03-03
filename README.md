# Multi output plugin for Embulk

This plugin can copies an output to multiple destinations.

### Notes
- It's still very experimental version, so might change its configuration names or styles without notification. 
- It might have performance issues or bugs when loading large records.
- It might not working on other executors than LocalExecutor.
- I would appreciate it if you use this and give me reports/feedback!

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: yes
* **Cleanup supported**: yes

## Configuration

- **outputs**: Configuration of output plugins (array, required)
- **stop_on_failed_output**: Set true if you want to immediately stop outputting of all plugins with a plugin's error (boolean, default "false")

## Example

```yaml
out:
  type: multi
  outputs:
    # Output to stdout
    - type: stdout
    # Output to file as CSV
    - type: file
      path_prefix: out_file_
      file_ext: csv
      formatter:
        type: csv
    # Output to file as TSV
    - type: file
      path_prefix: out_file_
      file_ext: tsv
      formatter:
        type: csv
        delimiter: "\t"
    # And any outputs you want..
    - type: ...
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
