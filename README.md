# Multi output plugin for Embulk

This plugin copies an output to multiple destinations.

### Notes
- It's still very experimental version, so might change its configuration names or styles without notification. 
- As this plugin peform multiple output methods, it might have a performance issue with large records.
- It might not work on other executors than LocalExecutor.
- I would appreciate it if you use this and give me reports/feedback!

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: yes
* **Cleanup supported**: yes

## Configuration

- **outputs**: Configuration of output plugins (array, required)

## Example

```yaml
out:
  type: multi
  outputs:
    # Output to stdout
    - type: stdout
    # Output to files as CSV
    - type: file
      path_prefix: out_file_
      file_ext: csv
      formatter:
        type: csv
    # Output to files as TSV
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
