# Multi output plugin for Embulk

This plugin can copies an output to multiple destinations.

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
  type: copy
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
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
