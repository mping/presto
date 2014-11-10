# How to use

## Download presto + presto-hll
Then download this project to the root.
It should look like this:

```
wxr-xr-x  4 mping miguel 4.0K Oct 30 11:31 presto-docs/
...
drwxr-xr-x  5 mping miguel 4.0K Nov  3 17:39 presto-hll/
drwxr-xr-x  5 mping miguel 4.0K Oct 30 16:38 presto-jdbc/
drwxr-xr-x  5 mping miguel 4.0K Oct 30 16:38 presto-kafka/
...
drwxr-xr-x  4 mping miguel 4.0K Sep 22 12:08 src/
drwxr-xr-x  5 mping miguel 4.0K Oct 31 11:19 target/
-rw-r--r--  1 mping miguel  256 Oct 29 14:31 .gitignore
-rw-r--r--  1 mping miguel   77 Sep 22 12:08 .travis.yml
-rw-r--r--  1 mping miguel  611 Sep 22 12:08 CONTRIBUTING.md
-rw-r--r--  1 mping miguel  12K Sep 22 12:08 LICENSE
-rw-r--r--  1 mping miguel 3.3K Sep 22 12:08 README.md
-rw-r--r--  1 mping miguel  33K Oct 30 11:13 pom.xml
...
```

## Config modules and plugins

Main ```pom.xml``` should include the presto-hll:

```
    <modules>
        <module>presto-spi</module>
        ...
        <module>presto-hll</module>
    </modules>
```

## If developing, update the loaded plugins

Change  ```presto-main/etc/config.properties```

```
plugin.bundles=\
  ../presto-raptor/pom.xml,\
  ../presto-hive-hadoop2/pom.xml,\
  ../presto-example-http/pom.xml,\
  ../presto-hll/pom.xml,\
  ../presto-tpch/pom.xml
```

## Build
Now you can build: ```mvn package ``` and the resulting tarball will include the plugin