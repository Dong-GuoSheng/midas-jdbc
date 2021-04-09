Midas JDBC driver
===============
[![midas-jdbc](https://maven-badges.herokuapp.com/maven-central/cn.synway.bigdata/midas-jdbc/badge.svg)](https://maven-badges.herokuapp.com/maven-central/cn.synway.bigdata/midas-jdbc) ![Build Status(https://github.com/Midas/midas-jdbc/workflows/Build/badge.svg)](https://github.com/Midas/midas-jdbc/workflows/Build/badge.svg)

This is a basic and restricted implementation of jdbc driver for Midas.
It has support of a minimal subset of features to be usable.

### Usage
```xml
<dependency>
    <groupId>cn.synway.bigdata</groupId>
    <artifactId>midas-jdbc</artifactId>
    <version>0.2.6</version>
</dependency>
```

URL syntax: 
`jdbc:midas://<host>:<port>[/<database>]`, e.g. `jdbc:midas://localhost:8123/test`

JDBC Driver Class:
`cn.synway.bigdata.MidasDriver`

additionally, if you have a few instances, you can use `BalancedMidasDataSource`.


### Extended API
In order to provide non-JDBC complaint data manipulation functionality, proprietary API exists.
Entry point for API is `MidasStatement#write()` method.

#### Importing file into table
```java
import cn.synway.bigdata.midas.MidasStatement;
MidasStatement sth = connection.createStatement();
sth
    .write() // Write API entrypoint
    .table("default.my_table") // where to write data
    .option("format_csv_delimiter", ";") // specific param
    .data(new File("/path/to/file.csv.gz"), MidasFormat.CSV, MidasCompression.gzip) // specify input     
    .send();
```
#### Configurable send
```java
import cn.synway.bigdata.midas.MidasStatement;
MidasStatement sth = connection.createStatement();
sth
    .write()
    .sql("INSERT INTO default.my_table (a,b,c)")
    .data(new MyCustomInputStream(), MidasFormat.JSONEachRow)
    .dataCompression(MidasCompression.brotli)    
    .addDbParam(MidasQueryParam.MAX_PARALLEL_REPLICAS, 2)
    .send();
```
#### Send data in binary formatted with custom user callback
```java

MidasStatement sth = connection.createStatement();
sth.write().send("INSERT INTO test.writer", new MidasStreamCallback() {
    @Override
    public void writeTo(MidasRowBinaryStream stream) throws IOException {
        for (int i = 0; i < 10; i++) {
            stream.writeInt32(i);
            stream.writeString("Name " + i);
        }
    }
},
MidasFormat.RowBinary); // RowBinary or Native are supported
```
### Compiling with maven
The driver is built with maven.
`mvn package -DskipTests=true`

To build a jar with dependencies use

`mvn package assembly:single -DskipTests=true`

### Build requirements
In order to build the jdbc client one need to have jdk 1.7 or higher.
