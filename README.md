# Connection Pool

This is a minimalistic implementation of a connection pool for dealing with creating, managing and destroying a number of active connections to the database. The idea is to have a pool of active connections to a particular DB configuration and manage them as and when clients request access to those connections. Creating a connection is a very expensive process and hence the idea is to re-use connections stored in a connection pool that have been created previously by maintaining active or idle states for each one of them.

## Goal

While there are many production-ready implementations of connection pools already available out there, this is not written with the idea of having  yet another option (hence minimalistic). The goal of the project is to understand some of the aspects of multi-threaded programming and exploring the java.util.concurrent package in depth. It also allows exploration of the various concurrent data structures out there and using them for atomic operations instead of using the 'synchronize' pattern to avoid race conditions.

## Usage

The project has been developed using Maven. `pom.xml` has been included to build the project and run other commands. It also contains
information on downloading dependent jars which contains JUnit, Mockito, Slf4J and Log4J. Unit tests are written in JUnit and mock objects are created using Mockito. Slf4j is used for web logs at runtime.

    mvn compile      # compiles your code in src/main/java
    mvn test-compile # compile test code in src/test/java
    mvn test         # run tests in src/test/java for files named Test*.java

## TODO

1. Use ThreadPoolExecutor to create an initial set of connections in a separate thread on start.
2. Use a ConcurrentHashMap instead of LinkedBlockingQueue for set of active connections so that deletion of an invalid connection takes constant time.
3. More functionality in the Pool Manager to actively reap connections that have been leased for some configurable amount of time and to
periodically check if available connections are still usable and removing those that are not.

## License

MIT