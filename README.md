# MetacatIndexSizePoller

Java mini-utility to periodically poll a Metacat index task queue (a Hazelcast Map) for its size or list the objects inside it.

## Running

0. Install `metacat-common`

This project depends on [metacat-common](https://github.com/nceas/metacat) which isn't on Maven so you'll need to install it first:

```sh
git clone https://github.com/nceas/metacat
cd metacat/metacat-common
mvn install
```

1. Set up an SSH tunnel to the host (Optional if you run this on the same host as Metacat)

_Replace with the appropriate `{REMOTE_HOST}`_

```sh
ssh -L 5701:localhost:5701 {REMOTE_HOST}
```

2. Start polling

_Replace with the appropriate Hazelcast `groupName` and `groupPassword`_

With Maven `exec:java`:

```sh
mvn -X exec:java -Dexec.mainClass="edu.ucsb.nceas.MetacatIndexPoller" -Dexec.args="groupName={NAME} groupPassword={PASSWORD}"
```

As a JAR:

```sh
mvn package
java -cp target/metacat-index-poller-1.0-SNAPSHOT-jar-with-dependencies.jar edu.ucsb.nceas.MetacatIndexPoller groupName={NAME} groupPassword={PASSWORD}
```

## Options

Configurable options include:

- `action`: One of:
  - `poll`: Periodically displays the size of the index queue
  - `list`: Lists the tasks in the index queue by PID
  - `evict` Evicts a task in the index queue by PID
- `address`: The address of the Hazelcast cluster. Default: `127.0.0.1:5701`.
- `groupName`: The `groupName` for the Hazelcast cluster. Default: `''`.
- `groupPassword`: The `groupPassword` for hte Hazelcast cluster. Default: `''`.
- `delay`: The number of milliseconds of delay between polling attempts. Default: `5000`.
- `duration`: The number of milliseconds to poll for. Default: `60000`.
- `pid`: The PID of the index queue task to evict. Only used if `action` is `evict`.

Override any of these by adding to `-Dexec.args` (see above for an example) following a pattern of `{option}={value}`. For example, to set a custom delay of 6 seconds, run:

```sh
mvn -X exec:java -Dexec.mainClass="edu.ucsb.nceas.MetacatIndexPoller" -Dexec.args="delay=6000"
```
