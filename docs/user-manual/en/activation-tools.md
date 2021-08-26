# Activation Sequence Tools

You can use the Artemis CLI to execute activation sequence maintenance/recovery tools
for [Pluggable Quorum Replication](ha.md).

The 2 main commands are `activation list` and `activation set`, that can be used together to recover some disaster
happened to local/coordinated activation sequences.

Here is a disaster scenario built around the RI (using [Apache Zookeeper](https://zookeeper.apache.org/)
and [Apache curator](https://curator.apache.org/)) to demonstrate the usage of such commands.

## Troubleshooting Case: Zookeeper Cluster disaster

A proper Zookeeper cluster should use at least 3 nodes, but what happens if all these nodes crash loosing any activation
state information required to run an Artemis replication cluster?

During the disaster ie Zookeeper nodes no longer reachable, brokers:

- live ones shutdown (and if restarted by a script, should hang awaiting to connect to the Zookeeper cluster again)
- replicas become passive, awaiting to connect to the Zookeeper cluster again

Admin should:

1. stop all Artemis brokers
2. restart Zookeeper cluster
3. search brokers with the highest local activation sequence for their `NodeID`, by running this command from the `bin`
   folder of the broker:

```bash
$ ./artemis activation list --local
Local activation sequence for NodeID=7debb3d1-0d4b-11ec-9704-ae9213b68ac4: 1
```

4. from the `bin` folder of the brokers with the highest local activation sequence

```bash
# assuming 1 to be the highest local activation sequence obtained at the previous step 
# for NodeID 7debb3d1-0d4b-11ec-9704-ae9213b68ac4
$ ./artemis activation set --remote --to 1
Forced coordinated activation sequence for NodeID=7debb3d1-0d4b-11ec-9704-ae9213b68ac4 from 0 to 1
```

5. restart all brokers: previously live ones should be able to be live again

The higher the number of Zookeeper nodes are, the less the chance are that a disaster like this requires Admin
intervention, because it allows the Zookeeper cluster to tolerate more failures.