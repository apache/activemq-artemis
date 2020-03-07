# JMS Colocated Failover Recover Only Example

To run the example, simply type **mvn verify** from this directory. This example will always spawn and stop multiple servers.

This example demonstrates how you can colocate live and backup servers in the same VM. We do this by creating an HA Policy that is colocated. Colocated means that backup servers can be created and maintained by live servers on behalf of other requesting live servers. In this example we create a colocated shared store broker that will scale down. That is it will not become live but scale down the journal to the colocated live server.

This example starts 2 live servers each will request the other to create a backup.

The first live broker will be killed and the backup in the second will recover the journal and recreate its state in the live broker it shares its VM with.

The following shows how to configure the backup, the slave is configured **<scale-down/>** which means that the backup broker will not fully start on fail over, instead it will just recover the journal and write it to its parent live server.

    <ha-policy>
       <shared-store>
          <colocated>
             <backup-port-offset>100</backup-port-offset>
             <backup-request-retries>-1</backup-request-retries>
             <backup-request-retry-interval>2000</backup-request-retry-interval>
             <max-backups>1</max-backups>
             <request-backup>true</request-backup>
             <master/>
             <slave>
                <scale-down/>
             </slave>
          </colocated>
       </shared-store>
    </ha-policy>

Notice that we dont need to specify a scale down connector as it will use most appropriate from the list of available connectors which in this case is the first INVM connector

One other thing to notice is that the cluster connection has its reconnect attempts set to 5, this is so it will disconnect instead of trying to reconnect to a backup that doesn't exist.