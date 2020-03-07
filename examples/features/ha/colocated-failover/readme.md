# JMS Colocated Failover Shared Store Example

To run the example, simply type **mvn verify** from this directory. This example will always spawn and stop multiple brokers.

This example demonstrates how you can colocate live and backup brokers in the same VM. We do this by creating an HA Policy that is colocated. Colocated means that backup brokers can be created and maintained by live brokers on behalf of other live brokers requesting a backup. In this example we create a colocated shared store broker.

This example starts 2 live brokers each with a backup broker that backs up the other live broker.

The first live broker will be killed and the backup in the second will become live

The following shows how to configure the live brokers to request and allow backups to be deployed

    <ha-policy>
       <shared-store>
          <colocated>
             <backup-port-offset>100</backup-port-offset>
             <backup-request-retries>-1</backup-request-retries>
             <backup-request-retry-interval>2000</backup-request-retry-interval>
             <max-backups>1</max-backups>
             <request-backup>true</request-backup>
             <master/>
             <slave/>
          </colocated>
       </shared-store>
    </ha-policy>

notice that we have used a template to set some sensible defaults but overridden the backup strategy so back ups are full brokers.