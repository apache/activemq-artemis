# Resource Limits

Sometimes it's helpful to set particular limits on what certain users can
do beyond the normal security settings related to authorization and 
authentication. For example, limiting how many connections a user can create
or how many queues a user can create. This chapter will explain how to 
configure such limits.

## Configuring Limits Via Resource Limit Settings

Here is an example of the XML used to set resource limits:

```xml
<resource-limit-settings>
   <resource-limit-setting match="myUser">
      <max-connections>5</max-connections>
      <max-queues>3</max-queues>
   </resource-limit-setting>
</resource-limit-settings>
```

Unlike the `match` from `address-setting`, this `match` does not use
any wild-card syntax. It's a simple 1:1 mapping of the limits to a **user**.

- `max-connections` defines how many connections the matched user can make
to the broker. The default is -1 which means there is no limit.

- `max-queues` defines how many queues the matched user can create. The default
is -1 which means there is no limit.