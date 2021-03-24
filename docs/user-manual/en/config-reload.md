# Configuration Reload

The system will perform a periodic check on the configuration files, configured
by `configuration-file-refresh-period`, with the default at `5000`, in
milliseconds. These checks can be disabled by specifying `-1`.

Once the configuration file is changed (broker.xml) the following modules will
be reloaded automatically:

- Address Settings
- Security Settings
- Diverts
- Addresses & queues

If using [modulised broker.xml](configuration-index.md#modularising-broker.xml) ensure you also read [Reloading modular configuration files](configuration-index.md#reloading-modular-configuration-files)

**Note:**

Deletion of Address's Queue's and diverts not auto created is controlled by Address Settings

* config-delete-addresses
   * OFF (DEFAULT) - will not remove upon config reload.
   * FORCE - will remove the address and its queues upon config reload, even if messages remains, losing the messages in the address & queues.

* config-delete-queues
   * OFF (DEFAULT) - will not remove upon config reload.
   * FORCE - will remove the queue upon config reload, even if messages remains, losing the messages in the queue.

* config-delete-diverts
   * OFF (DEFAULT) - will not remove upon config reload.
   * FORCE - will remove the queue upon config reload, even if messages remains, losing the messages in the queue.

By default both settings are OFF as such address & queues won't be removed upon
reload, given the risk of losing messages.

When OFF You may execute explicit CLI or Management operations to remove
address & queues.

## Reloadable Parameters

The broker configuration file has 2 main parts, `<core>` and `<jms>`. Some of
the parameters in the 2 parts are monitored and, if modified, reloaded into the
broker at runtime.

**Note:** Elements under `<jms>` are **deprecated**. Users are encouraged to
use `<core>` configuration entities.

> **Note:**
>
> Most parameters reloaded take effect immediately after reloading. However
> there are some that won’t take any effect unless you restarting the broker.
> Such parameters are specifically indicated in the following text.

### `<core>`

#### `<security-settings>`

* `<security-setting>` element

Changes to any `<security-setting>` elements will be reloaded. Each
`<security-setting>` defines security roles for a matched address.

* The `match` attribute

  This attribute defines the address for which the security-setting is
  defined. It can take wildcards such as ‘#’ and ‘*’.

* The `<permission>` sub-elements

Each `<security-setting>` can have a list of `<permission>` elements, each
of which defines a specific permission-roles mapping.  Each permission has 2
attributes ‘type’ and ‘roles’. The ‘type’ attribute defines the type of
operation allowed, the ‘roles’ defines which roles are allowed to perform such
operation. Refer to the user’s manual for a list of operations that can be
defined.

> **Note:**
>
> Once loaded the security-settings will take effect immediately. Any new
> clients will subject to the new security settings. Any existing clients will
> subject to the new settings as well, as soon as they performs a new
> security-sensitive operation.

Below lists the effects of adding, deleting and updating of an
element/attribute within the `<security-settings>` element, whether a change
can be done or can’t be done.

Operation | Add | Delete | Update
---|---|---|---
`<security-settings>` | X* (at most one element is allowed) | Deleting it means delete the whole security settings from the running broker. | N/A*
`<security-setting>` | Adding one element means adding a new set of security roles for an address in the running broker | Deleting one element means removing a set of security roles for an address in the running broker | Updating one element means updating the security roles for an address (if match attribute is not changed), or means removing the old match address settings and adding a new one (if match attribute is changed)
attribute `match` | N/A* | X* | Changing this value is same as deleting the whole <security-setting> with the old match value and adding
`<permission>` | Adding one means adding  a new permission definition to runtime broker | Deleting a permission from the runtime broker | Updating a permission-roles in the runtime broker
attribute `type` | N/A* | X* | Changing the type value means remove the permission of the old one and add the permission of this type to the running broker.
attribute `roles` | N/A* | X* | Changing the ‘roles’ value means updating the permission’s allowed roles to the running broker

> * `N/A` means this operation is not applicable.
> * `X` means this operation is not allowed.

#### `<address-settings>`

* `<address-settings>` element

Changes to elements under `<address-settings>` will be reloaded into runtime
broker. It contains a list of `<address-setting>` elements.

   * `<address-setting>` element

   Each address-setting element has a ‘match’ attribute that defines an address
   pattern for which this address-setting is defined. It also has a list of
   sub-elements used to define the properties of a matching address.

   > **Note:** 
   >
   > Parameters reloaded in this category will take effect immediately
   > after reloading. The effect of deletion of Address's and Queue's, not auto
   > created is controlled by parameter `config-delete-addresses` and
   > `config-delete-queues` as described in the doc.

Below lists the effects of adding, deleting and updating of an
element/attribute within the address-settings element, whether a change can be
done or can’t be done.

Operation | Add | Delete | Update
---|---|---|---
`<address-settings>` | X(at most one element is allowed) | Deleting it means delete the whole address settings from the running broker | N/A
`<address-setting>` | Adding one element means adding a set of address-setting for a new address in the running broker | Deleting one  means removing a set of address-setting for an address in the running broker | Updating one element means updating the address setting for an address (if match attribute is not changed), or means removing the old match address settings and adding a new one (if match attribute is changed)
attribute `match` | N/A | X | Changing this value is same as deleting the whole <address-setting> with the old match value and adding a new one with the new match value.
`<dead-letter-address>` | X (no more than one can be present) | Removing the configured dead-letter-address address from running broker. | The dead letter address of the matching address will be updated after reloading
`<expiry-address>` | X (no more than one can be present) | Removing the configured expiry address from running broker. | The expiry address of the matching address will be updated after reloading
`<expiry-delay>` | X (no more than one can be present) | The configured expiry-delay will be removed from running broker. | The expiry-delay for the matching address will be updated after reloading.
`<redelivery-delay>` | X (no more than one can be present) | The configured redelivery-delay will be removed from running broker after reloading | The redelivery-delay for the matchin address will be updated after reloading.
`<redelivery-delay-multiplier>` | X (no more than one can be present) | The configured redelivery-delay-multiplier will be removed from running broker after reloading. | The redelivery-delay-multiplier will be updated after reloading.
`<max-redelivery-delay>` |  X (no more than one can be present) | The configured max-redelivery-delay will be removed from running broker after reloading. | The max-redelivery-delay will be updated after reloading.
`<max-delivery-attempts>` | X (no more than one can be present) | The configured max-delivery-attempts will be removed from running broker after reloading. | The max-delivery-attempts will be updated after reloading.
`<max-size-bytes>` | X (no more than one can be present) | The configured max-size-bytes will be removed from running broker after reloading. | The max-size-bytes will be updated after reloading.
`<page-size-bytes>` | X (no more than one can be present) | The configured page-size-bytes will be removed from running broker after reloading. | The page-size-bytes will be updated after reloading.
`<page-max-cache-size>` | X (no more than one can be present) | The configured page-max-cache-size will be removed from running broker after reloading. | The page-max-cache-size will be updated after reloading.
`<address-full-policy>` | X (no more than one can be present) | The configured address-full-policy will be removed from running broker after reloading. | The address-full-policy will be updated after reloading.
`<message-counter-history-day-limit>` | X (no more than one can be present) | The configured message-counter-history-day-limit will be removed from running broker after reloading. | The message-counter-history-day-limit will be updated after reloading.
`<last-value-queue>` | X (no more than one can be present) | The configured last-value-queue will be removed from running broker after reloading (no longer a last value queue). | The last-value-queue will be updated after reloading.
`<redistribution-delay>` | X (no more than one can be present) | The configured redistribution-delay will be removed from running broker after reloading. | The redistribution-delay will be updated after reloading.
`<send-to-dla-on-no-route>` | X (no more than one can be present) | The configured send-to-dla-on-no-route will be removed from running broker after reloading. | The send-to-dla-on-no-route will be updated after reloading.
`<slow-consumer-threshold>` | X (no more than one can be present) | The configured slow-consumer-threshold will be removed from running broker after reloading. | The slow-consumer-threshold will be updated after reloading.
`<slow-consumer-policy>` | X (no more than one can be present) | The configured slow-consumer-policy will be removed from running broker after reloading. | The slow-consumer-policy will be updated after reloading.
`<slow-consumer-check-period>` | X (no more than one can be present) | The configured slow-consumer-check-period will be removed from running broker after reloading. (meaning the slow consumer checker thread will be cancelled) | The slow-consumer-check-period will be updated after reloading.
`<auto-create-queues>` | X (no more than one can be present) | The configured auto-create-queues will be removed from running broker after reloading. | The auto-create-queues will be updated after reloading.
`<auto-delete-queues>` | X (no more than one can be present) | The configured auto-delete-queues will be removed from running broker after reloading. | The auto-delete-queues will be updated after reloading.
`<config-delete-queues>` | X (no more than one can be present) | The configured config-delete-queues will be removed from running broker after reloading. | The config-delete-queues will be updated after reloading.
`<auto-create-addresses>` | X (no more than one can be present) | The configured auto-create-addresses will be removed from running broker after reloading. | The auto-create-addresses will be updated after reloading.
`<auto-delete-addresses>` | X (no more than one can be present) | The configured auto-delete-addresses will be removed from running broker after reloading. | The auto-delete-addresses will be updated after reloading.
`<config-delete-addresses>` | X (no more than one can be present) | The configured config-delete-addresses will be removed from running broker after reloading. | The config-delete-addresses will be updated after reloading.
`<management-browse-page-size>` | X (no more than one can be present) | The configured management-browse-page-size will be removed from running broker after reloading. | The management-browse-page-size will be updated after reloading.
`<default-purge-on-no-consumers>` | X (no more than one can be present) | The configured default-purge-on-no-consumers will be removed from running broker after reloading. | The default-purge-on-no-consumers will be updated after reloading.
`<default-max-consumers>` | X (no more than one can be present) | The configured default-max-consumers will be removed from running broker after reloading. | The default-max-consumers will be updated after reloading.
`<default-queue-routing-type>` | X (no more than one can be present) | The configured default-queue-routing-type will be removed from running broker after reloading. | The default-queue-routing-type will be updated after reloading.
`<default-address-routing-type>` | X (no more than one can be present) | The configured default-address-routing-type will be removed from running broker after reloading. | The default-address-routing-type will be updated after reloading.


#### `<diverts>`

All `<divert>` elements will be reloaded. Each `<divert>` element has a ‘name’
and several sub-elements that defines the properties of a divert.

> **Note:**
>
> Existing diverts get undeployed if you delete their `<divert>` element.

Below lists the effects of adding, deleting and updating of an
element/attribute within the diverts element, whether a change can be done or
can’t be done.

Operation | Add | Delete | Update
---|---|---|---
`<diverts>` | X (no more than one can be present) | Deleting it means delete (undeploy) all diverts in running broker. | N/A
`<divert>` | Adding a new divert. It will be deployed after reloading | Deleting it means the divert will be undeployed after reloading | No effect on the deployed divert (unless restarting broker, in which case the divert will be redeployed)
attribute `name` | N/A | X | A new divert with the name will be deployed. (if it is not already there in broker). Otherwise no effect.
`<transformer-class-name>` | X (no more than one can be present) | No effect on the deployed divert.(unless restarting broker, in which case the divert will be deployed without the transformer class) | No effect on the deployed divert.(unless restarting broker, in which case the divert has the transformer class)
`<exclusive>` | X (no more than one can be present) | No effect on the deployed divert.(unless restarting broker) | No effect on the deployed divert.(unless restarting broker)
`<routing-name>` | X (no more than one can be present) | No effect on the deployed divert.(unless restarting broker) | No effect on the deployed divert.(unless restarting broker)
`<address>` | X (no more than one can be present) | No effect on the deployed divert.(unless restarting broker) | No effect on the deployed divert.(unless restarting broker)
`<forwarding-address>` | X (no more than one can be present) | No effect on the deployed divert.(unless restarting broker) | No effect on the deployed divert.(unless restarting broker)
`<filter>` | X (no more than one can be present) | No effect on the deployed divert.(unless restarting broker) | No effect on the deployed divert.(unless restarting broker)
`<routing-type>` | X (no more than one can be present) | No effect on the deployed divert.(unless restarting broker) | No effect on the deployed divert.(unless restarting broker)

#### `<addresses>`

The `<addresses>` element contains a list `<address>` elements. Once changed,
all `<address>` elements in `<addresses>` will be reloaded.

> **Note:**
>
> Once reloaded, all new addresses (as well as the pre-configured queues) will
> be deployed to the running broker and all those that are missing from the
> configuration will be undeployed.

> **Note:**
>
> Parameters reloaded in this category will take effect immediately after
> reloading.  The effect of deletion of Address's and Queue's, not auto created
> is controlled by parameter `config-delete-addresses` and
> `config-delete-queues` as described in this doc.

Below lists the effects of adding, deleting and updating of an
element/attribute within the `<addresses>` element, whether a change can be
done or can’t be done.

Operation | Add | Delete | Update
---|---|---|---
`<addresses>` | X(no more than one is present) | Deleting it means delete  (undeploy) all diverts in running broker. | N/A
`<address>` | A new address will be deployed in the running broker | The corresponding address will be undeployed. | N/A
attribute `name` | N/A | X | After reloading the address of the old name will be undeployed and the new will be deployed.
`<anycast>` | X(no more than one is present) | The anycast routing type will be undeployed from this address, as well as its containing queues after reloading | N/A
`<queue>`(under `<anycast>`) | An anycast queue will be deployed after reloading | The anycast queue will be undeployed | For updating queues please see next section `<queues>`
`<multicast>` | X(no more than one is present) | The multicast routing type will be undeployed from this address, as well as its containing queues after reloading | N/A
`<queue>`(under `<multicast>`) | A multicast queue will be deployed after reloading | The multicast queue will be undeployed | For updating queues please see next section `<queues>`

#### `<queues>`

The `<queues>` element contains a list `<queue>` elements. Once changed, all
`<queue>` elements in `<queues>` will be reloaded.

> **Note:**
>
> Once reloaded, all new queues will be deployed to the running broker and all
> queues that are missing from the configuration will be undeployed.

> **Note:**
>
> Parameters reloaded in this category will take effect immediately after
> reloading.  The effect of deletion of Address's and Queue's, not auto created
> is controlled by parameter `config-delete-addresses` and
> `config-delete-queues` as described in this doc.

Below lists the effects of adding, deleting and updating of an
element/attribute within the `<queues>` element, and whether a change can be
done or can’t be done.

Operation | Add | Delete | Update
---|---|---|---
`<queues>` | X(no more than one is present) | Deleting it means delete  (undeploy) all queues from running broker. | N/A
`<queue>` | A new queue is deployed after reloading | The queue will be undeployed after reloading. | N/A
attribute `name` | N/A | X | A queue with new name will be deployed and the queue with old name will be updeployed after reloading (see Note above).
attribute `max-consumers` | If max-consumers > current consumers max-consumers will update on reload | max-consumers will be set back to the default `-1` | If max-consumers > current consumers max-consumers will update on reload
attribute `purge-on-no-consumers` | On reload purge-on-no-consumers will be updated | Will be set back to the default `false` | On reload purge-on-no-consumers will be updated
attribute `address` | N/A | No effect unless starting broker | No effect unless starting broker
attribute `filter` | The filter will be added after reloading | The filter will be removed after reloading | The filter will be updated after reloading
attribute `durable` | The queue durability will be set to the given value after reloading | The queue durability will be set to the default `true` after reloading | The queue durability will be set to the new value after reloading

### `<jms>` *(Deprecated)*

#### `<queue>`

Changes to any `<queue>` elements will be reloaded to the running broker.

> **Note:**
>
> Once reloaded, new queues defined in the new changes will be deployed to the
> running broker. However existing queues won’t get undeployed even if the
> matching element is deleted/missing. Also new queue elements matching
> existing queues won’t get re-created – they remain unchanged.

Operation | Add | Delete | Update
---|---|---|---
`<queue>` | A new jms queue will be deployed after reloading | No effect unless starting broker | No effect unless starting broker
attribute `<name>` | N/A | X | A jms queue of the new name will be deployed after reloading
`<selector>` | X(no more than one is present) | No effect unless starting broker | No effect unless starting broker
`<durable>` | X(no more than one is present) | No effect unless starting broker | No effect unless starting broker

#### `<topic>`

Changes to any `<topic>` elements will be reloaded to the running broker.

> **Note:**
>
> Once reloaded, new topics defined in the new changes will be deployed to the
> running broker. However existing topics won’t get undeployed even if the
> matching element is deleted/missing. Also any `<topic>` elements matching
> existing topics won’t get re-deployed – they remain unchanged.

Operation | Add | Delete | Update
---|---|---|---
`<topic>` | A new jms topic will be deployed after reloading | No effect unless starting broker | No effect unless starting broker
attribute `name` | N/A | X | A jms topic of the new name will be deployed after reloading
