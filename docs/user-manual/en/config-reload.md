# Configuration Reload

The system will perform a periodic check on the configuration files, configured by `configuration-file-refresh-period`, with the default at 5000, in milliseconds.

Once the configuration file is changed (broker.xml) the following modules will be reloaded automatically:

- Address Settings
- Security Settings
- Diverts
- Addresses & queues


Notice: 

Deletion of Address's and Queue's, not auto created is controlled by Address Settings

* config-delete-addresses
   * OFF (DEFAULT) - will not remove upon config reload.
   * FORCE - will remove the address and its queues upon config reload, even if messages remains, losing the messages in the address & queues.

* config-delete-queues
   * OFF (DEFAULT) - will not remove upon config reload.
   * FORCE - will remove the queue upon config reload, even if messages remains, losing the messages in the queue.


By default both settings are OFF as such address & queues won't be removed upon reload, given the risk of losing messages. 

When OFF You may execute explicit CLI or Management operations to remove address & queues.