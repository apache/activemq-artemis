# Configuration Reload

The system will perform a periodic check on the configuration files, configured by `configuration-file-refresh-period`, with the default at 5000, in milliseconds.

Once the configuration file is changed (broker.xml) the following modules will be reloaded automatically:

- Address Settings
- Security Settings
- Diverts
- Addresses & queues


Notice: Address & queues won't be removed upon reload, given the risk of losing messages. You may execute explicit CLI or Management operations to remove destinations.