Authorization
=====================================

To complete security migration, we need to deal with authorization policies as well. In ActiveMQ, authorization is specified using the appropriate broker plugin in `conf/activemq.xml`, like


```xml
<authorizationPlugin>
  <map>
	<authorizationMap>
	  <authorizationEntries>
		<authorizationEntry queue=">" read="admins" write="admins" admin="admins"/>
		<authorizationEntry queue="USERS.>" read="users" write="users" admin="users"/>
		<authorizationEntry queue="GUEST.>" read="guests" write="guests,users" admin="guests,users"/>
		<authorizationEntry topic=">" read="admins" write="admins" admin="admins"/>
		<authorizationEntry topic="USERS.>" read="users" write="users" admin="users"/>
		<authorizationEntry topic="GUEST.>" read="guests" write="guests,users" admin="guests,users"/>
		<authorizationEntry topic="ActiveMQ.Advisory.>" read="guests,users" write="guests,users" admin="guests,users"/>
	  </authorizationEntries>
	</authorizationMap>
  </map>
</authorizationPlugin>
```

The equivalent Artemis configuration is specified in `etc/broker.xml` and should look like this

```xml
<security-settings>
  <security-setting match="#">
	<permission type="createNonDurableQueue" roles="admins"/>
	<permission type="deleteNonDurableQueue" roles="admins"/>
	<permission type="createDurableQueue" roles="admins"/>
	<permission type="deleteDurableQueue" roles="admins"/>
	<permission type="consume" roles="admins"/>
	<permission type="browse" roles="admins"/>
	<permission type="send" roles="admins"/>
  </security-setting>

  <security-setting match="USERS.#">
	<permission type="createNonDurableQueue" roles="users"/>
	<permission type="deleteNonDurableQueue" roles="users"/>
	<permission type="createDurableQueue" roles="users"/>
	<permission type="deleteDurableQueue" roles="users"/>
	<permission type="consume" roles="users"/>
	<permission type="browse" roles="users"/>
	<permission type="send" roles="users"/>
  </security-setting>         

  <security-setting match="GUESTS.#">
	<permission type="createNonDurableQueue" roles="guests"/>
	<permission type="deleteNonDurableQueue" roles="guests"/>
	<permission type="createDurableQueue" roles="guests"/>
	<permission type="deleteDurableQueue" roles="guests"/>
	<permission type="consume" roles="guests"/>
	<permission type="browse" roles="guests"/>
	<permission type="send" roles="guests"/>
  </security-setting>                  
</security-settings>
```

As you can see, things are pretty comparable with some minor differences. The most important one is that policies in ActiveMQ are defined on destination names, while in Artemis they are applied to *core queues* (refresh your knowledge about relation between queues and addresses in previous sections and Artemis user manual).

The other notable difference is that policies are more fine-grained in Artemis. The following paragraphs and tables show Artemis policies that corresponds to ActiveMQ ones.

If you wish to allow users to send messages, you need to define the following policies in the respective brokers.

| ActiveMQ  | Artemis   |
| --        | --        |
| write		| send		|

In Artemis, policies for consuming and browsing are separated and you need to define them both in order to control `read` access to the destination.

| ActiveMQ  | Artemis   |
| --        | --        |
| read		| consume	|
|			| browse    |

It's the same story with `admin` privileges. You need to define separate create and delete policies for durable and non-durable core queues.

| ActiveMQ  |  Artemis              |
| --        | --                    |
| admin     | createNonDurableQueue |
|           | deleteNonDurableQueue |
|           | createDurableQueue    |
|           | deleteDurableQueue    |

Finally, there's a topic of using wildcards to define policies. The following table shows the wildcard syntax difference. 
 
| Wildcard | Description | ActiveMQ | Artemis |
| --       | --          | --       | --      |
| Delimiter| Separates words in the path | . | . |
| Single word | Match single word in the path | * | * |
| Any word    | Match any work recursively in the path | \> | \# |

Basically, by default only the *any word* character is different, so that's why we used `GUESTS.#` in Artemis example instead of ActiveMQ's `GUESTS.>` syntax.

Powered with this knowledge, you should be able to transform your current ActiveMQ authorization policies to Artemis. 

