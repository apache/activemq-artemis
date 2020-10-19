# Topic Hierarchy Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

ActiveMQ Artemis supports topic hierarchies. With a topic hierarchy you can register a subscriber with a wild-card and that subscriber will receive any messages routed to an address that match the wildcard.

ActiveMQ Artemis wild-cards can use the character `#` which means "match any number of words", and the character `*` which means "match a single word". Words are delimited by the character `.`.

For example if I subscribe using the wild-card `news.europe.#`, then that would match messages sent to the addresses `news.europe`, `news.europe.sport` and `news.europe.entertainment`, but it does not match messages sent to the address `news.usa.wrestling`.

For more information on the wild-card syntax please consult the user manual.