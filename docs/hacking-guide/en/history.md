History
=======

The Apache ActiveMQ Artemis project was started in October 2014. The Artemis code base was seeded with a code donation granted by Red Hat, of the HornetQ project. The code donation process consisted of taking a snapshot of the latest HornetQ code base and contributing this snapshot as an [initial git commit](https://issues.apache.org/jira/browse/ARTEMIS-1) into the Artemis git repository.

The HornetQ commit history is preserved and can be accessed here: [https://github.com/hornetq/hornetq/tree/apache-donation](https://github.com/hornetq/hornetq/tree/apache-donation)

Credit should be given to those developers who contributed to the HornetQ project. The top 10 committers are highlighted here:

- Clebert Suconic
- Tim Fox
- Francisco Borges
- Andy Taylor
- Jeff Mesnil
- Ovidiu Feodorov
- Howard Gao
- Justin Bertram
- Trustin Lee
- Adrian Brock

For more information please visit the [HornetQ GitHub project](https://github.com/hornetq/hornetq/tree/apache-donation).


Rebasing original donation
==========================

It may be useful to look at the donation history combined with the artemis history. It is the case when eventually looking at old changes.

For that there is a script that will rebase master against the donation branch under master/scripts:

- rebase-donation.sh