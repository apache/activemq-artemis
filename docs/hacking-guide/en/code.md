# Working with the Code

While the canonical Apache ActiveMQ Artemis git repository is hosted on Apache hardware at https://git-wip-us.apache.org/repos/asf?p=activemq-artemis.git
contributors are encouraged (but not required) to use a mirror on GitHub for collaboration and pull-request review
functionality. Follow the steps below to get set up with GitHub, etc.

If you do not wish to use GitHub for whatever reason you can follow the overall process outlined in the "Typical
development cycle" section below but instead attach [a patch file](https://git-scm.com/docs/git-format-patch) to the
related JIRA or an email to the [dev list](http://activemq.apache.org/mailing-lists.html).

## Initial Steps

1. Create a GitHub account if you don't have one already

   https://github.com
   
1. Fork the apache-artemis repository into your account

   https://github.com/apache/activemq-artemis
   
1. Clone your newly forked copy onto your local workspace:

        $ git clone git@github.com:<your-user-name>/activemq-artemis.git
        Cloning into 'activemq-artemis'...
        remote: Counting objects: 63800, done.
        remote: Compressing objects: 100% (722/722), done.
        remote: Total 63800 (delta 149), reused 0 (delta 0), pack-reused 62748
        Receiving objects: 100% (63800/63800), 18.28 MiB | 3.16 MiB/s, done.
        Resolving deltas: 100% (28800/28800), done.
        Checking connectivity... done.
     
        $ cd activemq-artemis
   
1. Add a remote reference to `upstream` for pulling future updates

        $ git remote add upstream https://github.com/apache/activemq-artemis

1. Build with Maven

   Typically developers will want to build using the `dev` profile which enables license and code style checks. For
   example:
   
        $ mvn -Pdev install
        ...
        [INFO] ------------------------------------------------------------------------
        [INFO] Reactor Summary:
        [INFO] 
        [INFO] ActiveMQ Artemis Parent ........................... SUCCESS [2.298s]
        [INFO] ActiveMQ Artemis Commons .......................... SUCCESS [1.821s]
        [INFO] ActiveMQ Artemis Selector Implementation .......... SUCCESS [0.767s]
        [INFO] ActiveMQ Artemis Native POM ....................... SUCCESS [0.189s]
        [INFO] ActiveMQ Artemis Journal .......................... SUCCESS [0.646s]
        [INFO] ActiveMQ Artemis Core Client ...................... SUCCESS [5.969s]
        [INFO] ActiveMQ Artemis JMS Client ....................... SUCCESS [2.110s]
        [INFO] ActiveMQ Artemis Server ........................... SUCCESS [11.540s]
        ...
        [INFO] ActiveMQ Artemis stress Tests ..................... SUCCESS [0.332s]
        [INFO] ActiveMQ Artemis performance Tests ................ SUCCESS [0.174s]
        [INFO] ------------------------------------------------------------------------
        [INFO] BUILD SUCCESS
        [INFO] ------------------------------------------------------------------------

## Typical development cycle

1. Identify a task (e.g. a bug to fix or feature to implement)

   https://issues.apache.org/jira/browse/ARTEMIS

1. Create a topic branch in your local git repo to do your work

         $ git checkout -b my_cool_feature

1. Make the changes and commit one or more times

         $ git commit

   <a name="commitMessageDetails"></a> When you commit your changes you will need to supply a commit message. We follow the
    50/72 git commit message format. An ActiveMQ Artemis commit message should be formatted in the following manner:
                                                                        
   1. Add the ARTEMIS JIRA (if one exists) followed by a brief description of the change in the first line. This line
      should be limited to 50 characters.
   1. Insert a single blank line after the first line.
   1. Provide a detailed description of the change in the following lines, breaking paragraphs where needed. These lines
      should be wrapped at 72 characters.
                                                                        
   An example correctly formatted commit message:
                                                                        
         ARTEMIS-123 Add new commit msg format to README
        
         Adds a description of the new commit message format as well as examples
         of well formatted commit messages to the README.md.  This is required 
         to enable developers to quickly identify what the commit is intended to 
         do and why the commit was added.

1. Occasionally you'll want to push your commit(s) to GitHub for safe-keeping and/or sharing with others.

         git push origin my_cool_feature  
    
   Note that git push references the branch you are pushing and defaults to `master`, not your working branch.
   
1. Discuss your planned changes (if you want feedback)

   On mailing list - http://activemq.apache.org/mailing-lists.html
   On IRC - irc://irc.freenode.org/apache-activemq or https://webchat.freenode.net/?channels=apache-activemq

1. Once you're finished coding your feature/fix then rebase your branch against the latest master (applies your patches 
   on top of master)
   
         git fetch upstream  
         git rebase -i upstream/master  
         # if you have conflicts fix them and rerun rebase  
         # The -f, forces the push, alters history, see note below  
         git push -f origin my_cool_feature
    
   The `rebase -i` triggers an interactive update which also allows you to combine commits, alter commit messages etc. 
   It's a good idea to make the commit log very nice for external consumption (e.g. by squashing all related commits 
   into a single commit. Note that rebasing and/or using `push -f` can alter history. While this is great for making a 
   clean patch, it is unfriendly to anyone who has forked your branch. Therefore you'll want to make sure that you 
   either work in a branch that you don't share, or if you do share it, tell them you are about to revise the branch 
   history (and thus, they will then need to rebase on top of your branch once you push it out).

1. Get your changes merged into upstream

    1. Send a GitHub pull request, by clicking the pull request link while in your repo's fork.
    1. An email will automatically be sent to the ActiveMQ developer list.
    1. As part of the review you may see an automated test run comment on your request.
    1. After review a maintainer will merge your PR into the canonical git repository at which point those changes will 
       be synced with the GitHub mirror repository (i.e. your `master`) and your PR will be closed by the `asfgit` bot.

## Other common tasks       
    
1. Pulling updates from upstream

        $ git pull --rebase upstream master
        
   (`--rebase` will automatically move your local commits, if any, on top of the latest branch you pull from; you can leave
   it off if you do not have any local commits).
   
   One last option, which some prefer, is to avoid using pull altogether, and just use fetch + rebase (this is of course
   more typing). For example:
   
        $ git fetch upstream
        $ git pull

1. Pushing pulled updates (or local commits if you aren't using topic branches) to your private GitHub repo (origin)
    
        $ git push  
        Counting objects: 192, done.  
        Delta compression using up to 4 threads.  
        Compressing objects: 100% (44/44), done.  
        Writing objects: 100% (100/100), 10.67 KiB, done.  
        Total 100 (delta 47), reused 100 (delta 47)  
        To git@github.com:<your-user-name>/apache-artemis.git  
           3382570..1fa25df  master -> master

   You might need to say -f to force the changes.

## Adding New Dependencies

Due to incompatibilities between some open source licenses and the Apache v2.0 license (that this project is licensed under)
care must be taken when adding new dependencies to the project.  The Apache Software Foundation 3rd party licensing 
policy has more information here: https://www.apache.org/legal/3party.html

To keep track of all licenses in ActiveMQ Artemis, new dependencies must be added in either the top level pom.xml or in test/pom.xml
(depending on whether this is a test only dependency or if it is used in the main code base).  The dependency should be
added under the dependency management section with version and labelled with a comment highlighting the license for the
dependency version.  See existing dependencies in the main pom.xml for examples.  The dependency can then be added to
individual ActiveMQ Artemis modules *without* the version specified (the version is implied from the dependency management
section of the top level pom).  This allows ActiveMQ Artemis developers to keep track of all dependencies and licenses.