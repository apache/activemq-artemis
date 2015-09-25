# Notes for Maintainers

Core ActiveMQ Artemis members have write access to the Apache ActiveMQ Artemis repositories and will be responsible for
acknowledging and pushing commits contributed via pull requests on GitHub.

Core ActiveMQ Artemis members are also able to push their own commits directly to the canonical Apache repository.
However, the expectation here is that the developer has made a good effort to test their changes and is reasonably
confident that the changes that are being committed will not break the build.

What does it mean to be reasonably confident? If the developer has run the same maven commands that the pull-request
builds are running they can be reasonably confident. Currently the [PR build](https://builds.apache.org/job/ActiveMQ-Artemis-PR-Build/)
runs this command:

    mvn compile test-compile javadoc:javadoc -Pfast-tests -Pextra-tests test

However, if the changes are significant, touches a wide area of code, or even if the developer just wants a second
opinion they are encouraged to engage other members of the community to obtain an additional review prior to pushing.
This can easily be done via a pull request on GitHub, a patch file attached to an email or JIRA, commit to a branch
in the Apache git repo, etc. Having additional eyes looking at significant changes prior to committing to the main
development branches is definitely encouraged if it helps obtain the "reasonable confidence" that the build is not
broken and code quality has not decreased.

If the build does break then developer is expected to make their best effort to get the builds fixed in a reasonable
amount of time. If it cannot be fixed in a reasonable amount of time the commit can be reverted and re-reviewed.

## Commit Messages

Please ensure the commit messages follow the 50/72 format as described [here](code.md#commitMessageDetails).

## Configuring git repositories

Aside from the traditional `origin` and `upstream` repositories committers will need an additional reference for the
canonical Apache git repository where they will be merging and pushing pull-requests. For the purposes of this document,
let's assume these ref/repo associations already exist as described in the [Working with the Code](code.md) section:

- `origin` : https://github.com/(your-user-name)/activemq-artemis.git
- `upstream` : https://github.com/apache/activemq-artemis

1. Add the canonical Apache repository as a remote. Here we call it `apache`.

        $ git remote add apache https://git-wip-us.apache.org/repos/asf/activemq-artemis.git

1. Add the following section to your <artemis-repo>/.git/config statement to fetch all pull requests sent to the GitHub
   mirror.  We are using `upstream` as the remote repo name (as noted above), but the remote repo name may be different
   if you choose. Just be sure to edit all references to the remote repo name so it's consistent.

        [remote "upstream"]
            url = git@github.com:apache/activemq-artemis.git
            fetch = +refs/heads/*:refs/remotes/upstream/*
            fetch = +refs/pull/*/head:refs/remotes/upstream/pr/*

## Merging and pushing pull requests

Here are the basic commands to retrieve pull requests, merge, and push them to the canonical Apache repository:

1. Download all the remote branches etc... including all the pull requests.

        $ git fetch --all
        Fetching origin
        Fetching upstream
        remote: Counting objects: 566, done.
        remote: Compressing objects: 100% (188/188), done.
        remote: Total 566 (delta 64), reused 17 (delta 17), pack-reused 351
        Receiving objects: 100% (566/566), 300.67 KiB | 0 bytes/s, done.
        Resolving deltas: 100% (78/78), done.
        From github.com:apache/activemq-artemis
         * [new ref]         refs/pull/105/head -> upstream/pr/105

1. Checkout the pull request you wish to review

        $ git checkout pr/105

1. Once you've reviewed the change and are ready to merge checkout `master`.

        $ git checkout master

1. Ensure you are up to date

        $ git pull

1. Create a new merge commit from the pull-request. IMPORTANT: The commit message here should be something like: "This
   closes #105" where "105" is the pull request ID.  The "#105" shows up as a link in the GitHub UI for navigating to
   the PR from the commit message.

        $ git merge --no-ff pr/105

1. Push to the canonical Apache repo.

        $ git push apache master

## Rebasing before you merge

If the pull request gets too behind master, it would be better to rebase the branch before merging it.
You can do that by either asking the author of the pull request to do such rebase (what is not always possible) or you could do it yourself during merging.

In case you rebase the pull request it is mandatory that write "This closes #105" Where 105 is the pull request ID.

There is a script that helps doing such thing at <project root>/scripts/merge-PR.sh.

The script is assuming you have defined remotes named at upstream, apache and origin like specified previously here.

to execute such script simply use:

```
$ <checkout-directory>/scripts/merge-pr.sh <PR number> Message on the PR
```

Example:

```
$  pwd
/checkouts/apache-activemq-artemis

$  ./scripts/merge-pr.sh 175 ARTEMIS-229 address on Security Interface
```

The previous example was taken from a real case that generated this [merge commit on #175](https://github.com/apache/activemq-artemis/commit/e85bb3ca4a75b0f1dfbe717ff90b34309e2de794).

- After this you can push to the canonical Apache repo.
```
$ git push apache master
```

## Notes:

The GitHub mirror repository (i.e. `upstream`) is cloning the canonical Apache repository.  Because of this there may be
a slight delay between when a commit is pushed to the Apache repo and when that commit is reflected in the GitHub mirror.
This may cause some difficulty when trying to push a PR to `apache` that has been merged on the out-of-date GitHub mirror.
You can wait for the mirror to update before performing the steps above or you can change your local master branch to
track the master branch on the canonical Apache repository rather than the master branch on the GitHub mirror:

    $ git branch master -u apache/master

Where `apache` points to the canonical Apache repository.

If you'd like your local master branch to always track `upstream/master` (i.e. the GitHub mirror) then another way to
achieve this is to add another branch that tracks `apache/master` and push from that branch e.g.

    $ git checkout master
    $ git branch apache_master --track apache/master
    $ git pull
    $ git merge --no-ff pr/105
    $ git push
