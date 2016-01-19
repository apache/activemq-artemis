# Building

We use Apache Maven to build the code, docs, distribution, etc. and to manage dependencies.

The minimum required Maven version is 3.0.0.

Note that there are some [compatibility issues with Maven 3.X](https://cwiki.apache.org/confluence/display/MAVEN/Maven+3.x+Compatibility+Notes)
still unsolved. This is specially true for the ['site' plugin](https://maven.apache.org/plugins-archives/maven-site-plugin-3.3/maven-3.html).

## Full Release

#### Upgrading the `gitbook` version and regenerating the `npm-shrinkwrap.json` file
The full release uses `gitbook` to build a static website from the documentation. This is automatically installed using
an `NPM` plugin and is controled via a package.json file.

Install `NPM` using the instructions below

    cd artemis-website
    alter the `package.json` changing the version
    npm cache clean; rm -rf ./node_modules/ ./node npm-shrinkwrap.json
    npm install --save-dev
    npm shrinkwrap --dev

The new npm-shrinkwrap.json should be written, commit it.

#### Install npm On Fedora

    $ yum install npm

#### Install npm On Mac-OS

The easiest way would be through brew [brew]

You first install brew using the instructions on the [brew] website.

After you installed brew you can install npm by:

    brew install npm

[brew]: <http://brew.sh>

To build the full release with documentation, Javadocs, and the full web site:

    $ mvn -Prelease package

To install it to your local maven repo:

    $ mvn -Prelease install

## Build the distribution without docs

It is possible to build a distribution with out the manuals and Javadocs.
simply run

    $ mvn package
