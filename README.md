Cascading Sample Recommender
============================
The goal for this project is to create a sample application in
[Cascading 2.0](http://www.cascading.org/) which shows how to build a
simple kind of [social
recommender](http://en.wikipedia.org/wiki/Recommender_system).

More detailed backgrond information and step-by-step documentation is provided at https://github.com/Cascading/SampleRecommender/wiki

Build Instructions
==================

To generate an IntelliJ project use:

    gradle ideaModule

To build the sample app from the command line use:

    gradle clean jar

Before running this sample app, be sure to set your `HADOOP_HOME` environment variable and clear the `output` directory, then to run on a desktop/laptop with Apache Hadoop in standalone mode:

    rm -rf output
    hadoop jar ./build/libs/recommender.jar data/en.stop data/tweets output/token output/similarity

To view the results:

    more output/similarity/part-00000

An example of log captured from a successful build+run is at https://gist.github.com/2949834

For more discussion, see the [cascading-user](https://groups.google.com/forum/?fromgroups#!forum/cascading-user) email forum.