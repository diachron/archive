archive
=======

The archive module consists of two sub modules, the archive system and the archive web services. These two are structured are as maven modules under a single parent project. The archive web services module is dependent on archive system module.

## Build instructions

1.Build first the archive system module using the following maven command on this module that will build it and install it to the local repository so as to be accessible for the other module
```
mvn clean install
```
There are two dependencies that might not be accessible in public online maven repositories. The virtuoso jdbc driver and the virtuoso provider for Jena. Currently they are accessible from here

http://maven.aksw.org/repository/internal/com/openlink/virtuoso/virtjdbc4/
http://maven.aksw.org/repository/internal/com/openlink/virtuoso/virt_jena2/

Alternatively you can download them from here
http://virtuoso.openlinksw.com/dataspace/doc/dav/wiki/Main/VOSDownload/virtjdbc4.jar
http://opldownload.s3.amazonaws.com/uda/virtuoso/rdfproviders/jena/210/virt_jena2.jar

and add them to your local repository following these instructions

http://maven.apache.org/guides/mini/guide-3rd-party-jars-local.html

=======

2.Edit the “virt-connection.properties” under the “diachron-archive\archive-web-services\src\main\resources\” in order to provide your virtuoso connection properties
More details about this in deliverable D6.3 (Section 3.1.5.2)

=======

3.The build and package the archive web services module by executing the following maven command
```
mvn clean package
```
This will produce a war file including all the necessary dependencies. You can then deploy the war file as standard web application in Tomcat or JBoss.


