
###sina uve data handle samza application
* you must install yarn,zookeeper and kafka first
* use mvn clean package
* then tar -zxf ./target/statsamza-0.0.1-SNAPSHOT-dist.tar.gz 
* edit the yarn.package.path
* submit the application to yarn using ./bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/common.properties




