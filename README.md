libzkafka
=========
A library for zookeeper enabled kafka access.

How to Compile
--------------
```
$ mvn clean package
```

UntamedConsumerExample
----------------------
```
$ java -Dgroup.id=example-test-group -Dtopic=example-test-topic -Dfetchsize=8192 -Dmaxread=10 -Dlatest=true -cp ./example/target/example-0.1.0.jar com.github.shinkou.zkafka.consumer.examples.UntamedConsumerExample
```

TamedConsumerExample
--------------------
```
$ java -Dgroup.id=example-test-group -Dtopic=example-test-topic -Dfetchsize=8192 -Dmaxread=10 -Dearliest=true -cp ./example/target/example-0.1.0.jar com.github.shinkou.zkafka.consumer.examples.TamedConsumerExample
```
