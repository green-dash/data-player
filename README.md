green-dash data-player
======================

A program that reads raw data files of timestamped events from a folder and replays them to Kafka.

Configuration
-------------
The default configuration can be found in the file src/main/resources/application.conf.
These are the most important parameters:

* fileFolder: the location of the folder with the data files

* kafkaBroker: connection parameters for the kafka producer client

* speedFactor: deltas between timestamps in the files are used to calculate the sleep period between event firing. 
You can speed this up by setting the speedFactor to a higher value, for example 1000.
Setting this value to 0 eliminates the sleep period, so this is the fatsest operating mode.

All these parameters can also be passed as options from the command line, for example:

```
./activator run -DspeedFactor=5000
```

Running
-------
To run the programs, execute:

```
./activator run
```


