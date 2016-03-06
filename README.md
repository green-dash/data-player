green-dash data-player
======================

A program that reads raw data files of timestamped events from a folder and replays them to Kafka.

Configuration
-------------
The default configuration can be found in the file src/main/resources/application.conf.
These are the most important parameters:

* file.folder: the location of the folder with the data files

* kafka.broker: connection parameters for the kafka producer client

* speed.factor: deltas between timestamps in the files are used to calculate the sleep period between event firing. 
You can speed this up by setting the speedFactor to a higher value, for example 1000.
Setting this value to 0 eliminates the sleep period, so this is the fastest operating mode.

Running
-------
To run the program, execute:

```
./activator run
```

