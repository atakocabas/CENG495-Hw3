#CENG495 - HW3

## Compilation

```
$ hadoop com.sun.tools.javac.Main *.java
$ jar cf Hw3.jar *.class
```

## RUN
```
$ hadoop jar Hw3.jar Hw3 total <input.csv> output_total
$ hadoop jar Hw3.jar Hw3 total <input.csv> output_average
$ hadoop jar Hw3.jar Hw3 total <input.csv> output_employment
$ hadoop jar Hw3.jar Hw3 total <input.csv> output_ratescore

```
