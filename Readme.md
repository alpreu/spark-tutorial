# Spark Handson
Our solution for the Spark hands-on of the Distributed Data Management course at the Hasso-Plattner-Institute.

## Running the system
Build the project with maven first.
To start the IND discovery run:

Assuming you are using 4 worker cores and your data is located in `./TPCH`:
`java -jar SparkTutorial-1.0.jar`

If you have a different configuration you can run the program as follows:
`java -jar SparkTutorial-1.0.jar --cores=<number of worker cores> --path=<path to the TPCH data>`


