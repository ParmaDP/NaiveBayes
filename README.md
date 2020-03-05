# NaiveBayes

to run the project we use this command:
reset && HADOOP_CLASSPATH="$HADOOP_CLASSPATH:/opt/MyJarTests/commons-math3-3.6.1.jar" hadoop jar parma2.jar parmanix.MapReduceEntrypoint /input_bigshop/bigshop.txt /parmanix_output500 bigshop parmanix.IdentityMapper count 1.0 1 0 50000 10000 15 900000

1- parmanix.MapReduceEntrypoint:is the main driver file. it can have other names depending on which file is the main driver file.
2- /input_bigshop/bigshop.txt : the path to the dataset
3-/parmanix_output : tghe path to hadoop output in HDFS
4- bigshop the name of the main function in the driver file
5- count 1.0 1 0 50000 10000 15 900000  : name of reducer/epsilon/delta(not working)/min range/ max range/ privacy budget (nor working)/N(max key per group)/n
