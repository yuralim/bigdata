 export SPARK_HOME="/usr/spark2.0.2/"
 export PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-0.10.3-src.zip:$SPARK_HOME/python/lib/pyspark.zip:$PYTHONPATH
 #PATH="$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin:$PYTHONPATH"
 export PATH=/usr/local/anaconda/bin:$PATH
 rm nohup.out
 nohup jupyter notebook --no-browser --ip 0.0.0.0 --port 8888 &
 tail -f nohup.out &
 