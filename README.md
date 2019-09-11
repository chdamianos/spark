# Spark
This repo holds code to practice and learn Spark

## Installation
* download latest Spark installation from http://spark.apache.org/downloads.html
* `tar -xzf spark-x.x.x-bin-hadoopx.x.tgz`
* `mv spark-x.x.x-bin-hadoopx.x /opt/spark-x.x.x`
* `nano ~/.bashrc`
* add the following
```bash
#add for spark
export SPARK_HOME=/opt/spark-x.x.x/spark-x.x.x-bin-hadoopx.x/
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PATH=$PATH:$SPARK_HOME/bin
export PYSPARK_DRIVE_PYTHON="jupyter"
export PYSPARK_DRIVE_PYTHON_OPTS="notebook"
```
* `source ~/.bashrc`
* check installation of `spark`
  * go to `/opt/spark-x.x.x/spark-x.x.x-bin-hadoopx.x/bin/` and run `./pyspark`. also run `pyspark` from any folder. in both cases you should get 
```
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version x.x.x
      /_/
Using Python version 3.5.2 (default, Jul  2 2016 17:53:06)
SparkSession available as 'spark'.
>>>
```
* `pip install findspark`
* `jupyter notebook`
```python
import findspark
findspark.init()
import pyspark
import random
sc = pyspark.SparkContext(appName="Pi")
num_samples = 10000000
def inside(p):     
    x, y = random.random(), random.random()
    return x*x + y*y < 1
count = sc.parallelize(range(0, num_samples)).filter(inside).count()
pi = 4 * count / num_samples
print(pi)
sc.stop()
>>> 3.1410216
```
* if you get a `py4j.protocol.Py4JJavaError` error
  * `sudo apt install openjdk-8-jdk`
  * select correct java version `sudo update-alternatives --config java`
  * `java -version`
  ```
  openjdk version "1.8.0_222"
  OpenJDK Runtime Environment (build 1.8.0_222-8u222-b10-1ubuntu1~18.04.1-b10)
  OpenJDK 64-Bit Server VM (build 25.222-b10, mixed mode)
  ```
