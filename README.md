# 实验五：Spark SQL

[TOC]

## 一、加载数据成为分布式表

### 1、mysql 数据：

#### (1) 从数据库中读取数据，并进行查询操作

**准备数据：**

![image-20230521150945262](./assets/image-20230521150945262.png)

**数据如下：**

![image-20230521150558586](./assets/image-20230521150558586.png)

**Code:**

![image-20230521155040963](./assets/image-20230521155040963.png)

**输出结果：**

**查询 1：输出整个表**

![image-20230521154837560](./assets/image-20230521154837560.png)

**查询 2：输出 ID 大于 1 的男同学的姓名、性别、学号**

![image-20230521154852511](./assets/image-20230521154852511.png)

#### (2) 从本地读取数据，将查询结果存入 mysql 数据库中

**准备数据：**

`test.json` ：本地文件位置自定义，同时修改 Code 中文件的位置

![image-20230521155522596](./assets/image-20230521155522596.png)

**操作文件：**

**pom.xml 同上**

**Code：提取年龄小于等于 20 的人的所有信息并存入 MyDB 数据库的 personTable 表中**

![image-20230521160348349](./assets/image-20230521160348349.png)

**程序输出结果**

![image-20230521160238260](./assets/image-20230521160238260.png)

**查看数据库中的表，新增了 personTable 表**

![image-20230521160448503](./assets/image-20230521160448503.png)

**查看 personTable 表中的信息**

![image-20230521160554361](./assets/image-20230521160554361.png)

### 2、其它三种数据类型文件：

**（文件位置可以自定义，同时修改 Code 中文件的位置）**

![image-20230521160754157](./assets/image-20230521160754157.png)

**Code (scala):**

![image-20230521161104004](./assets/image-20230521161104004.png)

**运行结果：**

**输出 data 中每列的名称和类型：**

![image-20230521161347272](./assets/image-20230521161347272.png)

**输出文件内容，不同文件类型有不同的形式**

![image-20230521161425384](./assets/image-20230521161425384.png)

![image-20230521161438332](./assets/image-20230521161438332.png)

![image-20230521161450750](./assets/image-20230521161450750.png)

## 二、spark sql 实现信息查询

在 SparkSQL 模块中，将结构化数据封装到 DataFrame 或 Dataset 集合中后，提供了两种方式分析处理数据:

1）SQL 编程，将 DataFrame/Dataset 注册为临时视图或表，编写 SQL 语句，类似HiveQL;

2） DSL (domain-specific language)编程，（类似于面向对象）调用 DataFrame/Dataset APIl(函数），类似 RDD 中的函数;

**查询文件 person.txt（文件位置可以自定义，同时修改 Code 中文件的位置）**

![image-20230521161627149](./assets/image-20230521161627149.png)

**查询需求：**

1、查看 name 字段的数据

2、查看 name 和 age 字段数据

3、查询所有的 name 和 age，并将 age+1

4、过滤 age 大于等于 25 的

5、统计年龄大于 30 的人数

6、按年龄进行分组并统计相同年龄的人数

7、查询姓名==张三的

**pom.xml**：同上

### sql 方式

#### **Code** 

![image-20230521162135512](./assets/image-20230521162135512.png)

#### **输出结果：**

personDF.printSchema()

personDF.show()

![image-20230521162021067](./assets/image-20230521162021067.png)

1.查看 name 字段的数据

2.查看 name 和 age 字段数据

![image-20230521162043125](./assets/image-20230521162043125.png)

3.查询所有的 name 和 age，并将 age+1

4.过滤 age 大于等于 25 的

![image-20230521162055752](./assets/image-20230521162055752.png)

5.统计年龄大于 30 的人数

6.按年龄进行分组并统计相同年龄的人数

7.查询姓名=张三的

![image-20230521162109015](./assets/image-20230521162109015.png)

### dsl 方式

#### **Code**

![image-20230521162539219](./assets/image-20230521162539219.png)

## 三、spark sql 实现 wordcount （单机）

**测试文件 word.txt**

**Code：**

![image-20230521163036276](./assets/image-20230521163036276.png)

**运行结果：**

![image-20230521163253803](./assets/image-20230521163253803.png)

## 四、Spark sql 集群形式运行 wordcount 程序

**将测试文件上传到 hdfs 中**

![image-20230521163705206](./assets/image-20230521163705206.png)

**pom.xml**

![image-20230521163748267](./assets/image-20230521163748267.png)

**code**

![image-20230521163857349](./assets/image-20230521163857349.png)

**将程序打成 jar 包，并移动到/opt/spark 文件夹中**

![image-20230521165649146](./assets/image-20230521165649146.png)

**6、运行**

（1）开启 hadoop

（2）开启 spark

（3) 提交任务

```shell
cd /opt/spark
bin/spark-submit --class org.bupt_2020212185_zy.WordCount --master spark://M2020212185:7077  /root/IdeaProjects/dsj_lab5/target/dsj_lab5-1.0-SNAPSHOT-jar-with-dependencies.jar
```

运行结果

![image-20230521171050969](./assets/image-20230521171050969.png)

这个路径就很迷惑

code里面是

```java
  val fileDS: Dataset[String] = spark.read.textFile("hdfs://M2020212185:9000/word.txt")
```

到这里面却多了个root

只能cp一下了

运行成功：

![image-20230521171306903](./assets/image-20230521171306903.png)