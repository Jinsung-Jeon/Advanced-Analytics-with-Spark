
sc

val rdd = sc.parallelize(Array(1,2,2,4),4)

val user = sc.textFile("u.user")

user.first

val head = user.take(10)

head.foreach(println)

val parsed = spark.read.
    option("header","false").
    option("nullValue","true").
    option("delimiter", "|").
    option("inferSchema", "true").
    csv("u.user")

parsed.printSchema()

val summary = parsed.describe()

summary.show()

val item = sc.textFile("u.item")

item.first()

val head2 = item.take(10)

head2.foreach(println)

val parsed2 = spark.read.
    option("header","false").
    option("nullValue","true").
    option("delimiter","|").
    option("inferSchema","true").
    csv("u.item")

parsed2.first()

parsed2.printSchema()

val summary2 = parsed2.describe()

summary2.show()

val data = sc.textFile("u.data")

data.first()

val head3 = data.take(10)

head3.foreach(println)

val parsed3 = spark.read.
    option("header","false").
    option("nullValue","true").
    option("inferSchema","true").
    option("delimiter", "\t").
    csv("u.data")    

parsed3.first()

parsed3.printSchema()

val summary3 = parsed3.describe()

summary3.show()

summary.select("summary","_c1","_c2").show()
