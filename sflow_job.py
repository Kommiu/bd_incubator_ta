from pyspark import SparkContext,SparkConf
import os, sys, getopt
import matplotlib.pyplot as plt
import pyspark.sql.functions as psf
from pyspark.sql.functions import col,udf
from  pyspark.sql.types import *
from pyspark.sql import SparkSession
from time import time
start = time()
appName = "sflow_assignment"
master = "local[4]"
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

#Не стал заморачиваться с нормальным парсингом аргументов/конфигов
args=sys.argv
if len(args) != 4:
    sys.exit("script takes exactly 3 arguments")
input_file_path = os.path.realpath(args[1])
giLite2db_path = os.path.realpath(args[2])
output_dir = os.path.realpath(args[3])
if not os.path.exists(output_dir):
    os.mkdir(output_dir)
print(sc.getConf().getAll())
names = "Timestamp FLOW_indicator Agent_address Input_port \
    Output_port Src_MAC Dst_MAC Ethernet_type In_vlan Out_vlan \
    Src_ip Dst_ip Ip_protocol Ip_tos Ip_ttl Udp_src_port_OR_tcp_src_port_OR_icmp_type \
    Udp_dst_port_OR_tcp_dst_port_OR_icmp_code \
    Tcp_flags Packet_size IP_size Sampling_rate".split()
schema = StructType([StructField(field_name, StringType(), True) for field_name in names])
df = spark.read.csv(input_file_path,schema = schema)

src = df.select(df["Src_ip"].alias("ip"), df["Packet_size"].cast(IntegerType()).alias('size'), df['Sampling_rate'].alias('Sr'))
dst = df.select(df["Dst_ip"].alias("ip"), df["Packet_size"].cast(IntegerType()).alias('size'), df['Sampling_rate'].alias('Sr'))
data = src.union(dst)
data.cache()

byIP = data.groupBy('ip').agg(psf.sum('size').alias('sum'))
byIP.cache()
byIP.write.json(os.path.join(output_dir,'byIP_sampled'), mode='overwrite')

def get_country(ip):
    from geoip2.database import Reader
    from geoip2.errors import AddressNotFoundError
    try:
        # Насколько я понимаю spark неспособен создать broadcast верси database.Reader
        # объект, поэтому приходится его создавать при каждом вызове функции
        # хотя возможно, производится какая-то оптимизация внутри udf()
        reader = Reader(giLite2db_path)
        response = reader.country(ip)
        country = response.country.name
        return  country if country else "NOT_FOUND"
    except AddressNotFoundError:
        return "NOT_FOUND"
udf_get_country = udf(get_country,StringType())

withCountry = byIP.withColumn('country',udf_get_country(col('ip')))
withCountry.cache()

byCountry = withCountry.groupBy('country').agg(psf.sum('sum').alias('sum'))
byCountry.cache()
byCountry.coalesce(1).write.json(os.path.join(output_dir,'byCountry_sampled'),mode='overwrite')

byCountry_pd = byCountry.sort('sum').toPandas()

index = range(byCountry_pd.shape[0])
fig,ax = plt.subplots(figsize=(20,50))
ax.barh(index, byCountry_pd['sum'])
ax.set_xscale('log')
ax.set_xlabel('Log(Byte)')
ax.set_title('Aggregate network trafic by country (on log scale)',y=1.01)
ax.margins(0,0)
ax.set_xlim(0,1e11)
ax.tick_params(labeltop='on')
plt.yticks(index, byCountry_pd['country'], rotation='horizontal',fontsize=12)
fig.tight_layout()
fig.savefig(os.path.join(output_dir,'vertical_plot.png'))
plt.close(fig)

fig,ax = plt.subplots(figsize=(60,15))
ax.bar(index, byCountry_pd['sum'])
ax.set_yscale('log')
ax.set_ylabel('Log(Byte)')
ax.set_title('Aggregate network trafic by country (on log scale)',y=1.01)
ax.margins(0,0)
ax.set_ylim(0,1e11)
ax.tick_params(labelright='on')
plt.xticks(index, byCountry_pd['country'], rotation='vertical',fontsize=12)
fig.tight_layout()
fig.savefig(os.path.join(output_dir,'horizontal_plot.png'))
plt.close(fig)

n = sc.broadcast(data.count())
sr = sc.broadcast(512)

from string import Template
query = Template('''
    SELECT $type, sum*$sr AS trafic_estimate,
        SQRT(vbm*vn+vn*POWER(sum/count,2)+vbm*POWER(count*$sr,2)) AS standard_deviation
    FROM (SELECT
            $type,count,sum,(sumSq - POWER(sum,2)/count)/(count-1) AS vbm,
            ($sr*count*($n-count))/($n-1) AS vn
        FROM $table)
''')

byIP_fxd = data.groupBy('ip').agg(psf.sum(col('size')).alias('sum'),\
        psf.sum(col('size')**2).alias('sumSq'),psf.count('ip').alias('count'))
byIP_fxd.cache()

byIP_fxd.createOrReplaceTempView('byIP')
byIP_est = spark.sql(query.substitute(table='byIP',sr=sr.value,n=n.value,type='ip'))
byIP_est.cache()

byCountry_fxd = byIP_fxd.withColumn('country',udf_get_country(col('ip')))\
    .groupBy('country').agg(psf.sum(col('sum')).alias('sum')\
    ,psf.sum(col('sumSq')).alias('sumSq'),psf.sum('count').alias('count'))

byCountry_fxd.createOrReplaceTempView('byCountry')
byCountry_est = spark.sql(query.substitute(table='byCountry',sr=sr.value,n=n.value,type='country'))
byCountry_est.cache()

byIP_est.write.json(os.path.join(output_dir,'byIP_estimate'),mode='overwrite')
byCountry_est.coalesce(1).write\
    .json(os.path.join(output_dir,'byCountry_estimate'),mode='overwrite')

stop = time()

print(f"Time elapsed(min): {(stop-start)/60}")
