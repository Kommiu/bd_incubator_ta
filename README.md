# bd_incubator_ta
Test assignment for Bitworks bigdata incubator

На выбор предлагается 2 варианта: jupyter-notebook  и скрипт для запуска с spark-submit. Комментарии и поясняния содержатся только в ноутбуке
Вычисляются как "некорректные" распределения трафика, так и "исправленные" (пояснения в ноутбуке)
### Запуск
#### Скрипт
/path/to/spark-submit sflow-job.py /path/to/input/csv /path/to/GeoIpLite2.mmdb /path/to/output/dir

#### jupyter-notebook
Запустить jupyter сервер
PYSPARK_PYTHONDRIVER=jupyter PYSPARK_PYTHONDRIVER_OPTS=notebook /path/to/pyspark
Открыть нужный ноутбук
Также необходимо в самом ноутбуке задать пути до датасета, GeoIPLite-Country.mmdb и желаемой папки для вывода

