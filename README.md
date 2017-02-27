# bd_incubator_ta
##Test assignment for Bitworks bigdata incubator
>Мисютин Роман Владимирович
>roman.misiutin@gmail.com
>+79234351762

На выбор предлагается 2 варианта: jupyter-notebook  и скрипт для запуска с spark-submit. Комментарии и поясняния содержатся только в ноутбуке
Вычисляются как "некорректные" распределения трафика, так и "исправленные" (пояснения в ноутбуке)
### Запуск
#### Скрипт
`/path/to/spark-submit sflow-job.py /path/to/input/csv /path/to/GeoLite2-Country.mmdb /path/to/output/dir`

#### jupyter-notebook
Запустить jupyter сервер
`PYSPARK_PYTHONDRIVER=jupyter PYSPARK_PYTHONDRIVER_OPTS=notebook /path/to/pyspark`
Открыть нужный ноутбук
Также необходимо в самом ноутбуке задать пути до датасета, GeoLite2-Country.mmdb и желаемой папки для вывода

