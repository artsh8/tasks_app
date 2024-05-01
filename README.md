Демо-проект по работе с airflow и pyspark.
Для запуска необходимо по пути tasks_app/airflow выполнить команду:

```docker-compose up -d```

Затем выполнить настройку:

* Создать пользователя с правами на чтение и запись в объектном хранилище minio с учеткой: "min_usr" | "min_pass";
* Создать бакеты "result", "source";
* Выполнить скрипт initial_script.py;
* Настроить соединение в веб-интерфейсе airflow/Admin/Connections/postgres_default:
    * Host=10.5.0.2;
    * Database=flowdb;
    * Password=postgres;
    * Port=5432;
* Настроить соединение airflow/Admin/Connections/spark_default: Host=local.

Запуск DAG-ов:
* install_postgres_drivers - загружает драйвер для работы работы spark с postgres;
* setup_schema - создает схему в БД и заполняет данными из .csv файла из объектного хранилища;
* spark_dag - ETL-процесс по аналитике исходных данных;
* upload_charts - по результатам аналитики создаются графики и таблица; происходит загрузка в объектное хранилище.

Для просмотра результатов необходимо выполнить скрипт check_results.py.