###ETL: на примере качества анализа воздуха

В репозитории собраны наработки для вебинара ["Подготовка, хранение и анализ данных в Яндекс.Облаке"](https://cloud.yandex.ru/events/153) .

На вебинаре мы рассматриваем пример того, как можно сделать выгрузку, обработку, обогащение и визуализацию данных, даже если у вас нет DataLake.
У вас могут быть большие наборы csv или логов и вы хотитет по ним построить графики для поиска зависимостей.
 
ETL процесс можно строить по-разному, но, в качестве примера, в вебинаре мы используем продукты: [Data Proc](https://cloud.yandex.ru/services/data-proc) для обработки данных, [DataLens](https://cloud.yandex.ru/services/datalens) для визуализации, [Managed Service for ClickHouse](https://cloud.yandex.ru/services/managed-clickhouse) как временное хранилище витрины и [Object Storage](https://cloud.yandex.ru/services/storage) как основное хранилище архивных данных.

Кластер [Data Proc](https://cloud.yandex.ru/services/data-proc) используется в парадигме [Transient Cluster](https://dzone.com/articles/transient-clusters-in-the-cloud-for-big-data).
Это позволяет использовать его только как processing engine и иметь возможность выключить или удалить его в любой момент.
Все данные лежат в [Object Storage](https://cloud.yandex.ru/services/storage) и могут быть использованы в любой момент из всех трёх зон доступности.
Разделение processing engine и storage layer позволяет значительно снизить стоимость хранения и обработки и повысить устойчивость.

Весь код написан для spark-2.4 на scala-2.11.

Используемый s3 bucket [dataproc-breathe](http://storage.yandexcloud.net/dataproc-breathe/) открыт наружу в режиме чтения, им **можно и нужно** и нужно пользоваться из Яндекс.Облака, т.к. это уменьшит время работы с данными.
Исходные данные о качестве воздуха  доступны в [архиве](https://archive.sensor.community/) сообщества [sensor.community](https://sensor.community/en/).
 