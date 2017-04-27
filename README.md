# mapinfo-to-pg
Загрузка данных из файлов MI (mif, tab) в PG, при конвертировании слои разделяются по типу геометрии, загружаются стили MI.
Утилита использует библиотеку python2 и ogr. Соответствено ogr должна быть устанавлена, а также биндинги для python. Самый простой спсоб это сделать через OS4Geo4W.
Для установки зависимостей необходимо выполнить pip install -r requirements.txt или python -m pip install -r requirements.txt
Особенность: при запусте в консоли OS4Geo4W предварительно нужно выполнить python -m pip install setuptools.

## Запуск утилиты
python convert_mapinfo_to_pg.py -f \<filepath> -H \<host> -u \<user> -w \<password> -d \<dbname>