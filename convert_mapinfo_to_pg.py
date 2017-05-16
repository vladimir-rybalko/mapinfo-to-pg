#!/usr/bin/python
# -*- coding: utf-8 -*-
 
###########################################################################
 
"""Usage: convert_mapinfo_to_pg.py -f <filepath> -H <host> -u <user> -w <password> -d <dbname>
Options:
  -h, --help            эта справка по работе с программой
  -H, --host            хост БД, в которую конвертируются данные
  -u, --user            имя пользователя для подключения к БД
  -w, --password        пароль пользользователя, под которым происходит подключение
  -d, --dbname          имя БД, в которую юудет производится импорт данных
  -p, --port            порт для подключения к БД
  -s, --schema          схема для загрузки данных
  -f, --file            путь до файла MapInfo с расширением mif или tab
"""
__author__ = 'vladimir.rybalko@gmail.com (Vladimir Rybalko)'
 
import os, sys, psycopg2, chardet
from optparse import OptionParser
try:
  from osgeo import ogr
except:
  sys.exit('ERROR: cannot find GDAL/OGR modules')
 
def stringDecode(str):
  code = chardet.detect(str)['encoding']
  if code != 'utf-8':
    code='cp1251'
  return str.decode(code)
 
def ParseInputs():
  parser = OptionParser()
  parser.add_option('-H', '--host', dest='host', default='localhost', help='The host name PG to connect.')
  parser.add_option('-u', '--user', dest='userName', default='postgres', help='The user to connect the PG.')
  parser.add_option('-w', '--password', dest='password', default='', help="The user's password")
  parser.add_option('-p', '--port', dest='port', default='5432', help='The port to connect the PG')
  parser.add_option('-d', '--dbname', dest='databaseName', default='postgres', help='The database name to connect the PG')
  parser.add_option('-s', '--schema', dest='schema', default='public', help='The schema for importing data to database')
   
  parser.add_option('-f','--file', dest='file', help='The file to import PG')
 
  (options, args) = parser.parse_args()
  if args:
    parser.print_help()
    parser.exit(msg='\nUnexpected arguments: %s\n' % ' '.join(args))
 
  invalid_args = False
  if options.host is None:
    print ('-H (host) is required')
    invalid_args = True
  if options.userName is None:
    print ('-u (userName) is required')
    invalid_args = True
  if options.password is None:
    print ('-w (password) is required')
    invalid_args = True
  if options.databaseName is None:
    print ('-d (dbname) is required')
    invalid_args = True
  if options.file is None or os.path.isfile(options.file) is False or (options.file.lower().endswith(('.mif')) or options.file.lower().endswith(('.tab'))) is False:
    print ('-f (file) задан не задан или задан не правильно')
    invalid_args = True
  conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (options.databaseName, options.userName, options.host, options.password))
  cur = conn.cursor()
  cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{}'".format(options.schema))
  if cur.fetchone() is None:
    print ('-s (schema) is not correct')
    invalid_args = True
  cur.close()
  conn.close()
  if invalid_args:
    sys.exit(4)
  return options
 
def connect(options):
  try:
    client = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (options.databaseName, options.userName, options.host, options.password))
  except:
    sys.exit('I am unable to connect to the database')
  return client
 
def main():
  Nonearth = False
  options = ParseInputs()
  conn = connect(options)
  cursor = conn.cursor()
  try:
    cursor.execute("SELECT COUNT(f_geometry_column) FROM public.geometry_columns")
  except psycopg2.Error, e:
    sys.exit('The database have not PostGIS extension')

  dataFile = options.file
  driver = ogr.GetDriverByName('MapInfo File')

  dataSource = driver.Open(dataFile, 0)
 
  for l in range( dataSource.GetLayerCount() ):
    layer = dataSource.GetLayer(l)
    layerName = layer.GetName()
    layerName = options.schema + '.' + layerName
    spatialRef = layer.GetSpatialRef()
    
    fields = []
    types = []
    features = []
    for feature in layer:
      geom = feature.GetGeometryRef()
      types.append(geom.GetGeometryName())
      features.append(feature)
    types = list(set(types))

    for t in types:
      cursor.execute("BEGIN TRANSACTION")
      cursor.execute("DROP TABLE IF EXISTS {0}_{1}".format(layerName, t))
      cursor.execute("CREATE TABLE {0}_{1}(fid SERIAL NOT NULL PRIMARY KEY)".format(layerName, t))
      cursor.execute("END TRANSACTION;")
      layerDefinition = layer.GetLayerDefn()

      for i in range(layerDefinition.GetFieldCount()):
        fieldName = stringDecode(layerDefinition.GetFieldDefn(i).GetName())
        if not fieldName in fields:
          fields.append(fieldName)
        fieldTypeCode = layerDefinition.GetFieldDefn(i).GetType()
        fieldType = layerDefinition.GetFieldDefn(i).GetFieldTypeName(fieldTypeCode)
        if fieldType == 'Integer':
          cursor.execute(' '.join(['ALTER TABLE', layerName + '_' + t, 'ADD COLUMN', fieldName, 'integer']))
        elif fieldType == 'String':
          fieldWidth = layerDefinition.GetFieldDefn(i).GetWidth()
          cursor.execute(' '.join(['ALTER TABLE', layerName + '_' + t, 'ADD COLUMN', fieldName, 'character varying('+str(fieldWidth)+')']))
        elif fieldType == 'Real':
          fieldWidth = layerDefinition.GetFieldDefn(i).GetWidth()
          cursor.execute(' '.join(['ALTER TABLE', layerName + '_' + t, 'ADD COLUMN', fieldName, 'double precision']))
      cursor.execute(' '.join(['ALTER TABLE', layerName + '_' + t, 'ADD COLUMN style text']))
      cursor.execute(' '.join(['ALTER TABLE', layerName + '_' + t, 'ADD COLUMN geometry geometry']))

      for feature in features:
        if feature.GetGeometryRef().GetGeometryName() == t:
          insertValues=[]
          for j in fields:
            if type(feature.GetField(fields.index( j ))) is str:
              insertValues.append( "'" + stringDecode(feature.GetField(fields.index( j ))) + "'" )
            else:
              insertValues.append(str(feature.GetField(fields.index( j ))))

          styles = feature.GetStyleString().split(';', 1 )
          p=[]
          b=[]
          for style in styles:
            patern = style[0:style.index('(')]
            style = style[style.index('(')+1:-1].split(',') 
            if(patern == 'PEN'):
              for element in style:
                e = element.split(':')
                if e[0]=='w':
                  p.insert(0, e[1][:-2])
                elif e[0]=='c':
                  p.insert(2, str(int(e[1][1:],16)))
                elif e[0]=='id':
                  p.insert(1, e[1].split(',')[0].split('-')[2])             
            elif(patern == 'BRUSH'):
              for element in style:
                e = element.split(':')
                if e[0]=='fc':
                  b.insert(1, str(int(e[1][1:],16)))
                elif e[0]=='bc':
                  b.insert(2, str(int(e[1][1:],16)))
                elif e[0]=='id':
                  b.insert(0, e[1].split(',')[0].split('-')[2])

          styles=''
          if len(p)>0:
            styles=styles+"PEN({})".format(",".join(p))
          if len(b)>0:
            styles=styles+"BRUSH({})".format(",".join(b))
          insertValues.append( "'" + styles + "'" )
          cursor.execute( "".join(["INSERT INTO " + layerName + '_' + t + "(" + ",".join(fields) + ", style, geometry) VALUES ("+",".join(insertValues)+",ST_GeomFromText('",feature.GetGeometryRef().ExportToWkt(),"'))"]) )
      print 'Finished importing the table {0}_{1}'.format(layerName, t)
    cursor.close()
  if conn:
    conn.close()
  return 0
 
if __name__ == '__main__':
  main()

#python convert_mapinfo_to_pg.py -f /work/convertData/test1.mif -H db.w.devel -u postgres -w postgres1 -d generalization_z
