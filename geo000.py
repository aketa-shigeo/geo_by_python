#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pyodbc
import pprint
import geo_adr_latlng
import time

conn = pyodbc.connect("DSN=ORA12C;UID=g015582;PWD=p015582")
conn.setencoding(str, encoding='utf-8')
cursor = conn.cursor()

#sql_1 ="SELECT mt.* FROM G015582.MS22_TMP mt "
#sql_1 ="SELECT mt.* FROM G015582.MS22 mt "
sql_1 ="SELECT mt.* FROM G015582.MS22_TMP2 mt "
sql_1+="WHERE (MS2LATIT IS NULL "
sql_1+="    or MS2LATIT IN('@error@','ERR','')) "
sql_1+="ORDER BY MS2ACTCD, MS2PSTCD"
#sql_1+=" FETCH FIRST 5 ROWS ONLY "
rc = cursor.execute( sql_1 )

rows = cursor.fetchall()

#print(rows)
nor=0
for row in rows:
  nor +=1
  time.sleep(10)
  ## Retreiving geo code from customer address
  try:
    geo_list = geo_adr_latlng.adr2geo(row[5]) 
    ## result of t
    #####print(row[0].encode('utf-8'),row[2].encode('utf-8')) # works!

    # SQL組み立て
    sql_2 ="update MS22_TMP2 a set "
#    sql_2 ="update MS22_TMP a set "
    sql_2+=" MS2LATIT='"+str(geo_list[0])+"',"
    sql_2+=" MS2LONGI='"+str(geo_list[1])+"' "
    sql_2+=" where MS2ACTCD='"+row[0].encode('utf-8')+"' and MS2CSTCD='"+row[2].encode('utf-8')+"'"
    
    cursor.execute(sql_2)
    cursor.commit()

    #print('No.    :'+str(nor)) #+' /('+count+')') 
    #print('Divi   :'+row[1].encode('utf-8')) 
    #print('CustCD :'+row[2].encode('utf-8'))
    #print('CustNam:'+row[3].encode('utf-8'))
    #print('CustAdr:'+row[4].encode('utf-8'))
    #print('Latitude:'+str(geo_list[0]))
    #print('Longitude:'+str(geo_list[1]))

    #print(sql_2)
    #print('----------------------------------------------')

    ## due to explanation of geocoding api http://www.geocoding.jp/
    ##time.sleep(10)

  except AttributeError:
    sql_2 ="update MS22_TMP2 a set "
    sql_2+=" MS2LATIT='ERR' "
    sql_2+=" where MS2ACTCD='"+row[0].encode('utf-8')+"' and MS2CSTCD='"+row[2].encode('utf-8')+"'"
    
    cursor.execute(sql_2)
    cursor.commit()
    print('No.:'+str(nor)+' is AttributeError!!!')
    print(row)
    print(sql_2)
    print('----------------------------------------------')
    pass

  #except UnicodeError, e:
  #  pass
  #else:
  except Exception as e:
    print '=== エラー内容 === ' +str(nor)
    print 'type:' + str(type(e))
    print 'args:' + str(e.args)
    #print 'message:' + e.message
    print 'e自身:' + str(e)
    #print('Error found @ '+str(nor))
    pass
conn.close()

### end of the pgm ### 
