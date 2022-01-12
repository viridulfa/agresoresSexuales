#!/usr/bin/env python
# coding: utf-8

#-- ********************************************************************************************************************
#-- *
#-- * a) Objeto                        : agresores_sexuales_v2.py
#-- * b) Aplicacion                    : Carga de información de agresores_sexuales a Base de datos
#-- * c) Autor                         : viridiana y gerardo
#-- * d) Fecha Creacion (YYYYMMDD)     : 20210428
#-- * e) Objetivo                      : Realiza la carga hacia la base de datos de PostgreSql.
#-- * f) Version                       : 1.0.0 (inicial)
#-- ************************************
#-- * g) Historico De Modificaciones
#-- *   i.   Modificacion                 : 
#-- *   ii.  Modifico                     : 
#-- *   iii. Fecha Modificacion (YYYYMMDD): 
#-- *   iv.  Objetivo                     : 
#-- *   v.   Version                      : 
#-- ************************************
#-- * h) Parametros
#-- *   i.      Sin parametros
#-- ********************************************************************************************************************

#-- **********************************************--#
#--                Area de imports                --#
#-- **********************************************--#

import jpype
import jaydebeapi
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
import json
from time import time
from datetime import datetime

# 1. Siguen estando los mismos delitos que no son sexuales (preguntar qué etiqueta tomar y de dónde)
# 2. Hay personas sin delitos
# 3. 

table = 'agresores_sexuales_aux'
esquema = 'penitenciario'
insert_mode = 'replace'

jHome = jpype.getDefaultJVMPath()
jpype.startJVM(jHome, "-Djava.class.path=/usr/lib/oracle/10.2.0.5/client64/lib/ojdbc14.jar")
conn = jaydebeapi.connect("oracle.jdbc.driver.OracleDriver","jdbc:oracle:thin:ADIP_SIIP/51P4dm1_#$*db@//10.20.0.1:1521/DBSIIP")

cur = conn.cursor()

print("---------------------------- INGESTA AGRESORES SEXUALES PENITENCIARIO ----------------------------")
cur.execute("""
SELECT f.partida, f.recl, f.anoi, 
ced.fec_ing,
i.id_persona,i.nom_int, i.apell_pat, i.apell_mat, i.fec_nac, i.edadi, i.pais_nac, i.agresor_sexual,
o.nomb_o,o.apell_po,o.apell_mo,o.alias,ctd.cve_tipo,ctd.descripcion cattipo_del,d.cve_del,cd.descripcion catdelito,
f.estatura,
f.peso,
f.talla,
f.long_pie,
f.cve_tipo_ceja, CATTIPO_CEJA.descripcion descripcion_tipo_ceja,
f.cve_naltura, CATNALTURA.descripcion descripcion_naltura,
f.cve_nancho, CATNANCHO.descripcion descripcion_nancho,
f.cve_frente,  CATFRENTE.descripcion descripcion_frente,
f.cve_complexion, CATCOMPLEXION.descripcion descripcion_tipo_complexion,
f.cve_piel, CATPIEL.descripcion descripcion_piel,
f.cve_cara, CATCARA.descripcion descripcion_cara,
f.cve_color_ojos, CATCOLOR_OJOS.descripcion descripcion_color_ojos,
f.cve_tipo_ojos, CATTIPO_OJO.descripcion descripcion_tipo_ojos,
f.cve_tipo_labios,  CATTIPO_LABIOS.descripcion descripcion_tipo_labios,
f.cve_tipo_boca, CATTIPO_BOCA.descripcion descripcion_tipo_boca,
f.cve_nariz, CATNARIZ.descripcion descripcion_nariz,
f.cve_base, CATNBASE.descripcion descripcion_base,
f.cve_orejas, CATOREJA.descripcion descripcion_orejas,
f.cve_menton, CATMENTON.descripcion descripcion_menton,
f.cve_tipo_cab, CATTIPO_CABELLO.descripcion descripcion_tipo_cabello,
f.cve_color_cab, CATCOLOR_CABELLO.descripcion descripcion_color_cabello,
f.cve_helix, CATHELIX.descripcion descripcion_helix,
f.cve_lobulo, CATLOBULO.descripcion descripcion_lobulo,
f.cve_trago, CATTRAGO.descripcion descripcion_trago,
f.cve_antitrago, CATANTITRAGO.descripcion descripcion_antitrago,
f.cve_color_cejas, CATCOLOR_CEJAS.descripcion descripcion_color_cejas,
f.cve_inclinacion, CATINCLINACION.descripcion descripcion_inclinacion,
f.sen_part
FROM siip.interno i
left join  siip.otros_nomb o
	on i.partida = o.partida and i.recl = o.recl and i.anoi = o.anoi
left join siip.filiacion f 
	on f.partida = i.partida and f.recl = i.recl and f.anoi = i.anoi
left join siip.delint d 
	on f.partida = d.partida and f.recl = d.recl and f.anoi = d.anoi
left join siip.catdelito cd
	on d.cve_del = cd.cve_del
left join siip.cattipo_del ctd
	on ctd.cve_del = d.cve_del and ctd.cve_tipo = d.cve_tipo
left join siip.CATTIPO_CEJA CATTIPO_CEJA
	on CATTIPO_CEJA.CVE_TIPO_CEJA = f.cve_tipo_ceja
left join siip.CATNALTURA CATNALTURA
	on CATNALTURA.CVE_NALTURA	 = f.cve_naltura	
left join siip.CATNANCHO CATNANCHO
	on CATNANCHO.CVE_NANCHO	 = f.cve_nancho		
left join siip.CATFRENTE CATFRENTE
	on CATFRENTE.CVE_FRENTE	 = f.cve_frente		
left join siip.CATCOMPLEXION CATCOMPLEXION
	on CATCOMPLEXION.cve_complexion	 = f.cve_complexion			
left join siip.CATPIEL CATPIEL
	on CATPIEL.cve_piel	 = f.cve_piel			
left join siip.CATCARA CATCARA
	on CATCARA.cve_cara	 = f.cve_cara			
left join siip.CATCOLOR_OJOS CATCOLOR_OJOS
	on CATCOLOR_OJOS.cve_color_ojos	 = f.cve_color_ojos			
left join siip.CATTIPO_OJO CATTIPO_OJO
	on CATTIPO_OJO.cve_tipo_ojos	 = f.cve_tipo_ojos		
left join siip.CATTIPO_LABIOS CATTIPO_LABIOS
	on CATTIPO_LABIOS.cve_tipo_labios	 = f.cve_tipo_labios
left join siip.CATTIPO_BOCA CATTIPO_BOCA
	on CATTIPO_BOCA.cve_tipo_boca	 = f.cve_tipo_boca
left join siip.CATNARIZ CATNARIZ
	on CATNARIZ.cve_nariz	 = f.cve_nariz
left join siip.CATNBASE CATNBASE
	on CATNBASE.cve_base	 = f.cve_base
left join siip.CATOREJA CATOREJA
	on CATOREJA.cve_orejas	 = f.cve_orejas
left join siip.CATMENTON CATMENTON
	on CATMENTON.cve_menton	 = f.cve_menton
left join siip.CATTIPO_CABELLO CATTIPO_CABELLO
	on CATTIPO_CABELLO.cve_tipo_cab	 = f.cve_tipo_cab
left join siip.CATCOLOR_CABELLO CATCOLOR_CABELLO
	on CATCOLOR_CABELLO.cve_color_cab	 = f.cve_color_cab
left join siip.CATHELIX CATHELIX
	on CATHELIX.cve_helix	 = f.cve_helix
left join siip.CATLOBULO CATLOBULO
	on CATLOBULO.cve_lobulo	 = f.cve_lobulo
left join siip.CATTRAGO CATTRAGO
	on CATTRAGO.cve_trago	 = f.cve_trago
left join siip.CATANTITRAGO CATANTITRAGO
	on CATANTITRAGO.cve_antitrago	 = f.cve_antitrago	
left join siip.CATCOLOR_CEJAS CATCOLOR_CEJAS
	on CATCOLOR_CEJAS.cve_color_cejas	 = f.cve_color_cejas	
left join siip.CATINCLINACION CATINCLINACION
	on CATINCLINACION.cve_inclinacion	 = f.cve_inclinacion	
left join siip.cedula_ingreso ced
	on i.partida = ced.partida and i.recl = ced.recl and i.anoi = ced.anoi
WHERE i.agresor_sexual = 'S' AND i.recl!='RC' AND d.agresor_sexual='S'""") 

tabl=cur.fetchall()
columns=[]
for i in cur.description:
    columns.append(i[0])

df = pd.DataFrame(tabl, columns =columns)
df = df.drop_duplicates()

### LOWERCASE A LAS COLUMNAS
df.columns = df.columns.str.lower()
df.columns = [u'partida', u'recl', u'anoi', u'fec_ing',u'id_persona', u'nom_int', u'apell_pat',
       u'apell_mat', u'fec_nac', u'edadi', u'pais_nac', u'agresor_sexual',
       u'nomb_o', u'apell_po', u'apell_mo', u'alias', u'cve_tipo', u'cattipo_del',
       u'cve_del', u'catdelito', u'estatura', u'peso', u'talla', u'long_pie',
       u'cve_tipo_ceja', u'descripcion_tipo_ceja', u'cve_naltura',
       u'descripcion_naltura', u'cve_nancho', u'descripcion_nancho',
       u'cve_frente', u'descripcion_frente', u'cve_complexion',
       u'descripcion_tipo_complexion', u'cve_piel', u'descripcion_piel',
       u'cve_cara', u'descripcion_cara', u'cve_color_ojos',
       u'descripcion_color_ojos', u'cve_tipo_ojos', u'descripcion_tipo_ojos',
       u'cve_tipo_labios', u'descripcion_tipo_labios', u'cve_tipo_boca',
       u'descripcion_tipo_boca', u'cve_nariz', u'descripcion_nariz',
       u'cve_base', u'descripcion_base', u'cve_orejas', u'descripcion_orejas',
       u'cve_menton', u'descripcion_menton', u'cve_tipo_cab',
       u'descripcion_tipo_cabello', u'cve_color_cab',
       u'descripcion_color_cabello', u'cve_helix', u'descripcion_helix',
       u'cve_lobulo', u'descripcion_lobulo', u'cve_trago',
       u'descripcion_trago', u'cve_antitrago', u'descripcion_antitrago',
       u'cve_color_cejas', u'descripcion_color_cejas', u'cve_inclinacion',
       u'descripcion_inclinacion', u'sen_part']

### SE AGREGA FECHA Y HORA DE INGESTA
now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
now = datetime.strptime(now, '%Y-%m-%d %H:%M:%S')
df['fecha_actualizacion']=now

### CASTEO A INTEGER
df['partida'] = df['partida'].astype('Int64')
df['anoi'] = df['anoi'].astype('Int64')
df['edadi'] = df['edadi'].astype('Int64')
df['pais_nac'] = df['pais_nac'].astype('Int64')
df['cve_del'] = df['cve_del'].astype('Int64')
df['cve_tipo_ceja'] = df['cve_tipo_ceja'].astype('Int64')
df['cve_naltura'] = df['cve_naltura'].astype('Int64')
df['cve_nancho'] = df['cve_nancho'].astype('Int64')
df['cve_frente'] = df['cve_frente'].astype('Int64')
df['cve_complexion'] = df['cve_complexion'].astype('Int64')
df['cve_piel'] = df['cve_piel'].astype('Int64')
df['cve_cara'] = df['cve_cara'].astype('Int64')
df['cve_color_ojos'] = df['cve_color_ojos'].astype('Int64')
df['cve_tipo_ojos'] = df['cve_tipo_ojos'].astype('Int64')
df['cve_tipo_labios'] = df['cve_tipo_labios'].astype('Int64')
df['cve_tipo_boca'] = df['cve_tipo_boca'].astype('Int64')
df['cve_nariz'] = df['cve_nariz'].astype('Int64')
df['cve_base'] = df['cve_base'].astype('Int64')
df['cve_orejas'] = df['cve_orejas'].astype('Int64')
df['cve_menton'] = df['cve_menton'].astype('Int64')
df['cve_tipo_cab'] = df['cve_tipo_cab'].astype('Int64')
df['cve_color_cab'] = df['cve_color_cab'].astype('Int64')
df['cve_helix'] = df['cve_helix'].astype('Int64')
df['cve_lobulo'] = df['cve_lobulo'].astype('Int64')
df['cve_trago'] = df['cve_trago'].astype('Int64')
df['cve_antitrago'] = df['cve_antitrago'].astype('Int64')
df['cve_color_cejas'] = df['cve_color_cejas'].astype('Int64')
df['cve_inclinacion'] = df['cve_inclinacion'].astype('Int64')

### INSERT A LA TABLA
with open('/home/norberto/json/config_dbadip.json','r') as json_data:
    config = json.load(json_data)
engine = create_engine('postgresql+psycopg2://'+config['username']+':'+config['password']+'@'+config['host']+':'+str(config['port'])+'/'+config['database'])
conn_psg = engine.connect()

df = df.drop_duplicates()
df['id'] = df.index+1

df = df[[u'id',u'partida', u'recl',
          u'anoi',                     u'fec_ing',
    u'id_persona',                     u'nom_int',
     u'apell_pat',                   u'apell_mat',
       u'fec_nac',                       u'edadi',
      u'pais_nac',              u'agresor_sexual',
        u'nomb_o',                    u'apell_po',
      u'apell_mo',                       u'alias',
      u'cve_tipo',                 u'cattipo_del',
       u'cve_del',                   u'catdelito',
      u'estatura',                        u'peso',
         u'talla',                    u'long_pie',
 u'cve_tipo_ceja',       u'descripcion_tipo_ceja',
   u'cve_naltura',         u'descripcion_naltura',
    u'cve_nancho',          u'descripcion_nancho',
    u'cve_frente',          u'descripcion_frente',
u'cve_complexion', u'descripcion_tipo_complexion',
      u'cve_piel',            u'descripcion_piel',
      u'cve_cara',            u'descripcion_cara',
u'cve_color_ojos',      u'descripcion_color_ojos',
 u'cve_tipo_ojos',       u'descripcion_tipo_ojos',
u'cve_tipo_labios',     u'descripcion_tipo_labios',
 u'cve_tipo_boca',       u'descripcion_tipo_boca',
     u'cve_nariz',           u'descripcion_nariz',
      u'cve_base',            u'descripcion_base',
    u'cve_orejas',          u'descripcion_orejas',
    u'cve_menton',          u'descripcion_menton',
  u'cve_tipo_cab',    u'descripcion_tipo_cabello',
 u'cve_color_cab',   u'descripcion_color_cabello',
     u'cve_helix',           u'descripcion_helix',
    u'cve_lobulo',          u'descripcion_lobulo',
     u'cve_trago',           u'descripcion_trago',
 u'cve_antitrago',       u'descripcion_antitrago',
u'cve_color_cejas',     u'descripcion_color_cejas',
u'cve_inclinacion',     u'descripcion_inclinacion',
      u'sen_part',         u'fecha_actualizacion',
            ]]

print('INSERTANDO...')
df.to_sql(name = table, con = engine, schema=esquema, if_exists = insert_mode, index = False)
print('TERMINA INGESTA AGRESORES SEXUALES')

conn.close()
conn_psg.close()
