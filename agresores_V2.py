#!/usr/bin/env python


#-- ********************************************************************************************************************
#-- *
#-- * a) Objeto                        : agresores_sexuales_v2_2.py
#-- * b) Aplicacion                    : Carga de información de agresores_sexuales a Base de datos
#-- * c) Autor                         : viridiana y gerardo
#-- * d) Fecha Creacion (YYYYMMDD)     : 20210428
#-- * e) Objetivo                      : Realiza la carga dhacia la base de datos de PostgreSql.
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

import pandas as pd
import paramiko
from paramiko import SSHClient
import json
from sqlalchemy import create_engine
import base64

### CONEXION DB POSTGRES
with open('/home/norberto/json/config_dbadip.json','r') as json_data:
    config = json.load(json_data)
engine = create_engine('postgresql+psycopg2://'+config['username']+':'+config['password']+'@'+config['host']+':'+str(config['port'])+'/'+config['database'])
conn = engine.connect()

table='agresores_sexuales'

conn.execute("""TRUNCATE TABLE penitenciario.agresores_sexuales""")  

conn.execute("""insert into penitenciario.agresores_sexuales
select T2.id, T2.partida, T2.recl, T2.anoi, T2.fec_ing, T2.id_persona, T2.nom_int, T2.apell_pat, T2.apell_mat, T2.fec_nac, T2.edadi, T2.pais_nac, T2.agresor_sexual, T2.nomb_o, T2.apell_po, T2.apell_mo, T2.alias, T2.cve_tipo, T2.cattipo_del, T2.cve_del, T2.catdelito, T2.estatura, T2.peso, T2.talla, T2.long_pie, T2.cve_tipo_ceja, T2.descripcion_tipo_ceja, T2.cve_naltura, T2.descripcion_naltura, T2.cve_nancho, T2.descripcion_nancho, T2.cve_frente, T2.descripcion_frente, T2.cve_complexion, T2.descripcion_tipo_complexion, T2.cve_piel, T2.descripcion_piel, T2.cve_cara, T2.descripcion_cara, T2.cve_color_ojos, T2.descripcion_color_ojos, T2.cve_tipo_ojos, T2.descripcion_tipo_ojos, T2.cve_tipo_labios, T2.descripcion_tipo_labios, T2.cve_tipo_boca, T2.descripcion_tipo_boca, T2.cve_nariz, T2.descripcion_nariz, T2.cve_base, T2.descripcion_base, T2.cve_orejas, T2.descripcion_orejas, T2.cve_menton, T2.descripcion_menton, T2.cve_tipo_cab, T2.descripcion_tipo_cabello, T2.cve_color_cab, T2.descripcion_color_cabello, T2.cve_helix, T2.descripcion_helix, T2.cve_lobulo, T2.descripcion_lobulo, T2.cve_trago, T2.descripcion_trago, T2.cve_antitrago, T2.descripcion_antitrago, T2.cve_color_cejas, T2.descripcion_color_cejas, T2.cve_inclinacion, T2.descripcion_inclinacion, T2.sen_part, T2.fecha_actualizacion
from (
	SELECT max(fec_ing) fec_ing, id_persona
	FROM penitenciario.agresores_sexuales_aux 
	where id_persona is not null and fec_ing is not null
	group by id_persona 
) T1
left join penitenciario.agresores_sexuales_aux T2
on T1.fec_ing = T2.fec_ing and T1.id_persona=T2.id_persona
union 
select * from penitenciario.agresores_sexuales_aux T3
where T3.id_persona is null""")

# Conexion al sftp
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('10.250.108.45',7977,'agresores_sexuales','gTy6rodCpG')
sftp = client.open_sftp()
lt = []
pathSftpFile = 'upload/'
a = sftp.listdir(pathSftpFile)

d = [i for i in a if (i.find("jpg") >= 0)]

if len(d)>0:
    csv_output=""
    for i in d:
        # Se copia el arhivo del sftp al local
        src_images = pathSftpFile + i
        dst_local = '/home/norberto/agresores/fotos_agresores/' + i
        sftp.get(src_images, dst_local)
        #print(dst_local)
        foto = open(dst_local,'rb').read()
        foto_encode = base64.encodebytes(foto)
        data = dst_local.split('/')[6].split('_')
        datos = (foto_encode)
        p,r,a=data[0],data[1],data[2][:-4]
        print(p,r,a)
        query="UPDATE penitenciario."+table+" set foto_frente = %s where partida = %s and recl=%s and anoi=%s"  
        conn.execute(query, (datos, p, r, a))

else:
    print("NO HAN SUBIDO EL ARCHIVO DEL: "+ayer)

conn.close()

