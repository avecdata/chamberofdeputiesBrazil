import urllib
import argparse
import os.path
import xml.etree.ElementTree as ET
import urllib.request
import urllib.parse
import pickle as pkl
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
import mysqldb

sessoesreunioes_url = ("http://www.camara.gov.br/sitcamaraws/SessoesReunioes.asmx/"
                    "ListarDiscursosPlenario?dataIni=16/11/2016&dataFim=16/11/2016&codigoSessao=&parteNomeParlamentar=&siglaPartido=&siglaUF=")
                    
                    
sessoesreunioes = []
fasesSessao = []
discursosSessao = []
with urllib.request.urlopen(sessoesreunioes_url) as res:
    data = ET.fromstring(res.read())
    for item in data:
        Scodigo = item.find('codigo').text,
        data = item.find('data').text,
        numero =    item.find('numero').text,
        tipo =    item.find('tipo').text
        sessoesreunioes.append([Scodigo, data, numero, tipo])
        for gbn in item.findall('fasesSessao'):
            for fs in gbn.findall('faseSessao'):
                Fcodigo = fs.find('codigo').text,
                descricao = fs.find('descricao').text
                fasesSessao.append([Scodigo,Fcodigo, descricao])
                for ds in fs.findall('discursos'):
                    for discurso in ds.findall('discurso'):
                        for ora in discurso.findall('orador'):
                            numero = ora.find('numero').text,
                            nome = ora.find('nome').text
                        horaInicioDiscurso = discurso.find('horaInicioDiscurso').text,
                        txtIndexacao = discurso.find('txtIndexacao').text,
                        numeroQuarto = discurso.find('numeroQuarto').text,                        
                        numeroInsercao = discurso.find('numeroInsercao').text,
                        sumario = discurso.find('sumario').text
                        discursosSessao.append([Scodigo,Fcodigo, numero, nome, horaInicioDiscurso, txtIndexacao, numeroQuarto, numeroInsercao, sumario])

sessoesreunioes_df = pd.DataFrame(sessoesreunioes, columns = ["Scodigo", "data", "numero", "tipo"])
fasesSessao_df = pd.DataFrame(fasesSessao, columns = ["Scodigo", "Fcodigo", "descricao"])
discursosSessao_df = pd.DataFrame(discursosSessao, columns = ["Scodigo","Fcodigo", "numero", "nome", "horaInicioDiscurso", "txtIndexacao", "numeroQuarto", "numeroInsercao", "sumario"])


ListarPresencasDia_url = ("http://www.camara.leg.br/SitCamaraWS/sessoesreunioes.asmx/"
                    "ListarPresencasDia?data=23/11/2016&numLegislatura=&numMatriculaParlamentar=&siglaPartido=&siglaUF=")

ListarPresencasDia = []
with urllib.request.urlopen(ListarPresencasDia_url) as res:
    data = ET.fromstring(res.read())

for item in data:
    for par in item:
        carteiraParlamentar = par.find('carteiraParlamentar').text,
        descricaoFrequenciaDia = par.find('descricaoFrequenciaDia').text,
        justificativa = par.find('justificativa').text,
        presencaExterna = par.find('presencaExterna').text,
        for se in par:
            for sed in se:
                inicio = sed.find('inicio').text,
                descricao = sed.find('descricao').text,
                frequencia = sed.find('frequencia').text
                ListarPresencasDia.append([carteiraParlamentar, descricaoFrequenciaDia, justificativa, presencaExterna, inicio, descricao, frequencia])
                
ListarPresencasDia_df = pd.DataFrame(ListarPresencasDia, columns=["carteiraParlamentar", "descricaoFrequenciaDia", "justificativa", "presencaExterna", "inicio", "descricao", "frequencia"])





                    
