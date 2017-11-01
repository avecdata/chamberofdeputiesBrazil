import urllib
import argparse
import os.path
import xml.etree.ElementTree as ET
import pickle as pkl
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine


"""Obtém a lista de situações para proposições."""
votoprop_url = ("http://www.camara.leg.br/SitCamaraWS/Proposicoes.asmx/"
                 "ObterVotacaoProposicao?tipo=PEC&numero=241&ano=2016")

votoprop = []
votopar = []

url = urllib.urlopen(votoprop_url)
data = ET.fromstring(url.read())
count = 0
for item in data:
    for vot in item.findall('Votacao'):
        count += 1
        for vots in vot.findall('votos'):
            for dep in vots.findall('Deputado'):
                deputado = dep.get('Nome')
                voto = dep.get('Voto')
                votoprop.append([deputado, voto, count])
        for orie in vot.findall('orientacaoBancada'):
            for ban in orie.findall('bancada'):
                partido = ban.get('Sigla')
                orientacao = dep.get('Voto')
                votopar.append([partido, orientacao, count])

votoprop_df = pd.DataFrame(votoprop, columns=["deputado", "voto", "votacao"])

votoprop_df = votoprop_df.pivot(index='deputado', columns='votacao', values='voto')
votoprop_df = votoprop_df.fillna('5')
votoprop_df.to_csv('votoprop.csv', sep=',', encoding="latin1")


votopar_df = pd.DataFrame(votopar, columns=["partido", "orientacao", "votacao"])

votopar_df = votopar_df.pivot(index='partido', columns='votacao', values='orientacao')
votopar_df = votopar_df.fillna('5')
votopar_df.to_csv('votopar.csv', sep=',', encoding="latin1")

