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

siglas_url = ("http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/"
              "ListarSiglasTipoProposicao")
siglas = []

with urllib.request.urlopen(siglas_url) as res:
    data = ET.fromstring(res.read())
    for item in data:
        tipoSigla = item.get('tipoSigla'),
        descricao = item.get('descricao'),
        ativa = item.get('ativa'),
        genero = item.get('genero')
        siglas.append([tipoSigla, descricao, ativa, genero])

siglas_df = pd.DataFrame(siglas, columns=["tipoSigla", "descricao", "ativa", "genero"])


"""Obtém a lista de situações para proposições."""
situacoes_url = ("http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/"
                 "ListarSituacoesProposicao")

situacoes = []
with urllib.request.urlopen(situacoes_url) as res:
    data = ET.fromstring(res.read())
    for item in data:
        id = item.get('id'),
        descricao = item.get('descricao'),
        ativa = item.get('ativa')
        situacoes.append([id, descricao, ativa])

situacoes_df = pd.DataFrame(situacoes, columns=["id", "descricao", "ativa"])        


def obter_tipos_autores():
    """Obtém a lista de tipos de autores das proposições."""
    tipos_url = ("http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/"
                 "ListarTiposAutores")
    tipos = []
    with urllib.request.urlopen(tipos_url) as res:
        data = ET.fromstring(res.read())
        for item in data:
            id = item.get('id'),
            descricao =  item.get('descricao')
            tipos.append([id, descricao])
    
    tipos_df = pd.DataFrame(tipos, columns=["id", "descricao"])        
    return tipos_df

tipos_df = obter_tipos_autores()


def monta_proposicao(item):
    """
    Monta as proposições recuperadas em data.
    Args:
        item (ElementTree): ElementTree de uma proposição do xml da camara.
    Return:
        prop (Proposicao): proposição.
    """
    prop_det_url = ("http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/"
                    "ObterProposicaoPorID?%s")
    prop = Proposicao(
        item.find('id').text,
        item.find('nome').text,
        item.find('numero').text,
        item.find('ano').text,
        item.find('datApresentacao').text,
        item.find('txtEmenta').text,
        item.find('txtExplicacaoEmenta').text,
        item.find('qtdAutores').text,
        item.find('indGenero').text,
        item.find('qtdOrgaosComEstado').text)

    #setando o tipo de proposicao
    tipo_prop = item.find('tipoProposicao')
    prop.set_tipo_proposicao(TipoProposicao(
        tipo_prop.find('id').text,
        tipo_prop.find('sigla').text,
        tipo_prop.find('nome').text))

    #setando o orgao numerador
    orgao_num = item.find('orgaoNumerador')
    prop.set_orgao_numerador(OrgaoNumerador(
        orgao_num.find('id').text,
        orgao_num.find('sigla').text,
        orgao_num.find('nome').text))

    #setando o regime
    regime = item.find('regime')
    prop.set_regime(Regime(
        regime.find('codRegime').text,
        regime.find('txtRegime').text))

    #setando a apreciacao
    apre = item.find('apreciacao')
    prop.set_apreciacao(Apreciacao(
        apre.find('id').text,
        apre.find('txtApreciacao').text))

    #setando autor1
    autor1 = item.find('autor1')
    prop.set_autor1(Autor(
        autor1.find('txtNomeAutor').text,
        autor1.find('idecadastro').text,
        autor1.find('codPartido').text,
        autor1.find('txtSiglaPartido').text,
        autor1.find('txtSiglaUF').text))

    #setando o último despacho
    ult_des = item.find('ultimoDespacho')
    prop.set_ultimo_despacho(UltimoDespacho(
        ult_des.find('datDespacho').text,
        ult_des.find('txtDespacho').text))

    #setando situacao
    sit = item.find('situacao')
    sit_prop = SituacaoProposicao(sit.find('id').text,
                                  sit.find('descricao').text)
    org = sit.find('orgao')
    orgao = Orgao(org.find('codOrgaoEstado').text,
                  org.find('siglaOrgaoEstado').text)
    sit_prop.set_orgao(orgao)

    principal = sit.find('principal')
    princ = {
        'cod_prop_principal':
            principal.find('codProposicaoPrincipal').text,
        'prop_principal':
            principal.find('proposicaoPrincipal').text}
    sit_prop.set_prop_principal(princ)
    prop.set_situacao(sit_prop)

    params_det = urllib.parse.urlencode({'IdProp': prop.id_})
    with urllib.request.urlopen(prop_det_url % params_det) as res_d:
        detalhes = ET.fromstring(res_d.read())
        prop.set_tema(detalhes.find('tema').text)
        prop.set_indexacao(
            detalhes.find('Indexacao').text.split(','))
        prop.set_link_inteiro_teor(
            detalhes.find('LinkInteiroTeor').text)
        apensadas = detalhes.find('apensadas')
        for apensada in apensadas:
            apens = (apensada.find('nomeProposicao').text,\
                 apensada.find('codProposicao').text)
            prop.add_apensada(apens)

    return prop

def obter_proposicoes(sigla, anos):
    """Obtém a lista de proposições que satisfaçam os argumentos.
    Args:
        sigla (str) - Padrão 'PL'
        anos (list) - Lista dos anos. Padrão [2011].
        apensadas (boolean) - Se deve ou não buscar as proposições apensadas.
        siglas (list) - lista dos tipos para buscas as proposições apensadas.
    """
    prop_url = ("http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/"
                "ListarProposicoes?numero=&datApresentacaoIni=&"
                "datApresentacaoFim=&idTipoAutor=&parteNomeAutor=&"
                "siglaPartidoAutor=&siglaUFAutor=&generoAutor=&"
                "codEstado=&codOrgaoEstado=&emTramitacao=&%s")
                
    props = []
    numeros = []
    params = urllib.parse.urlencode({'sigla': 'PL', 'ano': 2016})
    with urllib.request.urlopen(prop_url % params) as res:
        data = ET.fromstring(res.read())
        for item in data:
            id = item.find('id').text,
            nome = item.find('nome').text,
            numero = item.find('numero').text,
            ano = item.find('ano').text
            for tipo in item.findall('tipoProposicao'):
                tiposigla = tipo.find('sigla').text
            for orgao in item.findall('orgaoNumerador'):
                orgaosigla = orgao.find('sigla').text
            datApresentacao = item.find('datApresentacao').text,
            txtEmenta = item.find('txtEmenta').text
            for regime in item.findall('regime'):
                codRegime = regime.find('codRegime').text ,           
                txtRegime = regime.find('txtRegime').text
            for apreciacao in item.findall('apreciacao'):
                txtApreciacao = apreciacao.find('txtApreciacao').text
            for autor1 in item.findall('autor1'):
                txtNomeAutor = autor1.find('txtNomeAutor').text            
                idecadastro = autor1.find('idecadastro').text
                codPartido = autor1.find('codPartido').text            
                txtSiglaPartido = autor1.find('txtSiglaPartido').text
                txtSiglaUF = autor1.find('txtSiglaUF').text            
            for ultimoDespacho in item.findall('ultimoDespacho'):
                datDespacho = ultimoDespacho.find('datDespacho').text,                
                txtDespacho = ultimoDespacho.find('txtDespacho').text
            for situacao in item.findall('situacao'):
                situacaodescricao = situacao.find('descricao').text
                for orgao in situacao.findall('orgao'):
                    siglaOrgaoEstado = orgao.find('siglaOrgaoEstado').text  
                for principal in situacao.findall('principal'):
                    proposicaoPrincipal = principal.find('proposicaoPrincipal').text                     
            props.append([id, nome, numero, ano, tiposigla, orgaosigla, datApresentacao, txtEmenta, codRegime, txtRegime, txtApreciacao, txtNomeAutor, 
                       idecadastro, codPartido, txtSiglaPartido, txtSiglaUF, datDespacho, txtDespacho, situacaodescricao, siglaOrgaoEstado, proposicaoPrincipal  ])

props_df = pd.DataFrame(props, columns=["id", "nome", "numero", "ano", "tiposigla", "orgaosigla", "datApresentacao", "txtEmenta", "codRegime", "txtRegime", "txtApreciacao", "txtNomeAutor", 
                       "idecadastro", "codPartido", "txtSiglaPartido", "txtSiglaUF", "datDespacho", "txtDespacho", "situacaodescricao", "siglaOrgaoEstado", "proposicaoPrincipal"])            

props_df.

def obter_apensadas(apensadas, numeros):
    """
    Método que obtem as proposições apensadas de prop.
    Args:
        apensadas (list): lista de apensadas para baixar.
        numeros (list): número das proposições que já foram baixadas.
    Return:
        props (list): lista das proposicoes apensadas de prop.
    """
    prop_url = ("http://www.camara.gov.br/SitCamaraWS/Proposicoes.asmx/"
                "ListarProposicoes?datApresentacaoIni=&"
                "datApresentacaoFim=&idTipoAutor=&parteNomeAutor=&"
                "siglaPartidoAutor=&siglaUFAutor=&generoAutor=&"
                "codEstado=&codOrgaoEstado=&emTramitacao=&%s")
    props = []
    for nome, _ in apensadas:
        #recupera a sigla para a busca
        sigla = nome.split() #a sigla mesmo é o elemento 0
        numero = sigla[1][:sigla[1].find('/')] #tudo antes da /
        ano = sigla[1][-4:] #4 últimos dígitos

        #se a proposição já foi baixada, não baixar novamente
        if numero in numeros:
            continue
        params = urllib.parse.urlencode({'sigla': sigla[0],
                                         'numero': numero,
                                         'ano': ano})
        with urllib.request.urlopen(prop_url % params) as res:
            data = ET.fromstring(res.read())
            #se não retornou nada, continua
            if data.tag == 'erro':
                continue
            prop = monta_proposicao(data.find('proposicao'))
            props.append(prop)
            print('\tAPENSADA: {} - {} {} (id: {})'.format(len(props),
                                                           prop.nome,
                                                           prop.ano,
                                                           prop.id_))
    return props
