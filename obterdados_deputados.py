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

deputado_url = ("http://www.camara.gov.br/SitCamaraWS/Deputados.asmx/"
                    "ObterDeputados")

        deputados = []
        with urllib.request.urlopen(deputado_url) as res:
            data = ET.fromstring(res.read())
            for item in data:
                idecadastro = item.find('ideCadastro').text,
                condicao = item.find('condicao').text,
                nome =    item.find('nome').text,
                nomeParlamentar =    item.find('nomeParlamentar').text,
                urlFoto =    item.find('urlFoto').text,
                sexo =    item.find('sexo').text,
                uf =    item.find('uf').text,
                partido =    item.find('partido').text,
                gabinete =    item.find('gabinete').text,
                anexo =    item.find('anexo').text,
                fone =    item.find('fone').text,
                email =   item.find('email').text
                deputados.append([idecadastro, condicao, nome, nomeParlamentar, urlFoto, sexo, uf, partido, gabinete, anexo, fone, email])

df = pd.DataFrame(deputados, columns = ["idecadastro", "condicao", "nome", "nomeParlamentar", "urlFoto", "sexo", "uf", "partido", "gabinete", "anexo", "fone", "email"])

detalhes = [] 
partido_atual = []
gabinete = []
comissoes = []
cargosComissoes = []
periodosExercicio = []
historicoNomeParlamentar = []
filiacoesPartidarias = []
historicoLider = []

for x in deputados:
        detalhes_url = ("http://www.camara.gov.br/SitCamaraWS/Deputados.asmx/"
                "ObterDetalhesDeputado?%s")

        params = urllib.parse.urlencode({
            'ideCadastro': x[0][0],
            'numLegislatura': 55})
    
        with urllib.request.urlopen(detalhes_url % params) as res:
            data = ET.fromstring(res.read())
            dep = data.find('Deputado')
            ideCadastro = dep.find('ideCadastro').text,
            email = dep.find('email').text,
            nomeProfissao = dep.find('nomeProfissao').text,
            dataNascimento = dep.find('dataNascimento').text,
            dataFalecimento = dep.find('dataFalecimento').text,
            ufRepresentacaoAtual = dep.find('ufRepresentacaoAtual').text,
            situacaoNaLegislaturaAtual = dep.find('situacaoNaLegislaturaAtual').text,
            nomeParlamentarAtual = dep.find('nomeParlamentarAtual').text,
            nomeCivil = dep.find('nomeCivil').text,
            sexo = dep.find('sexo').text
            detalhes.append([ideCadastro, email, nomeProfissao, dataNascimento, dataFalecimento, ufRepresentacaoAtual, situacaoNaLegislaturaAtual, nomeParlamentarAtual, nomeCivil, sexo])
            #montando o partidoAtual
            pta = dep.find('partidoAtual')
            idPartido = pta.find('idPartido').text,
            sigla = pta.find('sigla').text,
            nome = pta.find('nome').text
            partido_atual.append([ideCadastro, idPartido, sigla, nome])
            #montando os gabinetes
            for gbn in dep.findall('gabinete'):
                numero = gbn.find('numero').text,
                anexo = gbn.find('anexo').text,
                telefone = gbn.find('telefone').text
                gabinete.append([ideCadastro, numero, anexo, telefone])
            #montando as comissoes que o deputado participa
            for comissao in dep.find('comissoes'):
                idOrgaoLegislativoCD = comissao.find('idOrgaoLegislativoCD').text,
                siglaComissao = comissao.find('siglaComissao').text,
                nomeComissao = comissao.find('nomeComissao').text,
                condicaoMembro = comissao.find('condicaoMembro').text,
                dataEntrada = comissao.find('dataEntrada').text,
                dataSaida = comissao.find('dataSaida').text
                comissoes.append([ideCadastro, idOrgaoLegislativoCD, siglaComissao, nomeComissao, condicaoMembro, dataEntrada, dataSaida])
            #montando cargoComissoes
            cargos_comissoes = dep.find('cargosComissoes')
            for cargo in cargos_comissoes:
                idOrgaoLegislativoCD = cargo.find('idOrgaoLegislativoCD').text,
                siglaComissao = cargo.find('siglaComissao').text,
                nomeComissao = cargo.find('nomeComissao').text,
                idCargo = cargo.find('idCargo').text,
                nomeCargo = cargo.find('nomeCargo').text,
                dataEntrada = cargo.find('dataEntrada').text,
                dataSaida = cargo.find('dataSaida').text
                cargosComissoes.append([ideCadastro, idOrgaoLegislativoCD, siglaComissao, nomeComissao, idCargo, nomeCargo, dataEntrada, dataSaida ])
            #montando periodosExercicio
            periodos_exercicio = dep.find('periodosExercicio')
            for periodo in periodos_exercicio:
                siglaUFRepresentacao = periodo.find('siglaUFRepresentacao').text,
                situacaoExercicio = periodo.find('situacaoExercicio').text,
                dataInicio = periodo.find('dataInicio').text,
                dataFim = periodo.find('dataFim').text,
                idCausaFimExercicio = periodo.find('idCausaFimExercicio').text,
                descricaoCausaFimExercicio = periodo.find('descricaoCausaFimExercicio').text,
                idCadastroParlamentarAnterior = periodo.find('idCadastroParlamentarAnterior').text
                periodosExercicio.append([ideCadastro, siglaUFRepresentacao, situacaoExercicio, dataInicio, dataFim, idCausaFimExercicio, descricaoCausaFimExercicio, idCadastroParlamentarAnterior ])
            #montando historicoNomeParlamentar
            historico_nome = dep.find('historicoNomeParlamentar')
            for nome in historico_nome:
                nomeParlamentarAnterior = nome.find('nomeParlamentarAnterior').text,
                nomeParlamentaPosterior = nome.find('nomeParlamentaPosterior').text,
                dataInicioVigenciaNomePosterior = nome.find('dataInicioVigenciaNomePosterior').text
                historicoNomeParlamentar.append([ideCadastro, nomeParlamentarAnterior, nomeParlamentaPosterior, dataInicioVigenciaNomePosterior])
            #montando filiacoesPartidarias
            filiacoes = dep.find('filiacoesPartidarias')
            for filiacao in filiacoes:
                idPartidoAnterior = filiacao.find('idPartidoAnterior').text,
                siglaPartidoAnterior = filiacao.find('siglaPartidoAnterior').text,
                nomePartidoAnterior = filiacao.find('nomePartidoAnterior').text,
                idPartidoPosterior = filiacao.find('idPartidoPosterior').text,
                siglaPartidoPosterior = filiacao.find('siglaPartidoPosterior').text,
                nomePartidoPosterior = filiacao.find('nomePartidoPosterior').text,
                dataFiliacaoPartidoPosterior = filiacao.find('dataFiliacaoPartidoPosterior').text
                filiacoesPartidarias.append([ideCadastro, idPartidoAnterior, siglaPartidoAnterior, nomePartidoAnterior, idPartidoPosterior, siglaPartidoPosterior, nomePartidoPosterior, dataFiliacaoPartidoPosterior])
            #montando historicoLideranca
            historico_lider = dep.find('historicoLider')
            for lider in historico_lider:
                idHistoricoLider = lider.find('idHistoricoLider').text,
                idCargoLideranca = lider.find('idCargoLideranca').text,
                descricaoCargoLideranca = lider.find('descricaoCargoLideranca').text,
                numOrdemCargo = lider.find('numOrdemCargo').text,
                dataDesignacao = lider.find('dataDesignacao').text,
                dataTermino = lider.find('dataTermino').text,
                codigoUnidadeLideranca = lider.find('codigoUnidadeLideranca').text,
                siglaUnidadeLideranca = lider.find('siglaUnidadeLideranca').text,
                idBlocoPartido = lider.find('idBlocoPartido').text
                historicoLider.append([ideCadastro, idHistoricoLider, idCargoLideranca, descricaoCargoLideranca, numOrdemCargo, dataDesignacao, dataTermino, codigoUnidadeLideranca, siglaUnidadeLideranca, idBlocoPartido])


df_detalhes = pd.DataFrame(detalhes, columns = ["ideCadastro", "email", "nomeProfissao", "dataNascimento", "dataFalecimento", "ufRepresentacaoAtual", "situacaoNaLegislaturaAtual", "nomeParlamentarAtual", "nomeCivil", "sexo" ])
df_partidoatual = pd.DataFrame(partido_atual, columns=["ideCadastro", "idPartido", "sigla", "nome"])
df_gabinete = pd.DataFrame(gabinete, columns=["ideCadastro", "numero", "anexo", "telefone"])
df_comissoes = pd.DataFrame(comissoes , columns=["ideCadastro", "idOrgaoLegislativoCD", "siglaComissao", "nomeComissao", "condicaoMembro", "dataEntrada", "dataSaida"])
df_cargosComissoes = pd.DataFrame(cargosComissoes , columns=["ideCadastro", "idOrgaoLegislativoCD", "siglaComissao", "nomeComissao", "idCargo", "nomeCargo", "dataEntrada", "dataSaida"])
df_periodosExercicio = pd.DataFrame(periodosExercicio , columns=["ideCadastro", "siglaUFRepresentacao", "situacaoExercicio", "dataInicio", "dataFim", "idCausaFimExercicio", "descricaoCausaFimExercicio", "idCadastroParlamentarAnterior"])
df_historicoNomeParlamentar = pd.DataFrame(historicoNomeParlamentar , columns=["ideCadastro", "nomeParlamentarAnterior", "nomeParlamentaPosterior", "dataInicioVigenciaNomePosterior"])
df_filiacoesPartidarias = pd.DataFrame(filiacoesPartidarias , columns=["ideCadastro", "idPartidoAnterior", "siglaPartidoAnterior", "nomePartidoAnterior", "idPartidoPosterior", "siglaPartidoPosterior", "nomePartidoPosterior", "dataFiliacaoPartidoPosterior"])
df_historicoLider = pd.DataFrame(historicoLider , columns=["ideCadastro", "idHistoricoLider", "idCargoLideranca", "descricaoCargoLideranca", "numOrdemCargo", "dataDesignacao", "dataTermino", "codigoUnidadeLideranca", "siglaUnidadeLideranca", "idBlocoPartido"])


engine = create_engine('mysql+pymysql://root:@127.0.0.1:3306/camara?charset=utf8', echo=False)
###########
df.to_sql(name='deputado', con=engine, if_exists='replace')
df_detalhes.to_sql(name='detalhes', con=engine, if_exists='replace')
df_partidoatual.to_sql(name='partidoatual', con=engine, if_exists='replace')
df_gabinete.to_sql(name='gabinete', con=engine, if_exists='replace')
df_comissoes.to_sql(name='comissoes', con=engine, if_exists='replace')
df_cargosComissoes.to_sql(name='cargosComissoes', con=engine, if_exists='replace')
df_periodosExercicio.to_sql(name='periodosExercicio', con=engine, if_exists='replace')
df_historicoNomeParlamentar.to_sql(name='historicoNomeParlamentar', con=engine, if_exists='replace')
df_filiacoesPartidarias.to_sql(name='filiacoesPartidarias', con=engine, if_exists='replace')
df_historicoLider.to_sql(name='historicoLider', con=engine, if_exists='replace')
###########
conn.close()





