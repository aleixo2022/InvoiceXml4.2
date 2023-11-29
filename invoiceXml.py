import os
import time
from datetime import datetime
from glob import glob
import lxml
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import ctypes


''''
instalar dependencias cx_freeze
pip install cx-Freeze
devlopment:cxfreeze ./invoiceXml.py
production:python ./setup.py build

ou 
python setup.py build
'''

inicio = time.time()

# limpando pasta com arquivos desnecessários
Caminho_xml_vendas  = 'C:/ETL_XML/xml_vendas'      
Caminho_Arquivos_invalidos = 'C:\ETL_XML\Arquivos_invalidos'

for file in os.listdir(Caminho_xml_vendas):
    if 'can' in file:
        locationComplete= os.path.join(Caminho_xml_vendas,file)
        newLocation = os.path.join(Caminho_Arquivos_invalidos, file)
        os.rename(locationComplete, newLocation)
    if not file.endswith('.xml'):
        locationComplete= os.path.join(Caminho_xml_vendas,file)
        newLocation = os.path.join(Caminho_Arquivos_invalidos, file)
        os.rename(locationComplete, newLocation)      

Caminho_Arquivos_invalidos = 'C:\ETL_XML\Arquivos_invalidos'
fileInvalidos = len(Caminho_Arquivos_invalidos)  
mensagem = f'**** ATENÇÃO ****\nFoi localizado {fileInvalidos} arquivos invalidos. Pressione OK para continuar o processamento.'
ctypes.windll.user32.MessageBoxW(0, mensagem, 'InvoiceXml - Message', 0)       
            

# substituir arquivo
alter = '''
<detPag>
<tPag>99</tPag>
</detPag>
'''
archive = sorted(glob(r'C:/ETL_XML/xml_vendas/*.xml'))
lenFile = os.listdir('C:/ETL_XML/xml_vendas')
file_xml_vendas = len(lenFile)
 
totalFile = 0
# listando e contando arquivos xml
for i in archive:
    totalFile += 1

# apagando dados da pasta para alocar novos dados
# filelist = glob(r'C:/xml_vendas/*.xml')
# for f in filelist:
#     os.remove(f)


# função prinicipal para processar dados e alterar xml
def updateNF():
    # sub função que processa xml a xml
    def process_nf(nf, filename):

        with open(nf, 'r') as f:  # lendo arquivos
            file = f.read()
        # alterando valores no XML
        soup = BeautifulSoup(file, 'xml')
        
        # fazendo backup dos arquivos XML intactos
        with open('C:/ETL_XML/xml_backup_alterados/venda_'+filename, 'w') as f:
            f.write(soup.prettify(formatter=None))
        # cnpj = soup.emit.find_all('CNPJ')
        # busc = soup.emit.find_all('CNPJ', string='36770176000170')
        # if busc:
        #     for i in cnpj:
        #         i.string = '36770176000270'
        # else:
        #     for i in cnpj:
        #         i.string = '17407833000274'

        # ean = soup.find_all('cEAN') informar todos os EAN
        tPagf = soup.find_all('tPag')
        if tPagf:
            # print("existe")
            soup.pag.tPag.string = '99'
        else:
            npag = soup.find_all('pag')
            for i in npag:
                i.string = "<detPag><tPag>99</tPag></detPag>"

        # print(soup.pag.prettify(formatter=None))
        # verificando cada SKU existente
        skuExist = soup.find_all('cProd')
        for sku in skuExist:
            ean = sku.find_next("cEAN")
            add = '00001'
            ean.string = (sku.string+add).replace(' ','')

        # local de processamento principal do arquivo
        dcnpj = soup.dest.find_all('CNPJ')
        # fazendo backup dos alterados
        with open('C:/ETL_XML/xml_backup_alterados/backup_'+filename, 'w') as f:
            f.write(soup.prettify(formatter=None))

        # salva o valor alterado no arquivo
        '''
        Se destinatario possui CNPJ então salva em processados caso contrario salva na pasta millennium.
        '''
        if dcnpj:
            with open('C:/ETL_XML/xml_processados/'+filename, 'w') as f:
                f.write(soup.prettify(formatter=None))
        else:
            with open('C:/ETL_XML/SYS/XML'+filename, 'w') as f:
                f.write(soup.prettify(formatter=None))
    '''
		buscar todos os arquivos a serem alterados
		'''
    file_list = sorted(glob(r'C:/ETL_XML/xml_vendas/*.xml'))

    # listando arquivos xml
    for i in file_list:         
        nf = i
        filename = os.path.basename(nf)         
        process_nf(nf, filename)
        
updateNF()
# apagando dados da pasta vendas
filelist = glob(r'C:/ETL_XML/xml_vendas/*.xml')
for f in filelist:
    os.remove(f)

# lendo arquivos na XML_processados 
lenFile = os.listdir('C:/ETL_XML/xml_processados')
file_xml_processados = len(lenFile)

# lendo arquivos na xml_backup_alterados 
lenFile = os.listdir('C:/ETL_XML/SYS')
file_SYS = len(lenFile)


fim = time.time()
with open('C:/ETL_XML/xml_processados/log.txt', 'w') as f:
    f.write(
        f"Log de execução:\nTotal de Arquivos Processados:{totalFile}\n * O tempo de Execução foi de {fim - inicio}s.\n Data processamento: {datetime.now()}")
    
mensagem = f'**** FINALIZADO LEITURA ****\n\nFoi processado {file_xml_vendas} arquivos. \n\n[ARQUIVOS ARMAZENADOS]\nxml_processados: {file_xml_processados}\nSYS: {file_SYS}'
ctypes.windll.user32.MessageBoxW(0, mensagem, 'InvoiceXml - Message', 0)
