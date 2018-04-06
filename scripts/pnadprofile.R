#' package: pnad
#' title: Importar arquivos PNAD disponiveis no IBGE
#' description: Analise exploratoria
#' version: 0.0.12
#' author: Carlos Perciliano Gaudencio
#' date: 2017-09-24
#' last update: 2018-01-13
#' maintainer: perciliano@outlook.com
#' licence: free, open source
#' Library: zoo, readr, RODBC, RODBCext - https://www.tidyverse.org/, http://r4ds.had.co.nz/
#' https://ww2.ibge.gov.br/home/estatistica/populacao/trabalhoerendimento/pnad2014/microdados.shtm

getwd()     #diretorio do projeto
dir()       #listar arquivos e pastas do projeto
ls()        #listar objetos carregados em memoria

####0 - CONFIGURACAO       ####
rm(list=ls(all=TRUE))              #limpar todas variaveis e dados em memoria
cat("\014")                        #clear console, CTRL+L
options("scipen" = 8)              # mostrar ate 8 casas decimais
PROJDIR <- '/home/perciliano/PNAD' #diretorio inicial, usar para ler e gravar arquivos
setwd(PROJDIR)                     #definir diretorio de trabalho, inicial
#usar encoding=UTF-8 , para nao ocorrer erro na interpretacao de strings com acentos
source("scripts/pnadpk.R", encoding="utf-8") # funcao: pnadFiles - Apenas importa arquivos necessaios para analise - https://stackoverflow.com/questions/5031630/how-to-source-r-file-saved-using-utf-8-encoding

#---------------------------------------------------------------------------------
ANOPNAD <- c(2011,2012,2013,2014,2015)  #Ano ou lista de anos a processar e obrigatorio! ex: c(2015)
EXTPNAD <- c("\\.sas$","\\.txt$") #c("\\.sas$","\\.txt$","\\.csv$") , csv=usar existente
lRECURSIVO <- TRUE #TRUE - ler nomes de arquivos pnad em todas pastas, recursivamente - usa o primeiro de cada ano
lLABELS    <- TRUE #TRUE - Usar labels para padronizar nomes de variaveis
lREADCSV   <- FALSE #FALSE - ler CSV se existir, no lugar de TXT - tem que ter csv de entrada
lWRITEOUT  <- FALSE #TRUE - Gerar arquivos de saida, csv e sql
lREADANX   <- TRUE  #FALSE - Ler arquivos de anexos i a viii para tipo=3
lPROFILE   <- TRUE #FALSE - Gerar analise exploratoria
lREMOVENUL <- FALSE #FALSE - Remover variaveis que apenas tiverem conteudo nulo
lCOVARIA   <- FALSE #FALSE - Gerar analise de covariancias
lUNIFICAR  <- TRUE #FALSE - Gera dataframe unico de arquivos anuais
FONTEPRIN  <- 'MSSQLDB|server=localhost\\servname;database=dbxx;uid=us;pwd=@xxxxxx' #MSSQLDB, LOCALFILE, SQLITE

cat('Diretorio do projeto:',getwd(),'\n')     #diretorio do projeto
cat('Parametros: AnoPNAD=',ANOPNAD,'\nArqPNAD=',EXTPNAD,'\nRecursivo=',lRECURSIVO,'\nLerCSVsetiver=',lREADCSV,'\nFontePrincipal=',FONTEPRIN,'\nGerarCSV_SQL=',lWRITEOUT,'\nGerarTXTprofiler=',lPROFILE,'\nGerarCovariancias=',lCOVARIA,'\n')

####1 - AQUISICAO DE DADOS ####
ARQPNAD <- loadFilesN('.',lRECURSIVO,EXTPNAD,',',FONTEPRIN,'PES') # varre todos diretorios abaixo do atual para localizar csv, txt e sas

for (iDataframe in 1:length(ANOPNAD)) { #iDataframe=1

  if (exists('pnadcLayout') == FALSE || is.null(pnadcLayout)) {
    cat('Inicio',ANOPNAD[iDataframe],format(Sys.time(),"%d/%m/%Y %X"),'. Processando: pnadcLayout\n')
    #if(!("zoo" %in% (.packages()))){ library(zoo) }
    #if(!("zoo" %in% (.packages()))){ library(zoo) } #erro tenta novamente
    pnadcLayout <- pnadFiles(0, #0.SAS file IN - sasFileIN
                             ARQPNAD[ARQPNAD$yearFiles==ANOPNAD[iDataframe],])

    if (!is.data.frame(pnadcLayout)==TRUE) { #pode retornar em lista, separar
      pnadDimen <- pnadcLayout[[2]] #sobrepor dataframe de dimensoes
      pnadcLayout <- pnadcLayout[[1]] #sobrepor dataframe de layout
    } #mais de um retorno em lista, 1=data.frame(layout) e 2=data.frame(dimension)
  } # arquivo SAS com descricao de layout para leitura de dados posicionais
  else cat('Dataframe de layout ja existe! pnadcLayout \n')

  p1 <- proc.time()[3] #user, sys, elapse
  Sys.sleep(0.1) # pausar para usar todas variaveis corretamente
  cat('Pausa',proc.time()[3] - p1,'- atributos:',nrow(pnadcLayout),'\n') #exibir tempo de espera
  rm(p1)
  if (nrow(pnadcLayout)>0) {
    if (lREADANX==TRUE & any(pnadcLayout$TypeQu==3)) {
      grpOcup <- getPNADanx(ARQPNAD[ARQPNAD$yearFiles==ANOPNAD[iDataframe],],
                            pnadcLayout[pnadcLayout$TypeQu==3,])
      if (is.list(grpOcup)) {
        if (!any(is.null(grpOcup[['tpModEsp']])) & !any(is.na(grpOcup[['tpModEsp']]))) tpModEsp <- grpOcup[['tpModEsp']]
        if (!any(is.null(grpOcup[['tpAtivFis']])) & !any(is.na(grpOcup[['tpAtivFis']]))) tpAtivFis <- grpOcup[['tpAtivFis']]
        if (!any(is.null(grpOcup[['unEquiv']])) & !any(is.na(grpOcup[['unEquiv']]))) unEquiv <- grpOcup[['unEquiv']]
        if (!any(is.null(grpOcup[['codAtiv']])) & !any(is.na(grpOcup[['codAtiv']]))) codAtiv <- grpOcup[['codAtiv']]
        if (!any(is.null(grpOcup[['grpAtiv']])) & !any(is.na(grpOcup[['grpAtiv']]))) grpAtiv <- grpOcup[['grpAtiv']]
        if (!any(is.null(grpOcup[['codOcup']])) & !any(is.na(grpOcup[['codOcup']]))) codOcup <- grpOcup[['codOcup']] #split de lista para dataframe 
        if (!any(is.null(grpOcup[['grpOcup']])) & !any(is.na(grpOcup[['grpOcup']]))) grpOcup <- grpOcup[['grpOcup']] #sobrepor lista com dataframe
        else rm(grpOcup)
      } #mais de um retorno em lista
    } #lREADANX==TRUE

    if (!('Labels' %in% colnames(pnadcLayout))) lLABELS <- FALSE #Sem label volta para false, ver arquivo de labels
    if ((lREADCSV==TRUE) & file.exists(as.character(ARQPNAD[ARQPNAD$yearFiles==ANOPNAD[iDataframe],]$csvFileINOUT))) {
      cat('Carregando: pesData, origem em CSV:',lREADCSV,format(Sys.time(),"%d/%m/%Y %X"),'\n')
      if (exists('pesData') == FALSE || is.null(pesData)) { #opcional CSV no lugar do TXT, +rapido
        pesData <- pnadFiles(2, #2.CSV file IN - csvFileINOUT + csvDelimiter + pnadcLayout
                             ARQPNAD[ARQPNAD$yearFiles==ANOPNAD[iDataframe],],
                             -1L,
                             pnadcLayout)
      } # dados CSV opcional, com delimitador e dataframe de layout
    } else { #csv
      if (exists('pesData') == FALSE || is.null(pesData)) {
        cat('Carregando: pesData, origem em TXT:',!lREADCSV,format(Sys.time(),"%d/%m/%Y %X"),'\n')
        pesData <- pnadFiles(1, #1.TXT file IN - txtFileIN + csvDelimiter + pnadcLayout
                             ARQPNAD[ARQPNAD$yearFiles==ANOPNAD[iDataframe],],
                             -1L,
                             pnadcLayout)
      } # arquivo TXT com dados posicionais, depende da definicao de layout e volume (-1L total)  
    } #txt
  } # if layout >0
  else cat('Dataframe de layout esta vazio ou invalido!\n')
  p1 <- proc.time()[3] #user, sys, elapse
  Sys.sleep(0.1) # pausar para usar todas variaveis corretamente
  cat('Pausa',proc.time()[3] - p1,'- atributos:',ncol(pesData),'\n') #exibir tempo de espera
  rm(p1)
  
  cat('Gerando: sqlData',format(Sys.time(),"%d/%m/%Y %X"),'\n')
  if (exists('pesData') && (nrow(pesData)>0)) {
    if (exists('sqlData') == FALSE || is.null(sqlData)) {
      sqlData <- dfQueryCreate(ANOPNAD[iDataframe],
                               pesData[1,],     #uma linha eh o suficiente
                               paste0(ifelse((startsWith(FONTEPRIN,'MSSQLDB')==TRUE),'TB_PES_','pes'),ANOPNAD[iDataframe]), #nome fisico da tabela
                               FONTEPRIN,
                               pnadcLayout[,c('Labels','Names','Widths')],
                               TRUE, #drop
                               TRUE, #create, insert
                               TRUE) #select
      #sqlData <- pnadFiles(3, ARQPNAD[ARQPNAD$yearFiles==ANOPNAD[iDataframe],], -1L, list(pesData,pnadcLayout))
    } # dados SQL comandos para uso em bases como SQLite, depende de dataframe com ano
  } # if sqlData >0
  else cat('Dataframe de dados esta vazio ou invalido!\n')
  
  if (lWRITEOUT==TRUE) {
    ####2 - EXTRACAO DE DADOS    ####
    cat('Gravando: pesData',as.character(ARQPNAD[ARQPNAD$yearFiles==ANOPNAD[iDataframe],]$csvFileINOUT),format(Sys.time(),"%d/%m/%Y %X"),'\n')
    write.table(pesData, file=as.character(ARQPNAD[ARQPNAD$yearFiles==ANOPNAD[iDataframe],]$csvFileINOUT), row.names=FALSE, col.names=TRUE, sep=ARQPNAD[ARQPNAD$yearFiles==ANOPNAD[iDataframe],]$csvDelimiter) #csv para uso em outras ferramentas
    if (lUNIFICAR==FALSE) {
      cat('Gravando: sqlData',as.character(ARQPNAD[ARQPNAD$yearFiles==ANOPNAD[iDataframe],]$sqlFileOUT), row.names=FALSE, col.names=TRUE, sep=ARQPNAD[ARQPNAD$yearFiles==ANOPNAD[iDataframe],]$csvDelimiter,format(Sys.time(),"%d/%m/%Y %X"),'\n')
      write.table(sqlData$Querie, file=as.character(ARQPNAD[ARQPNAD$yearFiles==ANOPNAD[iDataframe],]$sqlFileOUT), row.names=FALSE, col.names=FALSE, sep="\t", quote = FALSE) #sql para uso em bases
    } #unificado gera depois
  } #if lWRITEOUT
  
  if (lPROFILE==TRUE) {
    cat('Analise exploratoria de todos dataset de dados:',ANOPNAD[iDataframe],format(Sys.time(),"%d/%m/%Y %X"),'\n')
    ####3 - ANALISE EXPLORATORIA ####
    lstNulls <- pnadProfiler(pesData,ARQPNAD[ARQPNAD$yearFiles==ANOPNAD[iDataframe],]$prfAnalysis,pnadcLayout)
    if (is.list(lstNulls)) {
      pnadcLayout <- lstNulls[[2]] #sobrepor dataframe enviado
      pnadcLayout[is.na(pnadcLayout$Class),]$Class <- pnadcLayout[is.na(pnadcLayout$Class),]$ClassR
      lstNulls <- lstNulls[[1]] #sobrepor lista de variaveis sem conteudo
    } #mais de um retorno em lista, 1=c() e 2=data.frame()
    #print(lstNulls)

    if (lREMOVENUL==TRUE & length(lstNulls)>0) { #retirar variaveis sem conteudo
      cat('Removendo atributos nulos:',lstNulls,'\n')
      pesData <- pesData[ , !(names(pesData) %in% lstNulls)]
    } else { #if
      cat('Atributos apenas com nulos:',unlist(lstNulls),'\n')
    } #else nulos attr

    if (lCOVARIA==TRUE) {
      cat('Correlacoes, Coeficiente de Determinacao e Covariancao de todos dataset:',ANOPNAD[iDataframe],format(Sys.time(),"%d/%m/%Y %X"),'\n')
      prNumerics <- sapply(pesData, is.numeric)
      Covariancia      <- data.frame(cov(pesData[,prNumerics])) #matriz: covariancia
      CoefCorrelacao   <- data.frame(cor(pesData[,prNumerics])) #matriz: coeficiente de correlacao
      CoefDeterminacao <- data.frame(round(cor(pesData[,prNumerics])^2,digits = 10)) #matriz: coeficiente de determinacao

      cat('Renomeando DF de Convariancia...',ANOPNAD[iDataframe],format(Sys.time(),"%d/%m/%Y %X"),'\n')
      assign(paste('covar',ANOPNAD[iDataframe],sep = ''),Covariancia)
      assign(paste('coefcor',ANOPNAD[iDataframe],sep = ''),CoefCorrelacao)
      assign(paste('coefdet',ANOPNAD[iDataframe],sep = ''),CoefDeterminacao)

      if (exists('Covariancia')) rm(Covariancia)
      if (exists('CoefCorrelacao')) rm(CoefCorrelacao)
      if (exists('CoefDeterminacao')) rm(CoefDeterminacao)
    } #if lCOVARIA
  } #if lPROFILE
  
  #renomeia dataframes para nao sobrepor
  cat('Renomeando todos dataframes: ',ANOPNAD[iDataframe],format(Sys.time(),"%d/%m/%Y %X"),'\n')
  if (lUNIFICAR==TRUE) {
    if (!exists(x = "sqlDF")) assign('sqlDF',sqlData) #criar df unificado
    else sqlDF <- rbind(sqlDF,sqlData) #atualizar df
    
    if (!exists(x = "layDF")) assign('layDF',pnadcLayout)
    else layDF <- rbind(layDF,pnadcLayout)
    
    if (exists("pnadDimen")) {
      if (!exists(x = "dimDF")) assign('dimDF',pnadDimen)
      else dimDF <- rbind(dimDF,pnadDimen)
    } # pnadDimen
  } else {
    assign(paste('pnLay',ANOPNAD[iDataframe],sep = ''),pnadcLayout)
    assign(paste('sqlDF',ANOPNAD[iDataframe],sep = ''),sqlData)
    if (exists("pnadDimen")) { assign(paste('pnDim',ANOPNAD[iDataframe],sep = ''),pnadDimen) }
  } #else apenas renomeia para nao sobrepor
  assign(paste('pesDF',ANOPNAD[iDataframe],sep = ''),pesData)

  if (exists('pnadcLayout')) rm(pnadcLayout) #&& is.data.frame(get(pnadcLayout))
  if (exists('pnadDimen')) rm(pnadDimen)
  if (exists('pesData')) {cat('atributos:',ncol(pesData),'\n'); rm(pesData)}
  if (exists('sqlData')) rm(sqlData)
  
  cat('Fim',ANOPNAD[iDataframe],format(Sys.time(),"%d/%m/%Y %X"),'\n')
} #for - varre anos a processar gera dataframes, e analise exploratoria
rm(iDataframe,lstNulls)
#if(("zoo" %in% (.packages()))){ detach("package:zoo", unload=TRUE) }
#View(ARQPNAD)
#View(dimDF)
#View(layDF)
#View(sqlDF)
#View(pesDF2015[1:100,])
#----[DUPLICIDADE DE CAMPOS]--------------------------------------------------------------------

layDF$WidtDesc <- paste0(layDF$Widths,',',layDF$Description) #para identificar variaveis iguais na descricao e tamanho
colDupls <- layDF[duplicated(layDF$Names) | duplicated(layDF$Names,fromLast = TRUE) | duplicated(layDF$Description),]

#----[ESTATISTICA]------------------------------------------------------------------------------
if (lWRITEOUT==TRUE) {
  if (exists('sqlDF')) {
    cat('Gravando: sqlDF',format(Sys.time(),"%d/%m/%Y %X"),'\n')
    write.table(sqlDF$Querie, file=as.character(paste0(PROJDIR,'/output/PNADqueries.sql')), row.names=FALSE, col.names=FALSE, sep="\t", quote = FALSE) #sql para uso em bases
  } #sqlDF
  if (exists('layDF')) {
    cat('Gravando: layouts',format(Sys.time(),"%d/%m/%Y %X"),'\n')
    write.table(layDF, file=as.character(paste0(PROJDIR,'/output/PNADlayouts.csv')), row.names=FALSE, col.names=TRUE, sep=",", quote = TRUE) #csv de layout geral com estatistica
  } #layDF
  if (exists('dimDF')) {
    cat('Gravando: Dimensoes',format(Sys.time(),"%d/%m/%Y %X"),'\n')
    write.table(dimDF, file=as.character(paste0(PROJDIR,'/output/PNADdimensoes.csv')), row.names=FALSE, col.names=TRUE, sep=",", quote = TRUE) #csv de todas dimensoes
  } #dimDF
  if (exists('ARQPNAD')) {
    cat('Gravando: Arquivos processados',format(Sys.time(),"%d/%m/%Y %X"),'\n')
    write.table(ARQPNAD, file=as.character(paste0(PROJDIR,'/output/PNADarquivos.csv')), row.names=FALSE, col.names=TRUE, sep=",", quote = TRUE) #csv de todas dimensoes
  } #ARQPNAD
  if (exists('tpModEsp')) {
    cat('Gravando: tpModEsp',format(Sys.time(),"%d/%m/%Y %X"),'\n')
    write.table(tpModEsp, file=as.character(paste0(PROJDIR,'/output/tpModEsp.csv')), row.names=FALSE, col.names=TRUE, sep=",", quote = TRUE) #csv de todas dimensoes
  } #tpModEsp
  if (exists('tpAtivFis')) {
    cat('Gravando: tpAtivFis',format(Sys.time(),"%d/%m/%Y %X"),'\n')
    write.table(tpAtivFis, file=as.character(paste0(PROJDIR,'/output/tpAtivFis.csv')), row.names=FALSE, col.names=TRUE, sep=",", quote = TRUE) #csv de todas dimensoes
  } #tpAtivFis
  if (exists('unEquiv')) {
    cat('Gravando: unEquiv',format(Sys.time(),"%d/%m/%Y %X"),'\n')
    write.table(unEquiv, file=as.character(paste0(PROJDIR,'/output/unEquiv.csv')), row.names=FALSE, col.names=TRUE, sep=",", quote = TRUE) #csv de todas dimensoes
  } #unEquiv
  if (exists('codAtiv')) {
    cat('Gravando: codAtiv',format(Sys.time(),"%d/%m/%Y %X"),'\n')
    write.table(codAtiv, file=as.character(paste0(PROJDIR,'/output/codAtiv.csv')), row.names=FALSE, col.names=TRUE, sep=",", quote = TRUE) #csv de todas dimensoes
  } #codAtiv
  if (exists('grpAtiv')) {
    cat('Gravando: grpAtiv',format(Sys.time(),"%d/%m/%Y %X"),'\n')
    write.table(grpAtiv, file=as.character(paste0(PROJDIR,'/output/grpAtiv.csv')), row.names=FALSE, col.names=TRUE, sep=",", quote = TRUE) #csv de todas dimensoes
  } #grpAtiv
  if (exists('codOcup')) {
    cat('Gravando: codOcup',format(Sys.time(),"%d/%m/%Y %X"),'\n')
    write.table(codOcup, file=as.character(paste0(PROJDIR,'/output/codOcup.csv')), row.names=FALSE, col.names=TRUE, sep=",", quote = TRUE) #csv de todas dimensoes
  } #codOcup
  if (exists('grpOcup')) {
    cat('Gravando: grpOcup',format(Sys.time(),"%d/%m/%Y %X"),'\n')
    write.table(grpOcup, file=as.character(paste0(PROJDIR,'/output/grpOcup.csv')), row.names=FALSE, col.names=TRUE, sep=",", quote = TRUE) #csv de todas dimensoes
  } #grpOcup  
}

#----[MSSQLDB-SETs]------------------------------------------------------------------------------
pnPriSource <- strsplit(FONTEPRIN, split = "|",fixed = TRUE)[[1]]
pnCnURL <- (ifelse(length(pnPriSource)>1,pnPriSource[2],''))
pnPriSource <- (ifelse('RODBC' %in% installed.packages(),pnPriSource[1],'')) #so funciona se tiver RODBC instalado
if (pnPriSource=='MSSQLDB' & !(exists('pnCnURL') && pnCnURL!='')) {
  conn <- RODBC::odbcDriverConnect(paste0('driver={SQL Server};',pnCnURL))

  if (exists('layDF')) {
    sqlInsDF <- setSQLinsert(layDF[,c('ID','Year','Names','CodeQu','Labels','TypeQu','TableQu','Description','Class','Start','End','Widths')],
                             900,
                             "INSERT INTO [dbo].[TB_LAYO] (ID_LAYO,CD_YEAR,CD_NAME,CD_PERG,CD_LABE,TP_PERG,CD_TABL,NM_DESC,VL_CLAS,NU_STAR,NU_END,NU_WIDT) VALUES ",
                             paste0('DELETE [dbo].[TB_LAYO] WHERE [CD_YEAR] ',
                                    ifelse(length(unique(layDF[!is.na(layDF$Year),]$Year))==1,'= ','IN ('),
                                    paste(unique(layDF[!is.na(layDF$Year),]$Year),collapse = ','),
                                    ifelse(length(unique(layDF[!is.na(layDF$Year),]$Year))==1,'',')'))
                             )
    for (iSQL in 1:nrow(sqlInsDF)) {
      sql_statement <- sqlInsDF[iSQL,]$QuerieSQL #sql_statement <- 'DELETE [dbo].[TB_LAYO] WHERE [CD_YEAR]=2015'
      RODBC::sqlQuery(conn,sql_statement)
    } #for
  } #layDF

  if (exists('dimDF')) {
    sqlInsDF <- setSQLinsert(dimDF[nchar(dimDF$Codes) < 31 & !is.na(dimDF$Codes),c('Year','Names','Codes','Labels')],
                             900,
                             "INSERT INTO [dbo].[TB_DIME] (CD_YEAR,CD_NAME,CD_ATTR,DS_ATTR) VALUES ",
                             paste0('DELETE [dbo].[TB_DIME] WHERE [CD_YEAR] ',
                                    ifelse(length(unique(dimDF[!is.na(dimDF$Year),]$Year))==1,'= ','IN ('),
                                    paste(unique(dimDF[!is.na(dimDF$Year),]$Year),collapse = ','),
                                    ifelse(length(unique(dimDF[!is.na(dimDF$Year),]$Year))==1,'',')'))
                             )
    for (iSQL in 1:nrow(sqlInsDF)) {
      sql_statement <- sqlInsDF[iSQL,]$QuerieSQL
      RODBC::sqlQuery(conn,sql_statement)
    } #for
  } #dimDF

  close(conn)
  rm(conn,sql_statement,iSQL,sqlInsDF)
} #MSSQLDB if

if (exists('FONTEPRIN') && FONTEPRIN!='') {
  if (exists('pnLabels')) {
    msgList <- sendPNADdb(pnLabels[,c('Names','Labels','Description','Class')],
                          FONTEPRIN,
                          900,
                          data.frame(ID=9,
                                     Year=2015, #REVISAR ANO
                                     Querie="INSERT INTO [dbo].[TB_LABE] (CD_NAME,CD_LABE,NM_DESC,VL_CLAS) VALUES (?, ?, ?, ?)"),
                          'DELETE [dbo].[TB_LABE]') #FULL DELETE
  } #pnLabels

  if (exists('pesDF2015') & exists('sqlDF')) { #sempre revisar o nome do dataframe e ano da transacao
    msgList <- sendPNADdb(pesDF2015, #REVISAR, pesDF2015[1:100,]
                          FONTEPRIN,
                          500,
                          sqlDF[sqlDF$Year==2015,], #REVISAR ANO
                          '')
  } #pesDF????

  #CUIDADO:apaga e cria tabela na base de dados, verificar os nomes fisico antes de aplicar
  if (exists('tpModEsp')) {
    tmpDF <- tpModEsp
    colnames(tmpDF) <- c('CD_MOD_ESP','DS_MOD_ESP') #View(tmpDF)
    tmpLay <- data.frame(Labels=c('CD_MOD_ESP','DS_MOD_ESP'), Names=c('CD_MOD_ESP','DS_MOD_ESP'), Widths=c(5,200)) #View(tmpLay)
    tmpSQLs <- dfQueryCreate(0,
                             tmpDF[1,],    #uma linha eh o suficiente
                             'TB_MOD_ESP', #nome fisico da tabela
                             FONTEPRIN,
                             tmpLay,
                             TRUE, #drop
                             TRUE, #create, insert
                             TRUE) #select
    #View(tmpSQLs)
    msgList <- sendPNADdb(tmpDF,
                          FONTEPRIN, #base de dados
                          500,
                          tmpSQLs,
                          '') #sem delete, usa drop

    rm(tmpDF,tmpLay,tmpSQLs)
  } #tpModEsp

  if (exists('tpAtivFis')) {
    tmpDF <- tpAtivFis
    colnames(tmpDF) <- c('CD_TIP_ATIV','DS_TIP_ATIV') #View(tmpDF)
    tmpLay <- data.frame(Labels=c('CD_TIP_ATIV','DS_TIP_ATIV'), Names=c('CD_TIP_ATIV','DS_TIP_ATIV'), Widths=c(5,200)) #View(tmpLay)
    tmpSQLs <- dfQueryCreate(0,
                             tmpDF[1,],     #uma linha eh o suficiente
                             'TB_TIP_ATIV', #nome fisico da tabela
                             FONTEPRIN,
                             tmpLay,
                             TRUE, #drop
                             TRUE, #create, insert
                             TRUE) #select
    msgList <- sendPNADdb(tmpDF,
                          FONTEPRIN, #base de dados
                          500,
                          tmpSQLs,
                          '') #sem delete, usa drop

    rm(tmpDF,tmpLay,tmpSQLs)
  } #tpAtivFis

  if (exists('unEquiv')) {
    tmpDF <- unEquiv
    colnames(tmpDF) <- c('DS_UN','EQV_M2') #View(tmpDF)
    tmpLay <- data.frame(Labels=c('DS_UN','EQV_M2'), Names=c('DS_UN','EQV_M2'), Widths=c(100,5)) #View(tmpLay)
    tmpSQLs <- dfQueryCreate(0,
                             tmpDF[1,],     #uma linha eh o suficiente
                             'TB_UN_EQV', #nome fisico da tabela
                             FONTEPRIN,
                             tmpLay,
                             TRUE, #drop
                             TRUE, #create, insert
                             TRUE) #select
    msgList <- sendPNADdb(tmpDF,
                          FONTEPRIN, #base de dados
                          500,
                          tmpSQLs,
                          '') #sem delete, usa drop

    rm(tmpDF,tmpLay,tmpSQLs)
  } #unEquiv

  if (exists('codAtiv')) {
    tmpDF <- codAtiv
    colnames(tmpDF) <- c('CD_ATIV_FISI','DS_ATIV_FISI') #View(tmpDF)
    tmpLay <- data.frame(Labels=c('CD_ATIV_FISI','DS_ATIV_FISI'), Names=c('CD_ATIV_FISI','DS_ATIV_FISI'), Widths=c(5,100)) #View(tmpLay)
    tmpSQLs <- dfQueryCreate(0,
                             tmpDF[1,],     #uma linha eh o suficiente
                             'TB_ATIV_FISI', #nome fisico da tabela
                             FONTEPRIN,
                             tmpLay,
                             TRUE, #drop
                             TRUE, #create, insert
                             TRUE) #select
    msgList <- sendPNADdb(tmpDF,
                          FONTEPRIN, #base de dados
                          500,
                          tmpSQLs,
                          '') #sem delete, usa drop

    rm(tmpDF,tmpLay,tmpSQLs)
  } #codAtiv

  if (exists('grpAtiv')) {
    tmpDF <- grpAtiv
    colnames(tmpDF) <- c('CD_GRP_ATIV','DS_GRP_ATIV') #View(tmpDF)
    tmpLay <- data.frame(Labels=c('CD_GRP_ATIV','DS_GRP_ATIV'), Names=c('CD_GRP_ATIV','DS_GRP_ATIV'), Widths=c(5,200)) #View(tmpLay)
    tmpSQLs <- dfQueryCreate(0,
                             tmpDF[1,],     #uma linha eh o suficiente
                             'TB_GRP_ATIV', #nome fisico da tabela
                             FONTEPRIN,
                             tmpLay,
                             TRUE, #drop
                             TRUE, #create, insert
                             TRUE) #select
    msgList <- sendPNADdb(tmpDF,
                          FONTEPRIN, #base de dados
                          500,
                          tmpSQLs,
                          '') #sem delete, usa drop

    rm(tmpDF,tmpLay,tmpSQLs)
  } #grpAtiv

  if (exists('codOcup')) {
    tmpDF <- codOcup
    colnames(tmpDF) <- c('CD_OCUP','DS_OCUP') #View(tmpDF)
    tmpLay <- data.frame(Labels=c('CD_OCUP','DS_OCUP'), Names=c('CD_OCUP','DS_OCUP'), Widths=c(5,200)) #View(tmpLay)
    tmpSQLs <- dfQueryCreate(0,
                             tmpDF[1,],     #uma linha eh o suficiente
                             'TB_OCUP', #nome fisico da tabela
                             FONTEPRIN,
                             tmpLay,
                             TRUE, #drop
                             TRUE, #create, insert
                             TRUE) #select
    msgList <- sendPNADdb(tmpDF,
                          FONTEPRIN, #base de dados
                          500,
                          tmpSQLs,
                          '') #sem delete, usa drop

    rm(tmpDF,tmpLay,tmpSQLs)
  } #codOcup

  if (exists('grpOcup')) {
    tmpDF <- grpOcup
    colnames(tmpDF) <- c('CD_OCUP','DS_OCUP') #View(tmpDF)
    tmpLay <- data.frame(Labels=c('CD_OCUP','DS_OCUP'), Names=c('CD_OCUP','DS_OCUP'), Widths=c(5,200)) #View(tmpLay)
    tmpSQLs <- dfQueryCreate(0,
                             tmpDF[1,],     #uma linha eh o suficiente
                             'TB_GRP_OCUP', #nome fisico da tabela
                             FONTEPRIN,
                             tmpLay,
                             TRUE, #drop
                             TRUE, #create, insert
                             TRUE) #select
    msgList <- sendPNADdb(tmpDF,
                          FONTEPRIN, #base de dados
                          500,
                          tmpSQLs,
                          '') #sem delete, usa drop

    rm(tmpDF,tmpLay,tmpSQLs)
  } #grpOcup
} #FONTEPRIN

#---- Sample DF random - 100 lines
srDF <- pesDF2015[sample(nrow(pesDF2015),100),]

View(srDF)
write.table(srDF, file=as.character(paste0(PROJDIR,'/output/samplePES.csv')), row.names=FALSE, col.names=TRUE, sep=",", quote = TRUE)
