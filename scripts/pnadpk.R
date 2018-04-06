#' package: pnad
#' title: Importar arquivos PNAD disponiveis no IBGE
#' description: Apenas importa arquivos necessaios para analise
#' version: 0.1.12
#' author: Carlos Perciliano Gaudencio
#' date: 2017-09-22
#' last update: 2018-01-13
#' maintainer: perciliano@outlook.com
#' licence: free, open source
#' library: RODBC, RODBCext, zoo, readr

loadFilesN <- function(LOADPATH, RECURSIVE, PATHERNLST, DELIMITER, FIRSTORIG, PNDOMPES) { #ARQPNAD <- loadFilesN('2015',TRUE,c("\\.sas$","\\.txt$","\\.csv$"),',','MSSQLDB')
  if (missing(RECURSIVE)) RECURSIVE <- FALSE
  if (missing(LOADPATH)) LOADPATH <- '.' #2011 (ano) ou . ou dir/x
  if (missing(PATHERNLST)) PATHERNLST <- ''
  if (missing(PNDOMPES)) PNDOMPES <- 'PES' #DOM/PES
  if (missing(DELIMITER)) DELIMITER <- ',' #delimitador padrao
  if (missing(FIRSTORIG)) FIRSTORIG <- 'LOCALFILE' #'MSSQLDB|server=localhost\\SRV01;database=dbname;uid=sa;pwd=123456' 
  #list.files(recursive = FALSE, pattern = '[0-9][0-9][0-9][0-9]', full.names = FALSE)

  dirYear <- suppressWarnings(as.integer(LOADPATH)) # verifica se eh pasta numerica

  if (is.na(dirYear)) {
    dirYear <- suppressWarnings(as.integer(list.dirs(path = LOADPATH,recursive = FALSE,full.names = FALSE))) # lista diretorios, valida se e numerico ocultando avisos
    dirYear <- as.character(dirYear[complete.cases(dirYear)]) # exibe apenas valores validos, sem NA - formado character
  } #if na

  lflReturn <- data.frame(yearFiles=as.integer(),      #ano da pesquisa
                          csvDelimiter=as.character(), #delimitador para CSV
                          sasFileIN=as.character(),    #layout SAS
                          txtFileIN=as.character(),    #dados posicionais, pesquisa
                          csvFileINOUT=as.character(), #CSV para uso em outras ferramentas
                          sqlFileOUT=as.character(),   #SQL para atualizacao de bases
                          prfAnalysis=as.character(),  #TXT arquivo de profile, de um dataframe
                          lblFileIN=as.character(),    #Labels para padronizar nome dos atributos
                          priSource=as.character(),    #Origem primaria de dados
                          anxFiles=as.character(),     #Lista de anexos i a viii
                          stringsAsFactors=FALSE)

  llblFile <- '' #label fica no diretorio rais do projeto, usado por todos anos
  lflPathern <- list.files(path = as.character('.'), full.names = TRUE, recursive = FALSE, no.. = TRUE, pattern = '\\.csv$', ignore.case = TRUE)
  llblFile <- ifelse(grepl('\\.csv$', tolower(lflPathern[grep(pattern = '*lab*',lflPathern)[1]])),lflPathern[grep(pattern = '*lab*',lflPathern)[1]],llblFile)

  if (length(dirYear)!=0){
    for (iLoop in 1:length(dirYear)) {
      if (is.na(suppressWarnings(as.integer(dirYear[iLoop])))==TRUE) { next } #ignora nao numerico
      lFullPath <- as.character(paste0(ifelse(LOADPATH=='.','',
                                              ifelse(nchar(LOADPATH)>1 & endsWith(LOADPATH,'/'),
                                                     LOADPATH,paste0(LOADPATH,'/'))),dirYear[iLoop]))
      lsasFile <- ''
      ltxtFile <- ''
      lcsvFile <- ''
      pathernFile1 <- ''
      pathernFile2 <- ''
      pathernFile3 <- ''
      pathernFile4 <- ''
      pathernFile5 <- ''
      pathernFile6 <- ''
      pathernFile7 <- ''

      lflPathern <- list.files(path = lFullPath, full.names = TRUE, recursive = RECURSIVE, no.. = TRUE, pattern = '\\.txt$', ignore.case = TRUE)
      pathernFile1 <- lflPathern[which(grepl('(?=.*dados/anexo i -.*\\.txt$)',tolower(lflPathern), perl = TRUE))]
      pathernFile1 <- ifelse(identical(character(0),pathernFile1),'',pathernFile1)
      pathernFile2 <- lflPathern[which(grepl('(?=.*dados/anexo ii -.*\\.txt$)',tolower(lflPathern), perl = TRUE))]
      pathernFile2 <- ifelse(identical(character(0),pathernFile2),'',pathernFile2)
      pathernFile4 <- lflPathern[which(grepl('(?=.*dados/anexo iv -.*\\.txt$)',tolower(lflPathern), perl = TRUE))]
      pathernFile4 <- ifelse(identical(character(0),pathernFile4),'',pathernFile4)
      pathernFile5 <- lflPathern[which(grepl('(?=.*dados/anexo v -.*\\.txt$)',tolower(lflPathern), perl = TRUE))]
      pathernFile5 <- ifelse(identical(character(0),pathernFile5),'',pathernFile5)
      
      lflPathern <- list.files(path = lFullPath, full.names = TRUE, recursive = RECURSIVE, no.. = TRUE, pattern = '\\.csv$', ignore.case = TRUE)
      pathernFile3 <- lflPathern[which(grepl('(?=.*dados/anexo iii -.*\\.csv$)',tolower(lflPathern), perl = TRUE))]
      pathernFile3 <- ifelse(identical(character(0),pathernFile3),'',pathernFile3)
      pathernFile6 <- lflPathern[which(grepl('(?=.*dados/anexo viii -.*atividade.*\\.csv$)',tolower(lflPathern), perl = TRUE))]
      pathernFile6 <- ifelse(identical(character(0),pathernFile6),'',pathernFile6)
      pathernFile7 <- lflPathern[which(grepl('(?=.*dados/anexo viii -.*modalidade.*\\.csv$)',tolower(lflPathern), perl = TRUE))]
      pathernFile7 <- ifelse(identical(character(0),pathernFile7),'',pathernFile7)
      
      for (iPathern in 1:length(PATHERNLST)) {
        #print(paste(dirYear[iLoop],RECURSIVE,PATHERNLST[iPathern],sep = ','))
        lflPathern <- list.files(path = lFullPath, full.names = TRUE, recursive = RECURSIVE, no.. = TRUE, pattern = PATHERNLST[iPathern], ignore.case = TRUE)
        #cat(lflPathern,'\n') #ver retornos
        lsasFile <- ifelse(grepl('\\.sas$', tolower(lflPathern[grep(pattern = paste0('*Dados*',PNDOMPES,'*'),lflPathern)[1]])),lflPathern[grep(pattern = paste0('*Dados*',PNDOMPES,'*'),lflPathern)[1]],lsasFile)
        if (lsasFile=='') lsasFile <- ifelse(grepl('\\.sas$', tolower(lflPathern[grep(pattern = '*Dados*',lflPathern)[1]])),lflPathern[grep(pattern = '*Dados*',lflPathern)[1]],lsasFile)
        ltxtFile <- ifelse(grepl('\\.txt$', tolower(lflPathern[grep(pattern = paste0('*Dados/',PNDOMPES,'*'),lflPathern)[1]])),lflPathern[grep(pattern = paste0('*Dados/',PNDOMPES,'*'),lflPathern)[1]],ltxtFile)
        if (ltxtFile=='' & PATHERNLST[iPathern]=='\\.txt$') { #tenta dentro do zip
          lflPathern <- list.files(path = lFullPath, full.names = TRUE, recursive = RECURSIVE, no.. = TRUE, pattern = '\\.zip$', ignore.case = TRUE)
          ltxtFile <- ifelse(grepl('\\.zip$', tolower(lflPathern[grep(pattern = '*Dados_*',lflPathern)[1]])),lflPathern[grep(pattern = '*Dados_*',lflPathern)[1]],ltxtFile)
          if (ltxtFile!='') {
            ltmpFile <- ''
            tmpPathern <- unzip(ltxtFile,list=TRUE)[,1]
            ltmpFile <- ifelse(grepl('\\.txt$', tolower(tmpPathern[grep(pattern = paste0('*Dados/',PNDOMPES,'*'),tmpPathern)[1]])),tmpPathern[grep(pattern = paste0('*Dados/',PNDOMPES,'*'),tmpPathern)[1]],ltmpFile)
            ltxtFile <- ifelse(ltmpFile=='',ltmpFile,paste(ltxtFile,ltmpFile, sep = '|'))
            rm(tmpPathern,ltmpFile)
          } #zip files - importa txt direto do zip
        } # usa zip como entrada
        lcsvFile <- ifelse(grepl('\\.csv$', tolower(lflPathern[grep(pattern = '*output*',lflPathern)[1]])),lflPathern[grep(pattern = '*output*',lflPathern)[1]],lcsvFile)
        if (lsasFile=='' & PATHERNLST[iPathern]=='\\.sas$') {
          lflPathern <- list.files(path = lFullPath, full.names = TRUE, recursive = RECURSIVE, no.. = TRUE, pattern = '\\.csv$', ignore.case = TRUE)
          lsasFile <- ifelse(grepl('\\.csv$', tolower(lflPathern[grep(pattern = paste0('*Dados/',PNDOMPES,'*'),lflPathern)[1]])),lflPathern[grep(pattern = paste0('*Dados/',PNDOMPES,'*'),lflPathern)[1]],lsasFile)
        } # usa csv de dicionario, se existir no lugar do sas
      } #for iPathern

      lflReturn <- rbind(lflReturn, data.frame(yearFiles=as.integer(dirYear[iLoop]),
                                               csvDelimiter=as.character(DELIMITER),
                                               sasFileIN=as.character(lsasFile),
                                               txtFileIN=as.character(ltxtFile),
                                               csvFileINOUT=as.character(ifelse(lcsvFile!='',lcsvFile,paste("output/pes",dirYear[iLoop],".csv", sep = ''))),
                                               sqlFileOUT=as.character(paste("output/",tolower(PNDOMPES),dirYear[iLoop],".sql", sep = '')),
                                               prfAnalysis=as.character(paste("output/",tolower(PNDOMPES),dirYear[iLoop],"prf.txt", sep = '')),
                                               lblFileIN=as.character(llblFile),
                                               priSource=as.character(FIRSTORIG),
                                               anxFiles=list(grpOcup=pathernFile1,
                                                             codOcup=pathernFile2,
                                                             grpAtiv=pathernFile3,
                                                             codAtiv=pathernFile4,
                                                             unEquiv=pathernFile5,
                                                             tpAtivFis=pathernFile6,
                                                             tpModEsp=pathernFile7),
                                               stringsAsFactors=FALSE))
      rm(lFullPath)
    } # for iLoop
    rm(iLoop) 
  } #if length>0
  rm(dirYear,llblFile) 

  cat('Foram encontrados',nrow(lflReturn),'pesquisas PNAD!')
  return(lflReturn) #fim
} #loadFilesN - carrega nome dos arquivos na estrutura de dataframe para carga

getMode <- function(gVal, na.rm = FALSE) {
  if(na.rm){ gVal = gVal[!is.na(gVal)] }
  uniqVal <- unique(gVal)
  uniqVal[which.max(tabulate(match(gVal, uniqVal)))]
} #getMode - numbers

getLabels <- function(pnDelimiter,pnPriSource,lbFile) {
  if (missing(pnDelimiter) | is.null(pnDelimiter)) pnDelimiter <- ','
  if (missing(lbFile) | is.null(lbFile)) lbFile <- ''
  if (lbFile!='' & !file.exists(lbFile)) {lbFile <- ''} #so usa se existir
  if (missing(pnPriSource) | is.null(pnPriSource)) pnPriSource <- ''
  else {
    pnPriSource <- strsplit(pnPriSource, split = "|",fixed = TRUE)[[1]]
    pnCnURL <- (ifelse(length(pnPriSource)>1,pnPriSource[2],''))
    pnPriSource <- (ifelse('RODBC' %in% installed.packages(),pnPriSource[1],'')) #so funciona se tiver RODBC instalado
  } #split source

  if (pnPriSource=='MSSQLDB' & (exists('pnCnURL') && pnCnURL!='')) {
    conn <- RODBC::odbcDriverConnect(paste0('driver={SQL Server};',pnCnURL))
    tmpLabels <- as.data.frame(RODBC::sqlQuery(conn, "SELECT [CD_NAME] AS Names,[CD_LABE] AS Labels,[VL_CLAS] AS Class FROM [dbo].[TB_LABE]"), stringsAsFactors = FALSE)
    close(conn)
    rm(conn)
    if (nrow(tmpLabels) < 1) { rm(tmpLabels) }
  } #pnPriSource == MSSQLDB

  if (lbFile!='' & !(exists('tmpLabels') && is.data.frame(tmpLabels))) {
    tmpLabels <- as.data.frame(read.table(lbFile, sep=pnDelimiter, header=TRUE, na.strings=c("",'NA'), stringsAsFactors=FALSE, colClasses = c('character','character','NULL','character'), encoding = "UTF-8")) #LATIN1
    tmpLabels <- na.omit(tmpLabels)
    tmpLabels$Names <- tolower(tmpLabels$Names)
    tmpLabels$Labels <- tolower(tmpLabels$Labels)
  } #lbfile

  if (exists('tmpLabels') && is.data.frame(tmpLabels)) {
    cat('Existem:',nrow(tmpLabels),'labels!\n')
    return(tmpLabels)
  }
  else {
    cat('Sem LABELS para usar! getLabels\n')
    return('Sem LABELS para usar! getLabels')
  }
} #getLabels - return dataframe

getLayout <- function(OPTION,pnDelimiter,pnPriSource,pnYear,pnFile,pnLines,lbFile) {
  if (missing(OPTION) | is.null(OPTION)) OPTION <- 0
  if (missing(pnDelimiter) | is.null(pnDelimiter)) pnDelimiter <- ','
  if (missing(pnYear) | is.null(pnYear)) pnYear <- NA
  if (missing(pnFile) | is.null(pnFile)) pnFile <- ''
  if (missing(lbFile) | is.null(lbFile)) lbFile <- ''
  if (missing(pnLines)) pnLines <- -1L #ler arquivo todo
  if (pnFile!='' & !file.exists(pnFile)) {pnFile <- ''} #so usa se existir
  if (missing(pnPriSource) | is.null(pnPriSource)) pnPriSource <- ''
  else {
    pnPriSourceTMP <- pnPriSource
    pnPriSource <- strsplit(pnPriSource, split = "|",fixed = TRUE)[[1]]
    pnCnURL <- (ifelse(length(pnPriSource)>1,pnPriSource[2],''))
    pnPriSource <- (ifelse('RODBC' %in% installed.packages(),pnPriSource[1],'')) #so funciona se tiver RODBC instalado
  } #split source

  if (pnPriSource=='MSSQLDB' & (exists('pnCnURL') && pnCnURL!='')) {
    conn <- RODBC::odbcDriverConnect(paste0('driver={SQL Server};',pnCnURL))
    pnTemp <- as.data.frame(RODBC::sqlQuery(conn, "SELECT [ID_LAYO] AS ID,[CD_YEAR] AS Year,[CD_NAME] AS Names,[CD_PERG] AS CodeQu,[CD_LABE] AS Labels,[TP_PERG] AS TypeQu,[CD_TABL] AS TableQu,[NM_DESC] AS Description,[VL_CLAS] AS Class,[NU_STAR] AS Start,[NU_END] AS End_,[NU_WIDT] AS Widths FROM [dbo].[TB_LAYO]"), stringsAsFactors = FALSE)
    pnResDIM <- as.data.frame(RODBC::sqlQuery(conn, "SELECT [CD_YEAR] AS Year,[CD_NAME] AS Names,[CD_ATTR] AS Codes,[DS_ATTR] AS Labels FROM [dbo].[TB_DIME]"), stringsAsFactors = FALSE)
    close(conn)
    rm(conn)

    cat('MSSQLDB, layout:',nrow(pnTemp),'e dimessao:',nrow(pnResDIM),'\n')
    if (nrow(pnTemp) > 0 & nrow(pnResDIM)) {
      pnTemp <- transform(pnTemp, Names = as.character(as.character(Names))) #convertendo para texto
      pnTemp <- transform(pnTemp, Labels = as.character(as.character(Labels))) #convertendo para texto
      pnTemp <- transform(pnTemp, TableQu = as.character(as.character(TableQu))) #convertendo para texto
      pnTemp <- transform(pnTemp, Description = as.character(as.character(Description))) #convertendo para texto
      pnTemp <- transform(pnTemp, Class = as.character(as.character(Class))) #convertendo para texto
      pnResDIM <- transform(pnResDIM, Names = as.character(as.character(Names))) #convertendo para texto
      pnResDIM <- transform(pnResDIM, Codes = as.character(as.character(Codes))) #convertendo para texto
      pnResDIM <- transform(pnResDIM, Labels = as.character(as.character(Labels))) #convertendo para texto
      isEndCol = which(colnames(pnTemp) == 'End_', arr.ind=TRUE)
      colnames(pnTemp)[isEndCol] <- 'End'
      tmpLayouts <- list(pnTemp,pnResDIM) 
      rm(isEndCol)
    } #retorna 2 dataset
    else if (nrow(pnTemp)) {
      pnTemp <- transform(pnTemp, Names = as.character(as.character(Names))) #convertendo para texto
      pnTemp <- transform(pnTemp, Labels = as.character(as.character(Labels))) #convertendo para texto
      pnTemp <- transform(pnTemp, TableQu = as.character(as.character(TableQu))) #convertendo para texto
      pnTemp <- transform(pnTemp, Description = as.character(as.character(Description))) #convertendo para texto
      pnTemp <- transform(pnTemp, Class = as.character(as.character(Class))) #convertendo para texto
      isEndCol = which(colnames(pnTemp) == 'End_', arr.ind=TRUE) #11
      colnames(pnTemp)[isEndCol] <- 'End'
      tmpLayouts <- pnTemp
      rm(isEndCol)
    } #retorna 1 dataset
    else {rm(pnTemp,pnResDIM)}
  } #pnPriSource == MSSQLDB

  if (pnFile!='' & !(exists('tmpLayouts') && (is.data.frame(tmpLayouts) | is.list(tmpLayouts)))) {
    if (lLABELS==TRUE) {
      pnLabels <- getLabels(pnDelimiter,pnPriSourceTMP,lbFile)
      if (!is.data.frame(pnLabels) & !is.list(pnLabels)) {
        cat("Sem labels:",pnLabels)
        rm(pnLabels)
      } #if pnlabels
    } #ler labels para usar em layout
    
    if (OPTION == 0) {  # 0.LAYOUT DE DADOS - SAS
      flCon  <- file(pnFile, open = "r") #abre arquivo
      tmpLayouts <- readLines(flCon, n = pnLines, warn = FALSE,skipNul = TRUE, encoding = "latin1", ok = TRUE) #le linhas
      close(flCon) #fecha conexao com arquivo #tmpLayouts <- iconv(tmpLayouts)

      pnTemp <- data.frame(Year=integer(), Start=integer(), End=integer(), Widths=integer(), Names=character(), Description=character())
      for (iLine in 1:length(tmpLayouts)) {
        fLineTxt <- trimws(gsub("\\s+", " ",tmpLayouts[iLine])) #limpar espaços na linha, retira multiplos espacos
        if (substring(fLineTxt, 1, 5)=='INPUT') fLineTxt <- trimws(substring(fLineTxt, 6)) #limpar espaços e INPUT na linha
        if (substring(fLineTxt, 1, 1)!='@') next #ler apenas linhas iniciadas por @

        pnListt <- strsplit(as.character(fLineTxt), split = " ") #ler a linha, separando por ' '
        pnStart <- gsub("@", "",vapply(pnListt,`[`, 1, FUN.VALUE = character(1))) # 1o texto do array sem @
        pnNames <- trimws(tolower(vapply(pnListt,`[`, 2, FUN.VALUE = character(1)))) # 2o texto do array caixa baixa

        pnListt <- strsplit(as.character(fLineTxt), split = "\\.") #ler a linha, separando por .
        pnWidths <- vapply(pnListt,`[`, 1, FUN.VALUE = character(1))  # 1o texto do array
  
        pnListt <- strsplit(as.character(pnWidths), split = " ") # separa texto obtido por ' '
        pnWidths <- gsub("[$]", "",vapply(pnListt,`[`, length(pnListt[[1]]), FUN.VALUE = character(1))) # Ultimo texto do array sem [$]

        pnListt <- strsplit(as.character(fLineTxt), split = "[*]") #ler a linha, separando por *
        pnDescription <- trimws(vapply(pnListt,`[`, 2, FUN.VALUE = character(1))) # 2o texto do array

        #pnTemp <- data.frame(pnStart, pnWidths, pnNames, pnDescription)
        pnTemp <- rbind(pnTemp, data.frame(Year=pnYear, Start=pnStart, Widths=pnWidths, Names=pnNames, Description=pnDescription))
        #print(pnTemp)
      } #for iLine

      pnTemp <- transform(pnTemp, Year = as.integer(as.character(Year))) #convertendo para inteiro
      pnTemp <- transform(pnTemp, Start = as.integer(as.character(Start))) #convertendo para inteiro, rSAS$Start <- as.integer(as.character(rSAS$Start))
      pnTemp <- transform(pnTemp, Widths = as.integer(as.character(Widths))) #convertendo para inteiro
      pnTemp$End <- (pnTemp$Start + pnTemp$Widths - 1) #Calculo posicao final
      pnTemp <- transform(pnTemp, Names = as.character(as.character(Names))) #convertendo para texto
      pnTemp <- transform(pnTemp, Description = as.character(as.character(Description))) #convertendo para texto

      if (exists('pnLabels')) {
        pnTemp <- merge(x=pnTemp, y=pnLabels[,c('Names','Labels','Class')], by = 'Names', all.x = TRUE)
        pnTemp <- transform(pnTemp, Labels = as.character(as.character(Labels)))
        pnTemp[is.na(pnTemp$Labels),]$Labels <- pnTemp[is.na(pnTemp$Labels),]$Names
        rm(pnLabels)
      } #definir labels em layout

      pnTemp <- pnTemp[order(pnTemp$Start),] #ordenar pela menor posicao
      pnTemp$ID <- seq(1, length(pnTemp$Start), by = 1) #gerar sequencia
      pnTemp$CodeQu <- gsub("[^0-9\\.]", "", pnTemp$Names) #gerar codigo numerico
      pnTemp$TypeQu <- NA #tipo de questao, 1.num dec, 2.lista, 3.tabela
      pnTemp$TableQu <- NA

      tmpLayouts <- pnTemp
      rm(pnTemp,iLine)
      cat('SAS File, layout:',nrow(tmpLayouts),'\n')
    } #if (OPTION == 0)
    else if (OPTION == 9) {  # 9.LAYOUT DE DADOS - SAS usando CSV convertido do XLS
      tmpLayouts <- data.frame(read.table(pnFile, sep=pnDelimiter, header=FALSE, skip = 5, na.strings=c("",'NA'), stringsAsFactors=FALSE, encoding = "UTF-8"))
      #---dataframe de dimensoes---inicio
      #fLdetachZoo=FALSE
      pnResDIM <- tmpLayouts[(is.na(tmpLayouts$V6)==FALSE | is.na(tmpLayouts$V7)==FALSE),] #Retirando linhas que nao serao usadas
      pnResDIM$V5 <- pnResDIM$V4 <- pnResDIM$V2 <- pnResDIM$V1 <- NULL
      pnResDIM <- pnResDIM[!(tolower(pnResDIM$V3)=='código de variável' & tolower(pnResDIM$V6)=='categorias') & !(tolower(pnResDIM$V7)=='descrição' & tolower(pnResDIM$V6)=='tipo'),]
      pnResDIM <- pnResDIM[!(is.na(pnResDIM$V6)==TRUE & is.na(pnResDIM$V7)==TRUE),]
      #if(!("zoo" %in% (.packages()))){ library(zoo); fLdetachZoo=TRUE }
      pnResDIM$V3 <- zoo::na.locf(pnResDIM$V3) #usando mapeamento de pacote (zoo::...) para nao carregar a library
      pnResDIM$Year <- pnYear
      names(pnResDIM)[names(pnResDIM)=="V3"] <- "Names"
      names(pnResDIM)[names(pnResDIM)=="V6"] <- "Codes"
      names(pnResDIM)[names(pnResDIM)=="V7"] <- "Labels"
      pnResDIM$Names <- tolower(pnResDIM$Names)
      pnResDIM$TypeQu <- NA  #-----definicao de tipos de variaveis conhecidas-------
      #? pnResDIM[tolower(pnResDIM$Codes)=='quantidade' | tolower(pnResDIM$Codes)=='valor' | tolower(pnResDIM$Codes)=='valor em reais',]$TypeQu <- 1 #moeda, valor numerico ou decimal
      pnResDIM$TypeQu <- ifelse(tolower(pnResDIM$Codes)=='quantidade' | tolower(pnResDIM$Codes)=='valor' | tolower(pnResDIM$Codes)=='valor em reais',1,pnResDIM$TypeQu)
      pnResDIM[grep(" a ",pnResDIM$Codes),]$TypeQu <- 2 #range, de-ate
      #? pnResDIM[startsWith(tolower(pnResDIM$Codes),'idem'),]$TypeQu <- NA #retira excessao
      pnResDIM$TypeQu <- ifelse(startsWith(tolower(pnResDIM$Codes),'idem'),NA,pnResDIM$TypeQu)
      #? pnResDIM[startsWith(tolower(pnResDIM$Codes),'ver \"') | tolower(pnResDIM$Codes)=='código',]$TypeQu <- 3 #tabelas
      pnResDIM$TypeQu <- ifelse(startsWith(tolower(pnResDIM$Codes),'ver \"') | tolower(pnResDIM$Codes)=='código',3,pnResDIM$TypeQu)
      pnResDIM$TableQu <- as.character(ifelse(pnResDIM$TypeQu==1,'dimRange',ifelse(pnResDIM$TypeQu==2,'dimDF',NA)))
      pnResDIM[which(pnResDIM$TypeQu==3),]$TableQu <- as.character(ifelse(which(grepl('(?=.*grupamentos ocupacionais)',tolower(pnResDIM[pnResDIM$TypeQu==3,][c('Codes','Labels')]), perl = TRUE)),'grpOcup', #(?=.*dados/anexo i -.*\\.txt$) , .*classificação de ocupações
                                                                          ifelse(which(grepl('(?=.*classificação de ocupações)',tolower(pnResDIM[pnResDIM$TypeQu==3,][c('Codes','Labels')]), perl = TRUE)),'codOcup', #(?=.*dados/anexo ii -.*\\.txt$)
                                                                                 ifelse(which(grepl('(?=.*grupamentos de atividade)',tolower(pnResDIM[pnResDIM$TypeQu==3,][c('Codes','Labels')]), perl = TRUE)),'grpAtiv', #(?=.*dados/anexo iii -.*\\.csv$) , .*códigos de atividades
                                                                                        ifelse(which(grepl('(?=.*códigos de Atividades)',tolower(pnResDIM[pnResDIM$TypeQu==3,][c('Codes','Labels')]), perl = TRUE)),'codAtiv', #(?=.*dados/anexo iv -.*\\.txt$)
                                                                                               ifelse(which(grepl('(?=.*dados/anexo v -.*\\.txt$)',tolower(pnResDIM[pnResDIM$TypeQu==3,][c('Codes','Labels')]), perl = TRUE)),'unEquiv', #(?=.*dados/anexo v -.*\\.txt$)
                                                                                                      ifelse(which(grepl('(?=.*dados/anexo viii -.*atividade.*\\.csv$)',tolower(pnResDIM[pnResDIM$TypeQu==3,][c('Codes','Labels')]), perl = TRUE)),'tpAtivFis', #(?=.*dados/anexo viii -.*atividade.*\\.csv$)
                                                                                                             ifelse(which(grepl('(?=.*dados/anexo viii -.*modalidade.*\\.csv$)',tolower(pnResDIM[pnResDIM$TypeQu==3,][c('Codes','Labels')]), perl = TRUE)),'tpModEsp', #(?=.*dados/anexo viii -.*modalidade.*\\.csv$)
                                                                                                                    NA)))))))
                                                                   )
      #if(fLdetachZoo==TRUE && ("zoo" %in% (.packages()))){ detach("package:zoo", unload=TRUE) }
      #---dataframe de dimensoes---fim
      tmpLayouts <- tmpLayouts[is.na(tmpLayouts$V1)==FALSE & is.na(tmpLayouts$V2)==FALSE & suppressWarnings(is.na(as.numeric(tmpLayouts$V1)))==FALSE,] #Retirando linhas que nao serao usadas

      pnTemp <- data.frame(Year=integer(), Start=integer(), End=integer(), Widths=integer(), Names=character(), Description=character())
      pnTemp <- rbind(pnTemp, data.frame(Year=pnYear, Start=as.integer(tmpLayouts$V1), Widths=as.integer(tmpLayouts$V2), Names=as.character(tolower(tmpLayouts$V3)), Description=as.character(tmpLayouts$V5)))
      pnTemp <- transform(pnTemp, Year = as.integer(as.character(Year))) #convertendo para inteiro
      pnTemp <- transform(pnTemp, Start = as.integer(as.character(Start))) #convertendo para inteiro,
      pnTemp <- transform(pnTemp, Widths = as.integer(as.character(Widths))) #convertendo para inteiro
      pnTemp$End <- (pnTemp$Start + pnTemp$Widths - 1) #Calculo posicao final
      pnTemp <- transform(pnTemp, Names = as.character(as.character(Names))) #convertendo para texto
      pnTemp <- transform(pnTemp, Description = as.character(as.character(Description))) #convertendo para texto
      
      if (exists('pnLabels')) {
        pnTemp <- merge(x=pnTemp, y=pnLabels[,c('Names','Labels','Class')], by = 'Names', all.x = TRUE)
        pnTemp <- transform(pnTemp, Labels = as.character(as.character(Labels)))
        pnTemp[is.na(pnTemp$Labels),]$Labels <- pnTemp[is.na(pnTemp$Labels),]$Names
        rm(pnLabels)
      } #definir labels em layout

      pnTemp <- pnTemp[order(pnTemp$Start),] #ordenar pela menor posicao
      pnTemp$ID <- seq(1, length(pnTemp$Start), by = 1) #gerar sequencia
      pnTemp$CodeQu <- gsub("[^0-9\\.]", "", pnTemp$Names) #gerar codigo numerico
      pnTemp$TypeQu <- NA #tipo de questao, 1.num dec, 2.lista, 3.tabela
	  pnTemp$TableQu <- NA
      iListVarName <- pnTemp[pnTemp$Names %in% pnResDIM[!is.na(pnResDIM$TypeQu),]$Names,]$Names
      for (iVarName in iListVarName) {
        pnTemp[pnTemp$Names==iVarName,]$TypeQu <- unique(pnResDIM[!is.na(pnResDIM$TypeQu) & pnResDIM$Names==iVarName,]$TypeQu)[1]
		pnTemp[pnTemp$Names==iVarName,]$TableQu <- unique(pnResDIM[!is.na(pnResDIM$TableQu) & pnResDIM$Names==iVarName,]$TableQu)[1]
      } #for iVarName, isTRUE(all.equal(x, rep(x[1], length(x))))
      pnTemp[!is.na(pnTemp$TypeQu) & pnTemp$TypeQu==1,]$Class <- 'double' #1.decimal/numerica - sobrepoem pnLabels

      cat('CSV e TXT, layout:',nrow(pnTemp),'e dimessao:',nrow(pnResDIM),'\n')
      tmpLayouts <- list(pnTemp,pnResDIM) #retorna 2 dataset
      rm(pnTemp,pnResDIM,iVarName,iListVarName)
    } #if (OPTION == 9)
  } #pnFile

  if (exists('tmpLayouts') && (is.data.frame(tmpLayouts) | is.list(tmpLayouts))) {
    return(tmpLayouts)
  }
  else {
    return('Sem LAYOUT para usar! getLayout')
  }
} #getLayout - return dataframe

pnadFiles <- function(OPTION,pnLayout,pnLines,pnDFsuport) {
  if (missing(OPTION) | is.null(OPTION)) stop("Opcao do processo obrigatorio! onde: 0-SAS definicao, 1-TXT posicional, 2-CSV delimitado")
  if (missing(pnLayout) | is.null(pnLayout)) stop("Definicao de Processamento e obrigatorio! Dataframe(yearFiles,csvDelimiter,sasFileIN,txtFileIN,csvFileOUT,sqlFileOUT)")
  if (!is.data.frame(pnLayout)) stop("Parametros e layout devem estar em um dataframe com uma linha de dados!")
  else {
    if (("yearFiles" %in% colnames(pnLayout)) & ("csvDelimiter" %in% colnames(pnLayout)) & ("sasFileIN" %in% colnames(pnLayout))) {
      pnYear <- pnLayout[1,]$yearFiles
      pnDelimiter <- pnLayout[1,]$csvDelimiter
      pnPriSource <- pnLayout[1,]$priSource
    } else {
      stop("Dataframe de configuracao invalido! Esperado dataframe(yearFiles,csvDelimiter,sasFileIN,txtFileIN,csvFileINOUT,sqlFileOUT)") 
    } #else

    if ((OPTION >= 0) & (OPTION < 3)) { #arquivo de origem existe
      lbFile <- pnLayout[1,]$lblFileIN
      pnFile <- ifelse(OPTION==0, pnLayout[1,]$sasFileIN,
                       ifelse(OPTION==1, strsplit(pnLayout[1,]$txtFileIN, split = '|', fixed = TRUE),
                              ifelse(OPTION==2, pnLayout[1,]$csvFileINOUT,'')))
      if (OPTION==1) pnFile <- pnFile[[1]] #ajuste do split
      if (length(pnFile)>1) {
        zpFile <- pnFile[2] #txt
        pnFile <- pnFile[1] #zip
        tmFile <- unzip(zipfile = pnFile, files = zpFile,exdir = tempdir()) #tmp, pnFile <- unz( pnFile, zpFile,'r')
      } #arquivo dentro de um zip, extract before use

      if (!file.exists(pnFile)){
        stop(paste("Arquivo de origem nao encontrado! Data[Option=",OPTION,",File=",pnFile,"]"))
      } #if file
    } #if OPTION
  } #else

  if (missing(pnYear) | is.null(pnYear) | (pnYear<1000)) stop("Informar o ano da pesquisa!")
  if (missing(pnLayout)) pnDelimiter <- ',' #delimitador padrao
  if (missing(pnLines)) pnLines <- -1L #ler arquivo todo
  if (missing(pnDFsuport)) pnDFsuport <- NA

  if (is.data.frame(pnDFsuport)) {
    if ((OPTION==2) & !("Names" %in% colnames(pnDFsuport))) {
      stop("Para opcao 2, CSV necessario dataframe de layout! dataframe(Names...)")
    } else if ((OPTION==1) & !("Names" %in% colnames(pnDFsuport)) & ("Widths" %in% colnames(pnDFsuport))) {
      stop("Para opcao 2, TXT necessario dataframe de layout! dataframe(Names,Widths...)")
    }
  } else {
    if ((OPTION>0) & (OPTION<4)) stop("Dataframe de layout invalido ou faltando!")
  }

  if (OPTION == 0) { #verifica arquivo de parametros csv ou sas
    tFileType <- strsplit(as.character(tolower(pnFile)), split = "\\.")[[1]] #ler o nome arquivo, separando por .
    tFileType <- tFileType[length(tFileType)]  # ultimo texto do array

    if (!exists('lLABELS')==TRUE) {lLABELS <- FALSE} #se nao tiver definicao desativa

    if (tFileType == 'csv') {
      OPTION <- 9 #muda opcao de importacao
    }
  } # if OPTION==0

  if (OPTION == 0) {  # 0.LAYOUT DE DADOS - SAS
    pnResult <- getLayout(OPTION,pnDelimiter,pnPriSource,pnYear,pnFile,pnLines,lbFile)

    if (!is.data.frame(pnResult) & !is.list(pnResult)) {
      cat("Sem layout:",pnResult)
      rm(pnResult)
    } #if pnResult

  } else if (OPTION==1) { #1.fwf com dataframe de configuracao, tem Names e Widths
    pnDFsuport$somacols <- c(pnDFsuport[1:(nrow(pnDFsuport)-1), 'End']>pnDFsuport[2:nrow(pnDFsuport), 'Start'],FALSE) #true=pular
    pnDFsuport$StartReal <- c(FALSE,pnDFsuport[1:(nrow(pnDFsuport)-1), 'End']>pnDFsuport[2:(nrow(pnDFsuport)), 'Start'])
    pnDFsuport$StartReal <- ifelse(pnDFsuport$StartReal==TRUE,pnDFsuport[1:(nrow(pnDFsuport)-1), 'Start'],pnDFsuport[1:(nrow(pnDFsuport)), 'Start']) #usar start atual ou anterior
    pnVarNome <- ifelse(lLABELS==TRUE,'Labels','Names') #& 'Labels' %in% colnames(pnDFsuport)  install.packages("readr") - 10x mais rapido

    #pnResult <- read.fwf(file=pnFile, widths=pnDFsuport[pnDFsuport$somacols==FALSE,]$Widths, header = FALSE,col.names=pnDFsuport[pnDFsuport$somacols==FALSE,pnVarNome]) #importar arquivo txt posicional
    if (exists('tmFile')==TRUE) {
      cat('Usando arquivo temporario:',pnFile,':',tmFile)
      pnFile <- tmFile
    } #usar temporario extraido
    pnColTypes <- pnDFsuport[pnDFsuport$somacols==FALSE,]$Class
    if (any(is.na(pnColTypes))==TRUE) pnColTypes[is.na(pnColTypes)] <- '?' #guess data
    if (any(is.null(pnColTypes))==TRUE) pnColTypes[is.null(pnColTypes)] <- '_' #skip column
    pnColTypes[nchar(as.character(pnColTypes))>1] <- substr(pnColTypes[nchar(as.character(pnColTypes))>1],1,1) #types
    pnColTypes <- paste0(as.character(pnColTypes), collapse = '') #change list to string, c = character, i = integer, n = number, d = double, l = logical, D = date, T = date time, t = time, ? = guess, or _/- to skip the column.
    pnColNames <- as.character(pnDFsuport[pnDFsuport$somacols==FALSE,pnVarNome])
    #options(OutDec= ",") -- locale("pt_BR", decimal_mark = ".", grouping_mark = ' ') -- default_locale()
    pnResult <- readr::read_fwf(pnFile, col_types = pnColTypes, #https://www.tidyverse.org/, http://r4ds.had.co.nz/
                                locale = readr::locale(decimal_mark = ".", grouping_mark = ' '),
                                readr::fwf_positions(start = pnDFsuport[pnDFsuport$somacols==FALSE,]$StartReal,
                                                     end = pnDFsuport[pnDFsuport$somacols==FALSE,]$End,
                                                     col_names = pnColNames), progress = FALSE)
    #options(OutDec= ".")
    # readr::spec(pnResult) --ver: character, integer/double, https://github.com/tidyverse/readr/issues/645

    if (any(pnDFsuport$somacols==TRUE)==TRUE) { #separar atributos unidos acima
      tmpSomacols <- pnDFsuport[pnDFsuport$somacols==TRUE | pnDFsuport$ID==pnDFsuport[pnDFsuport$somacols==TRUE,]$ID + 1,]

      for (iLine in 1:nrow(tmpSomacols[tmpSomacols$somacols==TRUE,])) {
        vfName <- tmpSomacols[iLine & tmpSomacols$somacols==TRUE,pnVarNome]
        vfID <- tmpSomacols[iLine & tmpSomacols$somacols==TRUE,'ID']
        vfName2 <- tmpSomacols[tmpSomacols$ID==(vfID+1),pnVarNome]
        vfStart <- (tmpSomacols[iLine & tmpSomacols$somacols==TRUE,'Start'] - tmpSomacols[tmpSomacols$ID==(vfID+1),'Start'])
        vfEnd <- (tmpSomacols[iLine & tmpSomacols$somacols==TRUE,'End'] - tmpSomacols[tmpSomacols$ID==(vfID+1),'Start'] + 1)

        pnResult[vfName] <- substr(pnResult[,vfName2][[1]],vfStart,vfEnd)
      } #for - adiciona coluna pendente, agrupada
      pnResult <- pnResult[intersect(pnDFsuport[,pnVarNome],names(pnResult))] #reordena colunas
      if (exists('tmFile')==TRUE) {
        Sys.sleep(0.10) #aguarda 10 milisegundos para apagar
        if (file.exists(tmFile)) file.remove(tmFile)
        rm(zpFile,tmFile)
      } #remove arquivo teporario
      rm(tmpSomacols,vfName,vfName2,vfID,vfStart,vfEnd,iLine)
    } #if any somacols - separa coluna unificada
    rm(pnDFsuport,pnVarNome,pnColTypes,pnColNames)

  } else if (OPTION==2) { #2.CSV com dataframe de configuracao de Names e delimitador
    if (any(is.na(pnDFsuport$Names))==TRUE) { #any= 1 elementro true
      pnResult <- data.frame(read.table(pnFile, sep=pnDelimiter, header=TRUE, na.strings=c("",'NA'), stringsAsFactors=FALSE, encoding = "UTF-8"))
    } else if (lLABELS==TRUE & 'Labels' %in% colnames(pnDFsuport)) {
      pnResult <- data.frame(read.table(pnFile, sep=pnDelimiter, header=TRUE, na.strings=c("",'NA'), stringsAsFactors=FALSE, col.names=pnDFsuport[order(pnDFsuport$Start),]$Labels, encoding = "UTF-8"))
    } else { #if usar labels
      pnResult <- data.frame(read.table(pnFile, sep=pnDelimiter, header=TRUE, na.strings=c("",'NA'), stringsAsFactors=FALSE, col.names=pnDFsuport[order(pnDFsuport$Start),]$Names, encoding = "UTF-8"))
    } #else usar names

    rm(pnDFsuport)

  } else if (OPTION == 9) {  # 9.LAYOUT DE DADOS - SAS usando CSV convertido do XLS
    pnResult <- getLayout(OPTION,pnDelimiter,pnPriSource,pnYear,pnFile,pnLines,lbFile)

    if (!is.data.frame(pnResult) & !is.list(pnResult)) {
      cat("Sem layouts:",pnResult)
      rm(pnResult)
    } #if pnResult
  } else stop("Opcao de processo invalida! usar: 0-SAS definicao, 1-TXT posicional, 2-CSV delimitado, 3-SQL gerar")

  return(pnResult) #fim
} # pnadFiles - function
#'Exemplos:
#'pnadcLayout <- pnadFiles(0, ARQPNAD[ARQPNAD$yearFiles==ANOPNAD,])
#'txtData <- pnadFiles(1, ARQPNAD[ARQPNAD$yearFiles==ANOPNAD,],-1L, pnadcLayout)
#'sqlData <- pnadFiles(3, ARQPNAD[ARQPNAD$yearFiles==ANOPNAD,], -1L, pesData)
#'csvData <- pnadFiles(2, ARQPNAD[ARQPNAD$yearFiles==ANOPNAD,], -1L, pnadcLayout)

pnadProfiler <- function(pnData,pnOutfile,pnDescr) {
  if (!is.data.frame(pnData)) stop("Dados para analise devem estar em um dataframe!")

  iLines <- nrow(pnData)
  iCollums <- ncol(pnData)
  iNumNA <- sum(is.na(pnData))  # Verificando se a dados nulos no dataset 
  prfResult <- character()

  if (!is.na(pnOutfile)) sink(pnOutfile) # Inicia redirecionamento e gravacao em arquivo
  cat(paste(". Analise de dimensoes: [linhas=", iLines, ",colunas=", iCollums, ",nulos=", iNumNA,"]."))
  cat(paste('\n . Disponivel em: [file=', pnOutfile,'].'))

  cat("=============[STR]====================\n")
  str(pnData) #tmpText <- capture.output(str(pnData))

  if (is.data.frame(pnDescr)) {
    iYearDF <- pnDescr[1,]$Year
    pnDescr$ClassR <- ''
    pnDescr$ModeType <- ''
    pnDescr$CountNA <- 0
    pnDescr$Min <- NA
    pnDescr$Quartil1 <- NA
    pnDescr$Median <- NA
    pnDescr$Mean <- NA
    pnDescr$Mode <- NA
    pnDescr$Quartil3 <- NA
    pnDescr$Max <- NA
    pnDescr$SD <- NA
    pnDescr$Variance <- NA
    pnDescr$CV <- NA
    pnDescr$IQR <- NA
  } #if atualiza dataframe original com informacoes analiticas
  
  cat("========[SUMMARY.DETAIL===============\n")
  lstNames <- names(pnData)
  
  for (iloop in 1:iCollums) {
    pnVarLoop <- pnData[[iloop]] #pnData[,iloop]
    pnMode <- mode(pnVarLoop)
    pnClass <- class(pnVarLoop)
    pnName <- lstNames[iloop] #Nome da coluna
    pnTotNulos <- sum(is.na(pnVarLoop))
    pnVarNome <- ifelse(lLABELS==TRUE,'Labels','Names')
    pnVarDescr <- ifelse(!is.data.frame(pnDescr),'',pnDescr[pnDescr[,pnVarNome]==pnName,]$Description)

    if (pnTotNulos==iLines) prfResult <- c(prfResult, pnName)

    if (is.data.frame(pnDescr)) {
      pnDescr[pnDescr[,pnVarNome]==pnName & pnDescr$Year==iYearDF,]$ClassR <- pnClass
      pnDescr[pnDescr[,pnVarNome]==pnName & pnDescr$Year==iYearDF,]$ModeType <- pnMode
      pnDescr[pnDescr[,pnVarNome]==pnName & pnDescr$Year==iYearDF,]$CountNA <- pnTotNulos
      
      if (pnMode=='numeric' & pnClass!='factor') {
        pnDescr[pnDescr[,pnVarNome]==pnName & pnDescr$Year==iYearDF,]$Min <- min(pnVarLoop, na.rm = TRUE)
        pnDescr[pnDescr[,pnVarNome]==pnName & pnDescr$Year==iYearDF,]$Quartil1 <- quantile(pnVarLoop, 0.25, na.rm = TRUE)
        pnDescr[pnDescr[,pnVarNome]==pnName & pnDescr$Year==iYearDF,]$Median <- median(pnVarLoop, na.rm = TRUE)
        pnDescr[pnDescr[,pnVarNome]==pnName & pnDescr$Year==iYearDF,]$Mean <- mean(pnVarLoop, na.rm = TRUE)
        pnDescr[pnDescr[,pnVarNome]==pnName & pnDescr$Year==iYearDF,]$Mode <- getMode(pnVarLoop, na.rm = TRUE)
        pnDescr[pnDescr[,pnVarNome]==pnName & pnDescr$Year==iYearDF,]$Quartil3 <- quantile(pnVarLoop, 0.75, na.rm = TRUE)
        pnDescr[pnDescr[,pnVarNome]==pnName & pnDescr$Year==iYearDF,]$Max <- max(pnVarLoop, na.rm = TRUE)
        pnDescr[pnDescr[,pnVarNome]==pnName & pnDescr$Year==iYearDF,]$SD <- sd(pnVarLoop, na.rm = TRUE)
        pnDescr[pnDescr[,pnVarNome]==pnName & pnDescr$Year==iYearDF,]$Variance <- var(pnVarLoop, na.rm = TRUE)
        pnDescr[pnDescr[,pnVarNome]==pnName & pnDescr$Year==iYearDF,]$CV <- (pnDescr[pnDescr[,pnVarNome]==pnName & pnDescr$Year==iYearDF,]$SD / pnDescr[pnDescr[,pnVarNome]==pnName & pnDescr$Year==iYearDF,]$Mean) * 100
        pnDescr[pnDescr[,pnVarNome]==pnName & pnDescr$Year==iYearDF,]$IQR <- IQR(pnVarLoop, na.rm = TRUE) #interquantile variation 1-3
      } #pnMode - numeric
      if (pnMode=='logical') {
        pnDescr[pnDescr[,pnVarNome]==pnName & pnDescr$Year==iYearDF,]$Mode <- getMode(pnVarLoop, na.rm = TRUE)
      } #pnMode - logical
    } #if atualiza dataframe original com informacoes analiticas

    cat(paste('\n ...........',iloop,':',toupper(pnName),', classe:',pnClass,':', toupper(pnMode),'- Descr:', pnVarDescr,', Nulos:', pnTotNulos,sep = ''))
    #str(pnVarLoop)
    cat(' Sumario:\n', names(summary(pnVarLoop)),fill = TRUE)
    cat(summary(pnVarLoop),'\n',fill = TRUE) #summary.default(pnData)  summary.data.frame(pnData)

    if (length(table(pnVarLoop))<20) {
      cat(' Sort:\n', names(sort(table(pnVarLoop))),fill = TRUE)
      cat(sort(table(pnVarLoop)),fill = TRUE)
      cat(' Prop:\n', names(sort(prop.table(table(pnVarLoop)))),fill = TRUE)
      cat(sort(prop.table(table(pnVarLoop))),fill = TRUE)
      cat(' Levels:\n', names(length(levels(pnVarLoop))),fill = TRUE)
      cat(length(levels(pnVarLoop)),'\n',fill = TRUE)
    } else {
      cat('  Mais de 20 variacoes! Exibindo top 20.\n')
      cat(' Sort:\n', head(names(sort(table(pnVarLoop))),20),fill = TRUE)
      cat(head(sort(table(pnVarLoop)),20),fill = TRUE)
      cat(' Prop:\n', head(names(sort(prop.table(table(pnVarLoop)))),20),fill = TRUE)
      cat(head(sort(prop.table(table(pnVarLoop))),20),fill = TRUE)
      cat(' Levels:\n', head(names(length(levels(pnVarLoop))),20),fill = TRUE)
      cat(head(length(levels(pnVarLoop)),20),'\n',fill = TRUE)
    }

    #pnVar2 <- complete.cases(pnVarLoop) #----------------------ver
    #if (!is.na(pnVar2) & (pnMode == 'numeric') & (pnClass != 'character')) {
    #if (min(pnVar2) != max(pnVar2)) {
    #  cat(' Frequencia:\n',fill = TRUE)
    #  if (length(table(pnVar2))<11){pnBreaks <- names(sort(table(pnVar2)))}
    #  else {pnBreaks=seq(min(pnVar2),max(pnVar2),length.out = 10)}
    #  xFreq=cut(pnVar2,pnBreaks,right = FALSE, include.lowest = TRUE)
    #  f = table(xFreq) 
    #  cat(names(f),fill = TRUE)
    #  cat(f,fill = TRUE)
    #} else {cat('  Apenas uma frequencia numerica!\n')}
    #} else {cat('  Formato nao numerico!\n')}
  } #for
  if (is.data.frame(pnDescr)) {cat('Atributos apenas com nulos:',unlist(pnDescr),'\n')}
  if (!is.na(pnOutfile)) sink()          # para gravacao em arquivo, fim

  rm(pnData, iLines, iCollums)
  
  if (is.data.frame(pnDescr)) {
    return(list(prfResult,pnDescr))
  } else {
    return(prfResult)
  } #fim
} #pnadProfiler - function

#' DFsql <- setSQLinsert(dimDF[,c('Year','Names','Codes','Labels')],100,"INSERT INTO [dbo].[TB_DIME] (CD_YEAR,CD_NAME,CD_ATTR,DS_ATTR) VALUES ",'DELETE [dbo].[TB_DIME]')
setSQLinsert <- function(idataframe,iby,idbinsert,idbdelete) {
  if (missing(idataframe) || !is.data.frame(idataframe)) stop('Obrigatorio passar o dataframe de dados!')
  if (missing(idbinsert) | is.null(idbinsert) | is.na(idbinsert)) stop('Obrigatorio informar querie inicial!')
  if (missing(iby) | is.null(iby) | is.na(iby) | iby<=0) iby <- 900
  if (missing(idbdelete) | is.null(idbdelete) | is.na(idbdelete)) idbdelete <- '' #DELETE [dbo].[TB_DIME]
  iMax <- nrow(idataframe)
  if (iMax<=0) stop('Dataframe deve ter ao menos uma linha de dados!')

  oDF <- data.frame(ID=integer(), QuerieSQL=character())
  if (idbdelete!='') {
    oDF <- rbind(oDF, data.frame(ID=0, QuerieSQL=idbdelete))
  } #delete data first
  
  idataframe[is.na(idataframe)] <- 'NULL' #definir NA como nulos para enviar ao banco - https://github.com/inbo/n2khelper/blob/master/R/odbc_insert.R

  if ((iby < iMax)==TRUE) {
    iLoops <- seq(from = 1, to = iMax, by = iby)
    
    for (iLoop in 1:length(iLoops)) {
      #cat(iLoops[iLoop],ifelse(iLoop<length(iLoops),iLoops[iLoop + 1] - 1,iMax),'\n')
      iInsertSQL <- paste(idbinsert,
                          paste0(apply(idataframe[iLoops[iLoop]:ifelse(iLoop<length(iLoops),iLoops[iLoop + 1] - 1,iMax),], 1, 
                                       function(x) paste0("('", paste0( gsub("'","",x) , collapse = "', '"), "')")), 
                                 collapse = ", "))
      oDF <- rbind(oDF, data.frame(ID=iLoop, QuerieSQL=iInsertSQL))
    } #for
  } #if
  else {
    #cat(1,iMax,'\n')
    iInsertSQL <- paste(idbinsert,
                        paste0(apply(idataframe, 1, 
                                     function(x) paste0("('", paste0(x, collapse = "', '"), "')")), 
                               collapse = ", "))
    oDF <- rbind(oDF, data.frame(ID=1, QuerieSQL=iInsertSQL))
  } #else
  
  if (nrow(oDF)>0) oDF$QuerieSQL <- gsub("'NULL'", "NULL", oDF$QuerieSQL) #ajuste de NULL
  return(oDF)
} #setSQLinsert - retorna dataframe de queries de insert

sendPNADdb <- function(iBigDF,pnPrimSrc,iBreak,iTabDF,iDelQ) {
  if (missing(iBigDF) || !is.data.frame(iBigDF)) stop('Obrigatorio passar o dataframe de dados!')
  if (!'RODBC' %in% installed.packages()) stop('Pacote RODBC nao disponivel! instalar.')
  if (!'RODBCext' %in% installed.packages()) stop('Pacote RODBCext nao disponivel! instalar.')
  if (missing(pnPrimSrc) | is.null(pnPrimSrc) | is.na(pnPrimSrc)) stop('Obrigatorio informar ORIGEM PRINCIPAL, base de dados!')
  else {
    pnPrimSrc <- strsplit(pnPrimSrc, split = "|",fixed = TRUE)[[1]]
    pnCnURL <- (ifelse(length(pnPrimSrc)>1,pnPrimSrc[2],''))
    pnPrimSrc <- (ifelse('RODBC' %in% installed.packages(),pnPrimSrc[1],'')) #so funciona se tiver RODBC instalado
  } #split source
  if (missing(iTabDF) || !is.data.frame(iTabDF)) stop('Obrigatorio informar DATAFRAME de queries!')
  if (missing(iBreak) | is.null(iBreak) | is.na(iBreak) | iBreak<=0) iBreak <- 300
  if (missing(iDelQ) | is.null(iDelQ) | is.na(iDelQ)) iDelQ <- '' #DELETE [dbo].[TB_DIME]
  iMaxD <- nrow(iBigDF)
  if (iMaxD<=0) stop('Dataframe de dados deve ter ao menos uma linha de dados!')
  if (nrow(iTabDF)>0) {
     if (length(unique(iTabDF$ID))!=4 & length(unique(iTabDF$Year))!=1) { stop('Devem existir 4 comandos e um ano apenas! Drop,Create,Insert,Select') }
     qTableRemove <- ifelse(any(iTabDF$ID==1),as.character(iTabDF[iTabDF$ID==1,]$Querie),'')
     qTableCreate <- ifelse(any(iTabDF$ID==2),as.character(iTabDF[iTabDF$ID==2,]$Querie),'')
     qInsert <- ifelse(any(iTabDF$ID==9),as.character(iTabDF[iTabDF$ID==9,]$Querie),'')
  } else { stop('Dataframe de queries deve possuir todos comandos!') }
  #if (missing(qTableRemove) | is.null(qTableRemove) | is.na(qTableRemove) | qTableRemove=='') stop('Querie de DROP nao encontrada!(id=1)')
  #if (missing(qTableCreate) | is.null(qTableCreate) | is.na(qTableCreate) | qTableCreate=='') stop('Querie de CREATE nao encontrada!(id=2)')
  if (missing(qInsert) | is.null(qInsert) | is.na(qInsert) | qInsert=='') stop('Querie de INSERT nao encontrada!(id=9)')
  msgRet <- list() #iniciar lista de retornos

  if (pnPrimSrc=='MSSQLDB' & (exists('pnCnURL') && pnCnURL!='')) {
    conn <- RODBC::odbcDriverConnect(paste0('driver={SQL Server};',pnCnURL))

    if (qTableRemove!='') { qRet <- RODBC::sqlQuery(conn,qTableRemove); msgRet[['DropTable']] <- list(datetime=format(Sys.time(),"%d/%m/%Y %X"),ret=qRet) }
    if (qTableCreate!='') { qRet <- RODBC::sqlQuery(conn,qTableCreate); msgRet[['CreateTable']] <- list(datetime=format(Sys.time(),"%d/%m/%Y %X"),ret=qRet) }
    if (iDelQ!='') { qRet <- RODBC::sqlQuery(conn,iDelQ); msgRet[['Delete']] <- list(datetime=format(Sys.time(),"%d/%m/%Y %X"),ret=qRet) }

    if ((iBreak < iMaxD)==TRUE) {
      iLoopsD <- seq(from = 1, to = iMaxD, by = iBreak)

      qRet <- RODBCext::sqlPrepare(conn,qInsert); msgRet[['Prepare']] <- list(datetime=format(Sys.time(),"%d/%m/%Y %X"),ret=qRet)
      for (iLoopD in 1:length(iLoopsD)) {
        cat('Enviando dados por parametros de',iBreak,'linhas ate o limite de',iMaxD,'!',iLoopD,'/',length(iLoopsD),'\n')
        qRet <- RODBCext::sqlExecute(conn, NULL, iBigDF[iLoopsD[iLoopD]:ifelse(iLoopD<length(iLoopsD),iLoopsD[iLoopD + 1] - 1,iMaxD),])
        msgRet[['Insert']][[iLoopD]] <- list(datetime=format(Sys.time(),"%d/%m/%Y %X"), set=paste0(iLoopsD[iLoopD],':',ifelse(iLoopD<length(iLoopsD),iLoopsD[iLoopD + 1] - 1,iMaxD)), ret=qRet)
      } #for iLoopD
      rm(iLoopsD,iLoopD)
    } #if
    else {
      cat('Enviando dados por parametros ate o limite de',iMaxD,'!\n')
      qRet <- RODBCext::sqlExecute(conn, query = qInsert, iBigDF)
      msgRet[['Insert']] <- list(datetime=format(Sys.time(),"%d/%m/%Y %X"), set=paste0(1,':',iMaxD), ret=qRet)
    } #else

    close(conn) #showConnections(all = FALSE);mycon <- getConnection(3)
    rm(conn,iBigDF,qRet)
  } #pnPrimSrc == MSSQLDB
  else {
    stop('Origem principal nao prevista ou nao foi possivel conectar!')
  } #else

  return(msgRet)
} #sendPNADdb - envia grandes tabelas fracionando para base SQL

is.letter <- function(x) grepl("[[:alpha:]]", x) #https://stat.ethz.ch/pipermail/r-help/2012-August/320764.html
#is.number <- function(x) grepl("[[:digit:]]", x) #http://stla.github.io/stlapblog/posts/Numextract.html - erro

getPNADanx <- function(pnFiles,pnLay3) {
  if (missing(pnFiles) || !is.data.frame(pnFiles)) stop('Obrigatorio passar o dataframe de arquivos!')
  if (missing(pnLay3) || !is.data.frame(pnLay3)) stop('Obrigatorio passar o dataframe de layout!')
  
  vNames <- names(pnFiles)[10:16]
  vNames <- vNames[!is.na(vNames)]
  pnResult <- list()

  if (!identical(character(0),vNames))  {
    for (nFile in vNames) {
      if (pnFiles[,nFile]=='' | is.na(pnFiles[,nFile])) next()
      vFileObj <- strsplit(nFile, '[.]')[[1]][2]

      if (vFileObj=='grpOcup') {
        #grpOcup <- read.fwf(file=pnFiles[,nFile], widths = 200,header = FALSE, skip = 5, stringsAsFactors = FALSE)
        #grpOcup <- readr::read_fwf(pnFiles[,nFile], col_types = 'c', readr::fwf_positions(start = 0, end = NULL, col_names = NULL), progress = FALSE)
        grpOcup <- as.data.frame(scan(file = pnFiles[,nFile], na.strings = c(' ','',NA),what = list(descript=character()), skip = 5, blank.lines.skip = TRUE, sep = '\n', encoding = 'UTF-8'))
        grpOcup$descript <- trimws(gsub("\\s+", " ",grpOcup$descript)) #retirar multiplos espacos
        grpOcup$descript <- ifelse(nchar(grpOcup$descript)==0,NA,grpOcup$descript)
        grpOcup$cod <- substr(grpOcup$descript,1,4)
        grpOcup <- grpOcup[!is.na(grpOcup$descript),]
        grpOcup <- transform(grpOcup, descript = as.character(descript))
        #copia codigo de grp gerencia da proxima linha 
        grpOcup$letters <- is.letter(grpOcup$cod)
        grpOcup$test <- c(grpOcup[1:(nrow(grpOcup)-1),'letters']==TRUE & grpOcup[2:nrow(grpOcup), 'letters']==FALSE,FALSE)
        grpOcup$cod <- ifelse(grpOcup$test==TRUE,paste0(substr(grpOcup[2:nrow(grpOcup), 'cod'],1,2),'00'),grpOcup$cod)
        #copia codigo de grp diretoria da proxima linha
        grpOcup$letters <- is.letter(grpOcup$cod)
        grpOcup$test <- c(grpOcup[1:(nrow(grpOcup)-1),'letters']==TRUE & grpOcup[2:nrow(grpOcup), 'letters']==FALSE,FALSE)
        grpOcup$cod <- ifelse(grpOcup$test==TRUE,paste0(substr(grpOcup[2:nrow(grpOcup), 'cod'],1,1),'000'),grpOcup$cod)
        #remover testes
        grpOcup[substr(grpOcup$descript,1,4)==grpOcup$cod,]$descript <- substr(grpOcup[substr(grpOcup$descript,1,4)==grpOcup$cod,]$descript,6,nchar(grpOcup[substr(grpOcup$descript,1,4)==grpOcup$cod,]$descript))
        grpOcup$letters <- NULL
        grpOcup$test <- NULL
        grpOcup <- grpOcup[,c(2,1)] #reordenar colunas

        pnResult[['grpOcup']] <- grpOcup
      } #grpOcup
      else if (vFileObj=='codOcup') {
        codOcup <- as.data.frame(scan(file = pnFiles[,nFile], na.strings = c(' ','',NA),what = list(cod=character()), skip = 2, blank.lines.skip = TRUE, sep = '\n', encoding = 'UTF-8'))
        #definir codigos com descricoes
        codOcup$letters <- is.letter(codOcup$cod)
        codOcup <- transform(codOcup, cod = as.character(cod))
        codOcup$test <- c(FALSE,codOcup[1:(nrow(codOcup)-1),'letters']==TRUE & codOcup[2:nrow(codOcup), 'letters']==FALSE)
        codOcup$descript <- ifelse(codOcup$test==TRUE,as.character(codOcup[2:nrow(codOcup), 'cod']),codOcup$cod)
        codOcup$cod <- ifelse(codOcup$test==FALSE,paste0(substr(codOcup[2:nrow(codOcup), 'cod'],1,2),'00'),codOcup$cod)
        #remover entrada 2 - duplicada
        codOcup$test <- c(FALSE,codOcup[1:(nrow(codOcup)-1),'descript']==codOcup[2:nrow(codOcup), 'descript'])
        codOcup <- codOcup[codOcup$test==FALSE,]
        #remover testes
        codOcup$letters <- NULL
        codOcup$test <- NULL

        pnResult[['codOcup']] <- codOcup
      } #codOcup
      else if (vFileObj=='grpAtiv') {
        grpAtiv <- as.data.frame(read.table(pnFiles[,nFile], sep=';', header=FALSE, na.strings=c("",'NA'), skip = 6, col.names = c('cod','descript'), stringsAsFactors=FALSE, encoding = "latin1"))
        grpAtiv[is.na(grpAtiv$descript),]$descript <- grpAtiv[is.na(grpAtiv$descript),]$cod
        #copia codigo de GRUPO da proxima linha 
        grpAtiv$letters <- is.letter(grpAtiv$cod)
        grpAtiv$test <- c(is.na(grpAtiv[1:(nrow(grpAtiv)-1),'cod'])==TRUE & grpAtiv[2:nrow(grpAtiv), 'letters']==FALSE,FALSE)
        grpAtiv$cod <- ifelse(grpAtiv$test==TRUE,paste0(substr(grpAtiv[2:nrow(grpAtiv), 'cod'],1,2),'000'),grpAtiv$cod)
        #copia codigo de TITULO da proxima linha
        grpAtiv$letters <- is.letter(grpAtiv$cod)
        grpAtiv$test <- c(grpAtiv[1:(nrow(grpAtiv)-1),'letters']==TRUE & grpAtiv[2:nrow(grpAtiv), 'letters']==FALSE,FALSE)
        grpAtiv$cod <- ifelse(grpAtiv$test==TRUE,paste0(substr(grpAtiv[2:nrow(grpAtiv), 'cod'],1,2),'000'),grpAtiv$cod)
        grpAtiv$cod <- ifelse(grpAtiv$test==FALSE & grpAtiv$letters==TRUE,paste0(substr(grpAtiv[2:nrow(grpAtiv), 'cod'],1,1),'0000'),grpAtiv$cod)
        #remover testes
        grpAtiv$letters <- NULL
        grpAtiv$test <- NULL

        pnResult[['grpAtiv']] <- grpAtiv
      } #grpAtiv
      else if (vFileObj=='codAtiv') {
        codAtiv <- as.data.frame(scan(file = pnFiles[,nFile], na.strings = c(' ','',NA),what = list(cod=character()), skip = 2, blank.lines.skip = TRUE, sep = '\n', encoding = 'UTF-8'))
        codAtiv <- transform(codAtiv, cod = as.character(cod))
        #copia DESCRICAO da proxima linha 
        codAtiv$letters <- is.letter(codAtiv$cod)
        codAtiv$descript <- ifelse(codAtiv$letters==TRUE,codAtiv$cod,codAtiv[2:nrow(codAtiv), 'cod'])
        #remover entrada 2 - duplicada
        codAtiv$test <- c(FALSE,codAtiv[1:(nrow(codAtiv)-1),'descript']==codAtiv[2:nrow(codAtiv), 'descript'])
        codAtiv <- codAtiv[codAtiv$test==FALSE,]
        #copia codigo de GRUPO da proxima linha
        codAtiv$test <- c(codAtiv[1:(nrow(codAtiv)-1),'letters']==TRUE & codAtiv[2:nrow(codAtiv), 'letters']==FALSE,FALSE)
        codAtiv$cod <- ifelse(codAtiv$test==TRUE,paste0(substr(codAtiv[2:nrow(codAtiv), 'cod'],1,2),'000'),codAtiv$cod)
        #remover testes
        codAtiv$letters <- NULL
        codAtiv$test <- NULL

        pnResult[['codAtiv']] <- codAtiv
      } #codAtiv
      else if (vFileObj=='unEquiv') {
        unEquiv <- as.data.frame(scan(file = pnFiles[,nFile], na.strings = c(' ','',NA),what = list(descript=character()), skip = 13, blank.lines.skip = TRUE, sep = '\n', encoding = 'UTF-8'))
        unEquiv <- transform(unEquiv, descript = as.character(descript))
        unEquiv$letters <- is.letter(unEquiv$descript)
        unEquiv$test <- c(unEquiv[1:(nrow(unEquiv)-1),'letters']==TRUE & unEquiv[2:nrow(unEquiv), 'letters']==FALSE,FALSE)
        unEquiv$eqvm2 <- ifelse(unEquiv$test==TRUE,unEquiv[2:nrow(unEquiv), 'descript'],unEquiv$descript)
        unEquiv <- unEquiv[unEquiv$letters==TRUE,] #" \t\n\r\v\f" # space, tab, newline, carriage return, vertical tab, form feed
        unEquiv$eqvm2 <- as.numeric(gsub(",", "\\.", gsub("\\s", "", unEquiv$eqvm2) ))
        unEquiv <- transform(unEquiv, eqvm2 = as.double(eqvm2))
        #remover testes
        unEquiv$letters <- NULL
        unEquiv$test <- NULL

        pnResult[['unEquiv']] <- unEquiv
      } #unEquiv
      else if (vFileObj=='tpAtivFis') {
        tpAtivFis <- as.data.frame(read.table(pnFiles[,nFile], sep=';', header=FALSE, na.strings=c("",'NA'), skip = 4, col.names = c('cod','descript'), stringsAsFactors=FALSE, encoding = "latin1"))
        tpAtivFis <- tpAtivFis[!is.na(tpAtivFis$descript),]
        tpAtivFis$descript <- trimws(gsub("\\s+", " ",tpAtivFis$descript)) #retirar multiplos espacos

        pnResult[['tpAtivFis']] <- tpAtivFis
      } #tpAtivFis
      else if (vFileObj=='tpModEsp') {
        tpModEsp <- as.data.frame(read.table(pnFiles[,nFile], sep=';', header=FALSE, na.strings=c("",'NA'), skip = 4, col.names = c('cod','descript'), stringsAsFactors=FALSE, encoding = "latin1"))
        tpModEsp <- tpModEsp[!is.na(tpModEsp$descript),]
        tpModEsp$descript <- trimws(gsub("\\s+", " ",tpModEsp$descript)) #retirar multiplos espacos

        pnResult[['tpModEsp']] <- tpModEsp
      } #tpModEsp
      else {
        #sem regra
      } #outros
      rm(vFileObj)
    } # for nome var arquivos

    return(pnResult)
  } #if vNames!=''
  else return(NA)

} #getPNADanx - retorna dados da tabela anexos i a viii

dfQueryCreate <- function(iYear,iDataframe,iTabName,iPriSrc,iDFlayout,lDrop,lCreIns,lSelect) {
  if (missing(iDataframe) || !is.data.frame(iDataframe)) stop('Obrigatorio passar o dataframe modelo e dados para tabela!') #pnDFsuport
  #if (missing(pnDFsuport) || is.null(pnDFsuport) || is.na(pnDFsuport)) stop('Obrigatorio passar o dataframe de definicoes de dados!')
  if (missing(iTabName) || is.null(iTabName) || is.na(iTabName)) stop('Obrigatorio informar nome fisico da tabela!')
  if (missing(iYear) || is.null(iYear) || is.na(iYear)) iYear <- 0
  if (missing(iPriSrc) || is.null(iPriSrc) || is.na(iPriSrc)) stop('Informar o tipo de conexao, base!')
  if (missing(lDrop) || is.null(lDrop) || is.na(lDrop)) lDrop <- FALSE
  if (missing(lCreIns) || is.null(lCreIns) || is.na(lCreIns)) lCreIns <- TRUE
  if (missing(lSelect) || is.null(lSelect) || is.na(lSelect)) lSelect <- FALSE
  if (is.null(iDFlayout) || is.na(iDFlayout)) rm(iDFlayout)

  pnTempSQL <- data.frame(ID=integer(), Year=integer(), Querie=character(), stringsAsFactors=FALSE)

  if (lDrop==TRUE) {  #1.DROP
    if (startsWith(iPriSrc,'MSSQLDB')==TRUE) { sqlQuerie <- paste0(
       'IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N\'[dbo].[',iTabName,']\') AND type in (N\'U\')) ',
       'DROP TABLE [dbo].[',iTabName,']') } #MSSQLDB
    else { sqlQuerie <- paste0('DROP TABLE ',iTabName,';') } #SQLITE
    pnTempSQL <- rbind(pnTempSQL, data.frame(ID=1, Year=iYear, Querie=sqlQuerie))
  } #lDrop

  if (lCreIns==TRUE) {  #2.CREATE / 9.INSERT*(DEMO)
    if (startsWith(iPriSrc,'MSSQLDB')==TRUE) { 
      sqlQuerie <- paste0('CREATE TABLE dbo.',iTabName,' (')
      insQuerie <- paste0('INSERT INTO [dbo].[',iTabName,'] (')
    } #MSSQL
    else { 
      sqlQuerie <- paste('CREATE TABLE IF NOT EXISTS ',iTabName,' (',sep='')
      insQuerie <- paste0('INSERT INTO ',iTabName,' (')
    } #SQLITE id integer PRIMARY KEY

    for (iLine in names(iDataframe)) {
      sqlTypeD <- tolower(class(iDataframe[[iLine]])) #iDataframe[,iLine] text [+factor,datetime-iso8601], integer [+boolean,datetime-sec], real [float,datetime-julian], blob, null
      iLength <- ifelse(exists('iDFlayout'), iDFlayout[iDFlayout[,'Labels']==iLine,]$Widths, 50)

      if(sqlTypeD=='character') sqlTypeD <- ifelse(startsWith(iPriSrc,'MSSQLDB')==TRUE, paste0('varchar(',iLength,')'), 'character')
      if(sqlTypeD=='factor') sqlTypeD <- ifelse(startsWith(iPriSrc,'MSSQLDB')==TRUE, paste0('varchar(',iLength,')'), 'text')
      if(sqlTypeD=='logical') sqlTypeD <- ifelse(startsWith(iPriSrc,'MSSQLDB')==TRUE, 'bit', 'integer')
      if(sqlTypeD=='integer') sqlTypeD <- ifelse(startsWith(iPriSrc,'MSSQLDB')==TRUE, 'int', 'integer')
      if(sqlTypeD=='numeric') sqlTypeD <- ifelse(startsWith(iPriSrc,'MSSQLDB')==TRUE, 'float(24)', 'real')

      sqlQuerie <- paste0(sqlQuerie,ifelse(endsWith(sqlQuerie,'(')==TRUE,'',', '),ifelse(startsWith(iPriSrc,'MSSQLDB')==TRUE, toupper(iLine), iLine),' ',sqlTypeD)
      insQuerie <- paste0(insQuerie,ifelse(endsWith(insQuerie,'(')==TRUE,'',', '),ifelse(startsWith(iPriSrc,'MSSQLDB')==TRUE, toupper(iLine), iLine))
    } #for ,max(nchar(pnadcLayout$Description), na.rm = TRUE) * 2

    if (startsWith(iPriSrc,'MSSQLDB')==TRUE) {
      sqlQuerie <- paste(sqlQuerie,')',sep='')
      insQuerie <- paste0(insQuerie,') VALUES (',paste(rep('?',ncol(iDataframe)),collapse = ","),')') #executar com parametros
    } #MSSQL
    else {
      sqlQuerie <- paste(sqlQuerie,') [WITHOUT ROWID];',sep='')
      insQuerie <- paste0(insQuerie,') VALUES ')
    } #SQLITE
    pnTempSQL <- rbind(pnTempSQL, data.frame(ID=2, Year=iYear, Querie=sqlQuerie))
  } #lCreIns

  if (lSelect==TRUE) {  #3.SELECT
    if (startsWith(iPriSrc,'MSSQLDB')==TRUE) { sqlQuerie <- paste0('SELECT * FROM [dbo].[',iTabName,']') } #MSSQL
    else { sqlQuerie <- paste('SELECT * FROM ', iTabName, ';', sep='') } #SQLITE
    pnTempSQL <- rbind(pnTempSQL, data.frame(ID=3, Year=iYear, Querie=sqlQuerie))

    pnTempSQL <- rbind(pnTempSQL, data.frame(ID=9, Year=iYear, Querie=insQuerie)) #demo insert
    if (exists('iDFlayout')) { rm(iDFlayout) }
  } #lSelect

  pnResult <- pnTempSQL
  rm(sqlQuerie,sqlTypeD,iLine,iDataframe,pnTempSQL,insQuerie,iLength)

  return(pnResult)
} #dfQueryCreate
