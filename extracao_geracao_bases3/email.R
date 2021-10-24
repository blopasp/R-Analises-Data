library(RDCOMClient)
library(stringr)
library(lubridate)
library(scales)
library(purrr)

#criando metricas para a base lançamento 
soma_lanc<-round(sum(ifelse(base$FLAG=="NOVO", (base$TT_CMPG),0))/1000000,2)
lanc<-gsub('//.', ',', soma_lanc)
soma_elanc<-round(sum(base_lanc$TT_CMPG)/1000000,2)
elanc<-gsub('//.', ',', soma_elanc)
per_lanc<-as.character(percent(soma_elanc/soma_lanc))

gc()

#criando metricas para a base regular
soma_reg<-round(sum(ifelse(base$FLAG=="ANTIGO", (base$TT_CMPG),0))/1000000,2)
reg<-gsub('//.', ',', soma_reg)
soma_ereg<-round(sum(base_reg$TT_CMPG)/1000000,2)
ereg<-gsub('//.', ',', soma_ereg)
per_reg<-as.character(percent(soma_ereg/soma_reg))

gc()

dia<-today()-1
m<-month(dia)
ano<-format(dia, '%y')
dia<-format(dia, '%d/%m')
dia<-as.character(dia)
mes<-c("jan", "fev", "mar", "abr", "maio", "jun", "jul", "ago", "set", "out", "nov", "dez")
mes_atual<-mes[m]
data<-paste0(mes_atual, "/", ano)

if(day(today()) > 5 & day(today())<=16){
  assunto<-str_c("Entradas Excesso CD (prévia)")
}else{
  assunto<-str_c("Entradas Excesso CD")
}


gc()

corpo<-str_glue('
<html>
<body>
<head>
<td>
<font face="Calibri">
<font size = 3>
  <b>Bom dia, prezados</b>
  <br>
  <br>
  As bases das entradas de produtos regulares e lancamentos de {data}, até o dia <b>{dia}</b>.
  <br>
  <br>
  &nbsp Total entrada lancamentos {data}: <b>R$ {soma_lanc} M</b><br>
  &nbsp Excesso lancamentos {data}: <b>R$ {soma_elanc} M({per_lanc})</b><br>
  <br>
  &nbsp Total entrada regular {data}: <b>R$ {soma_reg} M</b><br>
  &nbsp Excesso regular {data}: <b>R$ {soma_ereg} M ({per_reg})</b>

  <br>
  <br>
  Atenciosamente,
  <br><br>
</font>
</font>
</td>
</head>
</body>
</html>')

corpo<-as.character(corpo, Encoding('UTF-8'))

corpo<-iconv(corpo, from="UTF-8", to="latin1//TRANSLIT")

assunto<-iconv(assunto, from="UTF-8", to="latin1//TRANSLIT")

gc()

OutApp <- COMCreate("Outlook.Application")

entMail = OutApp$CreateItem(0)

entMail$GetInspector()
sign = entMail[["HTMLBody"]]

entMail[["To"]] = c("EMAILs SENDNG")
entMail[["bcc"]] = EMAIL
entMail[["subject"]] = assunto
entMail[["htmlbody"]] = paste0(corpo,sign)

entMail[["SentOnBehalfOfName"]] = "pablopereira@pmenos.com.br"

attachments <- c("C:/Estudos/ENTRADA CD/bases/base_lancamento.xlsx",
                 "C:/Estudos/ENTRADA CD/bases/base_regular.xlsx")

purrr::map(attachments, ~ entMail[["attachments"]]$Add(.))

entMail$Display()

#entMail$Send()

gc()
