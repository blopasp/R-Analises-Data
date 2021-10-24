library(tidyverse)
library(lubridate)
library(readxl)
library(gdata)
library(RODBC)
library(mondate)
library(zoo)
library(qcc)
library(openxlsx)

# Consulta base de produtos (base_produtos) ####################################

base_produtos <- str_c(consulta, pattern = " ")

# Consulta categorias (categoria) ##############################################
categoria <- str_c(consulta, pattern = " ")

# Consulta notas fiscais (notas_fiscais) #######################################
notas_ficais <- str_c(consultas, pattern = " ")

# Consulta motivos (base_motivos) #######################################
base_motivo <- str_c(consultas, pattern = " ")

# Consulta embalagem minima (base_minima) ######################################
base_minima <- str_c(consultas, pattern = " ")

# Consulta Fornecedor-comprador (base_comp) ####################################
base_comp <- str_c(consultas, pattern = " ")
