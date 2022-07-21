# ETL 05.01 - Clusterização 02
    # Objetivo: Popular as tabelas no banco DDTM_OiMasterDados
    # Desenvolvedor: Marcelo Muniz de Alencar
################################################################

rm (list = ls ()) #exclui todos os objetos na memoria

#SetaAmbienteDeTrabalho#########################################

# Definindo area de trabalho 
setwd("C:/Users/MarceloMuniz/OneDrive/# Oi Master Dados - 3ª Fase/_TrabalhoFinal/docsDrive")
#getwd()

#CarregaPacotes#################################################

# Importando pacotes
#install.packages("readr")
library(readr)
#install.packages("caret")
library(caret)
#install.packages("klaR")
library(klaR)
#install.packages("cluster")
library(cluster)
#install.packages("RODBC")
library(RODBC)
#install.packages("stats")
library(stats)
#install.packages("dplyr")
library(dplyr)
#install.packages("ggplot2")
library(ggplot2)
#install.packages("ggfortify")
library(ggfortify)
#install.packages("NbClust")
library(NbClust)
#install.packages("factoextra")
library(factoextra)

##################### create dataframe #######################

# criando conexao
dbcon= odbcDriverConnect('driver={SQL Server};
                                  server=PCMARCELO\\SQLEXPRESS;
                                 database=DSTG_OiMasterDados;
                                 trusted_connection=true')

# criando dataframe
df <- sqlQuery(dbcon,
               " 
SELECT [CodSkCidade]
      ,[NomCidade]
      ,[MinimaCidade]
      ,[MediaCidade]
      ,[MaximaCidade]
      ,[DesvPadCidade]
      ,[Categoria]
FROM [DSTG_OiMasterDados].[OiMasterDados].[TOiSTG_BaseClusterCategoriaEstacao] (NOLOCK)
ORDER BY 1")
#View(df)

####################### dummies ##############################
df2 = cbind(df$Categoria)
df2 = as.data.frame(df2)
names(df2)[]<-c("Categoria_")
#View(head(df2))

dummy <- dummyVars(~ ., data = df2)
df3 <- predict(dummy, df2)
#View(head(df3))

df_final2 = cbind(df, df3)
#View(df_final2)

####################### gravar tabela ##########################

# cria lista de tabela da conexao dbcon2
listaTabelas <- sqlTables(dbcon, as.is = TRUE)
# View(head(listaTabelas))

if (sum(listaTabelas$TABLE_NAME == 'TOiSTG_BaseClusterCidadeR') == 1) {
  sqlDrop(dbcon, "OiMasterDados.TOiSTG_BaseClusterCidadeR")
} 
sqlSave(dbcon, dat = df_final2, "OiMasterDados.TOiSTG_BaseClusterCidadeR", rownames = FALSE)

##################### create dataframe #######################

# criando dataframe
df4 <- sqlQuery(dbcon,
               " 
SELECT [CodSkCidade]
      ,[NomCidade]
      ,sum([Categoria_Inverno1de3]	) as [Categoria_Inverno1de3]
      ,sum([Categoria_Inverno2de3]	) as [Categoria_Inverno2de3]
      ,sum([Categoria_Inverno3de3]	) as [Categoria_Inverno3de3]
      ,sum([Categoria_Outono1de3]	) as [Categoria_Outono1de3]
      ,sum([Categoria_Outono2de3]	) as [Categoria_Outono2de3]
      ,sum([Categoria_Outono3de3]	) as [Categoria_Outono3de3]
      ,sum([Categoria_Primavera2de3]) as [Categoria_Primavera2de3]
      ,sum([Categoria_Primavera3de3]) as [Categoria_Primavera3de3]
      ,sum([Categoria_Verão1de3]	) as [Categoria_Verão1de3]
      ,sum([Categoria_Verão2de3]	) as [Categoria_Verão2de3]
      ,sum([Categoria_Verão3de3]	) as [Categoria_Verão3de3]
FROM [DSTG_OiMasterDados].[OiMasterDados].[TOiSTG_BaseClusterCidadeR] (NOLOCK)
GROUP BY [CodSkCidade]
        ,[NomCidade]
ORDER BY 1")
#View(df4)

####################### gravar tabela ##########################
# cria lista de tabela da conexao dbcon2
listaTabelas2 <- sqlTables(dbcon, as.is = TRUE)
#View(listaTabelas)

if (sum(listaTabelas2$TABLE_NAME == 'TOiSTG_BaseClusterCidade2R') == 1) {
  sqlDrop(dbcon, "OiMasterDados.TOiSTG_BaseClusterCidade2R")
} 
sqlSave(dbcon, dat = df4, "OiMasterDados.TOiSTG_BaseClusterCidade2R", rownames = FALSE)

##################### create dataframe #######################

# criando dataframe
df5 <- sqlQuery(dbcon,
                " 
SELECT [Categoria_Inverno1de3]
      ,[Categoria_Inverno2de3]
      ,[Categoria_Inverno3de3]
      ,[Categoria_Outono1de3]
      ,[Categoria_Outono2de3]
      ,[Categoria_Outono3de3]
      ,[Categoria_Primavera2de3]
      ,[Categoria_Primavera3de3]
      ,[Categoria_Verão1de3]
      ,[Categoria_Verão2de3]
     -- ,[Categoria_Verão3de3]
FROM [DSTG_OiMasterDados].[OiMasterDados].[TOiSTG_BaseClusterCidade2R] (NOLOCK)
ORDER BY CodSkCidade")
View(df5)

# Graficos para definir quantidade de clusters 
fviz_nbclust(df5, kmeans, method = "wss", k.max = 10)
fviz_nbclust(df5, kmeans, method = "silhouette", k.max = 10)

# Gerar CLuster 
modelo <- kmeans(df5, centers = 5)
av_Modelo <- modelo$betweenss / modelo$totss
df_modelo <- as.data.frame(modelo$cluster)
colnames(df_modelo)[1] <- 'Cluster'
print(av_Modelo)
#View(df_modelo)

# Graficos com a divisao dos clusters 
# autoplot(modelo,df5,frame=TRUE)

# gerar dataframe base para carga no banco
df_final <- cbind(df4$CodSkCidade,
                   df4$NomCidade,
                   df_modelo$Cluster)
df_final = as.data.frame(df_final)
names(df_final)[]<-c("CodSkCidade","NomCidade","Cluster")
#View(df_final)

####################### gravar tabela K-MEANS ##########################

# cria lista de tabela da conexao dbcon
listaTabelas3 <- sqlTables(dbcon, as.is = TRUE) 
#View(listaTabelas2)

if (sum(listaTabelas3$TABLE_NAME == 'TOiSTG_BaseClusterCidade3R') == 1) {
  sqlDrop(dbcon, "OiMasterDados.TOiSTG_BaseClusterCidade3R")
}  
sqlSave(dbcon, dat = df_final, "OiMasterDados.TOiSTG_BaseClusterCidade3R", rownames = FALSE)

####################### desconecta BD ##########################
close(dbcon)