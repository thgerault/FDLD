import pandas as pd
import numpy as np
from os import listdir
import os
import time
import pyodbc

cnxn_str = ("Driver={ODBC Driver 17 for SQL Server};Server=srv-ang-bdd170;Database=Carto;Trusted_Connection=yes;")

path=r"C:\Users\geraultt\OneDrive - ADEME\Documents\documents\FDLD\Fichiers_export_open_data\\" #Chemin d'accès au répertoire qui stockera les fichiers csv à mettre sur Koumoul

start=time.time() # On déclenche un chrono pour avoir à la fin le temps d'exécution du programme

#################################################################################
# Récupération des vues aggrégé
#################################################################################

#######################################
# Récupérartion des nom de colonnes
#######################################

def info_view(cnxn_str):
    cnxn = pyodbc.connect(cnxn_str)
    cur = cnxn.cursor()

    cur.execute("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=N'V_LOC_EXPORT_CSV'") #Cette requête nous permet de récuperer tte les colonnes de la View. Pratique car il y en beaucoup
    donnee=cur.fetchall()  #Récupération de ces données dans l'environnement python
    
    cur.close()
    cnxn.close()

    colonne=[]
    for col in donnee:
        colonne.append(col[3]) # On transforme ces données en une liste
    return colonne

colonne_name=info_view(cnxn_str)
#print(colonne_name)

#######################################
# Données communales
#######################################

def view_com(cnxn_str):
    cnxn = pyodbc.connect(cnxn_str)
    cur = cnxn.cursor()

    cur.execute("SELECT * FROM dbo.V_LOC_EXPORT_CSV") # Requete sql pour récuperer toute les données de la vu
    donnee=cur.fetchall()  #Récupération des données dans l'environnement python
    
    cur.close()
    cnxn.close()
    
    liste_df=[]
    
    for i in range(len(donnee)):
        dico={} # A chaque tour de boucle, on initialise un dictionnaire. La boucle allant de 0 à la longueur de la vu pour parcourir toute les ligne de la vu
        for j in range(len(colonne_name)): # boucle allant de 0 au nombre de colonne de la vu pour nous permettre de parcourir chaque colonne
            dico[colonne_name[j]]=donnee[i][j] # création d'un dictionnaire pour chaque ligne de la vu. Ligne 1 cela donne: {"colonne1":valeur(ligne1,colonne1), "colonne2":...}
        liste_df.append(dico) # On stocke tout ces dictionnaire dans une liste pour la création du dataframe
    
    df_com=pd.DataFrame(liste_df) #création du dataframe pandas
    df_com=df_com.sort_values(by=["CODE_COMMUNE"]) # On range par ordre croissant des code communes (ordre alphabétique puisque qu'il y a des lettres)
    df_com.index=[i for i in range(len(df_com))] # On réarrange les index pour qu'il soit de 0 à longueur du dataframe
    
    # On supprime les colonnes qui sont inutiles
    del df_com["C_CODE_GEO_LOGEMENT"]
    del df_com["C_CODE_GEO_FILOSOFI"]
    del df_com["C_CODE_GEO_TOURISME"]
    del df_com["C_CODE_GEO_DEMOGRAPHIE"]
    del df_com["C_CODE_GEO_ARTIFICIALISATION"]

    return df_com

#data_view_com=view_com(cnxn_str)

###############################################
# Données départementales, regionales et epci
###############################################

def copy_df_com():
    """
    Cette fonction est la pour copier le dataframe précédent pour pouvoir le manipuler sans
    modifier celui d'origine. On va le manipuler pour aggreger les données.
    """
    data_view_com=view_com(cnxn_str) # On récupere le dataframe des données niveau communale
    df=np.copy(data_view_com) # On effectue la copie. Attention cette opération retourne une matrice et non un dataframe, c'est pourquoi on effectue la suite de la fonction
    liste_df=[]
    colonne=list(data_view_com.columns) # Une liste des colonnes du dataframe niveau communale
    for elt in df: # Pour chaque element dans la copie
        dico={} # On initialise à chaque fois un dictionnaire
        for i in range(len(elt)): # boucle allant de 0 au nombre d'élément de chaque ligne de la matrice (la copie) ce qui correspond au ligne du dataframe
            dico[colonne[i]]=elt[i] # On recréer un dictionnaire
        liste_df.append(dico) #Qu'on met dans une liste

    new_df=pd.DataFrame(liste_df) # et on genere un dataframe qui sera la copie des données niv communale
    return new_df

def tx_en_valeur(dataframe,variable_ref,taux):
    """
    Cette fonction permet de tranformer une donnée qui est une proportion (un %) en un vrai 
    nombre déduit d'un autre indicateur dont le % était calculer. 
    Exemple: la population municipale et le taux de femmes parmi la population.
    """
    # Cette liste calcule la vrai valeur chiffré du nombre de femme parmi la population
    tx=[dataframe[variable_ref][i]*(dataframe[taux][i]/100) if dataframe[taux][i] != None else None for i in range(len(dataframe))] 
    dico_tx={taux:tx} # On créer un dictionaire {"TX_F": [...] }
    df_tx=pd.DataFrame(dico_tx) # # On créer un dataframe d'une colonne représentant le nombre de femme parmi les populations municipales
    dataframe[taux]=df_tx[taux] # Et on remplace les % par ces vrai valeurs
    return dataframe

def recalc_tx(dataframe,variable_ref,taux):
    """
    Cette fonction effectue le chemin inverse, elle calcul un %.
    On applique cette fonction une fois que l'on a aggrégé les données pour recalculer les taux.
    """
    for i in range(len(dataframe)):
        if dataframe[taux][i] != 0 and dataframe[taux][i] != None: # une condition pour éviter de diviser par 0 ou par None
            dataframe[taux][i]=round((dataframe[taux][i]/dataframe[variable_ref][i])*100,3) #recalcule du taux, la commande round() permet d'arrondir le résultat à 3 chiffres après la virgule
    return dataframe


def data_view(theme):
    """
    Cette fonction va générer le dataframe des données aggrégé en fonction du niveau renseigner en argument.
    Soit "CODE_DEPARTEMENT", soit "CODE_REGION", soit "CODE_EPCI".
    """
    df=copy_df_com() # On effectue la copie du dataframe niv communale

    tx_en_valeur(df,"POP_MUN","TX_F") # On replace les taux par les valeurs chiffrés et non les %

    # On supprime les colonnes qui ne nous intérressent pas en fonction du thème choisi
    del df["CODE_COMMUNE"]
    del df["COMMUNE"]
    del df["STATUT"]
    if theme =="CODE_DEPARTEMENT":
        del df["CODE_REGION"]
        del df["CODE_EPCI"]
    elif theme == "CODE_REGION":
        del df["CODE_DEPARTEMENT"]
        del df["CODE_EPCI"]    
    elif theme =="CODE_EPCI":    
        del df["CODE_DEPARTEMENT"]
        del df["CODE_REGION"]

    df=df.groupby(theme).aggregate('sum') # On aggrège les données sur le theme renseigné avec la fonction somme (équivalent du GOUPBY en sql)

    recalc_tx(df,"POP_MUN","TX_F") # On recalcule les taux une fois les données aggrégé

    #df=df.reset_index()
    return df

#data_view=view("CODE_REGION")
#print(data_view)

#################################################################################
# Export fichier csv
#################################################################################

dossier=[f for f in listdir(path)]

def export_csv():
    # Indication des nom des fichiers csv qui seront exporté
    filename_com="Donnée_communale.csv"
    filename_dep="Donnée_départementale.csv"
    filename_reg="Donnée_régionale.csv"
    filename_epci="Donnée_EPCI.csv"
    # Création d'une liste de tuple : [(nom du fichier csv, le dataframe associé), (...), ...]
    filename=[(filename_com,view_com(cnxn_str)),(filename_dep,data_view("CODE_DEPARTEMENT")),(filename_reg,data_view("CODE_REGION")),(filename_epci,data_view("CODE_EPCI"))]

    for fichier in filename: # pour chaque tuple de cette liste
        if fichier[0] in dossier: # Si le 1er élément de chaque tuple (donc le nom du fichier) est déjà présent dans le répertoire :
            os.remove(path+fichier[0]) # Alors on le supprime
            fichier[1].to_csv(path+fichier[0],sep=";") # On génère le fichier csv
        else:
            fichier[1].to_csv(path+fichier[0],sep=";") #Sinon on génère tout simplement le csv et on le déplace dans le répertoire.

export_csv() # On lance la fonction des exports en fichier csv

end=time.time()
print("Temps d'exécution : ",end-start,"secondes")  # Fin du chrono