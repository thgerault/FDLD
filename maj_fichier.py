import pandas as pd
from os import listdir
import os
import time
import pyodbc

cnxn_str = ("Driver={ODBC Driver 17 for SQL Server};Server=srv-ang-bdd070;Database=Carto;Trusted_Connection=yes;") # connexion au serveur sql

path= r"C:\Users\geraultt\OneDrive - ADEME\Documents\documents\FDLD\\"
path_donnee=path+"base de donnée" #chemin d'accès au repertoire des données
path_meta=path+"metadonnée" #chemin d'accès au métadonnée
path_reinitialise=path+"réinitialiser_table\\"

start=time.time()
#########################################################################
# Sauvegarde des imports et indicateur
#########################################################################

"""
On effectue pour commencer, une sauvegarde de la table LOC_FICH avant mise à jour. On sauvegarde dans un dataframe pandas.
Cette sauvegarde va nous permettre par la suite d'effectuer des comparaison pour savoir quelles fichiers sont ajouter/supprimer/recemment importer/... 
"""

def imports_initial(cnxn_str):
    cnxn = pyodbc.connect(cnxn_str) # connexion au table sql
    cur = cnxn.cursor()

    cur.execute("SELECT * FROM dbo.LOC_FICH") # requete sql pour récupérer le contenu de la table
    base=cur.fetchall()  #recupération du résultat de la requète en python. C'est récupérer de la forme [(fichier,theme,annee,anne geo,...), (...), ...]
    
    cur.close()
    cnxn.close() # fermeture de la connection

    if base != []:
        fichier=[]

        for elt in base:
            if elt[7]==False: elt[7]=0 # Les données booléennes des B_impr et B_EXPR sont récupérer en True/False dans la commande précédente, donc on les change en 0 ou 1
            else: elt[7]=1
            if elt[8]==False: elt[8]=0 
            else: elt[8]=1

            # Créer une liste de dictionnaire ou chaque dictionnaire correspond à 1 ligne du futur dataframe pandas. Les clefs des dictionnaire seront les noms des colonnes.
            fichier.append({"ID":elt[0],"FICHIER":elt[1],"ANNEE":elt[2],"ANNEE_GEO":elt[3],"THEME":elt[4],"SOURCE":elt[5],"NIVGEO":elt[6],"IMPORT":elt[7],"EXPORT":elt[8]})

    else: 
        fichier=[]
        fichier.append({"ID":None,"FICHIER":None,"ANNEE":None,"ANNEE_GEO":None,"THEME":None,"SOURCE":None,"NIVGEO":None,"IMPORT":None,"EXPORT":None})

    df=pd.DataFrame(fichier) #création du dataframe à partir de la liste de dictionnaire
    return df

sauvegarde_import=imports_initial(cnxn_str) # On récupère ce dataframe de sauvegarde pour les comparaison qui vont suivre
#print(sauvegarde_import,"\n")

#########################################################################
# Scan répertoire
#########################################################################

"""
On effectue dans un 1er temps un scan du répertoire des données. On scan également le repertoire des métadonnées et on identifie quelle métadonnée 
représente telle fichier de donnée (pour les fichiers de provenance INSEE). 
La 1er fonction retourne donc une liste de tuple => [(donnee,metadonnee) , (donnee,metadonnee) , ...]
Si on ne trouve pas de correspondance avec le répertoire des métadonnées, alors c'est qu'il ne s'agit pas d'un fichier INSEE donc on aura un tuple 
comme ceci: (donnee,)

Remarque: cette fonction scan ne retourne uniquement les fichiers qui ont été ajouté !!! La condition IF à la ligne 65 test si le fichier 
          en question se trouve parmi les fichiers présent dans la sauvegarde.

il s'agit du 1er point de la procédure maj de LOC_FICH
"""

def scan_fich_ajouter(path_donnee):
    fichiers = [f for f in listdir(path_donnee)] # liste de tout les éléments du répertoire base de donnée
    metadonnee = [f for f in listdir(path_meta)] # liste de tout les éléments du répertoire base de métadonnée
    liste_fichier_meta=[]
    for base in fichiers: # pour chaque fichiers du répertoire base de donnée
        if base not in list(sauvegarde_import["FICHIER"]): # si le fichier n'est pas dans la table LOC_FICH actuelle, c'est un nouveau fichier donc:
            match=False
            for meta in metadonnee: # pour chaque fichier méta du répertoire des métadonnées
                if meta[5:]==base: # si le nom du fichier est dans le nom d'un fichier de méta
                    liste_fichier_meta.append((base,meta)) # on associe le fichier et sa méta par un tuple que l'on stock dans une liste
                    match=True
            if match==False:
                liste_fichier_meta.append((base,)) # Sinon, si on ne trouve pas de métadonnées, alors soit on ne l'a pas renseigner, soit il s'agit d'un fichier non INSEE. Donc on ajoute le tuple (fichier,)
        
    return liste_fichier_meta

print("\n""Scan: ",scan_fich_ajouter(path_donnee),"\n") 

"""
On récupère les fichiers souhaitant être importer avant la mise à jour. Cette fonction retourne donc une liste des fichiers importés
"""

def fichier_importer(cnxn_str):
    liste_import=[]
    sauvegarde_import=imports_initial(cnxn_str)
    for i in range(len(sauvegarde_import)): # Pour chaque ligne de loc_fich
        if sauvegarde_import["IMPORT"][i]==1: # si le B_IMPR est à 1
            liste_import.append(sauvegarde_import["FICHIER"][i]) # on stock le fichier dans une liste

    return liste_import

liste_import=fichier_importer(cnxn_str)
#print("Fichiers importés: ",liste_import,"\n")

"""
On récupère les fichiers supprimés c'est à dire, après le scan, les fichiers se trouvant dans la sauvegarde mais pas dans le scan. 
Cette fonction retourne donc une liste des id des fichiers supprimés.
"""

def fichiers_corbeille(cnxn_str):
    id_fich=[]
    fichiers = [f for f in listdir(path_donnee)]
    sauvegarde_import=imports_initial(cnxn_str) 

    for i in range(len(sauvegarde_import)): # boucle for parccourant la sauvegarde 
        if sauvegarde_import["FICHIER"][i] not in fichiers: 
            if sauvegarde_import["FICHIER"][i] != None:
                id_fich.append(sauvegarde_import["ID"][i]) # On récupère les id des fichiers qui ne sont pas présent dans le répertoire mais présent en base.(fichier supprimé)

    return id_fich

liste_corbeille= fichiers_corbeille(cnxn_str)
print("Fichier supprimés du répertoire: ",liste_corbeille,"\n")

#########################################################################
# Mise à jour des imports
#########################################################################

#################################################
# Mise à jour fichier avec un nouveau dataframe
#################################################

"""
Créer un nouveau dataframe avec les mêmes colonnes que la table LOC_FICH en base (fichier,année, annee géo,...)
On remplit les informations du fichiers en fonction de s'il provient de l'INSEE ou non
"""

def df_maj_import(path_donnee):
    df_import=[]
    liste_fichier_meta=scan_fich_ajouter(path_donnee)
    for i in range(len(liste_fichier_meta)): #pour chaque tuple générer par la fonction scan_fich_ajouter
        if "insee" in liste_fichier_meta[i][0]: # si le caractère "insee" est dans le 1er élément de chaque tuple (c'est à dire le nom du fichier csv)
            # On ajoute à la liste un dictionnaire caractérisant les données d'une ligne du future dataframe qui sera créer.
            df_import.append({"FICHIER":liste_fichier_meta[i][0],"ANNEE_DONNEE":liste_fichier_meta[i][0][-16:-12], "ANNEE_GEOGRAPHIQUE":liste_fichier_meta[i][0][-8:-4],"THEME":liste_fichier_meta[i][0][11:-17],"SOURCE":"INSEE","NIVGEO":"COM","IMPORT":0,"EXPORT":0})
        else: # sinon, on remplit quand même la ligne mais on ne renseigne pas l'année, l'année geographique, le thème et le niveau géographique.
            df_import.append({"FICHIER":liste_fichier_meta[i][0],"ANNEE_DONNEE":None, "ANNEE_GEOGRAPHIQUE":None,"THEME":None,"SOURCE":"AUTRE","NIVGEO":None,"IMPORT":0,"EXPORT":0})
    
    Import=pd.DataFrame(df_import) # création du dataframe pandas
    return Import

"""
On appelle cette fonction uniquement si ce dataframe n'est pas vide. Car s'il était vide, il n'y aurait pas de mise à jour à faire.
"""

importer=df_maj_import(path_donnee)
if len(importer)!=0: # on ne l'affiche que s'il est non vide
    print(importer)

print("\n""Patientez... La table va bientôt s'afficher""\n")

#################################################
# Mise à jour dans sql serveur
#################################################

"""
Il s'agit de l'insertion des nouvelles données en bases. On définit une fonction insert_into qui rempli ligne par ligne 
à chaque fois quelle est appelé. On insert les données du dataframe constitué des nouvelles données. On a deux
fonctions, une insérant les données des fichiers insee et une autre pour les fichiers non insee.
"""

def insert_into_insee(fichier,annee_donnee,annee_geo,theme,source,nivgeo,a_importer,exporter):
	cnxn = pyodbc.connect(cnxn_str)
	cur = cnxn.cursor()

	value = (fichier,annee_donnee,annee_geo,theme,source,nivgeo,a_importer,exporter)
	cur.execute("INSERT INTO dbo.LOC_FICH(L_FICH, N_ANNEE_DONNEE, N_ANNEE_GEOG, L_THEME,L_SOURCE,L_NIVGEO,B_IMPR,B_EXPR) VALUES (?, ?, ?, ?, ?, ?, ?,?)",value)
	cnxn.commit()
	cur.close()
	cnxn.close()

def insert_into_autre(fichier,source,a_importer,exporter):
	cnxn = pyodbc.connect(cnxn_str)
	cur = cnxn.cursor()

	value = (fichier,source,a_importer,exporter)
	cur.execute("INSERT INTO dbo.LOC_FICH(L_FICH,L_SOURCE,B_IMPR,B_EXPR) VALUES (?, ?, ?, ?)",value)
	cnxn.commit()
	cur.close()
	cnxn.close()

def insert_import(importer):
    for i in range(len(importer)):
        if importer["SOURCE"][i]=="INSEE":
            insert_into_insee(importer["FICHIER"][i],int(importer["ANNEE_DONNEE"][i]),int(importer["ANNEE_GEOGRAPHIQUE"][i]),importer["THEME"][i],importer["SOURCE"][i],importer["NIVGEO"][i],int(importer["IMPORT"][i]),int(importer["EXPORT"][i]))
        else:
            insert_into_autre(importer["FICHIER"][i],importer["SOURCE"][i],int(importer["IMPORT"][i]),int(importer["EXPORT"][i]))

if len(importer) !=0: # on appelle la fonction uniquement si le dataframe est non vide
    insert_import(importer)

#######################################################
# Gérer les 2 cas de suppression
#######################################################

"""
On a 2 cas de suppression :

    -> Si le fichier supprimé (n'est plus présent dans le répertoire mais l'est en base) est lié à des données dans LOC_DATA alors on ne fais rien d'autre 
       que de mettre l'import à 0 en base.
    -> Si le fichier supprimé n'a pas de données dans LOC_DATA alors on le supprime en base et ses indicateurs dans LOC_FICH_COLN

On commence alors par récupérer toute la table LOC_DATA dans un dataframe pandas.
"""

def loc_data(cnxn_str):
    cnxn = pyodbc.connect(cnxn_str)
    cur = cnxn.cursor()

    cur.execute("SELECT * FROM dbo.LOC_DATA")
    base=cur.fetchall()  
    
    cur.close()
    cnxn.close()

    donnee=[]
    for elt in base:
        donnee.append({"ID_FICHIER":elt[0],"ID_INDC":elt[1],"CODGEO":elt[2],"ANNEE":elt[3],"VAL_INDC":elt[4]})

    df=pd.DataFrame(donnee)
    return df

df_Loc_Data=loc_data(cnxn_str)

"""
La fonction suivante cherche parmi les fichiers qui ont été supprimé, s'il y a des données correspondantes dans LOC_DATA
ainsi soit on update, soit on delete.
"""

def sup_ou_pas(cnxn_str,df_Loc_Data):
    liste_corbeille=fichiers_corbeille(cnxn_str) # Fichiers supprimer après comparaison entre le scan et en base. Se sont des id fichiers
    liste_fich_data=list(set(df_Loc_Data["ID_FICHIER"])) # liste des id fichier de loc_data
    cnxn = pyodbc.connect(cnxn_str)
    cur = cnxn.cursor()
    for id_corbeille in liste_corbeille: # pour chaque id fichier de la liste des fichiers supprimées
        if id_corbeille in liste_fich_data: # si l'id est dans la liste des id fichier de loc_data
            cur.execute("UPDATE dbo.LOC_FICH SET B_IMPR = 0 WHERE ID_LOC_FICH= ?",(int(id_corbeille),)) # alors on execute une requete sql mettant à jour le B_IMPR de ce fichier à 0
        else: 
            cur.execute("DELETE FROM dbo.LOC_FICH WHERE ID_LOC_FICH= ?",(int(id_corbeille),)) # sinon on supprime dans loc_fich
            cur.execute("DELETE FROM dbo.LOC_FICH_COLN WHERE ID_LOC_FICH= ?",(int(id_corbeille),)) # et dans loc_fich_coln
    cur.commit()
    cur.close()
    cnxn.close()
    

if len(liste_corbeille)!=0: # on applique la fonction précédente uniquement si on a des fichiers qui ont été supprimé
    sup_ou_pas(cnxn_str,df_Loc_Data)

print(imports_initial(cnxn_str)) # On affiche la nouvelle table loc_fich, il ne manque plus que la partie manuelle.


#######################################################
# Export de fichier Excel des tables loc_fich, loc_fich_coln et loc_indc
#######################################################

"""
Cette parti là est faite pour sauvegarder une version avant mise à jour des tables
car imaginons que nous nous sommes trompé au cours de route, il faut avoir la possibilité de revenir en arriere.
C'est une fonction Retour en quelque sorte.
"""

def loc_fich_coln(cnxn_str):
    cnxn = pyodbc.connect(cnxn_str)
    cur = cnxn.cursor()

    cur.execute("SELECT * FROM dbo.LOC_FICH_COLN")
    indc=cur.fetchall()  
    
    cur.close()
    cnxn.close()

    fichier=[]

    for elt in indc:
        if elt[7]==False: elt[7]=0 # idem que pour la table LOC_FICH
        else: elt[7]=1
        fichier.append({"ID":elt[0],"FICHIER":elt[1],"INDC_INSEE":elt[2],"INDC_ADEME":elt[3],"LIBELLE":elt[4],"DEFINITION":elt[5],"ANNEE_DONNEE":elt[6],"IMPORT":elt[7]})

    df=pd.DataFrame(fichier)
    return df

def loc_indc(cnxn_str):
    cnxn = pyodbc.connect(cnxn_str)
    cur = cnxn.cursor()

    cur.execute("SELECT * FROM dbo.LOC_INDC")
    base=cur.fetchall()  
    
    cur.close()
    cnxn.close()

    fichier=[]

    for elt in base:
        fichier.append({"ID":elt[0],"INDC":elt[1],"LIBELLE":elt[2],"THEME":elt[3]})

    df=pd.DataFrame(fichier)
    return df

filename_fichier="loc_fich_sav.xlsx" # la sauvegarde de loc_fich est déjà généré au début du script, on a donc juste à l'exporter en Excel.

df_loc_fich_coln=loc_fich_coln(cnxn_str)
filename_correspondance="loc_fich_coln_sav.xlsx"

df_loc_indc=loc_indc(cnxn_str)
filename_indicateur="loc_indc_sav.xlsx"

fdld=[f for f in listdir(path_reinitialise)] # liste du contenu du répertoire ou se trouve ou se trouverons les fichier excel.

def sav_excel(filename,data):
    if filename in fdld:  # si le nom renseigner en argument se trouve dans le scan
        os.remove(path_reinitialise+filename) # on supprime le fichier présent dedans
        data.to_excel(path_reinitialise+filename) # On génère le fichier excel à partir du dataframe associé au nom renseigner également en argument
    else:
        data.to_excel(path_reinitialise+filename) # sinon on génère simplement le fichier excel

liste_data=[(sauvegarde_import,filename_fichier),(df_loc_fich_coln,filename_correspondance),(df_loc_indc,filename_indicateur)] # liste de tuple (dataframe de la table, nom du fichier excel)

for elt in liste_data:
    sav_excel(elt[1],elt[0]) # appel de la fonction pour chaque tuple

#######################################################
# Parti manuelle: renseigner les fichiers souhaités
#######################################################

print("\n\n""Veuillez passez à la saisie en base.")
end=time.time() # Fin du chrono
print("Temps d'exécution : ",end-start,"secondes") 

"""
On arrive à la parti manuel. L'utilisateur, après avoir lance ce script, doit complété directement en base la colonne
B_IMPR en remplacent par 1 les fichiers qu'il souhaite importer et 0 ceux qu'il ne souhaite pas faire de mise à jour des données
et laisse à 1 les fichiers dont il souhaite mettre à jour les indicateurs (sous réserve qu'il y ait eu des modification dans le fichier en question)
"""
