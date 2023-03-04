import pandas as pd
from os import listdir
import os
import time
import pyodbc

cnxn_str = ("Driver={ODBC Driver 17 for SQL Server};Server=srv-ang-bdd070;Database=Carto;Trusted_Connection=yes;")

path_donnee=r"C:\Users\geraultt\OneDrive - ADEME\Documents\documents\FDLD\base de donnée"
path_meta=r"C:\Users\geraultt\OneDrive - ADEME\Documents\documents\FDLD\metadonnée"
path=r"C:\Users\geraultt\OneDrive - ADEME\Documents\documents\FDLD\tables mise à jour\\"
path_reinitialise=r"C:\Users\geraultt\OneDrive - ADEME\Documents\documents\FDLD\réinitialiser_table\\"

start=time.time()

#########################################################################
# récupération de LOC_FICH avec B_IMPR à 1
#########################################################################

"""
On récupère tout les fichiers de la table loc fich à b_impr à 1, car se sont ces fichiers qui seront importé dans loc_data.
Cette fonction est identique à "recup_loc_fich" de maj_correspondance.
"""

def loc_fich(cnxn_str):
    cnxn = pyodbc.connect(cnxn_str) # connexion au table sql
    cur = cnxn.cursor()

    cur.execute("SELECT * FROM dbo.LOC_FICH WHERE B_IMPR=1") # requete sql pour récupérer le contenu de la table
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

df_loc_fich=loc_fich(cnxn_str)
#print(df_loc_fich)

#########################################################################
# récupération de LOC_FICH_COLN
#########################################################################

"""
On récupère les données de la table LOC_FICH_COLN pour comparaison par suite.
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
        if elt[7]==False: elt[7]=0
        else: elt[7]=1
        fichier.append({"ID":elt[0],"FICHIER":elt[1],"INDC_INSEE":elt[2],"INDC_ADEME":elt[3],"LIBELLE":elt[4],"DEFINITION":elt[5],"ANNEE_DONNEE":str(elt[6]),"IMPORT":elt[7]})

    df=pd.DataFrame(fichier)
    df["ANNEE_DONNEE"].replace('nan', None, inplace=True)
    return df

df_loc_fich_coln=loc_fich_coln(cnxn_str)
#print(df_loc_fich_coln)

#########################################################################
# récupération de LOC_INDC
#########################################################################

"""
On récupère les données de la table LOC_INDC.
"""

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

df_loc_indc=loc_indc(cnxn_str)
#print(df_loc_indc)

#########################################################################
# Import des 3 tables Excel
#########################################################################

filename_fichier="loc_fich_maj.xlsx"
filename_correspondance="loc_fich_coln_maj.xlsx"
filename_indicateur="loc_indc_maj.xlsx"

fdld=[f for f in listdir(path[:-2])] # Scan du répertoire où se trouverons ces fichiers xlsx.
def sav_excel(filename,data):
    if filename in fdld: # test si le fichier renseigner en argument se trouve dans le scan, si oui:
        os.remove(path+filename) # on le supprime
        data.to_excel(path+filename) # on génère le fichier xlsx
    else:                                                               
        data.to_excel(path+filename)

liste_data=[(df_loc_fich,filename_fichier),(df_loc_fich_coln,filename_correspondance),(df_loc_indc,filename_indicateur)]

for elt in liste_data:
    sav_excel(elt[1],elt[0])

#########################################################################
# Vérification
#########################################################################

def verif_indc():
    """
    Vérifier qu'il existe un code INDC renseigné et connu de la table LOC_INDC.
    """
    validation=True
    indc_importer=[]
    for i in range(len(df_loc_fich_coln)):
        if df_loc_fich_coln["IMPORT"][i]==1:
            indc_importer.append((df_loc_fich_coln["INDC_INSEE"][i],df_loc_fich_coln["INDC_ADEME"][i]))

    erreur=[]
    for indc in indc_importer:
        if indc[1] not in list(df_loc_indc["INDC"]): # Pour chaque indc de LOC_INDC, on test si ils sont présent dans la liste d'indc non nulle de LOC_FICH_COLN
            validation=False
            erreur.append(indc[0]) # On stock les indc manquant pour savoir lesquelles on aurait oublié
    
    if validation == False:
        print("L'importation n'a pas été effectué. Les variables suivantes n'ont pas de correspondance dans LOC_INDC",erreur)

    return validation # retourne True/False 

def verif_annee():
    """
    Vérifier que pour toutes les lignes il existe une année, soit dans LOC_FICH, 
    soit dans LOC_FICH_COLN car si pas d'année insertion impossible dans la table LOC_DATA.

    Comme dans la création du dataframe par la suite, on priorise l'année présente dans loc_fich_coln,
    ici, on a juste à vérifier. (Ce dernier aspect de la vérification est directement traité dans 
    la création du dataframe plus bas dans le script.)  
    """

    liste_loc_fich=list(df_loc_fich["ANNEE"]) # liste des années de la table LOC_FICH
    liste_tuple=[]

    for i in range(len(df_loc_fich_coln)):
        if df_loc_fich_coln["IMPORT"][i]==1:
            liste_tuple.append((df_loc_fich_coln["FICHIER"][i],df_loc_fich_coln["ANNEE_DONNEE"][i],df_loc_fich_coln["INDC_INSEE"][i])) # Pour chaque ligne de loc_fich_coln, on stock un 
                                                                                                                                       # tuple dans une liste comme ceci : [(fichier,année)]
    
    validation=True
    erreur=[]
    for annee in liste_tuple:
        if annee[1]== "None":  # Si par hasard il manquerai une année renseigné pour un indicateur alors on regarde si une année est renseigner dans 
            for i in range (len(liste_loc_fich)): # la table loc_fich au fichier associé à l'indicateur.
                if df_loc_fich["FICHIER"][i]==annee[0]:
                    if df_loc_fich["ANNEE"][i]==None: # Si l'on ne trouve pas d'année également, alors il y a une erreur et on renvoit immédiatement False 
                        validation=False
                        erreur.append(annee[2])
    
    if validation==False:
        print("L'importation n'a pas été effectué. Les indicateurs suivant n'ont pas d'année renseigné dans LOC_FICH et dans LOC_FICH_COLN : ",erreur)
    return validation


def verif_nivgeo():
    """
    Vérifier dans LOC_FICH que la colonne NIV_GEO est renseignée 
    """
    if None in list(df_loc_fich["NIVGEO"]):
        print("Un niveau géographique n'a pas été renseigné dans la table LOC_FICH") # On regarde si None fait parti de la colonne des niveau géographique de loc_fich.
        return False                        # Si None y ait, alors on renvoie False immédiatement.
    else : return True

def indc_importer():
    imp=[]
    for i in range(len(df_loc_fich_coln)):
        if df_loc_fich_coln["IMPORT"][i]==1 and df_loc_fich_coln["INDC_ADEME"][i] != None: # Pour chaque ligne de loc_fich_coln, on stock tout les indicateurs ADEME (les correspondances)
            imp.append(df_loc_fich_coln["INDC_ADEME"][i])                                  # non nulle et que l'on souhaite importer (B_IMPR=1)
    return imp                                                                             # On a besoin de cette liste pour la verification qui va suivre.

importer=indc_importer()
#print(importer)

def verif_correspondance_uniq():
    """
    Vérifier dans LOC_FICH_COLN que pour une même variable INSEE 
    (qui peut être présente plusieurs fois dans des fichiers différents) 
    il n'existe qu'une seule correspondance avec la table LOC_INDC. Cette verif
    est global.
    """
    # Verifie si LOC_INDC est unique
    validation=True
    l_indc=list(df_loc_indc["INDC"]) # liste de tout les indc de la table loc_indc
    if len(l_indc)!=len(set(l_indc)): # On regarde si cette liste est à valeur unique ( si longueur de la liste = longueur de la liste sans les doublons)
        print("Un même indicateur est présent plusieurs fois dans la table LOC_INDC")
        validation=False
    else:
        erreur=[]
        for indc in importer:
            if indc not in l_indc: # et on regarde aussi si ces indc sont présent dans la liste des indc importer de loc_fich_coln
                erreur.append(indc)
                validation = False
    if validation == False:
        print("L'importation n'a pu être effectué. Les indicateur suivant ne sont pas référencer dans la table LOC_INDC",erreur)

    return validation

#print(verif_correspondance_uniq())

def repetition(liste):
    """
    Cette fonction renvoie une liste de tout les élément présent plusieurs fois dans la liste
    renseigner en argument.
    """
    rep=[]
    for i in range(len(liste)):
        for j in range(i+1,len(liste)):
            if liste[i]==liste[j] and liste[i] != "":
                rep.append(liste[i])
    return rep


def verif_locfichcoln_uniq():
    """
    On regarde si les codes indc de loc_fich_coln non nulle, qui sont importés, sont bien unique.
    """
    len_indc_importer=len(importer) # la longueur de la liste des indicateur importer. N'oublions pas que cette liste s'appelle "importer" à ne pas confondre avec le dataframe dans maj_correspondance. 
    len_indc_verif=len(set(importer)) # longueur de cette même liste mais rendu unique.
    if len_indc_importer != len_indc_verif: # test si ces 2 longueurs sont identique. Si elles sont différentes, alors on affiche quelles indicateurs sont présent plusieurs fois.
        print("Voici les indicateurs présent plusieurs fois: ",repetition(importer))
        return False
    else: return True

#print(verif_locfichcoln_uniq())

"""
Ces fonction de vérification seront appelé juste avant l'insertion des données.
"""

#########################################################################
# Mise en place du nouveau dataframe
#########################################################################

# liste tuple (id, fichier, année fichier)
def tuple_fichier():
    fichier_impr_tuple=[]

    for i in range(len(df_loc_fich)):
        fichier_impr_tuple.append((df_loc_fich["ID"][i],df_loc_fich["FICHIER"][i],df_loc_fich["ANNEE"][i])) #Si ce n'est pas un fich insee alors il n'y aura rien au dernier elt du tuple

    return fichier_impr_tuple

"""
Dictionnaire : cle est un tuple (id_fichier, fichier) et la valeur est la liste de tuple de tout les indicateurs
ces tuples sont : (id_indc, indc_insee, indc_ademe). id_indc est un entier incrémenter à chaque tour de boucle
"""

fichier_impr_tuple=tuple_fichier()
#print(fichier_impr_tuple)

# liste tuple (fichier, indicateur insee, indicateur ademe, année indicateur)
def tuple_fich_coln():
    indc_impr=[]
    fichier_importer=list(df_loc_fich["FICHIER"]) # liste de tout les fichier de LOC_FICH
    for i in range(len(df_loc_fich_coln)): # boucle for parcourant toute les ligne de LOC_FICH_COLN
        if df_loc_fich_coln["IMPORT"][i]==1 and df_loc_fich_coln["FICHIER"][i] in fichier_importer:  # Si le B_IMPR =1 et le fichier se trouve dans LOC_FICH
            indc_impr.append((df_loc_fich_coln["FICHIER"][i],df_loc_fich_coln["INDC_INSEE"][i],df_loc_fich_coln["INDC_ADEME"][i],df_loc_fich_coln["ANNEE_DONNEE"][i])) # on incremente une liste par des tuples (fichier, indicateur insee, indicateur ademe, année indicateur)
    return indc_impr

indc_impr=tuple_fich_coln()
#print(indc_impr)

# liste tuple (fichier, indicateur insee, indicateur ademe, année indicateur, id indicateur)
def tuple_fich_coln_key():
    indc_import=[]
    for elt in indc_impr: # Pour chaque tuple de la liste créer précédemment
        for i in range(len(df_loc_indc)): # on parcours chaque ligne de la table LOC_INDC
            if elt[2] == df_loc_indc["INDC"][i]: # Si le code ademe (elt[2]) se trouve dans LOC_INDC
                indc_import.append((elt[0],elt[1],elt[2],elt[3],df_loc_indc["ID"][i])) #On incremente une liste par des tuples (fichier, indicateur insee, indicateur ademe, année indicateur, id indicateur)
    return indc_import

indc_import=tuple_fich_coln_key()
#print(indc_import)

# dictionnaire : {(id fichier, fichier, année fichier) : [(id indicateur, indicateur insee, indicateur ademe, année indicateur),...], (...): [...], ...}
def dico_tuple():
    dico_data={}
    indc_list=[]

    for i in range(len(fichier_impr_tuple)): # boucle parcourant la liste de tuple (id, fichier, année fichier)
        fich_init=fichier_impr_tuple[i][1] # initialise la variable fich_init par le nom du fichier de chaque tuple.
        indc_list=[] # on initialise une liste à chaque tour de boucle
        for fich in indc_import: # pour chaque tuple de la liste indc_import : (fichier, indicateur insee, indicateur ademe, année indicateur, id indicateur)
            if fich[0]==fich_init: # Si le fichier = le fich_init
                indc_list.append((fich[4],fich[1],fich[2],fich[3])) # on ajoute dans la liste initialiser le tuple (id indicateur, indicateur insee, indicateur ademe, année indicateur)

        dico_data[(fichier_impr_tuple[i][0],fichier_impr_tuple[i][1],fichier_impr_tuple[i][2])]=indc_list # une fois la boucle terminer on rempli un dictionnaire avec comme clef le tuple (id fichier, fichier, année fichier) et comme valeur associé : [(id indicateur, indicateur insee, indicateur ademe, année indicateur),...]

    return dico_data

dico_data=dico_tuple()
#print(dico_data)

# Création du dataframe

def df_data():
    data=[]
    date=0
    for key,value in dico_data.items():
        new_path=path_donnee+"\\"+key[1] # on adapte le chemin d'accès au fichier de donnée
        donnee_csv=pd.read_csv(new_path,sep=";",dtype={'CODGEO':'string'}) # on ouvre le fichier de donnée en précisant que la colonne des code geo sont de type str 
        codegeo=list(donnee_csv["CODGEO"]) # On créer une liste des codes geo du fichier 

        #correction des code geo s'il manque un 0 devant 
        for i in range(len(codegeo)):
            if len(str(codegeo[i])) == 4:
                codegeo[i]="0"+str(codegeo[i])

        for indc in value: # il s'agit de la verif 2, on choisi l'année de la donnée à insérer
            if key[2]==None: #s'il n'y a pas d'année renseigner dans LOC_FICH, alors on prend l'année saisi dans LOC_FICH_COLN
                date=indc[3]
            elif key[2] != indc[3]: # s'il y a 2 année différente alors on prend celle de LOC_FICH_COLN
                date=indc[3]
            else: date = key[2] # Sinon on prend celle de LOC_FICH.

            donnee=list(donnee_csv[indc[1]]) # On stock dans une liste toute les valeurs de l'indicateur
            for i in range(len(donnee)): # boucle parcourant toute ces valeurs
                s=str(donnee[i]) # on transforme la valeur en str
                s=s.replace(",",".") # on remplace la virgule par un point
                if date !="None" and s!="nan":
                    data.append({"id_fich":int(key[0]),"id_indc":int(indc[0]),"annee":int(date),"codegeo":codegeo[i],"VAL_INDC":s}) # puis on créer une liste de dictionnaire
    df=pd.DataFrame(data) # on créer alors le dataframe pandas
    return df

df=df_data()
print(df)

#########################################################################
# Importation
#########################################################################

def suppression(cnxn_str,id):
    """
    Fonction pour supprimer toute les lignes d'un fichier de LOC_DATA en rentrant
    l'id du fichier en argument.
    """
    cnxn = pyodbc.connect(cnxn_str)
    cur = cnxn.cursor()

    cur.execute("DELETE FROM dbo.LOC_DATA WHERE ID_LOC_FICH=?",(id,))  
    
    cnxn.commit()
    cur.close()
    cnxn.close()

def repartion_paquet():
    """
    Le principe est de diviser le dataframe en paquet de 1000 pour gagner sur le temps de l'insertion.
    La procédure est donc de diviser par 1000 le nombre total de ligne. On ajoute alors dans une liste
    des morceeaux du dataframe 1000 lignes par 1000 lignes et le dernier paquet constitue le reste des lignes 
    de la division.
    """
    paquet=[]
    quantite=[0]
    somme=0
    for i in range(len(df)//1000): # boucle de 0 jusqu'au dividende 
        quantite.append(quantite[i]+1000) #chaque tour de boucle on ajoute +1000, cela donne une liste : [0,1000,2000,3000,...]
        somme+=1000

    for i in range(len(quantite)-1): # ajout des morceaux du dataframe dans une liste
        paquet.append(df.iloc[quantite[i]:quantite[i+1],:]) # exemple: ce code ajoute dans la liste le dataframe des lignes 3000 à 3999

    if somme<len(df): # ajoute le reste de ligne restant dans la liste
        paquet.append(df.iloc[somme:,:])
    return paquet

paquet=repartion_paquet()

def insert_into_data():
	cnxn = pyodbc.connect(cnxn_str)
	cur = cnxn.cursor()
	compteur=0

	for i in range(len(paquet)):
		valeur=""

		for row in paquet[i].itertuples(index=False):
			if row[4]!="nan":
				valeur+= "('" + str(row[0])+"','"+ str(row[1])+"','"+ str(row[2])+"','"+ row[3]+"','"+ str(float(row[4]))+"'),"
		
		sql="INSERT INTO dbo.LOC_DATA(ID_LOC_FICH,ID_LOC_INDC,C_ANNEE,C_CODE_GEO,D_INDC) VALUES "+ valeur[:-1]+";"
		cur.execute(sql)
		compteur+=1000
		print(compteur)
	
	cnxn.commit()
	cur.close()
	cnxn.close()  

##############################################
# Option retour, création des fichiers Excel
##############################################

reinitialiser_table=[f for f in listdir(path_reinitialise)]
def sav_excel_loc_data():
    """
    Cette fonction va générer un fichier excel repertoriant tous les id fichier
    qui ont été importer dans loc data suite à la mise à jour. Ainsi, si on se rend compte 
    d'une erreur, on a la possibilité avec le script "truncate_loc_data.py" de supprimer
    les lignes de ces fichiers importer par la mise à jour.
    """
    id_sav={"id_fich":id_df}
    df_id_sav=pd.DataFrame(id_sav) # dataframe pandas des id fichier importer
    filename="df_ajout_data.xlsx" # nom du fichier excel
    if filename in reinitialiser_table: # test si le fichier existe déjà
        os.remove(path_reinitialise+filename) # si c'est le cas il est supprimer, générer puis délacer dans le bon répertoire
        df_id_sav.to_excel(path_reinitialise+filename)
    else:
        df_id_sav.to_excel(path_reinitialise+filename) # sinon, on génére simplement et on déplace

##############################################
# Vérification et importation en base
##############################################

print("\n\n""Vérification avant import...")

verif=[verif_indc(),verif_annee(),verif_nivgeo(),verif_locfichcoln_uniq(),verif_correspondance_uniq()]

if verif == [True,True,True,True,True] and df.empty == False: # Si toute les vérification sont bonne
    id_df=list(set(df["id_fich"])) # liste de tout les id fichier du dataframe de manière unique
    print("Sauvegarde des données à insérer...""\n\n")
    sav_excel_loc_data() # On génère le Excel des id fichier importer dans loc_data
    for id in id_df: 
        print("Suppression des données du fichier : ",id)
        suppression(cnxn_str,id) # Pour chaque id référencé dans la liste id_df, on supprime dans loc_data
    
    print("\n\n""Import de données...cela prendra un peu de temps""\n")
    insert_into_data() # puis on importe les donnée du dataframe
    print("Insertion terminer !!!")

end=time.time()
print("Temps d'exécution : ",end-start,"secondes") 