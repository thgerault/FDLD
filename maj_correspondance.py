import pandas as pd
from os import listdir
import time
import pyodbc

cnxn_str = ("Driver={ODBC Driver 17 for SQL Server};Server=srv-ang-bdd070;Database=Carto;Trusted_Connection=yes;")

path=r"C:\Users\geraultt\OneDrive - ADEME\Documents\documents\FDLD\\"
path_donnee=path+ "base de donnée"
path_meta=path+"metadonnée"

start=time.time() # Lancement du chrono pour évaluer le temps d'exécution du programme

###################################################################
# Sauvegarde des correspondances indc initiaux
###################################################################

"""
On commence par effectuer une sauvegarde des données avant mise à jour de la table LOC_FICH_COLN. 
On sauvegarde dans un dataframe pandas. Cette sauvegarde va nous permettre d'identifier certain indicateur
ou bien de préselectionner les indicateurs récemment importer.
"""

def indc_initial(cnxn_str):
    cnxn = pyodbc.connect(cnxn_str)
    cur = cnxn.cursor()

    cur.execute("SELECT * FROM dbo.LOC_FICH_COLN") # requete sql pour récupérer le contenu de la table LOC_FICH_COLN 
    indc=cur.fetchall() # recuperation des données dans l'environnement python sous forme de liste de tuple => [(id_loc_fich, L_fich, var_insee,...), (...), ...]
    
    cur.close()
    cnxn.close()

    fichier=[]

    if indc != []:
        for elt in indc: # pour chaque tuple de la liste, la colonne booléenne B_IMPR (colonne 7) est interpréter comme TRUE/FALSE par python.
            if elt[7]==False: elt[7]=0 # Donc on rebascule ces valeur booléennes par les entiers 0/1
            else: elt[7]=1
            fichier.append({"ID":elt[0],"FICHIER":elt[1],"INDC_INSEE":elt[2],"INDC_ADEME":elt[3],"LIBELLE":elt[4],"DEFINITION":elt[5],"ANNEE_DONNEE":elt[6],"IMPORT":elt[7]})
            # On incrémente une liste par des dictionnaires créer à partir des données de la liste de tuples. Chaque dictionnnaire correspondent à 1 ligne de la table
    
    else: fichier.append({"ID":None,"FICHIER":None,"INDC_INSEE":None,"INDC_ADEME":None,"LIBELLE":None,"DEFINITION":None,"ANNEE_DONNEE":None,"IMPORT":None})

    df=pd.DataFrame(fichier) # Création du dataframe pandas à partir de la liste de dictionnnaire
    return df

sauvegarde_indc=indc_initial(cnxn_str)
#print(sauvegarde_indc)

###################################################################
# Récupération de la table LOC_FICH après le scan et la mise à jour 
###################################################################

"""
On récupère les fichiers dont l'import est à 1 de la table LOC_FICH après avoir lancer le 1er script
"""

def recup_loc_fich(cnxn_str):
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

importer=recup_loc_fich(cnxn_str) 
print(importer)

###################################################################
# Déterminer quelle tables sont importer, source INSEE ou AUTRE
###################################################################

"""
La fonction suivante sert à filtrer les fichiers INSEE et NON INSEE. Si le fichier en question n'a pas de métadonnée correspondantes
alors c'est une source AUTRE. La fonction retourne une liste de tuple =>  [(donnee,source,metadonnee) , (donnee,source,metadonnee) , ...]

Si on ne trouve pas de correspondance avec le répertoire des métadonnées, alors c'est qu'il ne s'agit pas d'un fichier INSEE donc on aura un tuple 
comme ceci: (donnee,autre,"").

Attention, ici n'oublions pas que l'on travaille que sur les fichiers à b_impr à 1.
"""

def table_a_importer():
    metadonnee = [f for f in listdir(path_meta)] # liste de tous les fichiers de métadonnée 
    base_a_importer=[]
    
    # "importer" est le dataframe de LOC_FICH des fichiers importés
    for i in range(len(importer)): # Boucle for de la longueur du dataframe pour parcourir tout ses lignes
        if importer["SOURCE"][i]=="INSEE": # On regarde si "INSEE" est inscrit dans la colonne "SOURCE"
            for meta in metadonnee: # Si il s'agit donc d'un fichier insee, pour chaque fichier de métadonnée
                if importer["FICHIER"][i] in meta:  # Si le nom du fichier est dans le nom du fichier méta (car un fichier méta est nommé => ex: meta-base-insee-filosofi-2018-geo2020.csv)
                    base_a_importer.append((importer["FICHIER"][i],"INSEE",meta)) # Une fois la métadonnée identifier, on créer un tuple (fichier, source, métadonnée) que l'on stocke dans une liste
        else: base_a_importer.append((importer["FICHIER"][i],"AUTRE","")) # Comme indiquer dans les commentaires, si ce n'est pas INSEE, alors c'est le tuple (fichier, AUTRE,"") qui est ajouté.

    return base_a_importer

base_a_importer=table_a_importer()
#print(base_a_importer)

######################################################################################
# Récupération des indc par fichier importer, source INSEE ou AUTRE
######################################################################################

"""
Le but de cette fonction est de générer deux dictionnaires, insee et non insee de la forme:

    -> insee: {theme1: [(code_insee, libellé), (code_insee, libellé), ...], theme2: ...}
    -> autre: {theme1: [(code_non_filtrer, ""), (code_non_filtrer, ""), ...], theme2: ...}

Ce format est plus pratique à manipuler. 
"""

def verif_erreur(df,indicateur):
    """
    Cette fonction est en quelque sorte un filtre avant le stockage des données dans les dictionnaires.
    En effet, en parcourant chaque indicateur, avant le stockage, on regarde si c'est une variable qui contient des 
    données de type caractère. Hormis la colonne des codes géo, on ne lit pas ces colonnes.
    """
    validation =True
    if indicateur=="CODGEO": validation=False
    if type(df[indicateur][0]) == str:  # On test le type de la 1er donnée de chaque indicateur
        validation=False
    
    return validation 

print("\n""Ouverture des fichiers et récupération des variables :")

def dico_var_lib(path_donnee,path_meta,liste_base_a_importer): #dictionnaire où la clef est le fichier et valeur = liste tuple où les tuples sont (indc,libellé)
    var_insee={}
    var_autre={}
    for base in liste_base_a_importer:
        var_lib=[]

        if base[1]=="INSEE":
            new_path_meta=path_meta + "\\" + base[2] # On adapte le chemin d'import, on prend les metadonnee car elle identifie les indicateurs et leur libellé
            table_meta=pd.read_csv(new_path_meta,sep=";")
            theme=base[0] # On récupère le thème contenu dans le nom du fichier.
            for i in range(len(table_meta)):
                var_lib.append((table_meta["COD_VAR"][i],table_meta["LIB_VAR"][i]))
            var_insee[theme]=var_lib # On insère dans une liste des dictionnaires qui sont de la forme {le thème: la liste des variable associé}
        else:
            new_path=path_donnee + "\\" + base[0] # On adapte le chemin d'import en fonction du fichier
            table=pd.read_csv(new_path,sep=";") # On ouvre le fichier à la bonne feuille xcel pour après appliquer la fonction récupérant tout les noms de variable
            theme=base[0]
            for i in range(len(table.columns)):
                if verif_erreur(table,table.columns[i])==True:
                    var_lib.append((table.columns[i],""))
            var_autre[theme]=var_lib # On insère dans une liste des dictionnaires qui sont de la forme {le thème: la liste des variable associé}
    
    return var_insee,var_autre

var_insee,var_autre=dico_var_lib(path_donnee,path_meta,base_a_importer)

print("\n\n""Variables INSEE: ""\n\n",var_insee,"\n\n""Variables AUTRES: ""\n\n",var_autre)

######################################################################################
# Création dictionnaire spécialement pour les nouveaux fichiers importer
######################################################################################

"""
Dans cette fonction, on récupère les dictionnaires créée précemment en ne retenant que les nouveaux fichiers importé.
Il s'agit des mêmes dictionnaire mais ne répertoriant que les nouveau fichiers, soit les nouveau indc.
"""

def new_dico_var_lib(): # pareil que la fct précedente mais seulement sur les nouveaux fichiers ajoutés 
    new_var_insee={}
    new_var_autre={}
    for key,value in var_insee.items():
        if key not in list(sauvegarde_indc["FICHIER"]):
            new_var_insee[key]=value
    for key,value in var_autre.items():
        if key not in list(sauvegarde_indc["FICHIER"]):
            new_var_autre[key]=value
    return new_var_insee,new_var_autre
    
new_var_insee,new_var_autre=new_dico_var_lib()
#print("\n\n""Variables INSEE: ""\n\n",new_var_insee,"\n\n""Variables AUTRES: ""\n\n",new_var_autre)

######################################################################################
# Identification des base de sources INSEE et filtrage indc/libellé 
######################################################################################

"""
On procède au filtrage des données pour les fichiers INSEE. Ainsi, on aura 2 fonctions, une pour le filtrage des indicateurs insee et 
la 2e pour les libellés. La première fonction ci dessous permet de retourner une liste contenant les fichiers insee uniquement parmi la liste 
de tuple de la fonction table_a_importer.
"""

def base_insee():
    table_insee=[]
    for fichier in base_a_importer:
        if "insee" in fichier[0]:
            table_insee.append(fichier[0])
    return table_insee

table_insee=base_insee()
#print(table_insee)

# Les fonctions de filtrages. 1er fct pour les variables avec en argument le thème (car 1 filtre en fct du sujet) et le code de l'indicateur

def filtrage_insee(sujet,code_variable):
    sujet=sujet.lower() # On transforme le theme en minuscule
    for table in table_insee: # pour chaque fichier de la fonction base_insee()
        if sujet in table: # si le sujet renseigné en argument est contenu dans le nom du fichier:
            filtre=table[-14:-12] # On adapte un filtre. Les position -14 à -12 correspondent au 2 derniers chiffre le l'année du fichier, et c'est de ces chiffres que sont construit les indicateur insee.
            code_var_filtrer=""
            if code_variable[:4]=="P"+filtre+"_" or code_variable[:4]=="C"+filtre+"_": #test si P18_ est dans le nom des variables au debut
                code_variable=code_variable[4:]
            if code_variable[-2:]==filtre: #test si 18 est a la fin du nom de la variable
                code_variable=code_variable[:-2]
            code_var_filtrer=code_variable

    return code_var_filtrer

# La 2e est pour filtrer les libellée

def filtrage_libelle_insee(sujet,libelle): # idem que pour les indicateurs mais sur les libellés
    sujet=sujet.lower()
    for table in table_insee:
        if sujet in table:    
            filtre=table[-14:-12]
            if libelle[-7:]=="en 20"+filtre:
                libelle=libelle[:-7]
            if libelle[-15:]=="en 20"+filtre+" (princ)":
                libelle=libelle[:-15]
        libelle_filtrer=libelle
    return libelle_filtrer

######################################################################################
# Création du tableau des correspondance pour les nouveaux fichiers importés
######################################################################################

"""
On créer une liste de dictionnaire (plus pratique pour générer un dataframe). Cette liste contient à la fois les éléments insee et autre.
On créer un dataframe pandas avec cette liste. Ici, les éléments ont été filtrer, il ne manque plus que reporter les indicateurs cochers.

On a ici nos deux dictionnaires de la forme :

    -> insee: {theme1: [(code_insee, libellé), (code_insee, libellé), ...], theme2: ...}
    -> autre: {theme1: [(code_non_filtrer, ""), (code_non_filtrer, ""), ...], theme2: ...}

On effectue un traitement sur les deux dictionnaires. Pour chaque clef, on parcours la liste associé et on incréménte une liste de dictionnaire 
avec des données que l'on filtre au passage.
"""

def tab_correspondance():
    data=[]
    if len(new_var_insee) !=0: # traitement si le dictionnaire est non nulle.
        for clef,valeur in new_var_insee.items():
            for i in range(len(valeur)):
                data.append({"FICHIER":clef,"VARIABLE_INSEE":valeur[i][0],"INDICATEUR":filtrage_insee(clef[8:-17],valeur[i][0]),"LIBELLE":filtrage_libelle_insee(clef[8:-17],valeur[i][1]),"ANNEE":clef[-16:-12],"IMPORT":0})
    
    if len(new_var_autre) != 0:
        for clef,valeur in new_var_autre.items():
            for i in range(len(valeur)):            
                data.append({"FICHIER":clef,"VARIABLE_INSEE":valeur[i][0],"INDICATEUR":"","LIBELLE":"","IMPORT":0}) # On met par default le B_IMPR à 0. 

    if len(data) !=0: # Si on a ajouté des éléments dans la liste "data", alors on créer le dataframe
        Table_correspondance=pd.DataFrame(data)
        return Table_correspondance
    else : return [] # Sinon on ne retourne rien

#print(tab_correspondance())

######################################################################################
# Report des b_import sur les indc des fichiers deja importé mais d'année differente
######################################################################################

"""
On récupère dans une liste nommé "cocher" les indicateurs qui ont été séléctionner dans la sauvegarde des données avant la mise à jour.
Maintenant, comme les indicateurs INSEE sont filtrés, on a le même nom indicateurs pour des années différentes. Ainsi, si 
en parcourant le nouveau dataframe, on trouve ce cas et qu'en plus l'un des 2 est à B_IMPR à 1, alors on reporte le B_IMPR sur l'autre indicateur.
"""

def var_cocher():
    cocher=[]
    Table_correspondance=tab_correspondance()
    if len(Table_correspondance) != 0: # test si le rendu de la fonction tab_correspondance() est non nulle
        for i in range(len(sauvegarde_indc)): # Boucle for parcourant chaque ligne du dataframe de LOC_FICH_COLN
            if sauvegarde_indc["IMPORT"][i]==1: # test pour identifier les indicateurs sélectionner
                cocher.append(sauvegarde_indc["INDC_ADEME"][i])   # on les stock dans une liste
        for i in range(len(Table_correspondance)):
            if Table_correspondance["INDICATEUR"][i] in cocher: # pour chaque indicateur se trouvant dans le dataframe créer précédemment, on le retrouve dans la liste des variable cocher (importer)
                Table_correspondance["IMPORT"][i]=1 # alors on met son B_IMPR à 1.
        
        return Table_correspondance

Table_correspondance=var_cocher()
#print(Table_correspondance)

######################################################################################
# Ajout des variables qui ont été ajouté dans les fichiers deja importer
######################################################################################

"""
On récupère ici tout les fichiers qui étaient déjà importer avant.
"""

def fichier_toujours_importer(): # recupère tous les fichiers
    fichier_deja_impr=[]
    for fichier in base_a_importer:
        if fichier[0] in list(sauvegarde_indc["FICHIER"]):
            fichier_deja_impr.append(fichier[0])
    return fichier_deja_impr

fichier_deja_impr=fichier_toujours_importer()
#print(fichier_deja_impr)

"""
Cette fonction va regarder s'il y a eu du changement dans ces fichiers deja importer. Cette fonction ne traite que les ajouts.
Pour cela, on commence par fusionner nos 2 dictionnaires(insee et autre) et on regarde dans celui ci si le fichier en question
est dans la liste des fichiers deja importer et s'il est de source insee ou non. Ensuite, on compare avec la sauvegarde
s'il y a eu des ajouts d'indicateur. Si c'est le cas, on ajoute une ligne supplémentaire au dataframe avec B_IMPR à 0 par défault.
"""

def ajout():
    var={**var_insee,**var_autre} #fusion des 2 dictionnaires, on rappelle que les clefs sont les noms des fichiers
    for key,value in var.items():
        if key in fichier_deja_impr and key in table_insee: # si le fichier a déjà été importer par le passé et qu'il s'agit d'un fichier insee:
            for indc in value: # pour chaque indicateur dans la liste des indicateurs de chaque fichiers (clefs)
                if indc[0] not in list(sauvegarde_indc["INDC_INSEE"]): # indc[0] est le 1er élément du tuple, càd l'indicateur insee. S'il ne se trouve pas dans la liste des indicateurs d'avant la mise à jour, alors c'est qu'il a été rajouté dans le fichier csv.
                    Table_correspondance.loc[len(Table_correspondance)]=[key,indc[0],filtrage_insee(key[8:-17],indc[0]),filtrage_libelle_insee(key[8:-17],indc[1]),key[-16:-12],0] # donc on ajout une ligne à notre dataframe
        
        elif key in fichier_deja_impr and key not in table_insee: #Sinon si  le fichier a déjà été importer par le passé et qu'il ne s'agit pas d'un fichier insee:
            for indc in value: # pour chaque indicateur dans la liste des indicateurs de chaque fichiers (clefs)
                if indc[0] not in list(sauvegarde_indc["INDC_INSEE"]):
                    Table_correspondance.loc[len(Table_correspondance)]=[key,indc[0],0] # idem que le raisonnement précedent hormis qu'on ajoute sur la nouvelle ligne que le fichier, l'indicateur non filtrer et le B_IMPR par default à 0.

    return Table_correspondance

if len(fichier_deja_impr) != 0: # On applique cette fonction uniquement si la liste des fichier déjà importer est non vide.
    ajout()

"""
On a donc un dataframe complet des données à insérer dans la table LOC_FICH_COLN. Il ne manque plus que
le report des clefs étrangères et le cas de suppression.
"""

######################################################################################
# Report des clef étrangère
######################################################################################

"""
Ici, on reporte les clés primaires de la table LOC_FICH.
On commence par récupérer toute les clef des fichier à B_IMPR à 1. C'est une liste de tuple => [(id,fichier), ...]
"""

def fk(cnxn_str):
    cnxn = pyodbc.connect(cnxn_str)
    cur = cnxn.cursor()

    cur.execute("SELECT ID_LOC_FICH, L_FICH FROM dbo.LOC_FICH WHERE B_IMPR=1") # Requete sql récupérant l'id fichier et le fichier de la table LOC_FICH à B_IMPR 1
    foreign_key=cur.fetchall() # récupération de la requete dans l'environement python sous la forme => [(id fichier, fichier), (...), ... ]

    cur.close()
    cnxn.close()
    return foreign_key

"""
Insertion des id fichiers dans le dataframe. Tout simplement, si un fichier du dataframe coincide
avec la liste de foreign key, alors on insert l'id fichier correspondant
"""

def insertion_fk(Table_correspondance):
    foreign_key=fk(cnxn_str)
    for id in foreign_key:
        for i in range(len(Table_correspondance)):
            if Table_correspondance["FICHIER"][i]==id[1]:
                Table_correspondance["FOREIGN_KEY"][i]=id[0]

dataframe=Table_correspondance

"""
Pour inserer ces clef etrangère, on créer une colonne dans le dataframe, or quand on va lancer plusieurs fois ce
script, on ne va pas créer une nouvelle colonne à chaque fois. Donc pour ça, on regarde si le nombre de colonne du
dataframe est <8. Si elle l'est, c'est que la colonne clef étrangère n'a pas encore été creer donc on la créer 
et on insert.
si le nb de colonne est = à 8, alors la colonne existe deja et dans ce cas là, on la supprime puis on réinsert tous dans le dataframe.
"""

if Table_correspondance.empty == False: # .empty test si le dataframe est vide ou non
    if len(dataframe.columns)<8: # si la col n"existe pas, on l'ajoute
        Table_correspondance.insert(0,"FOREIGN_KEY",0, allow_duplicates=False)
        df=Table_correspondance
        insertion_fk(df)

    if len(dataframe.columns)==8: # si la col existe, on la supprime puis on l'ajoute pour mettre à jour
        del Table_correspondance["FOREIGN_KEY"]
        Table_correspondance.insert(0,"FOREIGN_KEY",0, allow_duplicates=False)
        df=Table_correspondance
        insertion_fk(df)

else: print("Pas de nouveau fichier importé, seul des anciens fichiers sont souhaités mettre à jour")

######################################################################################
# Suppression de colonne
######################################################################################

"""
Cette 1er fonction a pour objectif de supprimer la ligne ayant la variable insee renseigner en argument.
"""

def delete(variable):
	cnxn = pyodbc.connect(cnxn_str)
	cur = cnxn.cursor()

	cur.execute("DELETE FROM dbo.LOC_FICH_COLN WHERE C_VARB_INSEE=?",(variable,))
	cnxn.commit()
	cur.close()
	cnxn.close()    

"""
On regarde maintenant si pour un fichier qui était deja importer, ces variables sont toutes présente dans la sauvegarde.
Si il manque un indicateur dans le fichier, c'est qu'il a été supprimer, par conséquent on supprime cet indicateur en base.
"""

def suppression():
    indicateur=[]
    indc_sav=[]
    var={**var_insee,**var_autre} #fusion des 2 dictionnaires
    for key,value in var.items():
        if key in fichier_deja_impr:
            for indc in value:
                indicateur.append(indc[0]) # liste de tout les indc du fichier filtré et qui ont déjà été importer
    
    for i in range(len(sauvegarde_indc)):
        if sauvegarde_indc["FICHIER"][i] in fichier_deja_impr:
            indc_sav.append(sauvegarde_indc["INDC_INSEE"][i]) # On ne prend que les indicateurs des fichiers déjà importer
    
    for i in range(len(indc_sav)):
            if indc_sav[i] not in indicateur:
                indice = sauvegarde_indc.index[sauvegarde_indc["INDC_INSEE"] == indc[i]].tolist() # cette commande renvoie dans une liste l'index de l'indicateur de la sauvegarde qui a été supprimé d'un fichier.
                delete(sauvegarde_indc["INDC_INSEE"][indice[0]]) # On supprime cet indicateur

suppression()

print("Données à insérer dans LOC_FICH_COLN:""\n\n",Table_correspondance)

######################################################################################
# Insertion indc des nouveaux fichiers en base
######################################################################################

"""
On insert les données du dataframe en base. S'il s'agit d'un fichier INSEE alors on rempli toute la ligne.
Si le fichier est de source autre, alors on ne rempli que les 3 première colonne et la dernière avec le B_IMPR
à 0 par default.
"""

def insert_into_colonne_insee(id,fichier,var_insee,var_correspondante,libelle,annee,a_importer):
    cnxn = pyodbc.connect(cnxn_str)
    cur = cnxn.cursor()

    value = (id,fichier,var_insee,var_correspondante,libelle,annee,a_importer)
    cur.execute("INSERT INTO dbo.LOC_FICH_COLN(ID_LOC_FICH,L_FICH, C_VARB_INSEE,C_INDC,L_LIBL,N_ANNEE_DONNEE,B_IMPR) VALUES (?, ?, ?, ?,?,?,?)",value)

    cnxn.commit()
    cur.close()
    cnxn.close()

def insert_into_colonne_autre(id,fichier,var_insee,a_importer):
    cnxn = pyodbc.connect(cnxn_str)
    cur = cnxn.cursor()

    value=(id,fichier,var_insee,a_importer)
    cur.execute("INSERT INTO dbo.LOC_FICH_COLN(ID_LOC_FICH,L_FICH, C_VARB_INSEE,B_IMPR) VALUES (?, ?, ?, ?)",value)
    
    cnxn.commit()
    cur.close()
    cnxn.close()

def insert_colonne(Table_correspondance):
    for i in range(len(Table_correspondance)):
        if Table_correspondance["FICHIER"][i] in table_insee:
            insert_into_colonne_insee(int(Table_correspondance["FOREIGN_KEY"][i]),Table_correspondance["FICHIER"][i],Table_correspondance["VARIABLE_INSEE"][i],Table_correspondance["INDICATEUR"][i],Table_correspondance["LIBELLE"][i],int(Table_correspondance["ANNEE"][i]),int(Table_correspondance["IMPORT"][i]))
        else:
            insert_into_colonne_autre(int(Table_correspondance["FOREIGN_KEY"][i]),Table_correspondance["FICHIER"][i],Table_correspondance["VARIABLE_INSEE"][i],int(Table_correspondance["IMPORT"][i]))


"""
On insert les donnée du dataframe uniquement si celui ci est non vide.
"""

if Table_correspondance.empty == False:
    insert_colonne(Table_correspondance)
    print("Insertion effectuée ! Veuillez passer à la saisie manuelle des correspondances, des années et des imports")

end=time.time() # Fin du chrono
print("Temps d'exécution : ",end-start,"secondes") 