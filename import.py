import mysql.connector
from mysql.connector import Error
import time
import progressbar

annees = [2017,2018,2019,2020,2021]
nombre = 10000

widgets = [
    progressbar.Percentage(),
    progressbar.Bar(),
    ' ', progressbar.SimpleProgress(),
    ' ', progressbar.ETA(),
    ' ', progressbar.AdaptiveTransferSpeed(unit='it'),
]

def convert_date(date): # convert date from DD/MM/YYYY to format YYYY-MM-DD
    return date.split("/")[2] + "-" + date.split("/")[1] + "-" + date.split("/")[0]
    
try:
    connection = mysql.connector.connect(host='xxxx',
                                         port='xxxx',
                                         database='xxxx',
                                         user='xxxx',
                                         password='xxxx')
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connecté au serveur MySQL version: ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("Connecté à la database: ", record)
        print("\n")
        
        # ---------------------------------------
        for annee in annees:
            print("Année : " + str(annee) + " - Lignes : " + str(nombre))
            query = "SELECT * FROM `" + str(annee) + "` ORDER BY RAND() LIMIT " + str(nombre)
            cursor.execute(query)
            result = cursor.fetchall()
            for i in progressbar.progressbar(range(nombre), widgets=widgets):
                date_mutation = convert_date(result[i][8])
                nature_mutation = result[i][9]
                if result[i][11] != '':
                    valeur_fonciere = float(result[i][11].replace(",", "."))
                else:
                    valeur_fonciere = 0.0
                if result[i][11] != '':
                    no_voie = int(result[i][11])
                else:
                    no_voie = 0
                type_de_voie = result[i][13]
                voie = result[i][15]
                if result[i][16] != '':
                    code_postal = int(result[i][16])
                else:
                    code_postal = 0
                commune = result[i][17]
                code_departement = result[i][18]
                code_commune = int(result[i][19])
                if result[i][35] != '':
                    code_type_local = int(result[i][35])
                else:
                    code_type_local = 0
                type_local = result[i][36]
                if result[i][38] != '':
                    surface_reelle_bati = int(result[i][38])
                else:
                    surface_reelle_bati = 0
                if result[i][39] != '':
                    nombre_pieces_principales = int(result[i][39])
                else:
                    nombre_pieces_principales = 0
                if not result[i][42] == "":
                    surface_terrain = int(result[i][42])
                else:
                    surface_terrain = 0

                data = {'date_mutation': date_mutation, 'nature_mutation': nature_mutation, 'valeur_fonciere': valeur_fonciere, 'no_voie': no_voie, 'type_de_voie': type_de_voie, 'voie': voie, 'code_postal': code_postal, 'commune': commune, 'code_departement': code_departement, 'code_commune': code_commune, 'code_type_local': code_type_local, 'type_local': type_local, 'surface_reelle_bati': surface_reelle_bati, 'nombre_pieces_principales': nombre_pieces_principales, 'surface_terrain': surface_terrain}

                # Peuplement de la table Lieu
                query_lieu = "INSERT IGNORE INTO `Lieu` (`Commune`, `Code_departement`, `Code_postal`) VALUES (\"%s\", \"%s\", %d)" % (commune, code_departement, code_postal)
                cursor.execute(query_lieu)
                connection.commit()

                # Peuplement de la table Coordonnees
                query_coord = "INSERT IGNORE INTO `Coordonnees` (`Code_commune`, `Numero_de_voie`, `Type_de_voie`, `Voie`, `Commune`) VALUES (%d, %d,\"%s\",\"%s\",\"%s\")" % (code_commune, no_voie, type_de_voie, voie, commune)
                cursor.execute(query_coord)
                connection.commit()

                # Peuplement de la table Bien_immobilier
                query_bien = "INSERT IGNORE INTO `Bien_immobilier` (`Type_local`, `Surface_terrain`, `Surface_reelle_bati`, `Nombre_de_pieces_principales`, `Code_type_local`, `Code_commune`) VALUES (\"%s\", %d, %d, %d, %d, %d)" % (type_local, surface_terrain, surface_reelle_bati, nombre_pieces_principales, code_type_local, code_commune)
                cursor.execute(query_bien)
                connection.commit()

                # Peuplement de la table Transaction
                query_transaction = "INSERT IGNORE INTO `Transaction` (`Date_mutation`, `Nature_mutation`, `Valeur_fonciere`, `IDBien`) VALUES (\"%s\", \"%s\", %f, %d)" % (date_mutation, nature_mutation, valeur_fonciere, cursor.lastrowid)
                cursor.execute(query_transaction)
                connection.commit()

except Error as e:
    print("\nErreur de connexion: ", e)
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("Connexion fermée")
        
 
