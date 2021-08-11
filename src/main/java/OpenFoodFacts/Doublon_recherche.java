/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package OpenFoodFacts;

import OpenFoodFacts_doublons.Connexion_MONGODB;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.eq;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.bson.Document;

/**
 *
 * @author xavior
 */
public class Doublon_recherche {
   
    // Pour toutes les connexions étalies
    private MongoClient mongodbclient1 ;
    private MongoClient mongodbclient2 ;
    private MongoClient mongodbclient3 ;
    private MongoClient mongodbclient4 ;
    
    
/**
 * Méthode princiaple de recherche sur une ou plusieurs bases
 *   possibilité de recherche sur une ou plusieurs base en même temps
 * @param base_rech1 base de référence (obligatoire)
 * @param base_rech2 base de recherche ou null
 * @param base_rech3 autre base de recherche ou null
 * @param base_rech4 autre base de recherche ou null
 * @return résultat dans une liste d'objet : List<Doublon_recherche_struct_resultat> 
 */
    public List<Doublon_recherche_struct_resultat> compare(Connexion_MONGODB base_rech1, Connexion_MONGODB base_rech2, Connexion_MONGODB base_rech3, Connexion_MONGODB base_rech4) {
        
        
        List<Doublon_recherche_struct_resultat> liste_resultat = new ArrayList<>();
        
        
        // Récupération des collections des bases MongoDb
        
        MongoCollection collection1 = Get_Collection_Mongo(base_rech1);
        MongoCollection collection2 = Get_Collection_Mongo(base_rech2);
        MongoCollection collection3 = Get_Collection_Mongo(base_rech3);
        MongoCollection collection4 = Get_Collection_Mongo(base_rech4);
        
        // Parcours de la base 1
        int nb_en_cours = 0;
        int nb_doublon = 0;
        long nb_produits_total = collection1.count();
        
        // Lance requête (equivalent select *
        MongoCursor curseur =  collection1.find().noCursorTimeout(true).iterator();

        boolean continuer = true;
        while (continuer) {
            // De temps en temps error -5 sur le hasnext() ...
            try {
                // On retente ..
                continuer = curseur.hasNext();
            } catch (Exception e) {
                try {
                    continuer = curseur.hasNext();
                } catch (Exception ex) {
                    // 2 ème erreur consécutive --> on arrête
                    continuer = false;
                    break;
                }
            }
            
            // Récupération du document (de temps en temsp erreur sur le .next)
            Document doc1;
            try {
                doc1 = (Document) curseur.next();
            } catch (Exception e) {
                try {
                    doc1 = (Document) curseur.next();
                } catch (Exception ex) {
                     doc1 = null;
                     continuer = false;
                }
            }
            
            nb_en_cours++;

            String Code = Get_Document_code(doc1);            
            
            //////////////////////////////////////////
            //   Recherche sur les autres colections
            Document doc2 = recherche(collection2,Code);
            Document doc3 = recherche (collection3,Code);
            Document doc4 = recherche (collection4,Code);
            
            // Si une des recherches doublons est concluante
            if (doc2 !=null || doc3 !=null || doc4 != null) {
                nb_doublon ++; 
                // Ajoute dans la liste_resultat
                Ajoute_resultat(liste_resultat, doc1, base_rech1.base_de_donnees);
                Ajoute_resultat(liste_resultat, doc2, base_rech2.base_de_donnees);  // si null ne sera pas pris en compte
                Ajoute_resultat(liste_resultat, doc3, base_rech3.base_de_donnees);  // si null ne sera pas pris en compte
                Ajoute_resultat(liste_resultat, doc4, base_rech4.base_de_donnees);  // si null ne sera pas pris en compte
                
                System.out.println();               
            }
            
            if (nb_en_cours % 10000 == 0) {
                System.out.println("En cours : " + nb_en_cours + "/"+ nb_produits_total +") --> nb doublons : " + nb_doublon );
            }
            
            
            //////
            /// Debug
            /*
            if (nb_en_cours > 20000) {
                continuer = false;
            }
            */
            
            
        }
        
        System.out.println ("############");
        System.out.println ("## FIN   ###");
        System.out.println ("############");
        
        System.out.println("Nb produits analysés : " + nb_en_cours + " (total : "+ nb_produits_total +") --> nb doublons : " + nb_doublon + "");
        System.out.println("");

        
        // Ferme les connexions établies
        if (mongodbclient1 != null) { mongodbclient1.close();}
        if (mongodbclient2 != null) { mongodbclient2.close();}
        if (mongodbclient3 != null) { mongodbclient3.close();}
        if (mongodbclient4 != null) { mongodbclient4.close();}
        
        return liste_resultat;
        
    }
    
    /**
     * Renvoie la collection definie dans l'objet Connexion_MONGODB.serveur et Connexion_MONGODB.base_de_donnees
     * @param une_base
     * @return 
     */    
    private MongoCollection Get_Collection_Mongo (Connexion_MONGODB une_base) {
        MongoCollection une_collection = null;
        if (une_base != null) {
            // Connexion au serveur
            MongoClient mongoClient = new MongoClient( une_base.serveur , Integer.valueOf(une_base.port) );
            
            // Stocke les connexions établies
            if (mongodbclient1 != null) { mongodbclient1 = mongoClient ;}
            if (mongodbclient2 != null) { mongodbclient2 = mongoClient ;}
            if (mongodbclient3 != null) { mongodbclient3 = mongoClient ;}
            if (mongodbclient4 != null) { mongodbclient4 = mongoClient ;}
            
            // Connexion à la base de données
            MongoDatabase base = mongoClient.getDatabase(une_base.base_de_donnees);
            // Selection d'une collection de documents
            une_collection = base.getCollection(une_base.collection);     
            
        }       
        return une_collection;
    }
    
    
    /**
     * Recherche un document dans une collection particulière
     * @param collection_rech la collection de recherche
     * @param Code la valeur de l'_id
     * @return Document le document trouvé sinon null
     */
    private Document recherche(MongoCollection collection_rech, String Code) {
        Document doc = null;
        if (collection_rech!= null) {
          // Construction requete pour reccherche doc 2
          BasicDBObject whereQuery = new BasicDBObject();
          whereQuery.put("employeeId", 5);
          MongoCursor curseur2 = collection_rech.find(eq("_id",Code)).iterator();
          // parcours résultat
          while(curseur2.hasNext()) {                                             
              doc = (Document)curseur2.next();                 
          }      
          curseur2.close();
        }
        return doc;
    }
    
    
    /**
     * Ajoute un résultat de doublon dans une liste de résultat prévue à cet effet
     * @param liste_resultat Liste à recevoir les résultats
     * @param doc le document concerné
     * @param Source la base de données source
     */
    public void Ajoute_resultat(List<Doublon_recherche_struct_resultat> liste_resultat,Document doc, String Source) {

        if (doc!= null) {
           
           Doublon_recherche_struct_resultat resultat = new Doublon_recherche_struct_resultat();
           
           resultat.setBDD(Source);
           resultat.setNom_produit( Get_Document_string(doc, "product_name"));
           resultat.setCode(Get_Document_code(doc));
           resultat.setCategories(Get_Document_string(doc, "categories"));
           resultat.setTaille(Get_Document_size(doc));
           resultat.setDoc_JSON(doc.toJson());

           liste_resultat.add(resultat);
           
           Affiche_doublon(doc, Source);          
        }

        
    }
    
    /**
     * Affiche dans la console le doublon trouvé
     * @param doc
     * @param Source 
     */    
    private void Affiche_doublon(Document doc, String Source) {       
        if (doc != null) {
           String Nom_produit = Get_Document_string(doc,"product_name");               
           String Code        = Get_Document_code(doc);
           String Categories  = Get_Document_string(doc, "categories");
           int Taille         = Get_Document_size(doc);

           System.out.println("Bdd : " + Source + "\t Size :" + Taille + " \t code :" + Code + "\tProduct name :" + Nom_produit +" \t\t Categories " + Categories);           
        } 
    }

    
    /**
     * Récupère la taille d'un document JSON
     * Prend en compte si l'objet est null
     * @param doc
     * @return 
     */
    private int Get_Document_size(Document doc) {
        int taille = -1;
        try {
            taille = doc.size();
        } catch (Exception e) {
        }
        
        return taille;
    }
    
    /**
     * Récupère la valeur "_id" d'un objet JSON (parfois intégrée en String, parfois en Int
     *    Cette méthode pour simplifier l'écriture du code dans d'autres blocs
     * @param doc
     * @return String 
     */
    private String Get_Document_code(Document doc) {
        boolean is_code_ok = false;
        String Code = "";
        try {
            Code = doc.getString("_id");
            is_code_ok = true;
        } catch (Exception e) {
        }

        if (!is_code_ok) {
            try {
                Code = doc.getInteger("code").toString();
                is_code_ok = true;
            } catch (Exception e) {
            }
        }
        return Code;
    }
    
    /**
     * Récupère une propriété de type String d'un document JSON
     *   Cette méthode pour simplifier l'écriture du code dans d'autres blocs
     * @param doc
     * @param propriete
     * @return String
     */
    private String Get_Document_string(Document doc, String propriete) {
        String valeur = "";
        try {
            valeur = doc.getString(propriete);
        } catch (Exception e) {
        }
        return valeur;
    }     
    
    
    /**
     * Transforme une liste de résultat en String sous forme CSV de la forme :
     *    BDD;Code;Nom_produit;Taille;Categories;url;
     * @param liste_resultat 
     * @return String résulat en forme CSV 
     */
    public String get_csv_resultat (List<Doublon_recherche_struct_resultat> liste_resultat) {
        
        String csv_resultat = "";
        
        String entete = "BDD;Code;Nom_produit;Taille;Categories;url;";
        
        String donnees = "";
        
        for ( Doublon_recherche_struct_resultat un_resultat : liste_resultat) {
            String url = "";
            donnees += un_resultat.getBDD() + ";";
            donnees += un_resultat.getCode() + ";" ;
            donnees += un_resultat.getNom_produit().replaceAll("\n", "").replaceAll("\r", "")+ ";";
            donnees += un_resultat.getTaille() + ";" ;
            donnees += un_resultat.getCategories().replaceAll("\n", "").replaceAll("\r", "") + ";" ;    
            
            
            if (un_resultat.getBDD().equals("open_food_facts")) {
                url = "https://fr.openfoodfacts.org/produit/" + un_resultat.getCode() + "/";
            }
            
            if (un_resultat.getBDD().equals("open_beauty_facts")) {
                url = "https://fr.openbeautyfacts.org/produit/" + un_resultat.getCode() + "/";
            }
            
            if (un_resultat.getBDD().equals("open_products_facts")) {
                url = "https://fr.openproductsfacts.org/produit/" + un_resultat.getCode() + "/";
            }           
            
            if (un_resultat.getBDD().equals("open_pet_facts")) {
                url = "https://world.openpetfoodfacts.org/product/" + un_resultat.getCode() + "/";
            }   
            
            donnees += url + ";" ;
            
            //donnees += un_resultat.getDoc_JSON() + ";";
            donnees += "\r\n";
            
        }
        
        csv_resultat = entete + "\r\n";
        csv_resultat += donnees;
        
        return csv_resultat;
        
    }
    
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws FileNotFoundException, IOException {

        System.out.println("#########################################");
        System.out.println("# Recherche de doublons entre les bases #");
        System.out.println("#########################################");
        
        // Fichier de paramètres
        String pwd = System.getProperty("user.dir");
        System.out.println("Le répertoire courant est : " + pwd);
        
        
        Properties prop_param = new Properties();
        FileReader reader_param = new FileReader("./src/main/java/param_appli.properties") ;
        prop_param.load(reader_param);
        
        // Paramètres vers les objets connexion serveurs MongoDB : 4 ici
        Connexion_MONGODB conn_mongodb = new Connexion_MONGODB();
        conn_mongodb.serveur            = prop_param.getProperty("conn_mongodb.serveur");
        conn_mongodb.port               = prop_param.getProperty("conn_mongodb.port");
        conn_mongodb.base_de_donnees    = prop_param.getProperty("conn_mongodb.base_de_donnees");
        conn_mongodb.collection         = prop_param.getProperty("conn_mongodb.collection");
        conn_mongodb.user               = prop_param.getProperty("conn_mongodb.user");
        conn_mongodb.passwd_user        = prop_param.getProperty("conn_mongodb.passwd_user");
        
        Connexion_MONGODB conn_mongodb_beauty  = new Connexion_MONGODB();
        conn_mongodb_beauty.serveur            = prop_param.getProperty("conn_mongodb_beauty.serveur");
        conn_mongodb_beauty.port               = prop_param.getProperty("conn_mongodb_beauty.port");
        conn_mongodb_beauty.base_de_donnees    = prop_param.getProperty("conn_mongodb_beauty.base_de_donnees");
        conn_mongodb_beauty.collection         = prop_param.getProperty("conn_mongodb_beauty.collection");
        conn_mongodb_beauty.user               = prop_param.getProperty("conn_mongodb_beauty.user");
        conn_mongodb_beauty.passwd_user        = prop_param.getProperty("conn_mongodb_beauty.passwd_user");
        
        Connexion_MONGODB conn_mongodb_pet  = new Connexion_MONGODB();
        conn_mongodb_pet.serveur            = prop_param.getProperty("conn_mongodb_pet.serveur");
        conn_mongodb_pet.port               = prop_param.getProperty("conn_mongodb_pet.port");
        conn_mongodb_pet.base_de_donnees    = prop_param.getProperty("conn_mongodb_pet.base_de_donnees");
        conn_mongodb_pet.collection         = prop_param.getProperty("conn_mongodb_pet.collection");
        conn_mongodb_pet.user               = prop_param.getProperty("conn_mongodb_pet.user");
        conn_mongodb_pet.passwd_user        = prop_param.getProperty("conn_mongodb_pet.passwd_user");
        
        Connexion_MONGODB conn_mongodb_products  = new Connexion_MONGODB();
        conn_mongodb_products.serveur            = prop_param.getProperty("conn_mongodb_products.serveur");
        conn_mongodb_products.port               = prop_param.getProperty("conn_mongodb_products.port");
        conn_mongodb_products.base_de_donnees    = prop_param.getProperty("conn_mongodb_products.base_de_donnees");
        conn_mongodb_products.collection         = prop_param.getProperty("conn_mongodb_products.collection");
        conn_mongodb_products.user               = prop_param.getProperty("conn_mongodb_products.user");
        conn_mongodb_products.passwd_user        = prop_param.getProperty("conn_mongodb_products.passwd_user");
        
        // Instanciation classe 
        Doublon_recherche recherche_doublon = new Doublon_recherche();
        
        // Déclaration variable de la Liste des résultats 
        List<Doublon_recherche_struct_resultat> resultat = new ArrayList<>();
        
        // Recherche les doublons dans les multiples bases
        resultat = recherche_doublon.compare(conn_mongodb, conn_mongodb_beauty,conn_mongodb_pet,conn_mongodb_products);
        
        // Convertir le résultat en CSV
        String resultat_csv = recherche_doublon.get_csv_resultat(resultat);
        
        // Sauvegarde résultat dans un fichier
        BufferedWriter writer = new BufferedWriter(new FileWriter("resultat.csv"));
        writer.write(resultat_csv);
        writer.close();
        
    }
    
    
    
}


