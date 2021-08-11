/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package OpenFoodFacts;

/**
 *
 * @author xavior
 */
public class Doublon_recherche_struct_resultat {
    String BDD          = "";
    String Code         = "";
    int taille          = 0 ;
    String Categories   = "";
    String Nom_produit  = "";
    String Doc_JSON     = "";

    public String getBDD() {
        return BDD;
    }

    public void setBDD(String BDD) {
        this.BDD = BDD;
    }

    public String getCode() {
        return Code;
    }

    public void setCode(String Code) {
        this.Code = Code;
    }

    public int getTaille() {
        return taille;
    }

    public void setTaille(int taille) {
        this.taille = taille;
    }

    public String getCategories() {
        String retour = "";
        if (this.Categories != null) {
            retour = this.Categories;
        } 
        return retour;
    }

    public void setCategories(String Categories) {
        this.Categories = Categories;
    }

    public String getNom_produit() {
        String retour = "";
        if (this.Nom_produit!=null) {
            retour = this.Nom_produit;
        }
        return retour;
    }

    public void setNom_produit(String Nom_produit) {
        this.Nom_produit = Nom_produit;
    }

    public String getDoc_JSON() {
        return Doc_JSON;
    }

    public void setDoc_JSON(String Doc_JSON) {
        this.Doc_JSON = Doc_JSON;
    }
    
    
    
    
}
