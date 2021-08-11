

# Introduction

Ce mini-projet vise à trouver les doublons dans les bases de données OpenFoodFacts.

Ces bases au nombre de 4 sont les suivantes :
 - openfoodfacts - [site](https://fr.openfoodfacts.org) regroupant les produits alimentaires ;
 - openbeautyfacts - [site](https://fr.openbeautyfacts.org) regroupant les produits cosmétiques ;
 - openpetfoodfacts - [site](https://world.openpetfoodfacts.org) regroupant les produits alimentaires pour animaux ;
 - openproductsfacts - [site](https://fr.openproductsfacts.org) regroupant tous les autres produits.
 
Un fichier de type CSV est généré et liste tous les doublons trouvés. Ce fichier contient :
- la base de données source ;
- le code id du produit ;
- le nom du produit ;
- la taile de l'objet JSON ;
- la catégorie du produit ;
- le lien URL du produit.

# Paramètrage

Ce projet se connecte sur des bases de données MongoDb déjà existantes (à charge de monter ses bases en local par exemple).
Pour déclarer l'accès aux bases MongoDB, il faut modifier le fichier **/src/main/param_appli.properties**
  --> la partie User/passaword n'est pas pris en compte à ce jour.

# Execution
La classe qui doit être executée est la suivante **"/src/main/java/OpenFoodsFacts/Doublon_recherche.java"** .

 # Langage / IDE
Ce projet est developpé en JAVA avec l'IDE NetBeans. Le type de projet est "MAVEN", ce dernier permettra de charger automatiquement toutes les libraires nécesaires.

 
 


