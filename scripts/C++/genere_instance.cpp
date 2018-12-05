/*
   Création HDT
*/

#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <time.h>

using namespace std;

int main(int argc, char const *argv[])
{
	int nblignes = 1000000; //nb de lignes du dataset * 3

    //Données
    string s;
    string p;
    string o;

    // ouverture en écriture avec effacement du fichier ouvert
	string fic = argv[1];
    ofstream fichier;
    fichier.open(fic.c_str(), ios::out | ios::trunc);

    if(fichier)
    {
 	//Remplissage
    for (int i = 1; i <= nblignes; ++i)
    {

        fichier << "a " << "b " << "c" + std::to_string(i) << endl;

    }
/*
     for (int i = 1; i <= nblignes; ++i)
    {


        fichier << "a " << "b" + std::to_string(i) << " c"  << endl;



    }

    for (int i = 1; i <= nblignes; ++i)
    {


        fichier << "a" + std::to_string(i) << " b " << "c"  << endl;

    }

  */  



		  fichier.close();
        }
	   else
        {
            cerr << "Impossible d'ouvrir le fichier !" << endl;
        }

	return 0;
}
