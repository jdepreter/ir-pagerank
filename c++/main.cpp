#include <iostream>
#include <fstream>
#include <map>
#include <vector> 

using namespace std;

int total_nodes = 875713;
double start_rank = 1.0 / total_nodes;

double alpha = 0.15;
double beta  = 1 - alpha;
double eps = 0.000001;

double base_rank = alpha / total_nodes;


void readfile(string filepath, std::map<int, std::vector<int>>& links, std::map<int, double>& ranks) {
    ifstream myfile;
    myfile.open(filepath);

    std::string line;
    std::string delimiter = "\t";
    size_t pos = 0;

    std::string token;
    std::string token2;

    while (std::getline(myfile, line))
    {
        if (line[0] == '#') continue;
        pos = line.find(delimiter);
        token = line.substr(0, pos);
        token2 = line.substr(pos + delimiter.length(), line.length());

        int node1 = std::stoi(token);
        int node2 = std::stoi(token2);

        links[node1].push_back(node2);
        ranks[node1] = start_rank;
        ranks[node2] = start_rank;
    }  
    
}

double iterate(std::map<int, double>& ranks, std::map<int, std::vector<int>>& links) {

    map<int, double> new_ranks;
    for (map<int, vector<int>>::iterator it = links.begin(); it != links.end(); it++)
    {
        // std::vector<int> out = it->second;
        
        for (std::vector<int>::iterator v_it = it->second.begin() ; v_it != it->second.end(); ++v_it)
        {
            new_ranks[*v_it] += beta * ranks[it->first] / it->second.size();
        }
        
    }

    for (map<int, double>::iterator it = ranks.begin(); it != ranks.end(); it++)
    {
        new_ranks[it->first] += base_rank;
    }
    double error = 0;

    for (map<int, double>::iterator it = ranks.begin(); it != ranks.end(); it++)
    {
        error = max(error, abs(ranks[it->first] - new_ranks[it->first]));
    }
    
    ranks = new_ranks;
    return error;
    
}


int main() {
    std::map<int, vector<int>> links;
    std::map<int, double> ranks;

    readfile("../data/web-Google.txt", links, ranks);
    cout << "Done Reading" << endl;

    // for (size_t i = 0; i < links[0].size(); i++)
    // {
    //     cout << links[0][i] << " ";
    // }
    // cout << endl;
    
    // cout << ranks.size() << endl;
    int i = 0;
    while (true)
    {
        std::cout << "Start Iteration " << i << endl;
        double error = iterate(ranks, links);
        std::cout << "End Iteration " << i << " with error " << error << endl;
        if (error < eps) {
            std::cout << "Convergence achieved" << endl;
            break;
        }
        i++;
    }
    

    // cout << ranks.size() << endl;
    cout << "Writing file" << endl;
    ofstream out;
    out.open("out.csv");
    for (map<int, double>::iterator it = ranks.begin(); it != ranks.end(); it++)
    {
        out << it->first << ";" << it->second << endl;
    }
    out.close();

    return 0;
}